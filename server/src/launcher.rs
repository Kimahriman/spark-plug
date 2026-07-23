/// Module for launching Spark sessions
use std::{
    collections::HashMap,
    env,
    io::{self, Write},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use log::{debug, info};
use tempfile::TempPath;
use tokio::{
    process::{Child, Command},
    task::JoinHandle,
};
use which::which;

use crate::config::{ProxyConfig, SparkVersion};

static SPARK_HOME: &str = "SPARK_HOME";
static APP_NAME_CONFIG: &str = "spark.app.name";
static TOKEN_CONFIG: &str = "spark.connect.authenticate.token";
static CALLBACK_CONFIG: &str = "spark.plug.callback";
static TIMEOUT_CONFIG: &str = "spark.plug.idle.timeout";
static LISTENER_CONFIG: &str = "spark.extraListeners";
static INTERCEPTOR_CONFIG: &str = "spark.connect.grpc.interceptor.classes";
static GRPC_PORT_CONFIG: &str = "spark.connect.grpc.binding.port";

static LISTENER_CLASS: &str = "org.apache.spark.sql.sparkplug.SparkPlugListener";
static INTERCEPTOR_CLASS: &str = "org.apache.spark.sql.sparkplug.SparkPlugInterceptor";
static SERVER_CLASS: &str = "org.apache.spark.sql.sparkplug.SparkPlugServer";

#[cfg(feature = "embed-plugin")]
static PLUGIN_BINARY: Option<&[u8]> = Some(include_bytes!(
    "../../plugin/target/scala-2.13/spark-plug_2.13-0.1.0.jar"
));
#[cfg(not(feature = "embed-plugin"))]
static PLUGIN_BINARY: Option<&[u8]> = None;

#[async_trait::async_trait]
pub trait Launcher: Clone + Send + Sync {
    fn get_versions(&self) -> Vec<String>;

    #[allow(clippy::too_many_arguments)]
    async fn launch(
        &self,
        version_name: Option<&str>,
        session_id: i32,
        app_name: Option<String>,
        username: String,
        token: String,
        user_config: HashMap<String, String>,
        python_packages: Option<Vec<String>>,
    ) -> Result<JoinHandle<()>, io::Error>;
}

#[derive(Clone)]
#[allow(dead_code)]
pub struct SparkLauncher {
    // Map of Spark version key to path it's located at
    versions: Vec<SparkVersion>,
    callback_addr: String,
    launch_timeout: u32,
    session_timeout: u32,
    plugin_path: String,
    plugin_temp_path: Option<Arc<TempPath>>,
}

impl SparkLauncher {
    pub fn from_config(config: &ProxyConfig) -> Self {
        let versions = config.spark_versions.clone().unwrap_or_else(|| {
            let python_executable = which("python3")
                .ok()
                .map(|p| p.to_string_lossy().to_string());
            // Check if SPARK_HOME is defined and use that as the default
            if let Ok(home) = env::var(SPARK_HOME) {
                vec![SparkVersion {
                    name: "default".to_string(),
                    home,
                    python_executable,
                    ..Default::default()
                }]
            } else {
                // Otherwise check if there is a `spark-submit` on the path and infer the home dir
                vec![SparkVersion {
                    name: "default".to_string(),
                    home: Self::find_default_spark_home(),
                    python_executable,
                    ..Default::default()
                }]
            }
        });
        let callback_addr = config.get_callback_addr();
        info!("Using callback address {callback_addr}");

        // Check there is exactly one default
        assert!(!versions.is_empty(), "No Spark versions provided");

        // Check all the Spark directories exist
        for version in versions.iter() {
            assert!(
                std::path::Path::new(&version.home).exists(),
                "Home directory not found for version {}: {}",
                version.name,
                version.home
            );
        }

        let (plugin_path, plugin_temp_path) = PLUGIN_BINARY
            .map(|content| {
                let mut plugin_file = tempfile::Builder::new()
                    .prefix("spark-plug-plugin")
                    .suffix(".jar")
                    .tempfile()
                    .expect("Failed to create temporary file for plugin");
                plugin_file
                    .write_all(content)
                    .expect("Failed to write plugin binary to temporary file");
                plugin_file
                    .flush()
                    .expect("Failed to flush plugin binary to temporary file");

                let plugin_temp_path = plugin_file.into_temp_path();
                (
                    plugin_temp_path.to_string_lossy().to_string(),
                    Some(plugin_temp_path),
                )
            })
            .unwrap_or_else(|| {
                (
                    config.plugin_path.clone().unwrap_or(format!(
                        "{}/../plugin/target/scala-2.13/spark-plug_2.13-0.1.0.jar",
                        env!("CARGO_MANIFEST_DIR")
                    )),
                    None,
                )
            });

        info!("Using plugin at {plugin_path}");

        info!("Using Spark versions:");
        for version in versions.iter() {
            info!("{version:?}");
        }

        Self {
            versions,
            callback_addr,
            launch_timeout: config.launch_timeout.unwrap_or(300),
            session_timeout: config.session_timeout.unwrap_or(3600),
            plugin_path,
            plugin_temp_path: plugin_temp_path.map(Arc::new),
        }
    }

    fn find_default_spark_home() -> String {
        if let Ok(find_spark_home) = which("find_spark_home.py") {
            // If PySpark is installed, use the find_spark_home.py script to find the appropriate
            // Spark home directory
            let path: String = std::process::Command::new(find_spark_home)
                .output()
                .expect("Failed to execute find_spark_home.py")
                .stdout
                .try_into()
                .expect("Failed to decode Spark home to UTF8");

            path.trim_end().to_string()
        } else if let Ok(submit_path) = which("spark-submit") {
            // Otherwise check if there is a `spark-submit` on the path and infer the home dir
            submit_path
                .parent()
                .expect("Failed to get parent of spark-submit command")
                .parent()
                .expect("Failed to get parent of spark-submit command")
                .to_string_lossy()
                .to_string()
        } else {
            panic!("Unable to find a default Spark installation");
        }
    }

    fn build_conf(
        version: &SparkVersion,
        user_config: HashMap<String, String>,
    ) -> HashMap<String, String> {
        // Start with the default config for this version
        let mut configs = version.default_configs.clone().unwrap_or_default();

        // Overwrite by user provided configs
        configs.extend(user_config);

        // Merge any comma-separated configs
        if let Some(merge_configs) = version.merge_configs.as_ref() {
            for (key, value) in merge_configs.iter() {
                match configs.get(key) {
                    Some(existing) => {
                        configs.insert(key.to_string(), format!("{existing},{value}"));
                    }
                    None => {
                        configs.insert(key.to_string(), value.to_string());
                    }
                };
            }
        }

        // Next overwrite by forced configs for this version
        if let Some(override_configs) = version.override_configs.as_ref() {
            configs.extend(override_configs.clone());
        }

        configs
    }

    async fn build_python_venv(
        python_executable: Option<&str>,
        packages: Vec<String>,
    ) -> io::Result<String> {
        let python = python_executable.ok_or(io::Error::new(
            io::ErrorKind::NotFound,
            "No python executable specified for creating virtual environment",
        ))?;

        let venv_dir = tempfile::Builder::new()
            .prefix("spark-plug-venv")
            .tempdir()?;
        let venv_path = venv_dir.path().to_string_lossy().to_string();

        // Create the virtual environment with uv using the configured Python.
        let status = Command::new("uv")
            .args(["venv", "--python", python, &venv_path])
            .status()
            .await?;

        if !status.success() {
            return Err(io::Error::other(format!(
                "Failed to create virtual environment with uv using {python}"
            )));
        }

        // Install the packages with uv, targeting the newly created environment.
        let venv_python = if cfg!(target_os = "windows") {
            format!("{venv_path}/Scripts/python.exe")
        } else {
            format!("{venv_path}/bin/python")
        };

        let mut install_args = vec!["pip", "install", "--python", &venv_python];
        install_args.push("venv-pack");
        install_args.extend(packages.iter().map(String::as_str));

        let status = Command::new("uv").args(install_args).status().await?;

        if !status.success() {
            return Err(io::Error::other(
                "Failed to install packages in virtual environment",
            ));
        }

        let venv_pack_executable = if cfg!(target_os = "windows") {
            format!("{venv_path}/Scripts/venv-pack")
        } else {
            format!("{venv_path}/bin/venv-pack")
        };

        let status = Command::new(venv_pack_executable)
            .args([
                "-p",
                &venv_path,
                "-o",
                &format!("{venv_path}.tgz"),
                "--exclude",
                "*/venv-pack/*",
            ])
            .status()
            .await?;

        if !status.success() {
            return Err(io::Error::other(
                "Failed to install packages in virtual environment",
            ));
        }

        Ok(format!("{venv_path}.tgz"))
    }

    /// Validates that file-based configs don't include local files
    /// Checks spark.files, spark.archives, and spark.submit.pyFiles
    /// All files must be fully qualified with a protocol (e.g., s3://, hdfs://)
    /// and cannot use the file:// protocol
    fn validate_file_configs(configs: &HashMap<String, String>) -> Result<(), io::Error> {
        let file_config_keys = ["spark.files", "spark.archives", "spark.submit.pyFiles"];

        for key in file_config_keys.iter() {
            if let Some(value) = configs.get(*key) {
                for file_path in value.split(',') {
                    let trimmed = file_path.trim();
                    if trimmed.is_empty() {
                        continue;
                    }

                    // Check if the path has a protocol (contains ://)
                    if !trimmed.contains("://") {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!(
                                "Config {} contains file path without protocol: {}",
                                key, trimmed
                            ),
                        ));
                    }

                    // Check if the protocol is file://
                    if trimmed.starts_with("file://") {
                        return Err(io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            format!("Config {} cannot use file:// protocol: {}", key, trimmed),
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn build_submit_command(
        &self,
        version: &SparkVersion,
        session_id: i32,
        app_name: Option<String>,
        username: String,
        token: String,
        user_config: HashMap<String, String>,
        venv_tarball: Option<String>,
    ) -> Result<(PathBuf, Vec<String>), io::Error> {
        // Validate file-based configs for security
        Self::validate_file_configs(&user_config)?;

        let mut configs = Self::build_conf(version, user_config);

        if let Some(app_name) = app_name {
            configs.insert(APP_NAME_CONFIG.to_string(), app_name);
        } else if !configs.contains_key(APP_NAME_CONFIG) {
            configs.insert(
                APP_NAME_CONFIG.to_string(),
                format!("spark-plug-session-{}", session_id),
            );
        }

        // Finally add our internal configs
        configs.insert(TOKEN_CONFIG.to_string(), token);
        configs.insert(CALLBACK_CONFIG.to_string(), self.callback_addr.clone());
        configs.insert(TIMEOUT_CONFIG.to_string(), self.session_timeout.to_string());
        configs.insert(LISTENER_CONFIG.to_string(), LISTENER_CLASS.to_string());
        configs.insert(
            INTERCEPTOR_CONFIG.to_string(),
            INTERCEPTOR_CLASS.to_string(),
        );
        configs.insert(GRPC_PORT_CONFIG.to_string(), "0".to_string());

        let submit_path = PathBuf::from(&version.home)
            .join("bin")
            .join("spark-submit");

        let mut args = vec![];

        if let Some(master) = version.master.as_ref() {
            args.extend(["--master".to_string(), master.clone()]);
        }

        if let Some(deploy_mode) = version.deploy_mode.as_ref() {
            args.extend(["--deploy-mode".to_string(), deploy_mode.clone()]);
        }

        if let Some(venv_tarball) = venv_tarball {
            args.extend([
                "--archives".to_string(),
                format!("{venv_tarball}#environment"),
            ]);
            // Python workers get run with a session-specific sub directory
            // so we need to get the environment from the parent directory
            configs.insert(
                "spark.sql.execution.pyspark.python".to_string(),
                "../environment/bin/python".to_string(),
            );
        }

        for (key, value) in configs.iter() {
            args.extend(["--conf".to_string(), format!("{key}={value}")]);
        }

        args.extend(["--class".to_string(), SERVER_CLASS.to_string()]);

        if version.proxy_user.unwrap_or_default() {
            args.extend(["--proxy-user".to_string(), username]);
        }

        args.push(self.plugin_path.clone());

        Ok((submit_path, args))
    }

    fn track_launch(&self, mut child: Child) -> JoinHandle<()> {
        let launch_timeout = self.launch_timeout;

        // Create a future that returns true on successful launch and false otherwise
        let launch_success = async move { child.wait().await.is_ok_and(|s| s.success()) };

        // Spawn a task that finishes when either
        // 1. The launch process finishes and did not succeed
        // 2. The launch timeout expires
        tokio::spawn(async move {
            tokio::select! {
                false = launch_success => {}
                _ = tokio::time::sleep(Duration::from_secs(launch_timeout as u64)) => {}
            }
        })
    }
}

#[async_trait::async_trait]
impl Launcher for SparkLauncher {
    fn get_versions(&self) -> Vec<String> {
        self.versions.iter().map(|v| v.name.clone()).collect()
    }

    async fn launch(
        &self,
        version_name: Option<&str>,
        session_id: i32,
        app_name: Option<String>,
        username: String,
        token: String,
        user_config: HashMap<String, String>,
        python_packages: Option<Vec<String>>,
    ) -> Result<JoinHandle<()>, io::Error> {
        let version = if let Some(name) = version_name {
            self.versions
                .iter()
                .find(|v| v.name == name)
                .ok_or(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Version named {name} not found"),
                ))?
        } else {
            &self.versions[0]
        };

        let mut env = version.env.clone().unwrap_or_default();
        env.insert("SPARK_HOME".to_string(), version.home.clone());

        // Spark has trouble using the actual IP of the local host on Macs
        #[cfg(target_os = "macos")]
        env.insert("SPARK_LOCAL_IP".to_string(), "127.0.0.1".to_string());

        let venv_tarball = match python_packages {
            Some(packages) if !packages.is_empty() => Some(
                Self::build_python_venv(
                    version.python_executable.as_ref().map(String::as_ref),
                    packages,
                )
                .await?,
            ),
            _ => None,
        };

        let (submit_path, args) = self.build_submit_command(
            version,
            session_id,
            app_name,
            username,
            token,
            user_config,
            venv_tarball,
        )?;

        debug!("Running {:?} {}", submit_path, args.join(" "));

        let child = Command::new(submit_path)
            .args(args)
            .envs(env)
            // .stdout(std::process::Stdio::piped())
            // .stderr(std::process::Stdio::piped())
            .spawn()?;

        Ok(self.track_launch(child))
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::{
        config::SparkVersion,
        launcher::{
            APP_NAME_CONFIG, CALLBACK_CONFIG, GRPC_PORT_CONFIG, INTERCEPTOR_CLASS,
            INTERCEPTOR_CONFIG, LISTENER_CLASS, LISTENER_CONFIG, SERVER_CLASS, SparkLauncher,
            TIMEOUT_CONFIG, TOKEN_CONFIG,
        },
    };

    #[test]
    fn test_build_config() {
        let version = SparkVersion {
            name: "default".to_string(),
            default_configs: Some(
                vec![
                    ("a".to_string(), "default".to_string()),
                    ("b".to_string(), "default".to_string()),
                ]
                .into_iter()
                .collect(),
            ),
            merge_configs: Some(
                vec![
                    ("c".to_string(), "merge".to_string()),
                    ("e".to_string(), "merge-only".to_string()),
                ]
                .into_iter()
                .collect(),
            ),
            override_configs: Some(
                vec![("d".to_string(), "override".to_string())]
                    .into_iter()
                    .collect(),
            ),
            ..Default::default()
        };

        let user_config = vec![
            ("a".to_string(), "user".to_string()),
            ("c".to_string(), "user".to_string()),
            ("d".to_string(), "user".to_string()),
        ]
        .into_iter()
        .collect();

        let built = SparkLauncher::build_conf(&version, user_config);

        assert_eq!(
            built,
            vec![
                ("a".to_string(), "user".to_string()),
                ("b".to_string(), "default".to_string()),
                ("c".to_string(), "user,merge".to_string()),
                ("d".to_string(), "override".to_string()),
                ("e".to_string(), "merge-only".to_string()),
            ]
            .into_iter()
            .collect()
        )
    }

    #[test]
    fn test_build_command() {
        let launcher = SparkLauncher {
            versions: vec![SparkVersion {
                name: "default".to_string(),
                home: "/opt/spark".to_string(),
                ..Default::default()
            }],
            callback_addr: "http://localhost:8100".to_string(),
            launch_timeout: 60,
            session_timeout: 60,
            plugin_path: "/path/to/plugin".to_string(),
            plugin_temp_path: None,
        };

        let (command, args) = launcher
            .build_submit_command(
                &launcher.versions[0],
                1,
                None,
                "user".to_string(),
                "abcd".to_string(),
                HashMap::default(),
                None,
            )
            .unwrap();

        let args_ref: Vec<&str> = args.iter().map(String::as_ref).collect();

        assert_eq!(command.to_str().unwrap(), "/opt/spark/bin/spark-submit");
        let (pairs, script) = args_ref.as_chunks::<2>();

        assert_eq!(script[0], "/path/to/plugin");

        assert!(pairs.contains(&["--conf", &format!("{APP_NAME_CONFIG}=spark-plug-session-1")]));
        assert!(pairs.contains(&["--conf", &format!("{TOKEN_CONFIG}=abcd")]));
        assert!(pairs.contains(&[
            "--conf",
            &format!("{CALLBACK_CONFIG}=http://localhost:8100")
        ]));
        assert!(pairs.contains(&["--conf", &format!("{TIMEOUT_CONFIG}=60")]));
        assert!(pairs.contains(&["--conf", &format!("{LISTENER_CONFIG}={LISTENER_CLASS}")]));
        assert!(pairs.contains(&[
            "--conf",
            &format!("{INTERCEPTOR_CONFIG}={INTERCEPTOR_CLASS}")
        ]));
        assert!(pairs.contains(&["--conf", &format!("{GRPC_PORT_CONFIG}=0")]));

        assert!(pairs.contains(&["--class", SERVER_CLASS]));
    }

    #[test]
    fn test_build_command_uses_explicit_app_name() {
        let launcher = SparkLauncher {
            versions: vec![SparkVersion {
                name: "default".to_string(),
                home: "/opt/spark".to_string(),
                ..Default::default()
            }],
            callback_addr: "http://localhost:8100".to_string(),
            launch_timeout: 60,
            session_timeout: 60,
            plugin_path: "/path/to/plugin".to_string(),
            plugin_temp_path: None,
        };

        let (_command, args) = launcher
            .build_submit_command(
                &launcher.versions[0],
                1,
                Some("custom-app-name".to_string()),
                "user".to_string(),
                "abcd".to_string(),
                HashMap::default(),
                None,
            )
            .unwrap();

        let args_ref: Vec<&str> = args.iter().map(String::as_ref).collect();
        let (pairs, _script) = args_ref.as_chunks::<2>();

        assert!(pairs.contains(&["--conf", &format!("{APP_NAME_CONFIG}=custom-app-name")]));
    }

    #[test]
    fn test_validate_file_configs_rejects_local_paths() {
        let mut configs = HashMap::new();
        configs.insert(
            "spark.files".to_string(),
            "/local/path/file.txt".to_string(),
        );

        assert!(SparkLauncher::validate_file_configs(&configs).is_err());
    }

    #[test]
    fn test_validate_file_configs_rejects_file_protocol() {
        let mut configs = HashMap::new();
        configs.insert("spark.files".to_string(), "file:///etc/passwd".to_string());

        assert!(SparkLauncher::validate_file_configs(&configs).is_err());
    }

    #[test]
    fn test_validate_file_configs_allows_remote_paths() {
        let mut configs = HashMap::new();
        configs.insert(
            "spark.files".to_string(),
            "s3://bucket/file.txt,hdfs://namenode/data".to_string(),
        );
        configs.insert(
            "spark.archives".to_string(),
            "gs://bucket/archive.tar.gz".to_string(),
        );

        assert!(SparkLauncher::validate_file_configs(&configs).is_ok());
    }

    #[test]
    fn test_validate_file_configs_allows_empty_values() {
        let configs = HashMap::new();

        assert!(SparkLauncher::validate_file_configs(&configs).is_ok());
    }
}
