/// Module for launching Spark sessions
use std::{
    collections::HashMap,
    env,
    io::{self, Write},
    path::PathBuf,
    sync::Arc,
};

use log::{debug, info};
use tempfile::TempPath;
use tokio::process::{Child, Command};
use which::which;

use crate::config::{ProxyConfig, SparkVersion};

static SPARK_HOME: &str = "SPARK_HOME";
static TOKEN_CONFIG: &str = "spark.connect.authenticate.token";
static CALLBACK_CONFIG: &str = "spark.connect.proxy.callback";
static TIMEOUT_CONFIG: &str = "spark.connect.proxy.idle.timeout";

#[cfg(feature = "embed-plugin")]
static PLUGIN_BINARY: Option<&[u8]> = Some(include_bytes!(
    "../../plugin/target/scala-2.13/spark-connect-proxy_2.13-0.1.0.jar"
));
#[cfg(not(feature = "embed-plugin"))]
static PLUGIN_BINARY: Option<&[u8]> = None;

pub trait Launcher: Clone + Send + Sync {
    fn get_versions(&self) -> Vec<String>;

    fn launch(
        &self,
        version_name: Option<&str>,
        username: String,
        token: String,
        user_config: HashMap<String, String>,
    ) -> Result<Child, io::Error>;
}

#[derive(Clone)]
#[allow(dead_code)]
pub struct SparkLauncher {
    // Map of Spark version key to path it's located at
    versions: Vec<SparkVersion>,
    callback_addr: String,
    session_timeout: u32,
    plugin_path: String,
    plugin_temp_path: Option<Arc<TempPath>>,
}

impl SparkLauncher {
    pub fn from_config(config: &ProxyConfig) -> Self {
        let versions = config.spark_versions.clone().unwrap_or_else(|| {
            // Check if SPARK_HOME is defined and use that as the default
            if let Ok(home) = env::var(SPARK_HOME) {
                vec![SparkVersion {
                    name: "default".to_string(),
                    home,
                    ..Default::default()
                }]
            } else {
                // Otherwise check if there is a `spark-submit` on the path and infer the home dir
                vec![SparkVersion {
                    name: "default".to_string(),
                    home: Self::find_default_spark_home(),
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
                    .prefix("spark-connect-proxy-plugin")
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
                        "{}/../plugin/target/scala-2.13/spark-connect-proxy_2.13-0.1.0.jar",
                        env!("CARGO_MANIFEST_DIR")
                    )),
                    None,
                )
            });

        info!("Using plugin at {plugin_path}");

        Self {
            versions,
            callback_addr,
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
        &self,
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
                if let Some(existing) = configs.get(key) {
                    configs.insert(key.to_string(), format!("{existing},{value}"));
                }
            }
        }

        // Next overwrite by forced configs for this version
        if let Some(override_configs) = version.override_configs.as_ref() {
            configs.extend(override_configs.clone());
        }

        configs
    }

    fn create_submit_command(
        &self,
        version: &SparkVersion,
        username: String,
        token: String,
        user_config: HashMap<String, String>,
    ) -> (PathBuf, Vec<String>) {
        let mut configs = self.build_conf(version, user_config);

        // Finally add our internal configs
        configs.insert(TOKEN_CONFIG.to_string(), token);
        configs.insert(CALLBACK_CONFIG.to_string(), self.callback_addr.clone());
        configs.insert(TIMEOUT_CONFIG.to_string(), self.session_timeout.to_string());
        configs.insert(
            "spark.extraListeners".to_string(),
            "org.apache.spark.sql.connect.proxy.SparkConnectProxyListener".to_string(),
        );
        configs.insert(
            "spark.connect.grpc.interceptor.classes".to_string(),
            "org.apache.spark.sql.connect.proxy.SparkConnectProxyInterceptor".to_string(),
        );
        configs.insert(
            "spark.connect.grpc.binding.port".to_string(),
            "0".to_string(),
        );

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

        for (key, value) in configs.iter() {
            args.extend(["--conf".to_string(), format!("{key}={value}")]);
        }

        args.extend([
            "--class".to_string(),
            "org.apache.spark.sql.connect.proxy.SparkConnectProxyServer".to_string(),
        ]);

        if version.proxy_user.unwrap_or_default() {
            args.extend(["--proxy-user".to_string(), username]);
        }

        args.push(self.plugin_path.clone());

        (submit_path, args)
    }
}

impl Launcher for SparkLauncher {
    fn get_versions(&self) -> Vec<String> {
        self.versions.iter().map(|v| v.name.clone()).collect()
    }

    fn launch(
        &self,
        version_name: Option<&str>,
        username: String,
        token: String,
        user_config: HashMap<String, String>,
    ) -> Result<Child, io::Error> {
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

        let (submit_path, args) = self.create_submit_command(version, username, token, user_config);

        debug!("Running {:?} {}", submit_path, args.join(" "));

        let child = Command::new(submit_path)
            .args(args)
            .envs(env)
            // .stdout(std::process::Stdio::piped())
            // .stderr(std::process::Stdio::piped())
            .spawn()?;

        Ok(child)
    }
}
