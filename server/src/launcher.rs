/// Module for launching Spark sessions
use std::{
    collections::HashMap,
    env,
    io::{self},
    path::{Path, PathBuf},
    // process::Stdio,
};

use log::info;
use tokio::process::Command;
use which::which;

use crate::config::{ProxyConfig, SparkVersion};

static SPARK_HOME: &str = "SPARK_HOME";
static TOKEN_CONFIG: &str = "spark.connect.authenticate.token";
static CALLBACK_CONFIG: &str = "spark.connect.proxy.callback";
static TIMEOUT_CONFIG: &str = "spark.connect.proxy.idle.timeout";

#[derive(Clone)]
pub struct Launcher {
    // Map of Spark version key to path it's located at
    versions: Vec<SparkVersion>,
    callback_addr: String,
}

impl Launcher {
    pub fn from_config(config: &ProxyConfig) -> Self {
        let versions = config.spark_versions.clone().unwrap_or_else(|| {
            // Check if SPARK_HOME is defined and use that as the default
            if let Ok(home) = env::var(SPARK_HOME) {
                vec![SparkVersion {
                    name: "default".to_string(),
                    home,
                    default: true,
                    ..Default::default()
                }]
            } else if let Ok(submit_path) = which("spark-submit") {
                // Otherwise check if there is a `spark-submit` on the path and infer the home dir
                vec![SparkVersion {
                    name: "default".to_string(),
                    home: submit_path
                        .parent()
                        .unwrap()
                        .parent()
                        .unwrap()
                        .to_string_lossy()
                        .to_string(),
                    default: true,
                    ..Default::default()
                }]
            } else {
                panic!("Unable to find a default Spark installation")
            }
        });
        let callback_addr = config.get_callback_addr();
        info!("Using callback address {callback_addr}");

        // Check there is exactly one default
        assert_eq!(
            versions.iter().filter(|v| v.default).count(),
            1,
            "Exactly one default version must be specified"
        );

        // Check all the Spark directories exist
        for version in versions.iter() {
            assert!(
                Path::new(&version.home).exists(),
                "Home directory not found for version {}: {}",
                version.name,
                version.home
            );
        }

        Self {
            versions,
            callback_addr,
        }
    }

    pub fn get_versions(&self) -> Vec<String> {
        self.versions.iter().map(|v| v.name.clone()).collect()
    }

    pub async fn launch(
        &self,
        version_name: Option<&str>,
        username: String,
        token: String,
        user_config: HashMap<String, String>,
    ) -> Result<(), io::Error> {
        let version = if let Some(name) = version_name {
            self.versions
                .iter()
                .find(|v| v.name == name)
                .ok_or(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("Version named {name} not found"),
                ))?
        } else {
            self.versions
                .iter()
                .find(|v| v.default)
                .ok_or(io::Error::new(
                    io::ErrorKind::NotFound,
                    "No default version found",
                ))?
        };

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

        // Finally add our internal configs
        configs.insert(TOKEN_CONFIG.to_string(), token);
        configs.insert(CALLBACK_CONFIG.to_string(), self.callback_addr.clone());
        configs.insert(TIMEOUT_CONFIG.to_string(), "60".to_string());
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

        let submit_path = PathBuf::from(&version.home).join("bin/spark-submit");

        let mut args = vec!["--master".to_string(), "local".to_string()];

        for (key, value) in configs.iter() {
            args.extend(["--conf".to_string(), format!("{key}={value}")]);
        }

        args.extend([
            "--jars".to_string(),
            "plugin/target/scala-2.13/spark-connect-proxy_2.13-*.jar".to_string(),
        ]);

        args.extend([
            "--class".to_string(),
            "org.apache.spark.sql.connect.service.SparkConnectServer".to_string(),
        ]);

        if version.proxy_user {
            args.extend(["--proxy-user".to_string(), username]);
        }

        info!("Running {:?} {}", submit_path, args.join(" "));

        Command::new(submit_path)
            .args(args)
            .envs(version.env.clone().unwrap_or_default())
            // .env("SPARK_HOME", &version.home)
            // .stdout(Stdio::piped())
            // .stderr(Stdio::piped())
            .spawn()?;
        Ok(())
    }
}
