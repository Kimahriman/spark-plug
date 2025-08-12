use std::collections::HashMap;

use figment::{
    Figment,
    providers::{Env, Format, Yaml},
};
use local_ip_address::local_ip;
use serde::Deserialize;

const DEFAULT_PORT: u16 = 8100;

#[derive(Clone, Default, Deserialize)]
pub struct SparkVersion {
    // Name shown to users
    pub name: String,
    // SPARK_HOME directory for this version
    pub home: String,
    pub master: Option<String>,
    pub deploy_mode: Option<String>,
    pub proxy_user: Option<bool>,
    pub env: Option<HashMap<String, String>>,
    pub default_configs: Option<HashMap<String, String>>,
    pub merge_configs: Option<HashMap<String, String>>,
    pub override_configs: Option<HashMap<String, String>>,
}

#[derive(Clone, Default, Deserialize)]
pub struct KerberosConfig {
    pub keytab: String,
    pub principal: String,
    pub renewal_interval: Option<u64>,
}

#[derive(Deserialize)]
pub struct TlsConfig {
    pub key: String,
    pub cert: String,
}

#[derive(Deserialize)]
pub struct AuthConfig {
    pub name: String,
    pub options: Option<HashMap<String, String>>,
}

#[derive(Deserialize, Default)]
pub struct ProxyConfig {
    pub bind_host: Option<String>,
    pub bind_port: Option<u16>,
    pub callback_address: Option<String>,
    pub plugin_path: Option<String>,
    pub launch_timeout: Option<u32>,
    pub session_timeout: Option<u32>,
    pub store: Option<String>,
    pub kerberos_config: Option<KerberosConfig>,
    pub tls: Option<TlsConfig>,
    pub spark_versions: Option<Vec<SparkVersion>>,
    pub auth_methods: Option<Vec<AuthConfig>>,
}

impl ProxyConfig {
    pub fn create(path: Option<impl AsRef<str>>) -> Self {
        let mut figment = Figment::new();

        if let Some(path) = path {
            figment = figment.merge(Yaml::file(path.as_ref()))
        }

        figment
            .merge(Env::prefixed("CONNECT_PROXY_"))
            .extract()
            .unwrap()
    }

    pub fn get_bind_port(&self) -> u16 {
        self.bind_port.unwrap_or(DEFAULT_PORT)
    }

    pub fn get_callback_addr(&self) -> String {
        self.callback_address.clone().unwrap_or_else(|| {
            let callback_scheme = if self.tls.is_some() { "https" } else { "http" };
            format!(
                "{}://{}:{}",
                callback_scheme,
                local_ip().unwrap(),
                self.get_bind_port()
            )
        })
    }
}
