use clap::Parser;
use spark_connect_proxy::{config::ProxyConfig, run, Args};

#[tokio::main]
pub async fn main() -> Result<(), anyhow::Error> {
    let args = Args::parse();
    let config = args
        .config_file
        .map(ProxyConfig::from_file)
        .unwrap_or_default();
    run(config).await
}
