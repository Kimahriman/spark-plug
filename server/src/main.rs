use clap::Parser;
use spark_connect_proxy::{Args, Server, config::ProxyConfig};

#[tokio::main]
pub async fn main() -> Result<(), anyhow::Error> {
    env_logger::Builder::new()
        .filter(Some("spark_connect_proxy"), log::LevelFilter::Debug)
        .init();

    let args = Args::parse();
    let config = ProxyConfig::create(args.config_file);

    Server::from_config(config).await?.run().await
}
