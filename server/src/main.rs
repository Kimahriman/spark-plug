use clap::Parser;
use spark_connect_proxy::{Args, ProxyCommand, Server, config::ProxyConfig};

#[tokio::main]
pub async fn main() -> Result<(), anyhow::Error> {
    env_logger::Builder::new()
        .filter(Some("spark_connect_proxy"), log::LevelFilter::Debug)
        .init();

    let args = Args::parse();
    let config = ProxyConfig::create(args.config_file);
    let command = args.command.unwrap_or(ProxyCommand::Start);

    let server = Server::from_config(config).await?;

    match command {
        ProxyCommand::Start => server.run().await,
        ProxyCommand::Prune { seconds } => server.prune(seconds).await,
        ProxyCommand::Check => server.check().await,
    }
}
