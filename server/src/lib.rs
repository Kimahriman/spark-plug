use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{fs, io};

use axum::Router;
use clap::{Parser, command};
use config::{KerberosConfig, ProxyConfig};
use futures::FutureExt;
use http::StatusCode;
use http::header::AUTHORIZATION;
use hyper::body::Incoming;
use hyper::service::Service;
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use log::{debug, error, info, warn};
use routes::get_router;
use rustls_pemfile::{certs, private_key};
use sea_orm::{ColumnTrait, Database, DatabaseConnection, EntityTrait, QueryFilter};
use sea_orm_migration::MigratorTrait;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::process::Command;
use tokio::signal;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_rustls::TlsAcceptor;
use tower::Service as TowerService;
use which::which;

use migration::Migrator;

use crate::entities::application;

mod auth;
pub mod config;
pub mod entities;
mod error;
mod launcher;
pub mod routes;

/// Start the Spark Connect Proxy server
#[derive(Parser, Debug, Default)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Path to the config file
    #[arg(short, long)]
    pub config_file: Option<String>,
}

pub struct Server {
    config: ProxyConfig,
    router: Router,
    db: DatabaseConnection,
    tls_acceptor: Option<TlsAcceptor>,
}

impl Server {
    pub async fn from_config(config: ProxyConfig) -> Result<Self, anyhow::Error> {
        let store_url = config
            .store
            .as_ref()
            .map(String::as_ref)
            .unwrap_or("sqlite::memory:");
        let db = Database::connect(store_url).await?;

        let router = get_router(&config, db.clone()).await;

        let tls_acceptor = load_tls_acceptor(&config)?;

        Ok(Self {
            config,
            router,
            db,
            tls_acceptor,
        })
    }

    pub async fn run(self) -> Result<(), anyhow::Error> {
        Migrator::up(&self.db, None).await?;

        if let Some(kerberos_config) = self.config.kerberos_config.as_ref() {
            kerberos_creds_task(kerberos_config.clone());
        }

        let bind_host = self
            .config
            .bind_host
            .clone()
            .unwrap_or("0.0.0.0".to_string());

        let bind_port = self.config.get_bind_port();

        let listener = tokio::net::TcpListener::bind(format!("{bind_host}:{bind_port}")).await?;
        info!("Listening on http://{:?}", listener.local_addr().unwrap());

        // For graceful shutdown, we use two pairs of watch channels. This is taken from the Axum implementation
        // of graceful shutdown which we can't use since we don't use the serve function of Axum.
        // - signal_*: The receiver is shutdown on a shutdown signal, which tells the senders the server is shutting
        //             down. This tells running tasks and connections that they should gracefully shutdown.
        // - close_*: The receivers are shutdown on connection completions, and the sender is the server itself
        //            that waits for all receivers to finish, letting all existing connections finish their work
        //            before shutting down the server.
        let (signal_tx, signal_rx) = watch::channel(());
        tokio::spawn(async move {
            Self::shutdown_signal().await;
            info!("Received shutdown signal. Telling tasks to shutdown.");
            drop(signal_rx);
        });

        let (close_tx, close_rx) = watch::channel(());

        loop {
            let (stream, _) = tokio::select! {
                s = listener.accept() => s.unwrap(),
                _ = signal_tx.closed() => {
                    info!("Shutting down server");
                    break;
                }
            };

            if let Some(acceptor) = self.tls_acceptor.as_ref() {
                self.serve_connection(acceptor.accept(stream).await?, &signal_tx, &close_rx);
            } else {
                self.serve_connection(stream, &signal_tx, &close_rx);
            };
        }

        drop(close_rx);
        drop(listener);

        info!("Waiting for {} tasks to finish", close_tx.receiver_count());
        close_tx.closed().await;

        info!("All connections finished");

        Ok(())
    }

    fn serve_connection<I>(
        &self,
        io: I,
        signal_tx: &watch::Sender<()>,
        close_rx: &watch::Receiver<()>,
    ) where
        I: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        info!("Serving new connection");
        let router = self.router.clone();
        let db = self.db.clone();
        let signal_tx = signal_tx.clone();
        let close_rx = close_rx.clone();

        tokio::task::spawn(async move {
            let builder = Builder::new(TokioExecutor::new());
            let mut conn =
                pin!(builder.serve_connection(TokioIo::new(io), ProxyService::new(router, db)));

            let mut signal_closed = pin!(signal_tx.closed().fuse());

            loop {
                tokio::select! {
                    result = conn.as_mut() => {
                        if let Err(e) = result {
                            error!("Error serving connection: {e:?}");
                        }
                        break;
                    }
                    _ = &mut signal_closed => {
                        info!("Signal received in task, starting graceful shutdown");
                        conn.as_mut().graceful_shutdown();
                    }
                }
            }

            drop(close_rx)
        });
    }

    async fn shutdown_signal() {
        let ctrl_c = async {
            signal::ctrl_c()
                .await
                .expect("failed to install Ctrl+C handler");
        };

        #[cfg(unix)]
        let terminate = async {
            signal::unix::signal(signal::unix::SignalKind::terminate())
                .expect("failed to install signal handler")
                .recv()
                .await;
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => {},
            _ = terminate => {},
        }
    }
}

fn load_tls_acceptor(config: &ProxyConfig) -> Result<Option<TlsAcceptor>, io::Error> {
    if let Some(tls_config) = &config.tls {
        let certs = certs(&mut io::BufReader::new(fs::File::open(&tls_config.cert)?))
            .collect::<io::Result<Vec<_>>>()?;
        let key = private_key(&mut io::BufReader::new(fs::File::open(&tls_config.key)?))?.unwrap();

        let mut config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;

        config.alpn_protocols = vec!["h2".as_bytes().to_vec(), "http/1.1".as_bytes().to_vec()];

        let acceptor = TlsAcceptor::from(Arc::new(config));
        Ok(Some(acceptor))
    } else {
        Ok(None)
    }
}

type UpstreamMessage = (
    Request<Incoming>,
    oneshot::Sender<Result<Response<axum::body::Body>, hyper::Error>>,
);

async fn upstream_connection(
    mut rx: mpsc::UnboundedReceiver<UpstreamMessage>,
    token: String,
    db: DatabaseConnection,
) {
    let mut sender = {
        let address = application::Entity::find()
            .filter(application::Column::Token.eq(token))
            .one(&db)
            .await
            .inspect_err(|e| error!("Failed to retrieve app by token: {e:?}"))
            .ok()
            .flatten()
            .and_then(|app| app.address);

        if let Some(address) = address {
            let client_stream = TcpStream::connect(&address).await.unwrap();
            let io = TokioIo::new(client_stream);

            let (sender, conn) = hyper::client::conn::http2::handshake(TokioExecutor::new(), io)
                .await
                .unwrap();
            tokio::task::spawn(async move {
                debug!("Spawned connection await");
                let res = conn.await;
                debug!("Upstream connection closed: {res:?}");
                if let Err(err) = res {
                    warn!("Upstream connection failed: {err:?}");
                }
            });
            Some((sender, address))
        } else {
            None
        }
    };

    while let Some((mut req, tx)) = rx.recv().await {
        if let Some((sender, address)) = sender.as_mut() {
            let uri_string = format!(
                "http://{address}{}",
                req.uri()
                    .path_and_query()
                    .map(|x| x.as_str())
                    .unwrap_or("/")
            );
            *req.uri_mut() = uri_string.parse().unwrap();

            info!("Proxying request {:?}", req.uri().path_and_query());

            let response = sender
                .send_request(req)
                .await
                .map(|response| response.map(axum::body::Body::new));

            info!("Proxying response {response:?}");

            tx.send(response).unwrap();
        } else {
            tx.send(Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(().into())
                .unwrap()))
                .unwrap();
        }
    }

    debug!("rx closed for upstream");
}

// Track ID for debugging purposes
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

struct ProxyService {
    id: u64,
    dispatch: Mutex<Option<mpsc::UnboundedSender<UpstreamMessage>>>,
    router: Router,
    db: DatabaseConnection,
}

impl ProxyService {
    fn new(router: Router, db: DatabaseConnection) -> Self {
        let id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
        debug!("Creating proxy service {id}");
        Self {
            id,
            dispatch: Mutex::new(None),
            router,
            db,
        }
    }

    fn dispatch(
        &self,
        req: Request<Incoming>,
    ) -> oneshot::Receiver<Result<Response<axum::body::Body>, hyper::Error>> {
        let mut dispatch = self.dispatch.lock().unwrap();
        let (tx, rx) = oneshot::channel();
        if dispatch.is_none() {
            let authorization = if let Some(auth) = req.headers().get(AUTHORIZATION) {
                auth.to_str().unwrap().to_string()
            } else {
                let response = Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(().into())
                    .unwrap();
                tx.send(Ok(response)).unwrap();
                return rx;
            };

            let split = authorization.split_once(' ');
            let token = match split {
                Some(("Bearer", token)) => token.to_string(),
                _ => {
                    let response = Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(().into())
                        .unwrap();
                    tx.send(Ok(response)).unwrap();
                    return rx;
                }
            };

            let (upstream_sender, upstream_receiver) = mpsc::unbounded_channel();
            let db = self.db.clone();
            tokio::task::spawn(
                async move { upstream_connection(upstream_receiver, token, db).await },
            );
            *dispatch = Some(upstream_sender);
        }
        dispatch.as_mut().unwrap().send((req, tx)).unwrap();
        rx
    }
}

impl Service<Request<hyper::body::Incoming>> for ProxyService {
    type Response = Response<axum::body::Body>;

    type Error = hyper::Error;

    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn call(&self, req: Request<hyper::body::Incoming>) -> Self::Future {
        info!("Handling call for {} {}", req.method(), req.uri());
        if req
            .uri()
            .path()
            .starts_with("/spark.connect.SparkConnectService")
        {
            let rx = self.dispatch(req);
            Box::pin(async move { Ok(rx.await.unwrap()?.map(axum::body::Body::new)) })
        } else {
            let mut router = self.router.clone();
            Box::pin(async move { Ok(router.call(req).await.unwrap()) })
        }
    }
}

impl Drop for ProxyService {
    fn drop(&mut self) {
        debug!(
            "Dropping proxy service {}: {}",
            self.id,
            self.dispatch.lock().unwrap().is_some()
        );
    }
}

fn kerberos_creds_task(kerberos_config: KerberosConfig) {
    let kinit = which("kinit").expect("Failed to find kinit executable");
    tokio::spawn(async move {
        info!("Starting Kerberos credential task");

        loop {
            let output = Command::new(&kinit)
                .args([
                    "-kt",
                    kerberos_config.keytab.as_ref(),
                    kerberos_config.principal.as_ref(),
                ])
                .output()
                .await;

            if let Err(error) = output {
                error!("Failed to kinit: {error:?}");
            }

            tokio::time::sleep(Duration::from_secs(
                kerberos_config.renewal_interval.unwrap_or(3600),
            ))
            .await;
        }
    });
}
