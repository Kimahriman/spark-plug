use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{fs, io};

use axum::Router;
use clap::{Parser, Subcommand};
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
use reqwest::ClientBuilder;
use routes::get_router;
use rustls_pemfile::{certs, private_key};
use sea_orm::prelude::DateTimeUtc;
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, Database, DatabaseConnection,
    EntityTrait, QueryFilter,
};
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

use crate::auth::UserAuth;
use crate::entities::application::{self, State};
use crate::launcher::SparkLauncher;

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

    #[command(subcommand)]
    pub command: Option<ProxyCommand>,
}

#[derive(Subcommand, Debug, Clone)]
pub enum ProxyCommand {
    /// Start the Spark Connect Proxy server
    Start,
    /// Delete failed/finished apps older than a threshold in seconds
    Prune {
        /// Minimum application age in seconds for deletion
        #[arg(short = 's', long = "seconds")]
        seconds: u64,
    },
    /// Probe running apps and mark unreachable ones as finished
    Check,
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
        let launcher = SparkLauncher::from_config(&config);
        let user_auth = UserAuth::from_config(&config).await;

        let router = get_router(user_auth, launcher, db.clone(), config.clone()).await;

        let tls_acceptor = load_tls_acceptor(&config)?;

        Ok(Self {
            config,
            router,
            db,
            tls_acceptor,
        })
    }

    pub async fn run(self) -> Result<(), anyhow::Error> {
        self.ensure_db().await?;

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

    pub async fn prune(self, older_than_seconds: u64) -> Result<(), anyhow::Error> {
        self.ensure_db().await?;

        let cutoff = DateTimeUtc::from_timestamp(
            (std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs() as i64)
                .saturating_sub(older_than_seconds as i64),
            0,
        )
        .ok_or_else(|| anyhow::anyhow!("Failed to create prune cutoff timestamp"))?;

        let result = application::Entity::delete_many()
            .filter(
                Condition::any()
                    .add(application::Column::State.eq(State::FAILED))
                    .add(application::Column::State.eq(State::FINISHED)),
            )
            .filter(application::Column::CreatedAt.lte(cutoff))
            .exec(&self.db)
            .await?;
        info!(
            "Pruned {} failed/finished applications older than {older_than_seconds}s",
            result.rows_affected
        );

        Ok(())
    }

    pub async fn check(self) -> Result<(), anyhow::Error> {
        self.ensure_db().await?;

        let running_apps = application::Entity::find()
            .filter(application::Column::State.eq(State::RUNNING))
            .all(&self.db)
            .await?;

        let mut finished_count = 0usize;
        for app in running_apps {
            let should_finish = match app.address.as_deref() {
                Some(address) => {
                    if let Err(e) = send_session_message(address, &app.token, "health").await {
                        warn!(
                            "Health check failed for app {} at {}: {e:?}. Marking as finished.",
                            app.id, address
                        );
                        true
                    } else {
                        false
                    }
                }
                None => {
                    warn!(
                        "App {} is RUNNING but has no address. Marking as finished.",
                        app.id
                    );
                    true
                }
            };

            if should_finish {
                application::ActiveModel {
                    id: ActiveValue::Set(app.id),
                    state: ActiveValue::Set(State::FINISHED),
                    address: ActiveValue::Set(None),
                    ..Default::default()
                }
                .update(&self.db)
                .await?;
                finished_count += 1;
            }
        }

        info!("Health check complete. Marked {finished_count} applications as finished.");
        Ok(())
    }

    async fn ensure_db(&self) -> Result<(), anyhow::Error> {
        Migrator::up(&self.db, None).await?;
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

pub(crate) async fn send_session_message(
    address: &str,
    token: &str,
    message: &str,
) -> anyhow::Result<()> {
    // Fake out a gRPC call that will get picked up by the server interceptor.
    let client = ClientBuilder::new().http2_prior_knowledge().build()?;
    let res = client
        .post(format!(
            "http://{address}/spark.connect.SparkConnectService/Config"
        ))
        .bearer_auth(token)
        .header("X-Connect-Proxy", message)
        .header("Content-Type", "application/grpc")
        .header("TE", "trailers")
        .send()
        .await?;

    res.error_for_status()?;
    Ok(())
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
    let (mut sender, address) = {
        let address = loop {
            if rx.is_closed() {
                return;
            }

            let app = match application::Entity::find()
                .filter(application::Column::Token.eq(&token))
                .one(&db)
                .await
            {
                Ok(Some(app)) => app,
                Ok(None) => {
                    error!("Failed to find application for token {token}");
                    return;
                }
                Err(e) => {
                    error!("Failed to retrieve application by token: {e:?}");
                    return;
                }
            };

            match app.state {
                State::RUNNING => break app.address.unwrap(),
                State::LAUNCHING => tokio::time::sleep(Duration::from_secs(1)).await,
                state => {
                    error!("Cannot connect to an application in state {state:?}");
                    return;
                }
            }
        };

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
        (sender, address)
    };

    while let Some((mut req, tx)) = rx.recv().await {
        let uri_string = format!(
            "http://{address}{}",
            req.uri()
                .path_and_query()
                .map(|x| x.as_str())
                .unwrap_or("/")
        );
        *req.uri_mut() = uri_string.parse().unwrap();

        debug!("Proxying request {:?}", req.uri().path_and_query());

        let response = sender
            .send_request(req)
            .await
            .map(|response| response.map(axum::body::Body::new));

        debug!("Proxying response {response:?}");

        tx.send(response).unwrap();
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

        match dispatch.as_mut().unwrap().send((req, tx)) {
            Ok(_) => rx,
            Err(mpsc::error::SendError((_, tx))) => {
                tx.send(Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(().into())
                    .unwrap()))
                    .unwrap();
                rx
            }
        }
    }
}

impl Service<Request<hyper::body::Incoming>> for ProxyService {
    type Response = Response<axum::body::Body>;

    type Error = hyper::Error;

    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn call(&self, req: Request<hyper::body::Incoming>) -> Self::Future {
        debug!("Handling call for {} {}", req.method(), req.uri());
        if req
            .uri()
            .path()
            .starts_with("/spark.connect.SparkConnectService")
        {
            let rx = self.dispatch(req);
            Box::pin(async move {
                match rx.await {
                    Ok(res) => Ok(res?.map(axum::body::Body::new)),
                    Err(_) => Ok(Response::builder()
                        .status(StatusCode::NOT_FOUND)
                        .body(().into())
                        .unwrap()),
                }
            })
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

#[cfg(test)]
pub(crate) mod test_utils {
    use std::{collections::HashMap, io, sync::Arc, time::Duration};

    use migration::{Migrator, MigratorTrait};
    use sea_orm::{Database, DatabaseConnection};
    use tokio::task::JoinHandle;

    use crate::{
        auth::{CurrentUserAuth, UserAuth},
        launcher::Launcher,
        routes::get_router,
    };

    use super::{ProxyConfig, Router, Server};

    #[derive(Clone)]
    pub(crate) struct MockLauncher;

    #[async_trait::async_trait]
    impl Launcher for MockLauncher {
        fn get_versions(&self) -> Vec<String> {
            vec!["4.0.0".to_string()]
        }

        async fn launch(
            &self,
            _version_name: Option<&str>,
            _session_id: i32,
            _app_name: Option<String>,
            _username: String,
            _token: String,
            _user_config: HashMap<String, String>,
            _python_packages: Option<Vec<String>>,
        ) -> Result<JoinHandle<()>, io::Error> {
            Ok(tokio::spawn(async {
                tokio::time::sleep(Duration::from_secs(60)).await;
            }))
        }
    }

    pub(crate) fn default_user_auth() -> UserAuth {
        UserAuth {
            auth_methods: vec![Arc::new(CurrentUserAuth {})],
        }
    }

    pub(crate) async fn create_test_router_with_config(
        config: ProxyConfig,
        user_auth: UserAuth,
    ) -> (Router, DatabaseConnection) {
        let _ = env_logger::Builder::new()
            .filter(Some("spark_connect_proxy"), log::LevelFilter::Debug)
            .is_test(true)
            .try_init();

        let db = Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&db, None).await.unwrap();

        let router = get_router(user_auth, MockLauncher, db.clone(), config).await;
        (router, db)
    }

    pub(crate) async fn create_test_server_with_config(
        config: ProxyConfig,
        user_auth: UserAuth,
    ) -> Server {
        let (router, db) = create_test_router_with_config(config.clone(), user_auth).await;

        Server {
            config,
            router,
            db,
            tls_acceptor: None,
        }
    }
}

#[cfg(test)]
mod test {
    use sea_orm::{ActiveModelTrait, ActiveValue, EntityTrait};
    use uuid::Uuid;

    use super::*;
    use crate::test_utils::{create_test_server_with_config, default_user_auth};

    async fn create_test_server() -> Server {
        create_test_server_with_config(
            ProxyConfig {
                store: Some("sqlite::memory:".to_string()),
                ..Default::default()
            },
            default_user_auth(),
        )
        .await
    }

    async fn insert_app(
        db: &DatabaseConnection,
        state: State,
        address: Option<&str>,
        created_at: DateTimeUtc,
    ) -> application::Model {
        application::ActiveModel {
            created_at: ActiveValue::Set(created_at),
            username: ActiveValue::Set("test-user".to_string()),
            state: ActiveValue::Set(state),
            token: ActiveValue::Set(Uuid::new_v4().to_string()),
            address: ActiveValue::Set(address.map(ToOwned::to_owned)),
            application_id: ActiveValue::Set(None),
            ..Default::default()
        }
        .insert(db)
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_prune() {
        let server = create_test_server().await;
        let db = server.db.clone();

        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let old = DateTimeUtc::from_timestamp(now_secs - 120, 0).unwrap();
        let recent = DateTimeUtc::from_timestamp(now_secs - 10, 0).unwrap();

        let old_failed = insert_app(&db, State::FAILED, None, old).await;
        let old_finished = insert_app(&db, State::FINISHED, None, old).await;
        let recent_failed = insert_app(&db, State::FAILED, None, recent).await;
        let old_running = insert_app(&db, State::RUNNING, Some("127.0.0.1:1"), old).await;

        server.prune(60).await.unwrap();

        assert!(
            application::Entity::find_by_id(old_failed.id)
                .one(&db)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            application::Entity::find_by_id(old_finished.id)
                .one(&db)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            application::Entity::find_by_id(recent_failed.id)
                .one(&db)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            application::Entity::find_by_id(old_running.id)
                .one(&db)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn test_check() {
        let server = create_test_server().await;
        let db = server.db.clone();

        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let now = DateTimeUtc::from_timestamp(now_secs, 0).unwrap();

        let no_address_running = insert_app(&db, State::RUNNING, None, now).await;
        let unreachable_running = insert_app(&db, State::RUNNING, Some("127.0.0.1:9"), now).await;
        let launching = insert_app(&db, State::LAUNCHING, None, now).await;

        server.check().await.unwrap();

        let no_address_after = application::Entity::find_by_id(no_address_running.id)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(no_address_after.state, State::FINISHED);
        assert_eq!(no_address_after.address, None);

        let unreachable_after = application::Entity::find_by_id(unreachable_running.id)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(unreachable_after.state, State::FINISHED);
        assert_eq!(unreachable_after.address, None);

        let launching_after = application::Entity::find_by_id(launching.id)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(launching_after.state, State::LAUNCHING);
    }
}
