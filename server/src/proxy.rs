use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use http::header::AUTHORIZATION;
use http::header::CONTENT_TYPE;
use http::header::{HeaderName, HeaderValue};
use http::{HeaderMap, StatusCode};
use hyper::body::Incoming;
use hyper::service::Service;
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use log::{debug, warn};
use migration::Expr;
use reqwest::ClientBuilder;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tower::Service as TowerService;

use crate::entities::application::{self, State};
use crate::error::Error as ProxyError;

type UpstreamMessage = (
    Request<Incoming>,
    oneshot::Sender<Result<Response<axum::body::Body>, ProxyError>>,
);

const MAX_UPSTREAM_CONNECT_RETRIES: usize = 3;

#[cfg(test)]
const UPSTREAM_CONNECT_RETRY_DELAY: Duration = Duration::from_millis(10);

#[cfg(not(test))]
const UPSTREAM_CONNECT_RETRY_DELAY: Duration = Duration::from_secs(1);

pub(crate) struct ProxyService {
    id: u64,
    upstreams: UpstreamConnectionCache,
    router: Router,
}

impl ProxyService {
    pub(crate) fn new(router: Router, upstreams: UpstreamConnectionCache) -> Self {
        let id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
        debug!("Creating proxy service {id}");
        Self {
            id,
            upstreams,
            router,
        }
    }
}

impl Service<Request<Incoming>> for ProxyService {
    type Response = Response<axum::body::Body>;
    type Error = hyper::Error;
    type Future =
        Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        debug!(
            "Handling call on service {} for {} {}",
            self.id,
            req.method(),
            req.uri()
        );
        if req
            .uri()
            .path()
            .starts_with("/spark.connect.SparkConnectService")
        {
            let upstreams = self.upstreams.clone();
            Box::pin(async move {
                match dispatch_request(req, upstreams).await {
                    Ok(res) => Ok(res),
                    Err(error) => {
                        warn!("Proxy request failed: {error}");
                        Ok(proxy_error_response(error))
                    }
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
        debug!("Dropping proxy service {}", self.id);
    }
}

/// Server-wide cache of token-specific Spark Connect dispatchers.
///
/// Each dispatcher lazily opens one HTTP/2 connection to its application's
/// upstream and reuses it across requests from any downstream connection.
#[derive(Clone)]
pub(crate) struct UpstreamConnectionCache {
    connections: Arc<Mutex<HashMap<String, mpsc::UnboundedSender<UpstreamMessage>>>>,
    db: DatabaseConnection,
}

impl UpstreamConnectionCache {
    pub(crate) fn new(db: DatabaseConnection) -> Self {
        Self {
            connections: Arc::new(Mutex::new(HashMap::new())),
            db,
        }
    }

    async fn sender_for(
        &self,
        token: &str,
    ) -> Result<mpsc::UnboundedSender<UpstreamMessage>, ProxyError> {
        {
            let connections = self.connections.lock().unwrap();
            if let Some(sender) = connections.get(token)
                && !sender.is_closed()
            {
                return Ok(sender.clone());
            }
        }

        // Only cache tokens that identify an application. Besides avoiding a
        // database lookup on subsequent requests, this prevents arbitrary
        // bearer tokens from growing the process-wide cache.
        let exists = application::Entity::find()
            .filter(application::Column::Token.eq(token))
            .one(&self.db)
            .await?
            .is_some();
        if !exists {
            return Err(ProxyError::ApplicationNotFound);
        }

        let mut connections = self.connections.lock().unwrap();
        if let Some(sender) = connections.get(token)
            && !sender.is_closed()
        {
            return Ok(sender.clone());
        }
        let (sender, receiver) = mpsc::unbounded_channel();
        let db = self.db.clone();
        let token = token.to_string();
        connections.insert(token.clone(), sender.clone());
        tokio::task::spawn(async move { upstream_connection(receiver, token, db).await });
        Ok(sender)
    }

    fn remove_closed(&self, token: &str) {
        let mut connections = self.connections.lock().unwrap();
        if connections
            .get(token)
            .is_some_and(|sender| sender.is_closed())
        {
            connections.remove(token);
        }
    }

    pub(crate) fn invalidate(&self, token: &str) {
        if self.connections.lock().unwrap().remove(token).is_some() {
            debug!(
                "Invalidated upstream connection for token {}",
                token_prefix(token)
            );
        }
    }
}

async fn dispatch_request(
    req: Request<Incoming>,
    upstreams: UpstreamConnectionCache,
) -> Result<Response<axum::body::Body>, ProxyError> {
    let token = extract_bearer_token(req.headers())?;
    let sender = upstreams.sender_for(&token).await?;
    let (tx, rx) = oneshot::channel();

    if let Err(mpsc::error::SendError((_, _))) = sender.send((req, tx)) {
        upstreams.remove_closed(&token);
        return Err(ProxyError::Internal(
            "Upstream unexpectedly closed".to_string(),
        ));
    }

    rx.await.map_err(|_| {
        ProxyError::Internal("Upstream response channel unexpectedly closed".to_string())
    })?
}

pub(crate) async fn send_session_message(
    address: &str,
    token: &str,
    message: &str,
) -> anyhow::Result<()> {
    // Fake out a gRPC call that will get picked up by the server interceptor.
    let client = ClientBuilder::new()
        .http2_prior_knowledge()
        .no_proxy()
        .build()?;
    let res = client
        .post(format!(
            "http://{address}/spark.connect.SparkConnectService/Config"
        ))
        .bearer_auth(token)
        .header("X-Spark-Plug", message)
        .header("Content-Type", "application/grpc")
        .header("TE", "trailers")
        .send()
        .await?;

    res.error_for_status()?;
    Ok(())
}

fn proxy_error_response(error: ProxyError) -> Response<axum::body::Body> {
    const GRPC_STATUS: HeaderName = HeaderName::from_static("grpc-status");
    const GRPC_MESSAGE: HeaderName = HeaderName::from_static("grpc-message");

    let grpc_status = match &error {
        ProxyError::MissingAuthorizationHeader => "16",
        ProxyError::InvalidAuthorizationHeader(_) => "16",
        ProxyError::InvalidAuthorizationScheme => "16",
        ProxyError::InvalidAuthorizationToken => "16",
        ProxyError::ApplicationNotFound => "5",
        ProxyError::MissingApplicationAddress => "14",
        ProxyError::InvalidApplicationState(State::LAUNCHING) => "14",
        ProxyError::InvalidApplicationState(_) => "9",
        ProxyError::Database(_) => "13",
        ProxyError::UpstreamConnect(_) => "14",
        ProxyError::UpstreamHandshake(_) => "14",
        ProxyError::InvalidUpstreamUri(_) => "13",
        ProxyError::UpstreamRequest(_) => "14",
        ProxyError::Authorization(_) => "16",
        ProxyError::Internal(_) => "13",
    };
    let grpc_message = percent_encode_grpc_message(&error.to_string());

    Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, "application/grpc")
        .header(GRPC_STATUS, grpc_status)
        .header(
            GRPC_MESSAGE,
            HeaderValue::from_str(&grpc_message)
                .unwrap_or_else(|_| HeaderValue::from_static("proxy%20error")),
        )
        .body(axum::body::Body::empty())
        .unwrap()
}

fn percent_encode_grpc_message(message: &str) -> String {
    let mut encoded = String::with_capacity(message.len());

    for byte in message.bytes() {
        match byte {
            b' '..=b'~' if byte != b'%' => encoded.push(byte as char),
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }

    encoded
}

fn extract_bearer_token(headers: &HeaderMap) -> Result<String, ProxyError> {
    let authorization = headers
        .get(AUTHORIZATION)
        .ok_or(ProxyError::MissingAuthorizationHeader)?;
    let authorization = authorization
        .to_str()
        .map_err(|err| ProxyError::InvalidAuthorizationHeader(err.to_string()))?;

    match authorization.split_once(' ') {
        Some(("Bearer", token)) if !token.is_empty() => Ok(token.to_string()),
        Some(("Bearer", _)) => Err(ProxyError::InvalidAuthorizationHeader(
            "missing bearer token".to_string(),
        )),
        Some(_) => Err(ProxyError::InvalidAuthorizationScheme),
        None => Err(ProxyError::InvalidAuthorizationHeader(
            "expected `Bearer <token>`".to_string(),
        )),
    }
}

fn token_prefix(token: &str) -> &str {
    token.get(..8).unwrap_or(token)
}

async fn resolve_upstream_address(
    rx: &mpsc::UnboundedReceiver<UpstreamMessage>,
    token: &str,
    db: &DatabaseConnection,
) -> Result<String, ProxyError> {
    loop {
        if rx.is_closed() {
            return Err(ProxyError::ApplicationNotFound);
        }

        let app = application::Entity::find()
            .filter(application::Column::Token.eq(token))
            .one(db)
            .await?
            .ok_or(ProxyError::ApplicationNotFound)?;

        match app.state {
            State::RUNNING => {
                return app.address.ok_or(ProxyError::MissingApplicationAddress);
            }
            State::LAUNCHING => tokio::time::sleep(Duration::from_secs(1)).await,
            state => return Err(ProxyError::InvalidApplicationState(state)),
        }
    }
}

async fn connect_upstream(
    address: &str,
) -> Result<hyper::client::conn::http2::SendRequest<Incoming>, ProxyError> {
    let client_stream = TcpStream::connect(&address)
        .await
        .map_err(ProxyError::UpstreamConnect)?;
    let io = TokioIo::new(client_stream);

    let (sender, conn) = hyper::client::conn::http2::handshake(TokioExecutor::new(), io)
        .await
        .map_err(ProxyError::UpstreamHandshake)?;
    tokio::task::spawn(async move {
        debug!("Spawned connection await");
        let res = conn.await;
        debug!("Upstream connection closed: {res:?}");
        if let Err(err) = res {
            warn!("Upstream connection failed: {err:?}");
        }
    });

    Ok(sender)
}

async fn mark_application_failed(token: &str, db: &DatabaseConnection) -> Result<(), ProxyError> {
    application::Entity::update_many()
        .col_expr(
            application::Column::Address,
            Expr::value::<Option<String>>(None),
        )
        .col_expr(application::Column::State, Expr::value(State::FAILED))
        .filter(application::Column::Token.eq(token))
        .exec(db)
        .await?;

    Ok(())
}

async fn resolve_upstream_connection(
    rx: &mpsc::UnboundedReceiver<UpstreamMessage>,
    token: &str,
    db: &DatabaseConnection,
) -> Result<(hyper::client::conn::http2::SendRequest<Incoming>, String), ProxyError> {
    let address = resolve_upstream_address(rx, token, db).await?;
    let mut last_error = None;

    for attempt in 0..=MAX_UPSTREAM_CONNECT_RETRIES {
        match connect_upstream(&address).await {
            Ok(sender) => return Ok((sender, address.clone())),
            Err(error @ ProxyError::UpstreamConnect(_))
            | Err(error @ ProxyError::UpstreamHandshake(_)) => {
                warn!(
                    "Failed to connect to upstream {address} for token after attempt {}: {error}",
                    attempt + 1
                );
                last_error = Some(error);
            }
            Err(error) => return Err(error),
        }

        if attempt < MAX_UPSTREAM_CONNECT_RETRIES {
            tokio::time::sleep(UPSTREAM_CONNECT_RETRY_DELAY).await;
        }
    }

    mark_application_failed(token, db).await?;
    Err(last_error.expect("connection retries should capture the last upstream error"))
}

async fn upstream_connection(
    mut rx: mpsc::UnboundedReceiver<UpstreamMessage>,
    token: String,
    db: DatabaseConnection,
) {
    let mut upstream = None;

    while let Some((mut req, tx)) = rx.recv().await {
        // Verify the token on the request matches that for the session
        match extract_bearer_token(req.headers()) {
            Ok(t) if t == token => (),
            Ok(t) => {
                warn!(
                    "Token mismatch on upstream request. Connection is using {}, request contained {}",
                    token_prefix(token.as_ref()),
                    token_prefix(t.as_ref())
                );
                let _ = tx.send(Err(ProxyError::InvalidAuthorizationToken));
                continue;
            }
            Err(error) => {
                warn!("Failed to parse token for request: {error}");
                let _ = tx.send(Err(error));
                continue;
            }
        };

        if upstream.is_none() {
            match resolve_upstream_connection(&rx, &token, &db).await {
                Ok(connection) => upstream = Some(connection),
                Err(error) => {
                    warn!(
                        "Failed to initialize upstream connection for token {}: {error}",
                        token_prefix(token.as_ref())
                    );
                    let _ = tx.send(Err(error));
                    continue;
                }
            }
        }

        let (sender, address) = upstream.as_mut().unwrap();
        let uri_string = format!(
            "http://{address}{}",
            req.uri()
                .path_and_query()
                .map(|x| x.as_str())
                .unwrap_or("/")
        );
        let uri = match uri_string.parse() {
            Ok(uri) => uri,
            Err(err) => {
                let _ = tx.send(Err(ProxyError::InvalidUpstreamUri(err)));
                continue;
            }
        };
        *req.uri_mut() = uri;

        debug!(
            "Proxying request for token {}: {:?}",
            token_prefix(token.as_ref()),
            req.uri().path_and_query()
        );

        if let Err(error) = sender.ready().await {
            upstream = None;
            let _ = tx.send(Err(ProxyError::UpstreamRequest(error)));
            continue;
        }

        let response = sender.send_request(req);
        let token_prefix = token_prefix(&token).to_string();
        tokio::task::spawn(async move {
            let response = response
                .await
                .map(|response| response.map(axum::body::Body::new))
                .map_err(ProxyError::UpstreamRequest);

            debug!("Proxying response for token {token_prefix}: {response:?}");

            if tx.send(response).is_err() {
                debug!("Request receiver dropped before upstream response was delivered");
            }
        });
    }

    debug!("rx closed for upstream");
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

#[cfg(test)]
mod test {
    use std::convert::Infallible;
    use std::sync::Arc;
    use std::time::Duration;

    use axum::body::Body;
    use http::header::{AUTHORIZATION, CONTENT_TYPE};
    use http::{HeaderMap, Request, Response, StatusCode};
    use hyper::body::Incoming;
    use hyper::service::service_fn;
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use sea_orm::{ActiveModelTrait, ActiveValue, EntityTrait};
    use tokio::sync::Barrier;
    use uuid::Uuid;

    use super::{
        ProxyService, UpstreamConnectionCache, extract_bearer_token, percent_encode_grpc_message,
        proxy_error_response, resolve_upstream_connection,
    };
    use crate::config::ProxyConfig;
    use crate::entities::application::{self, State};
    use crate::error::Error as ProxyError;
    use crate::test_utils::{create_test_router_with_config, default_user_auth};

    #[test]
    fn test_extract_bearer_token() {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, "Bearer test-token".parse().unwrap());

        assert_eq!(extract_bearer_token(&headers).unwrap(), "test-token");
    }

    #[test]
    fn test_extract_bearer_token_rejects_invalid_headers() {
        assert!(matches!(
            extract_bearer_token(&HeaderMap::new()),
            Err(ProxyError::MissingAuthorizationHeader)
        ));

        let mut wrong_scheme = HeaderMap::new();
        wrong_scheme.insert(AUTHORIZATION, "Basic abc123".parse().unwrap());
        assert!(matches!(
            extract_bearer_token(&wrong_scheme),
            Err(ProxyError::InvalidAuthorizationScheme)
        ));
    }

    #[tokio::test]
    async fn test_proxy_error_response_maps_status() {
        let not_found = proxy_error_response(ProxyError::ApplicationNotFound);
        assert_eq!(not_found.status(), StatusCode::OK);
        assert_eq!(
            not_found.headers().get(CONTENT_TYPE).unwrap(),
            "application/grpc"
        );
        assert_eq!(not_found.headers().get("grpc-status").unwrap(), "5");
        assert_eq!(
            not_found.headers().get("grpc-message").unwrap(),
            "application not found for provided token"
        );

        let launching = proxy_error_response(ProxyError::InvalidApplicationState(State::LAUNCHING));
        assert_eq!(launching.status(), StatusCode::OK);
        assert_eq!(launching.headers().get("grpc-status").unwrap(), "14");

        let missing_address = proxy_error_response(ProxyError::MissingApplicationAddress);
        assert_eq!(missing_address.status(), StatusCode::OK);
        assert_eq!(missing_address.headers().get("grpc-status").unwrap(), "14");

        let auth = proxy_error_response(ProxyError::Authorization("missing subject".into()));
        assert_eq!(auth.headers().get("grpc-status").unwrap(), "16");
        assert_eq!(
            auth.headers().get("grpc-message").unwrap(),
            "authorization error: missing subject"
        );
    }

    #[test]
    fn test_percent_encode_grpc_message() {
        assert_eq!(
            percent_encode_grpc_message("upstream failed: bad\nstate % value"),
            "upstream failed: bad%0Astate %25 value"
        );
    }

    #[tokio::test]
    async fn test_upstream_connections_are_cached_by_token() {
        let (_router, db) =
            create_test_router_with_config(ProxyConfig::default(), default_user_auth()).await;
        let cache = UpstreamConnectionCache::new(db);

        for token in ["first-token", "second-token"] {
            application::ActiveModel {
                username: ActiveValue::Set("test-user".to_string()),
                state: ActiveValue::Set(State::LAUNCHING),
                token: ActiveValue::Set(token.to_string()),
                ..Default::default()
            }
            .insert(&cache.db)
            .await
            .unwrap();
        }

        let first = cache.sender_for("first-token").await.unwrap();
        let first_again = cache.sender_for("first-token").await.unwrap();
        let second = cache.sender_for("second-token").await.unwrap();

        assert!(first.same_channel(&first_again));
        assert!(!first.same_channel(&second));
    }

    #[tokio::test]
    async fn test_unknown_tokens_are_not_cached() {
        let (_router, db) =
            create_test_router_with_config(ProxyConfig::default(), default_user_auth()).await;
        let cache = UpstreamConnectionCache::new(db);

        let result = cache.sender_for("unknown-token").await;

        assert!(matches!(result, Err(ProxyError::ApplicationNotFound)));
        assert!(cache.connections.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_upstream_connection_can_be_invalidated() {
        let (_router, db) =
            create_test_router_with_config(ProxyConfig::default(), default_user_auth()).await;
        let cache = UpstreamConnectionCache::new(db);
        let token = "test-token";
        application::ActiveModel {
            username: ActiveValue::Set("test-user".to_string()),
            state: ActiveValue::Set(State::LAUNCHING),
            token: ActiveValue::Set(token.to_string()),
            ..Default::default()
        }
        .insert(&cache.db)
        .await
        .unwrap();
        let sender = cache.sender_for(token).await.unwrap();
        let weak_sender = sender.downgrade();

        cache.invalidate(token);
        drop(sender);
        tokio::task::yield_now().await;

        assert!(!cache.connections.lock().unwrap().contains_key(token));
        assert!(weak_sender.upgrade().is_none());
    }

    #[tokio::test]
    async fn test_multiple_upstream_requests_can_be_in_flight() {
        let (router, db) =
            create_test_router_with_config(ProxyConfig::default(), default_user_auth()).await;
        let token = Uuid::new_v4().to_string();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        application::ActiveModel {
            username: ActiveValue::Set("test-user".to_string()),
            state: ActiveValue::Set(State::RUNNING),
            token: ActiveValue::Set(token.clone()),
            address: ActiveValue::Set(Some(address.to_string())),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();

        let barrier = Arc::new(Barrier::new(2));
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                .serve_connection(
                    TokioIo::new(stream),
                    service_fn(move |_request: Request<Incoming>| {
                        let barrier = barrier.clone();
                        async move {
                            barrier.wait().await;
                            Ok::<_, Infallible>(Response::new(Body::empty()))
                        }
                    }),
                )
                .await
                .unwrap();
        });

        let (client_io, proxy_io) = tokio::io::duplex(64 * 1024);
        let cache = UpstreamConnectionCache::new(db);
        tokio::spawn(async move {
            hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(proxy_io), ProxyService::new(router, cache))
                .await
                .unwrap();
        });

        let (mut sender, connection) =
            hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(client_io))
                .await
                .unwrap();
        tokio::spawn(connection);

        let request = || {
            Request::builder()
                .uri("/spark.connect.SparkConnectService/ExecutePlan")
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap()
        };
        sender.ready().await.unwrap();
        let first = sender.send_request(request());
        sender.ready().await.unwrap();
        let second = sender.send_request(request());

        let (first, second) = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(first, second)
        })
        .await
        .expect("requests were serialized instead of being concurrently in flight");

        assert!(first.is_ok());
        assert!(second.is_ok());
    }

    #[tokio::test]
    async fn test_resolve_upstream_connection_marks_application_failed_after_retries() {
        let (_router, db) =
            create_test_router_with_config(ProxyConfig::default(), default_user_auth()).await;
        let token = Uuid::new_v4().to_string();

        let app = application::ActiveModel {
            username: ActiveValue::Set("test-user".to_string()),
            state: ActiveValue::Set(State::RUNNING),
            token: ActiveValue::Set(token.clone()),
            address: ActiveValue::Set(Some("127.0.0.1:9".to_string())),
            application_id: ActiveValue::Set(None),
            ..Default::default()
        }
        .insert(&db)
        .await
        .unwrap();

        let (_tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let error = resolve_upstream_connection(&rx, &token, &db)
            .await
            .unwrap_err();

        assert!(matches!(error, ProxyError::UpstreamConnect(_)));

        let app = application::Entity::find_by_id(app.id)
            .one(&db)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(app.state, State::FAILED);
        assert_eq!(app.address, None);
    }
}
