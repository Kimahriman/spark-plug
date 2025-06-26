use std::{collections::HashMap, sync::Arc};

use axum::response::IntoResponse;
use futures_util::future::BoxFuture;
use http::{header::AUTHORIZATION, HeaderMap, Request, Response, StatusCode};
use log::info;
use tower_http::auth::AsyncAuthorizeRequest;

use crate::config::ProxyConfig;

#[derive(Clone)]
pub struct UserId(pub String);

#[derive(Clone)]
pub struct BearerToken(pub String);

trait UserAuthMethod: Sync + Send {
    fn authorize_user(&self, header_map: &HeaderMap) -> Option<String>;
}

struct CurrentUserAuth {}

impl UserAuthMethod for CurrentUserAuth {
    fn authorize_user(&self, _: &HeaderMap) -> Option<String> {
        Some(whoami::username())
    }
}

/**
 * Authenticate users by simply checking a specific header for their username.
 * This assumes the user has already been authenticated by an upstream proxy
 * which is simply passing their username along.
 */
struct RemoteUserAuth {
    header: String,
}

impl RemoteUserAuth {
    fn create(options: &HashMap<String, String>) -> Self {
        let header = options.get("header").unwrap_or_else(|| {
            panic!("'header' option must be set for remote_user authentication")
        });

        Self {
            header: header.clone(),
        }
    }
}

impl UserAuthMethod for RemoteUserAuth {
    fn authorize_user(&self, header_map: &HeaderMap) -> Option<String> {
        header_map
            .get(&self.header)
            .and_then(|h| h.to_str().ok().map(|v| v.to_string()))
    }
}

#[cfg(feature = "pam")]
struct PamAuth {}

#[cfg(feature = "pam")]
impl UserAuthMethod for PamAuth {
    fn authorize_user(&self, auth_header: &str) -> Option<String> {
        if !auth_header.starts_with("Basic ") {
            return None;
        }
        let encoded = &auth_header[6..];
        let decoded = String::from_utf8(BASE64_STANDARD.decode(encoded).unwrap()).unwrap();
        let (username, password) = decoded.split_once(":").unwrap();

        let mut client = pam::Client::with_password("system-auth").unwrap();
        client
            .conversation_mut()
            .set_credentials(username, password);
        client.authenticate().map(|_| username)
    }
}

#[derive(Clone)]
pub struct UserAuth {
    auth_methods: Vec<Arc<dyn UserAuthMethod>>,
}

impl UserAuth {
    #[allow(clippy::vec_init_then_push)]
    pub(crate) fn new(config: &ProxyConfig) -> Self {
        let mut auth_methods = Vec::<Arc<dyn UserAuthMethod>>::new();

        let default_options = HashMap::new();
        if let Some(auth_configs) = &config.auth_methods {
            for auth_config in auth_configs {
                match auth_config.name.as_ref() {
                    "remote_user" => auth_methods.push(Arc::new(RemoteUserAuth::create(
                        auth_config.options.as_ref().unwrap_or(&default_options),
                    ))),
                    #[cfg(feature = "pam")]
                    "pam" => auth_methods.push(Arc::new(PamAuth {})),
                    name => panic!("Unknown authentication method: {}", name),
                }
            }
        } else {
            auth_methods.push(Arc::new(CurrentUserAuth {}));
        }

        Self { auth_methods }
    }
}

impl AsyncAuthorizeRequest<axum::body::Body> for UserAuth {
    type RequestBody = axum::body::Body;

    type ResponseBody = axum::body::Body;

    type Future =
        BoxFuture<'static, Result<Request<Self::RequestBody>, Response<Self::ResponseBody>>>;

    fn authorize(&mut self, mut request: hyper::Request<axum::body::Body>) -> Self::Future {
        let auth_methods = self.auth_methods.clone();
        Box::pin(async move {
            let mut username: Option<String> = None;
            for auth_method in auth_methods.iter() {
                if let Some(user) = auth_method.authorize_user(request.headers()) {
                    username = Some(user);
                }
            }

            if let Some(username) = username {
                request.extensions_mut().insert(UserId(username));
            } else {
                return Err(StatusCode::UNAUTHORIZED.into_response());
            }

            Ok(request)
        })
    }
}

#[derive(Clone)]
pub struct TokenAuth {}

impl AsyncAuthorizeRequest<axum::body::Body> for TokenAuth {
    type RequestBody = axum::body::Body;

    type ResponseBody = axum::body::Body;

    type Future =
        BoxFuture<'static, Result<Request<Self::RequestBody>, Response<Self::ResponseBody>>>;

    fn authorize(&mut self, mut request: hyper::Request<Self::RequestBody>) -> Self::Future {
        Box::pin(async {
            let authorization = request
                .headers()
                .get(AUTHORIZATION)
                .ok_or(StatusCode::UNAUTHORIZED.into_response())?
                .to_str()
                .map_err(|_| StatusCode::BAD_REQUEST.into_response())?
                .to_string();

            info!("Authorizing token: {}", authorization);

            let split = authorization.split_once(' ');
            let token = match split {
                Some(("Bearer", token)) => token,
                _ => return Err(StatusCode::UNAUTHORIZED.into_response()),
            };

            request
                .extensions_mut()
                .insert(BearerToken(token.to_string()));
            Ok(request)
        })
    }
}
