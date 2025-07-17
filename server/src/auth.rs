use std::{collections::HashMap, fs, sync::Arc};

use anyhow::Result;
use axum::response::IntoResponse;
use futures_util::future::BoxFuture;
use http::{HeaderMap, Request, Response, StatusCode, header::AUTHORIZATION};
use jsonwebtoken::{DecodingKey, TokenData, Validation, decode, decode_header};
use jwks::Jwks;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use tower_http::auth::AsyncAuthorizeRequest;

use crate::config::ProxyConfig;

#[derive(Clone)]
pub struct UserId(pub String);

#[derive(Clone)]
pub struct BearerToken(pub String);

fn extract_bearer_token(header_map: &HeaderMap) -> Result<Option<&str>> {
    Ok(header_map
        .get("Authorization")
        .map(|h| h.to_str())
        .transpose()?
        .filter(|h| h.starts_with("Bearer "))
        .map(|h| (&h[7..])))
}

trait UserAuthMethod: Sync + Send {
    fn authorize_user(&self, header_map: &HeaderMap) -> Result<Option<String>>;
}

struct CurrentUserAuth {}

impl UserAuthMethod for CurrentUserAuth {
    fn authorize_user(&self, _: &HeaderMap) -> Result<Option<String>> {
        Ok(Some(whoami::username()))
    }
}

///
/// Authenticate users by simply checking a specific header for their username.
/// This assumes the user has already been authenticated by an upstream proxy
/// which is simply passing their username along.
///
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
    fn authorize_user(&self, header_map: &HeaderMap) -> Result<Option<String>> {
        Ok(header_map
            .get(&self.header)
            .and_then(|h| h.to_str().ok().map(|v| v.to_string())))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct JWTClaims {
    sub: String,
}

struct JWTAuth {
    key: DecodingKey,
    validation: Validation,
}

impl JWTAuth {
    fn create(options: &HashMap<String, String>) -> Self {
        let pem = options.get("pem");
        let pem_file = options.get("pem_file");

        let pem_content = match (pem, pem_file) {
            (Some(_), Some(_)) => panic!("pem and pem_file cannot both be defined"),
            (Some(pem), None) => pem.to_string(),
            (None, Some(pem_file)) => {
                fs::read_to_string(pem_file).expect("Failed to read pem content from {pem_file}")
            }
            (None, None) => panic!("One of pem or pem_file must be defined for JWT auth"),
        };

        let key = match options.get("pem_type").map(String::as_ref).unwrap_or("rsa") {
            "rsa" => {
                DecodingKey::from_rsa_pem(pem_content.as_bytes()).expect("Failed to load RSA pem")
            }
            "ec" => {
                DecodingKey::from_ec_pem(pem_content.as_bytes()).expect("Failed to load EC pem")
            }
            "ed" => {
                DecodingKey::from_ed_pem(pem_content.as_bytes()).expect("Failed to load ED pem")
            }
            t => panic!("Unknown PEM type {t}"),
        };

        Self {
            key,
            validation: Validation::default(),
        }
    }
}

impl UserAuthMethod for JWTAuth {
    fn authorize_user(&self, header_map: &HeaderMap) -> Result<Option<String>> {
        if let Some(token) = extract_bearer_token(header_map)? {
            let decoded_token: TokenData<JWTClaims> = decode(token, &self.key, &self.validation)?;
            Ok(Some(decoded_token.claims.sub))
        } else {
            Ok(None)
        }
    }
}

struct JWKSAuth {
    jwks: Jwks,
    audience: Option<String>,
}

impl JWKSAuth {
    async fn create(options: &HashMap<String, String>) -> Self {
        let jwks = match (options.get("jwks_url"), options.get("oidc_url")) {
            (Some(_), Some(_)) => panic!("JWKS and OIDC URL cannot both be specified"),
            (Some(jwks_url), None) => Jwks::from_jwks_url(jwks_url)
                .await
                .expect("Failed to load JWKS info from {jwks_url}"),
            (None, Some(oidc_url)) => Jwks::from_oidc_url(oidc_url)
                .await
                .expect("Failed to load OIDC info from {oidc_url}"),
            (None, None) => panic!("Either jwks_url or oidc_url must be provided"),
        };

        let audience = options.get("audience").cloned();

        Self { jwks, audience }
    }
}

impl UserAuthMethod for JWKSAuth {
    fn authorize_user(&self, header_map: &HeaderMap) -> Result<Option<String>> {
        if let Some(token) = extract_bearer_token(header_map)? {
            let header = decode_header(token)?;
            let kid = header
                .kid
                .as_ref()
                .ok_or(crate::error::Error::AuthorizationError(
                    "jwt header should have a kid".to_string(),
                ))?;

            let jwk = self
                .jwks
                .keys
                .get(kid)
                .ok_or(crate::error::Error::AuthorizationError(
                    "jwt refer to a unknown key id".to_string(),
                ))?;

            let mut validation = Validation::new(jwk.alg.to_string().parse()?);
            if let Some(audience) = self.audience.as_ref() {
                validation.set_audience(&[audience]);
            }
            let decoded_token: TokenData<JWTClaims> =
                decode::<JWTClaims>(token, &jwk.decoding_key, &validation)?;
            Ok(Some(decoded_token.claims.sub))
        } else {
            Ok(None)
        }
    }
}

#[derive(Clone)]
pub struct UserAuth {
    auth_methods: Vec<Arc<dyn UserAuthMethod>>,
}

impl UserAuth {
    #[allow(clippy::vec_init_then_push)]
    pub(crate) async fn new(config: &ProxyConfig) -> Self {
        let mut auth_methods = Vec::<Arc<dyn UserAuthMethod>>::new();

        let default_options = HashMap::new();
        if let Some(auth_configs) = &config.auth_methods {
            for auth_config in auth_configs {
                let auth_options = auth_config.options.as_ref().unwrap_or(&default_options);
                match auth_config.name.as_ref() {
                    "remote_user" => {
                        auth_methods.push(Arc::new(RemoteUserAuth::create(auth_options)))
                    }
                    "jwt" => auth_methods.push(Arc::new(JWTAuth::create(auth_options))),
                    "jwks" => auth_methods.push(Arc::new(JWKSAuth::create(auth_options).await)),
                    name => panic!("Unknown authentication method: {name}"),
                }
                info!("Enabling auth method {}", auth_config.name);
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
                match auth_method.authorize_user(request.headers()) {
                    Ok(Some(user)) => {
                        username = Some(user);
                        break;
                    }
                    Err(e) => warn!("Error trying to authorize user: {e:?}"),
                    _ => (),
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

            info!("Authorizing token: {authorization}");

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
