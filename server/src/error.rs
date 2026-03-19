use thiserror::Error;

use crate::entities::application::State;

#[derive(Error, Debug)]
pub enum Error {
    #[error("authorization error: {0}")]
    Authorization(String),
    #[error("missing authorization header")]
    MissingAuthorizationHeader,
    #[error("invalid authorization header: {0}")]
    InvalidAuthorizationHeader(String),
    #[error("unsupported authorization scheme")]
    InvalidAuthorizationScheme,
    #[error("application not found for provided token")]
    ApplicationNotFound,
    #[error("application has no upstream address")]
    MissingApplicationAddress,
    #[error("application is not ready for proxying: {0:?}")]
    InvalidApplicationState(State),
    #[error("database error: {0}")]
    Database(#[from] sea_orm::DbErr),
    #[error("failed to connect to upstream: {0}")]
    UpstreamConnect(#[source] std::io::Error),
    #[error("failed to establish upstream HTTP/2 session: {0}")]
    UpstreamHandshake(#[source] hyper::Error),
    #[error("invalid upstream URI: {0}")]
    InvalidUpstreamUri(#[source] http::uri::InvalidUri),
    #[error("upstream request failed: {0}")]
    UpstreamRequest(#[source] hyper::Error),
}
