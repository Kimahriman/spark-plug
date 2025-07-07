use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("authorization error")]
    AuthorizationError(String),
}
