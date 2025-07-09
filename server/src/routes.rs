use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{Path, State},
    routing::{get, post},
    Extension, Json, Router,
};
use http::StatusCode;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use tower::ServiceBuilder;
use tower_http::auth::AsyncRequireAuthorizationLayer;
use uuid::Uuid;

use crate::{
    auth::{BearerToken, TokenAuth, UserAuth, UserId},
    config::ProxyConfig,
    launcher::Launcher,
    models::Application,
    store::ApplicationStore,
};

pub async fn get_router(config: &ProxyConfig, app_store: Arc<dyn ApplicationStore>) -> Router {
    let app_state = AppStateDyn {
        app_store,
        launcher: Arc::new(Launcher::from_config(config)),
    };

    let user_api = Router::new()
        .route("/apps", get(list_apps).post(create_app))
        .route("/apps/{app_id}", get(get_app).delete(delete_app))
        .route("/versions", get(list_versions))
        .route_layer(
            ServiceBuilder::new().layer(AsyncRequireAuthorizationLayer::new(
                UserAuth::new(config).await,
            )),
        )
        .with_state(app_state.clone());

    let callback_api = Router::new()
        .route("/callback", post(app_callback).delete(app_callback_delete))
        .route_layer(ServiceBuilder::new().layer(AsyncRequireAuthorizationLayer::new(TokenAuth {})))
        .with_state(app_state);

    Router::new().merge(user_api).merge(callback_api)
}

#[derive(Clone)]
struct AppStateDyn {
    app_store: Arc<dyn ApplicationStore>,
    launcher: Arc<Launcher>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct CreateApplicationRequest {
    version: Option<String>,
    config: Option<HashMap<String, String>>,
}

#[derive(Serialize)]
struct ApplicationInfo {
    id: i32,
    token: String,
    active: bool,
}

async fn create_app(
    State(state): State<AppStateDyn>,
    Extension(user): Extension<UserId>,
    Json(params): Json<CreateApplicationRequest>,
) -> Result<Json<ApplicationInfo>, StatusCode> {
    let token = Uuid::new_v4().to_string();
    let app = state
        .app_store
        .create_app(user.0.clone(), token.clone())
        .await;

    state
        .launcher
        .launch(
            params.version.as_ref().map(|v| v.as_ref()),
            user.0,
            token.clone(),
            params.config.unwrap_or_default(),
        )
        .await
        .map_err(|e| {
            warn!("{e:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(ApplicationInfo {
        id: app.id,
        token,
        active: app.addr.is_some(),
    }))
}

async fn get_app(
    State(state): State<AppStateDyn>,
    Path(app_id): Path<i32>,
    Extension(user): Extension<UserId>,
) -> Result<Json<ApplicationInfo>, StatusCode> {
    let app = state
        .app_store
        .get_app(user.0, app_id)
        .await
        .ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(ApplicationInfo {
        id: app.id,
        token: app.token,
        active: app.addr.is_some(),
    }))
}

async fn list_apps(
    State(state): State<AppStateDyn>,
    Extension(user): Extension<UserId>,
) -> Json<Vec<Application>> {
    Json(state.app_store.list_apps(user.0).await)
}

async fn delete_app(
    State(state): State<AppStateDyn>,
    Path(app_id): Path<i32>,
    Extension(user): Extension<UserId>,
) {
    state.app_store.delete_app(user.0, app_id).await;
}

async fn list_versions(State(state): State<AppStateDyn>) -> Json<Vec<String>> {
    Json(state.launcher.get_versions())
}

#[derive(Deserialize)]
struct ApplicationCallbackRequest {
    address: String,
}

async fn app_callback(
    State(state): State<AppStateDyn>,
    Extension(token): Extension<BearerToken>,
    Json(params): Json<ApplicationCallbackRequest>,
) -> Result<(), StatusCode> {
    info!("Got the callback for {}", token.0);
    state
        .app_store
        .set_app_addr(token.0, Some(params.address))
        .await;
    Ok(())
}

async fn app_callback_delete(
    State(state): State<AppStateDyn>,
    Extension(token): Extension<BearerToken>,
) -> Result<(), StatusCode> {
    info!("Got the delete callback for {}", token.0);
    state.app_store.set_app_addr(token.0, None).await;
    Ok(())
}
