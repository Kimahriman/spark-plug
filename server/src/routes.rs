use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{Path, State},
    routing::{get, post},
    Extension, Json, Router,
};
use http::StatusCode;
use log::{error, info, warn};
use migration::Expr;
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter,
};
use serde::{Deserialize, Serialize};
use tower::ServiceBuilder;
use tower_http::auth::AsyncRequireAuthorizationLayer;
use uuid::Uuid;

use crate::{
    auth::{BearerToken, TokenAuth, UserAuth, UserId},
    config::ProxyConfig,
    entities::application,
    launcher::Launcher,
};

pub async fn get_router(config: &ProxyConfig, db: DatabaseConnection) -> Router {
    let app_state = AppStateDyn {
        db,
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
    db: DatabaseConnection,
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

    let app = application::ActiveModel {
        username: ActiveValue::Set(user.0.clone()),
        token: ActiveValue::Set(token.clone()),
        ..Default::default()
    };
    let res = app.insert(&state.db).await.map_err(|e| {
        error!("Failed to insert application into db: {e:?}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

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
        id: res.id,
        token,
        active: res.address.is_some(),
    }))
}

async fn get_app(
    State(state): State<AppStateDyn>,
    Path(app_id): Path<i32>,
    Extension(user): Extension<UserId>,
) -> Result<Json<ApplicationInfo>, StatusCode> {
    let app = application::Entity::find()
        .filter(application::Column::Username.eq(user.0))
        .filter(application::Column::Id.eq(app_id))
        .one(&state.db)
        .await
        .map_err(|e| {
            error!("Failed to get application from from db: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(ApplicationInfo {
        id: app.id,
        token: app.token,
        active: app.address.is_some(),
    }))
}

async fn list_apps(
    State(state): State<AppStateDyn>,
    Extension(user): Extension<UserId>,
) -> Result<Json<Vec<application::Model>>, StatusCode> {
    let apps = application::Entity::find()
        .filter(application::Column::Username.eq(user.0))
        .all(&state.db)
        .await
        .map_err(|e| {
            error!("Failed to get applications from db: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(Json(apps))
}

async fn delete_app(
    State(state): State<AppStateDyn>,
    Path(app_id): Path<i32>,
    Extension(user): Extension<UserId>,
) -> Result<(), StatusCode> {
    let res = application::Entity::delete_many()
        .filter(application::Column::Username.eq(user.0))
        .filter(application::Column::Id.eq(app_id))
        .exec(&state.db)
        .await
        .map_err(|e| {
            error!("Failed to delete app from db: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    if res.rows_affected == 0 {
        Err(StatusCode::NOT_FOUND)
    } else {
        Ok(())
    }
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

    let res = application::Entity::update_many()
        .col_expr(application::Column::Address, Expr::value(params.address))
        .filter(application::Column::Token.eq(token.0))
        .exec(&state.db)
        .await
        .map_err(|e| {
            error!("Failed to set address from callback {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    if res.rows_affected == 0 {
        Err(StatusCode::NOT_FOUND)
    } else {
        Ok(())
    }
}

async fn app_callback_delete(
    State(state): State<AppStateDyn>,
    Extension(token): Extension<BearerToken>,
) -> Result<(), StatusCode> {
    info!("Got the delete callback for {}", token.0);

    let res = application::Entity::delete_many()
        .filter(application::Column::Token.eq(token.0))
        .exec(&state.db)
        .await
        .map_err(|e| {
            error!("Failed to delete app from db: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    if res.rows_affected == 0 {
        Err(StatusCode::NOT_FOUND)
    } else {
        Ok(())
    }
}
