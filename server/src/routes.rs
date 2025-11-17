use std::{collections::HashMap, sync::Arc};

use axum::{
    Extension, Json, Router,
    extract::{Path, State},
    routing::{get, post},
};
use http::StatusCode;
use log::error;
use migration::Expr;
use reqwest::ClientBuilder;
use sea_orm::{
    ActiveEnum, ActiveModelTrait, ActiveValue, ColumnTrait, DatabaseConnection, EntityTrait,
    QueryFilter,
};
use serde::{Deserialize, Serialize};
use tower::ServiceBuilder;
use tower_http::auth::AsyncRequireAuthorizationLayer;
use uuid::Uuid;

use crate::{
    auth::{BearerToken, TokenAuth, UserAuth, UserId},
    entities::application,
    launcher::Launcher,
};

pub(crate) async fn get_router<L>(
    user_auth: UserAuth,
    launcher: L,
    db: DatabaseConnection,
) -> Router
where
    L: Launcher + 'static,
{
    let app_state = AppStateDyn {
        db,
        launcher: Arc::new(launcher),
    };

    let user_api = Router::new()
        .route("/apps", get(list_apps).post(create_app))
        .route("/apps/{app_id}", get(get_app).delete(delete_app))
        .route("/versions", get(list_versions))
        .route_layer(ServiceBuilder::new().layer(AsyncRequireAuthorizationLayer::new(user_auth)))
        .with_state(app_state.clone());

    let callback_api = Router::new()
        .route("/callback", post(app_callback).delete(app_callback_delete))
        .route_layer(ServiceBuilder::new().layer(AsyncRequireAuthorizationLayer::new(TokenAuth {})))
        .with_state(app_state);

    Router::new().merge(user_api).merge(callback_api)
}

#[derive(Clone)]
struct AppStateDyn<L: Launcher + 'static> {
    db: DatabaseConnection,
    launcher: Arc<L>,
}

#[derive(Default, Serialize, Deserialize)]
struct CreateApplicationRequest {
    version: Option<String>,
    config: Option<HashMap<String, String>>,
    python_packages: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct ApplicationInfo {
    id: i32,
    token: String,
    state: String,
    active: bool,
}

impl From<application::Model> for ApplicationInfo {
    fn from(value: application::Model) -> Self {
        Self {
            id: value.id,
            token: value.token,
            state: value.state.to_value().to_string(),
            active: value.address.is_some(),
        }
    }
}

async fn create_app<L: Launcher>(
    State(state): State<AppStateDyn<L>>,
    Extension(user): Extension<UserId>,
    Json(params): Json<CreateApplicationRequest>,
) -> Result<Json<ApplicationInfo>, StatusCode> {
    let token = Uuid::new_v4().to_string();

    let app = application::ActiveModel {
        // created_at: ActiveValue::Set(Utc::now()),
        username: ActiveValue::Set(user.0.clone()),
        state: ActiveValue::Set(application::State::LAUNCHING),
        token: ActiveValue::Set(token.clone()),
        ..Default::default()
    };
    let res = app.insert(&state.db).await.map_err(|e| {
        error!("Failed to insert application into db: {e:?}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let launch = state
        .launcher
        .launch(
            params.version.as_ref().map(|s| s.as_ref()),
            res.id,
            user.0,
            token,
            params.config.unwrap_or_default(),
            params.python_packages,
        )
        .await;

    match launch {
        Ok(child) => {
            let db = state.db.clone();
            tokio::task::spawn(async move {
                let _ = child.await;

                // Update the application in the database if it is still in the launching state
                let update_result = application::Entity::update_many()
                    .col_expr(
                        application::Column::State,
                        Expr::value(application::State::FAILED),
                    )
                    .filter(application::Column::Id.eq(res.id))
                    .filter(application::Column::State.eq(application::State::LAUNCHING))
                    .exec(&db)
                    .await;

                if let Err(update_err) = update_result {
                    error!("Failed to set application state to failed: {update_err:?}");
                }
            });
        }
        Err(e) => {
            error!("Failed to launch application: {e:?}");

            let update_res = application::ActiveModel {
                id: ActiveValue::Set(res.id),
                state: ActiveValue::Set(application::State::FAILED),
                ..Default::default()
            }
            .update(&state.db)
            .await;

            if let Err(update_err) = update_res {
                error!("Failed to set application state to failed: {update_err:?}");
            }

            return Err(StatusCode::INTERNAL_SERVER_ERROR);
        }
    }

    Ok(Json(res.into()))
}

async fn get_app<L: Launcher>(
    State(state): State<AppStateDyn<L>>,
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

    Ok(Json(app.into()))
}

async fn list_apps<L: Launcher>(
    State(state): State<AppStateDyn<L>>,
    Extension(user): Extension<UserId>,
) -> Result<Json<Vec<ApplicationInfo>>, StatusCode> {
    let apps = application::Entity::find()
        .filter(application::Column::Username.eq(user.0))
        .all(&state.db)
        .await
        .map_err(|e| {
            error!("Failed to get applications from db: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(Json(apps.into_iter().map(Into::into).collect()))
}

async fn delete_app<L: Launcher>(
    State(state): State<AppStateDyn<L>>,
    Path(app_id): Path<i32>,
    Extension(user): Extension<UserId>,
) -> Result<(), StatusCode> {
    let model = application::ActiveModel {
        id: ActiveValue::Set(app_id),
        username: ActiveValue::Set(user.0),
        ..Default::default()
    };

    let res = application::Entity::delete(model)
        .exec_with_returning(&state.db)
        .await
        .map_err(|e| {
            error!("Failed to delete app from db: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    if let Some(app) = res {
        if let Some(address) = app.address {
            send_session_message(&address, &app.token, "stop")
                .await
                .map_err(|e| {
                    error!("Failed to stop session: {e:?}");
                    StatusCode::INTERNAL_SERVER_ERROR
                })?;
        }
        Ok(())
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}

async fn list_versions<L: Launcher>(State(state): State<AppStateDyn<L>>) -> Json<Vec<String>> {
    Json(state.launcher.get_versions())
}

#[derive(Serialize, Deserialize)]
struct ApplicationCallbackRequest {
    address: String,
}

async fn app_callback<L: Launcher>(
    State(state): State<AppStateDyn<L>>,
    Extension(token): Extension<BearerToken>,
    Json(params): Json<ApplicationCallbackRequest>,
) -> Result<(), StatusCode> {
    let res = application::Entity::update_many()
        .col_expr(application::Column::Address, Expr::value(params.address))
        .col_expr(
            application::Column::State,
            Expr::value(application::State::RUNNING),
        )
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

async fn app_callback_delete<L: Launcher>(
    State(state): State<AppStateDyn<L>>,
    Extension(token): Extension<BearerToken>,
) -> Result<(), StatusCode> {
    let res = application::Entity::update_many()
        .col_expr(
            application::Column::Address,
            Expr::value::<Option<String>>(None),
        )
        .col_expr(
            application::Column::State,
            Expr::value(application::State::FINISHED),
        )
        .filter(application::Column::Token.eq(token.0))
        .exec(&state.db)
        .await
        .map_err(|e| {
            error!("Failed to mark app as finished: {e:?}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    if res.rows_affected == 0 {
        Err(StatusCode::NOT_FOUND)
    } else {
        Ok(())
    }
}

async fn send_session_message(address: &str, token: &str, message: &str) -> anyhow::Result<()> {
    // Fake out a gRPC call that will get picked up by the server interceptor
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

#[cfg(test)]
mod test {
    use std::{sync::Arc, time::Duration};

    use axum_test::TestServer;
    use http::StatusCode;
    use migration::{Migrator, MigratorTrait};
    use sea_orm::Database;
    use tokio::task::JoinHandle;

    use crate::{
        auth::{CurrentUserAuth, RemoteUserAuth, UserAuth},
        launcher::Launcher,
        routes::{
            ApplicationCallbackRequest, ApplicationInfo, CreateApplicationRequest, get_router,
        },
    };

    #[derive(Clone)]
    struct MockLauncher {}

    #[async_trait::async_trait]
    impl Launcher for MockLauncher {
        fn get_versions(&self) -> Vec<String> {
            vec!["4.0.0".to_string()]
        }

        async fn launch(
            &self,
            _version_name: Option<&str>,
            _session_id: i32,
            _username: String,
            _token: String,
            _user_config: std::collections::HashMap<String, String>,
            _python_packages: Option<Vec<String>>,
        ) -> Result<JoinHandle<()>, std::io::Error> {
            Ok(tokio::spawn(async {
                tokio::time::sleep(Duration::from_secs(60)).await;
            }))
        }
    }

    async fn create_test_server() -> TestServer {
        let _ = env_logger::Builder::new()
            .filter(Some("spark_connect_proxy"), log::LevelFilter::Debug)
            .is_test(true)
            .try_init();

        let db = Database::connect("sqlite::memory:").await.unwrap();
        Migrator::up(&db, None).await.unwrap();
        let router = get_router(
            UserAuth {
                auth_methods: vec![
                    Arc::new(RemoteUserAuth {
                        header: "REMOTE_USER".to_string(),
                    }),
                    Arc::new(CurrentUserAuth {}),
                ],
            },
            MockLauncher {},
            db,
        )
        .await;

        TestServer::new(router).unwrap()
    }

    #[tokio::test]
    async fn test_routes() {
        let server = create_test_server().await;

        server
            .get("/apps")
            .await
            .assert_json::<Vec<ApplicationInfo>>(&vec![]);

        let res = server
            .post("/apps")
            .json(&CreateApplicationRequest::default())
            .await;

        res.assert_status(StatusCode::OK);
        let app = res.json::<ApplicationInfo>();

        server
            .get(&format!("/apps/{}", app.id))
            .await
            .assert_json(&app);

        server
            .post("/callback")
            .authorization_bearer(app.token)
            .json(&ApplicationCallbackRequest {
                address: "localhost:12345".to_string(),
            })
            .await
            .assert_status(StatusCode::OK);

        let res = server.get(&format!("/apps/{}", app.id)).await;

        res.assert_status(StatusCode::OK);
        let app = res.json::<ApplicationInfo>();
        assert!(app.active);
        assert_eq!(app.state, "RUNNING");

        server
            .delete("/callback")
            .authorization_bearer(app.token)
            .await
            .assert_status(StatusCode::OK);

        let res = server.get(&format!("/apps/{}", app.id)).await;

        res.assert_status(StatusCode::OK);
        let app = res.json::<ApplicationInfo>();
        assert!(!app.active);
        assert_eq!(app.state, "FINISHED");
    }

    #[tokio::test]
    async fn test_users() {
        let server = create_test_server().await;

        let res = server
            .post("/apps")
            .add_header("REMOTE_USER", "user1")
            .json(&CreateApplicationRequest::default())
            .await;

        res.assert_status(StatusCode::OK);
        let app = res.json::<ApplicationInfo>();

        server
            .get("/apps")
            .add_header("REMOTE_USER", "user2")
            .await
            .assert_json::<Vec<ApplicationInfo>>(&vec![]);

        server
            .get("/apps")
            .add_header("REMOTE_USER", "user1")
            .await
            .assert_json::<Vec<ApplicationInfo>>(&vec![app]);
    }
}
