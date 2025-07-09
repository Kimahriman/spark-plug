use async_trait::async_trait;
use deadpool_diesel::sqlite;
use diesel::prelude::*;

use crate::{
    models::{Application, NewApplication},
    schema::applications,
};

#[async_trait]
pub trait ApplicationStore: Send + Sync {
    async fn create_app(&self, username: String, token: String) -> Application;

    async fn get_app(&self, username: String, id: i32) -> Option<Application>;

    async fn get_app_by_token(&self, token: String) -> Option<Application>;

    async fn set_app_addr(&self, token: String, addr: Option<String>);

    async fn list_apps(&self, username: String) -> Vec<Application>;

    async fn delete_app(&self, username: String, id: i32);
}

// #[derive(Default)]
// pub struct InMemoryApplicationStore {
//     apps: Arc<Mutex<HashMap<String, HashMap<i32, Application>>>>,
//     next_app_id: AtomicI32,
// }

// #[async_trait]
// impl ApplicationStore for InMemoryApplicationStore {
//     fn create_app(&self, username: &str, token: String) -> Application {
//         let id = self
//             .next_app_id
//             .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

//         let app = Application {
//             id,
//             user: username.to_string(),
//             addr: None,
//             token,
//         };
//         self.apps
//             .lock()
//             .unwrap()
//             .entry(username.to_string())
//             .or_default()
//             .insert(id, app.clone());

//         app
//     }

//     fn get_app(&self, username: &str, id: i32) -> Option<Application> {
//         self.apps
//             .lock()
//             .unwrap()
//             .get(username)
//             .and_then(|apps| apps.get(&id))
//             .cloned()
//     }

//     fn get_app_by_token(&self, token: &str) -> Option<Application> {
//         self.apps
//             .lock()
//             .unwrap()
//             .values()
//             .flat_map(|apps| apps.values())
//             .find(|app| app.token == token)
//             .cloned()
//     }

//     fn set_app_addr(&self, token: &str, addr: Option<String>) {
//         if let Some(app) = self
//             .apps
//             .lock()
//             .unwrap()
//             .values_mut()
//             .flat_map(|apps| apps.values_mut())
//             .find(|app| app.token == token)
//         {
//             app.addr = addr
//         }
//     }

//     fn list_apps(&self, username: &str) -> Vec<Application> {
//         self.apps
//             .lock()
//             .unwrap()
//             .get(username)
//             .map(|apps| apps.values().cloned().collect())
//             .unwrap_or_default()
//     }

//     fn delete_app(&self, username: &str, id: i32) {
//         if let Some(apps) = self.apps.lock().unwrap().get_mut(username) {
//             apps.remove(&id);
//         }
//     }
// }

pub struct DieselSqliteStore {
    pool: sqlite::Pool,
}

impl DieselSqliteStore {
    pub fn new(pool: sqlite::Pool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ApplicationStore for DieselSqliteStore {
    async fn create_app(&self, username: String, token: String) -> Application {
        let app = NewApplication {
            user: username,
            token,
        };

        self.pool
            .get()
            .await
            .unwrap()
            .interact(move |conn| {
                diesel::insert_into(applications::table)
                    .values(&app)
                    .returning(Application::as_returning())
                    .get_result(&mut (*conn))
                    .expect("Error creating new application")
            })
            .await
            .unwrap()
    }

    async fn get_app(&self, username: String, app_id: i32) -> Option<Application> {
        use crate::schema::applications::dsl::*;

        self.pool
            .get()
            .await
            .unwrap()
            .interact(move |conn| {
                applications
                    .filter(user.eq(username))
                    .filter(id.eq(app_id))
                    .limit(1)
                    .select(Application::as_select())
                    .first(&mut (*conn))
                    .optional()
                    .expect("Failed to get application")
            })
            .await
            .unwrap()
    }

    async fn get_app_by_token(&self, app_token: String) -> Option<Application> {
        use crate::schema::applications::dsl::*;

        self.pool
            .get()
            .await
            .unwrap()
            .interact(move |conn| {
                applications
                    .filter(token.eq(app_token))
                    .limit(1)
                    .select(Application::as_select())
                    .first(&mut (*conn))
                    .optional()
                    .expect("Failed to get application by token")
            })
            .await
            .unwrap()
    }

    async fn set_app_addr(&self, app_token: String, app_addr: Option<String>) {
        use crate::schema::applications::dsl::*;

        self.pool
            .get()
            .await
            .unwrap()
            .interact(move |conn| {
                diesel::update(applications.filter(token.eq(app_token)))
                    .set(addr.eq(app_addr))
                    .execute(&mut (*conn))
                    .expect("Error setting application address");
            })
            .await
            .unwrap()
    }

    async fn list_apps(&self, username: String) -> Vec<Application> {
        use crate::schema::applications::dsl::*;

        self.pool
            .get()
            .await
            .unwrap()
            .interact(move |conn| {
                applications
                    .filter(user.eq(username))
                    .select(Application::as_select())
                    .get_results(&mut (*conn))
                    .expect("Failed to list applications")
            })
            .await
            .unwrap()
    }

    async fn delete_app(&self, username: String, app_id: i32) {
        use crate::schema::applications::dsl::*;

        self.pool
            .get()
            .await
            .unwrap()
            .interact(move |conn| {
                diesel::delete(applications.filter(user.eq(username)).filter(id.eq(app_id)))
                    .execute(&mut (*conn))
                    .expect("Failed to delete application");
            })
            .await
            .unwrap()
    }
}
