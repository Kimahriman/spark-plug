use std::{
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc, Mutex},
};

use serde::Serialize;

#[derive(Clone, Serialize)]
pub struct Application {
    pub id: u64,
    pub addr: Option<String>,
    pub token: String,
}

// #[async_trait]
pub trait ApplicationStore: Send + Sync {
    fn create_app(&self, username: &str, token: String) -> Application;

    fn get_app(&self, username: &str, id: u64) -> Option<Application>;

    fn get_app_by_token(&self, token: &str) -> Option<Application>;

    fn set_app_addr(&self, token: &str, addr: Option<String>);

    fn list_apps(&self, username: &str) -> Vec<Application>;

    fn delete_app(&self, username: &str, id: u64);
}

#[derive(Default)]
pub struct InMemoryApplicationStore {
    apps: Arc<Mutex<HashMap<String, HashMap<u64, Application>>>>,
    next_app_id: AtomicU64,
}

// #[async_trait]
impl ApplicationStore for InMemoryApplicationStore {
    fn create_app(&self, username: &str, token: String) -> Application {
        let id = self
            .next_app_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let app = Application {
            id,
            addr: None,
            token,
        };
        self.apps
            .lock()
            .unwrap()
            .entry(username.to_string())
            .or_default()
            .insert(id, app.clone());

        app
    }

    fn get_app(&self, username: &str, id: u64) -> Option<Application> {
        self.apps
            .lock()
            .unwrap()
            .get(username)
            .and_then(|apps| apps.get(&id))
            .cloned()
    }

    fn get_app_by_token(&self, token: &str) -> Option<Application> {
        self.apps
            .lock()
            .unwrap()
            .values()
            .flat_map(|apps| apps.values())
            .find(|app| app.token == token)
            .cloned()
    }

    fn set_app_addr(&self, token: &str, addr: Option<String>) {
        if let Some(app) = self
            .apps
            .lock()
            .unwrap()
            .values_mut()
            .flat_map(|apps| apps.values_mut())
            .find(|app| app.token == token)
        {
            app.addr = addr
        }
    }

    fn list_apps(&self, username: &str) -> Vec<Application> {
        self.apps
            .lock()
            .unwrap()
            .get(username)
            .map(|apps| apps.values().cloned().collect())
            .unwrap_or_default()
    }

    fn delete_app(&self, username: &str, id: u64) {
        if let Some(apps) = self.apps.lock().unwrap().get_mut(username) {
            apps.remove(&id);
        }
    }
}
