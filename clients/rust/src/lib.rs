use std::collections::HashMap;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use spark_connect_rs::{SparkSession, SparkSessionBuilder};
use url::Url;

// Re-export reqwest ClientBuilder if users need a custom client
pub use reqwest::ClientBuilder;

pub struct SparkPlugClient {
    base_url: String,
    client: Client,
}

impl SparkPlugClient {
    pub fn new(url: impl ToString) -> Self {
        let client = Client::new();

        Self {
            base_url: url.to_string(),
            client,
        }
    }

    pub fn from_client(url: impl ToString, client: Client) -> Self {
        Self {
            base_url: url.to_string(),
            client,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    pub async fn create_application(
        &self,
        version: Option<String>,
        config: HashMap<String, String>,
    ) -> anyhow::Result<Application> {
        let app = self
            .client
            .post(self.url("/apps"))
            .json(&CreateApplication { version, config })
            .send()
            .await?
            .json::<Application>()
            .await?;

        Ok(app)
    }

    pub async fn create_session(&self, app: &Application) -> anyhow::Result<SparkSession> {
        let url = Url::parse(&self.base_url)?;

        let port_str = url.port().map(|p| format!(":{p}")).unwrap_or_default();
        let sc_url = format!("sc://{}{port_str}", url.host_str().unwrap());

        let use_ssl = if self.base_url.starts_with("https") {
            ";use_ssl=true"
        } else {
            ""
        };

        let connection = format!("{}/;token={}{use_ssl}", sc_url, app.token);

        let session = SparkSessionBuilder::remote(&connection).build().await?;

        Ok(session)
    }

    pub async fn stop_application(&self, app_id: i32) -> anyhow::Result<()> {
        self.client
            .delete(self.url(&format!("/apps/{app_id}")))
            .send()
            .await?
            .error_for_status()?;

        Ok(())
    }
}

#[derive(Serialize)]
struct CreateApplication {
    version: Option<String>,
    config: HashMap<String, String>,
}

#[derive(Serialize, Deserialize)]
pub struct Application {
    pub id: i32,
    pub token: String,
    pub active: bool,
}
