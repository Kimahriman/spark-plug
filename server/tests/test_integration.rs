mod test {
    use std::collections::HashMap;

    use http::{StatusCode, header};
    use log::{error, info};
    use reqwest::{Client, Response};
    use spark_connect_proxy::{
        config::ProxyConfig, entities::application, routes::ApplicationInfo,
    };
    use spark_connect_proxy_client::ConnectProxyClient;

    struct Server {
        task: tokio::task::JoinHandle<()>,
        client: Client,
        base_url: String,
    }

    impl Server {
        fn start() -> Self {
            let task = tokio::task::spawn(async {
                let config = ProxyConfig {
                    callback_address: Some("http://127.0.0.1:8100".to_string()),
                    ..Default::default()
                };
                spark_connect_proxy::run(config).await.unwrap()
            });

            let mut headers = header::HeaderMap::new();
            headers.insert(
                header::CONTENT_TYPE,
                header::HeaderValue::from_static("application/json"),
            );

            let client = reqwest::ClientBuilder::new()
                .default_headers(headers)
                .build()
                .unwrap();
            Self {
                task,
                client,
                base_url: "http://localhost:8100".to_string(),
            }
        }

        async fn get_apps(&self) -> Vec<application::Model> {
            let res = self
                .client
                .get(format!("{}/apps", self.base_url))
                .send()
                .await
                .unwrap();
            res.json::<Vec<application::Model>>().await.unwrap()
        }

        async fn launch(&self) -> ApplicationInfo {
            self.client
                .post(format!("{}/apps", self.base_url))
                .json(&HashMap::<String, String>::new())
                .send()
                .await
                .inspect_err(|e| error!("{e:?}"))
                .unwrap()
                .json::<ApplicationInfo>()
                .await
                .unwrap()
        }
    }

    impl Drop for Server {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    #[tokio::test]
    async fn test_integration() {
        let server = Server::start();

        // assert_eq!(server.get_apps().await.len(), 0);

        // let res = server.launch().await;

        let proxy_client = ConnectProxyClient::new("http://localhost:8100");

        let app = proxy_client
            .create_application(None, HashMap::new())
            .await
            .unwrap();

        let session = proxy_client.create_session(&app).await.unwrap();
        let df = session.range(None, 10, 1, None);

        assert_eq!(df.count().await.unwrap(), 10);

        proxy_client.stop_application(app.id).await.unwrap();
    }
}
