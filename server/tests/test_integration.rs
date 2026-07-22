mod test {
    use std::{collections::HashMap, io::Write, time::Duration};

    use reqwest::ClientBuilder;
    use spark_plug::config::{ProxyConfig, TlsConfig};
    use spark_plug_client::SparkPlugClient;
    use tempfile::NamedTempFile;

    struct Server {
        task: tokio::task::JoinHandle<()>,
    }

    impl Server {
        async fn start(tls: Option<TlsConfig>) -> Self {
            let task = tokio::task::spawn(async {
                let protocol = if tls.is_some() { "https" } else { "http" };

                let config = ProxyConfig {
                    callback_address: Some(format!("{protocol}://127.0.0.1:8100")),
                    session_timeout: Some(60),
                    tls,
                    ..Default::default()
                };
                spark_plug::Server::from_config(config)
                    .await
                    .unwrap()
                    .run()
                    .await
                    .unwrap()
            });

            tokio::time::sleep(Duration::from_secs(1)).await;

            Self { task }
        }
    }

    impl Drop for Server {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    #[tokio::test]
    async fn test_integration() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();

        let _server = Server::start(None).await;

        let proxy_client = SparkPlugClient::new("http://localhost:8100");

        let app = proxy_client
            .create_application(None, HashMap::new())
            .await
            .unwrap();

        let session = proxy_client.create_session(&app).await.unwrap();
        let df = session.range(None, 10, 1, None);

        assert_eq!(df.count().await.unwrap(), 10);

        proxy_client.stop_application(app.id).await.unwrap();
    }

    #[tokio::test]
    #[ignore = "spark-connect-rs tls support is broken"]
    async fn test_tls() {
        use rcgen::{CertifiedKey, generate_simple_self_signed};
        let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];

        let CertifiedKey { cert, signing_key } =
            generate_simple_self_signed(subject_alt_names).unwrap();
        println!("{}", cert.pem());
        println!("{}", signing_key.serialize_pem());

        let mut cert_file = NamedTempFile::new().unwrap();
        let mut key_file = NamedTempFile::new().unwrap();

        cert_file
            .as_file_mut()
            .write_all(cert.pem().as_bytes())
            .unwrap();

        key_file
            .as_file_mut()
            .write_all(signing_key.serialize_pem().as_bytes())
            .unwrap();

        let _server = Server::start(Some(TlsConfig {
            key: key_file.path().to_string_lossy().to_string(),
            cert: cert_file.path().to_string_lossy().to_string(),
        }));

        let client = ClientBuilder::new()
            .add_root_certificate(reqwest::Certificate::from_pem(cert.pem().as_bytes()).unwrap())
            .build()
            .unwrap();

        let proxy_client = SparkPlugClient::from_client("https://localhost:8100", client);

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
