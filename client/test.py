from connect_proxy_client import ConnectProxyClient

client = ConnectProxyClient("http://localhost:8100")

app = client.create_application()

spark = client.create_session(app)
spark.range(5).show()
spark.stop()
