import requests
from connect_proxy_client import ConnectProxyClient

session = requests.Session()

client = ConnectProxyClient("http://localhost:8100", session)

app = client.create_application()

try:
    spark = client.create_session(app)

    spark.range(5).show()

    input()

    spark.stop()
finally:
    client.stop_application(app)
