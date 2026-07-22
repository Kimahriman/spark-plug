import requests
from spark_plug_client import SparkPlugClient

session = requests.Session()

client = SparkPlugClient("http://localhost:8100", session)

app = client.create_application()

try:
    spark = client.create_session(app)

    spark.range(5).show()

    input()

    spark.stop()
finally:
    client.stop_application(app)
