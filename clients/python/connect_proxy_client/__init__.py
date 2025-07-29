import time
from collections import namedtuple
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import grpc  # type: ignore
from pyspark.sql.connect.client.core import DefaultChannelBuilder
from pyspark.sql.connect.session import SparkSession
from requests import Session


@dataclass(frozen=True)
class Application:
    id: int
    token: str

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Application":
        return cls(data["id"], data["token"])


class ConnectProxyClient:
    def __init__(self, url: str, session: Optional[Session]):
        self.url = url
        self.session = session or Session()
        self.session.headers["Content-type"] = "application/json"

    def create_application(
        self,
        version: Optional[str] = None,
        config: Optional[Dict[str, str]] = None,
    ) -> Application:
        params: Dict[str, Any] = {}
        if version is not None:
            params["version"] = version
        if config is not None:
            params["config"] = config

        response = self.session.post(f"{self.url}/apps", json=params)
        response.raise_for_status()

        data = response.json()

        return Application.from_dict(data)

    def list_applications(self) -> List[Application]:
        response = self.session.get(f"{self.url}/apps")
        response.raise_for_status()

        return [Application.from_dict(data) for data in response.json()]

    def create_session(self, app: Application) -> SparkSession:
        parsed = urlparse(self.url)

        port_opt = ""
        if parsed.port is not None:
            port_opt = f":{parsed.port}"

        base_url = f"sc://{parsed.hostname}{port_opt}"

        if parsed.scheme == "https":
            channel_builder = DefaultChannelBuilder(
                f"{base_url}/;use_ssl=true;token=${app.token}"
            )
        else:
            channel_builder = DefaultChannelBuilder(base_url)
            channel_builder.add_interceptor(TokenInterceptor(app.token))

        return SparkSession.Builder().channelBuilder(channel_builder).create()

    def stop_application(self, app: Application):
        self.session.delete(f"{self.url}/apps/{app.id}").raise_for_status()


class _ClientCallDetails(
    namedtuple(
        "_ClientCallDetails",
        (
            "method",
            "timeout",
            "metadata",
            "credentials",
            "wait_for_ready",
            "compression",
        ),
    ),
    grpc.ClientCallDetails,
):
    pass


class TokenInterceptor(
    grpc.UnaryUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor,
    grpc.StreamUnaryClientInterceptor,
    grpc.StreamStreamClientInterceptor,
):
    def __init__(self, token: str):
        self.token = token

    def _intercept(self, continuation, client_call_details, request):
        metadata = []
        if client_call_details.metadata is not None:
            metadata = list(client_call_details.metadata)
        metadata.append(
            (
                "authorization",
                f"Bearer {self.token}",
            )
        )
        client_call_details = _ClientCallDetails(
            client_call_details.method,
            client_call_details.timeout,
            metadata,
            client_call_details.credentials,
            client_call_details.wait_for_ready,
            client_call_details.compression,
        )
        return continuation(client_call_details, request)

    def intercept_unary_unary(self, continuation, client_call_details, request):
        return self._intercept(continuation, client_call_details, request)

    def intercept_unary_stream(self, continuation, client_call_details, request):
        return self._intercept(continuation, client_call_details, request)

    def intercept_stream_unary(
        self, continuation, client_call_details, request_iterator
    ):
        return self._intercept(continuation, client_call_details, request_iterator)

    def intercept_stream_stream(
        self, continuation, client_call_details, request_iterator
    ):
        return self._intercept(continuation, client_call_details, request_iterator)
