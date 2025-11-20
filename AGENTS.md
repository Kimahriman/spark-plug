# Spark Connect Proxy - Architecture Guide

Authenticated HTTP/2 proxy for Apache Spark Connect. Provides session management, authentication, and gRPC proxying for multi-tenant Spark environments.

## Quick Overview

- **Server** (Rust/Tokio): HTTP/2 proxy, REST API, session store
- **Plugin** (Scala): Spark driver integration, callbacks, timeout management
- **Client** (Python): User API for session creation and management

## Problem Statement

1. Spark Connect has no built-in authentication → Use tokens + Bearer auth
2. No discovery mechanism in flat networks (YARN) → Central proxy stores addresses
3. Multi-tenant isolation → Per-user sessions with automatic cleanup

## Core Architecture

### Server (`/server`) - Rust

**Key Files:**
- `main.rs`: Entry point, CLI args, server startup
- `lib.rs`: ProxyService (dual-mode HTTP/2), graceful shutdown
- `routes.rs`: REST API endpoints + auth middleware
- `auth.rs`: Authentication methods (RemoteUser, JWT, JWKS, CurrentUser)
- `launcher.rs`: spark-submit orchestration with plugin injection
- `config.rs`: YAML config parsing + Spark version definitions
- `entities/application.rs`: SeaORM database model (id, username, token, address, state)

**Key APIs:**
- `POST /apps` → Create session (returns {id, token})
- `GET /apps` → List user's sessions
- `DELETE /apps/{id}` → Kill session
- `GET /versions` → List available Spark versions
- `POST /callback` → Driver registration (token-authenticated)
- `DELETE /callback` → Driver shutdown notification

**Request Routing:**
```
/spark.connect.SparkConnectService/* → ProxyService.dispatch() → upstream gRPC
/* → Axum router → REST API
```

### Plugin (`/plugin`) - Scala

**Key Files:**
- `SparkConnectProxyListener.scala`: SparkListener → POST /callback on startup, DELETE on shutdown
- `SparkConnectProxyInterceptor.scala`: gRPC interceptor for activity tracking
- `SparkConnectProxyServer.scala`: Registers plugin with Spark lifecycle
- `Config.scala`: Constants for callback URL, timeout, token

**Injected Spark Config:**
```properties
spark.connect.authenticate.token={token}
spark.connect.proxy.callback={callback_url}
spark.connect.proxy.idle.timeout={timeout_seconds}
spark.extraListeners=org.apache.spark.sql.connect.proxy.SparkConnectProxyListener
spark.connect.grpc.interceptor.classes=org.apache.spark.sql.connect.proxy.SparkConnectProxyInterceptor
```

### Client (`/clients/python`) - Python

**ConnectProxyClient:**
- `create_application(version, config, python_packages)` → POST /apps
- `list_applications()` → GET /apps
- `create_session(app)` → SparkSession with TokenInterceptor
- `stop_application(app)` → DELETE /apps/{id}

**TokenInterceptor:** Injects Bearer token in gRPC metadata (works around PySpark limitations)

## Request Flows

**Session Creation:**
```
User: POST /apps
  → Server: Validates auth, creates app (LAUNCHING)
  → Launcher: spark-submit with plugin
  → User: Returns {token, id}
  
Driver: Starts, Connect service starts
  → Plugin: Sends POST /callback with address
  → Server: Updates app (RUNNING, address=host:port)
```

**gRPC Proxying:**
```
User: gRPC /spark.connect.SparkConnectService/*
  → Server: Extract token from Authorization header
  → Lookup: Get upstream address from database
  → Forward: HTTP/2 proxy to upstream
  → Return: Response back to user
```

**Graceful Shutdown:**
```
Signal (SIGTERM/SIGINT)
  → signal_tx.closed()
  → All connection tasks detect closure
  → graceful_shutdown() on in-flight requests
  → New requests rejected
  → close_tx.closed() waits for all tasks
  → Exit cleanly
```

## Configuration

**conf/config.yaml:**
```yaml
bind_host: 0.0.0.0
bind_port: 8100
callback_address: http://proxy.internal:8100
store: sqlite:///var/lib/proxy.db  # or postgres://...

tls:
  cert: /path/to/cert.pem
  key: /path/to/key.pem

auth_methods:
  - name: remote_user
    options:
      header: X-Remote-User
  - name: jwks
    options:
      oidc_url: https://oidc.example.com/.well-known/openid-configuration
      audience: connect-proxy

spark_versions:
  - name: "4.0.0"
    home: /opt/spark
    master: yarn
    deploy_mode: cluster
    proxy_user: true
    env:
      JAVA_HOME: /usr/lib/jvm/java-17-openjdk
    default_configs:
      spark.yarn.stagingDir: /staging
    override_configs:
      spark.dynamicAllocation.enabled: "false"

kerberos_config:
  keytab: /etc/proxy/proxy.keytab
  principal: connect-proxy@EXAMPLE.COM
  renewal_interval: 3600
```

**Env Override:** `SPARK_CONNECT_PROXY_BIND_PORT=9000`

## Authentication Flow

**Methods applied in order; first success wins:**

1. **RemoteUserAuth**: Check header for username → Pass through
2. **JWTAuth**: Verify Bearer token signature (RSA/EC local key) → Extract subject
3. **JWKSAuth**: Verify Bearer token via remote JWKS/OIDC → Validate audience → Extract subject
4. **CurrentUserAuth**: Use system `whoami` (dev only)

**Token Auth (Callbacks):**
- Callbacks must include `Authorization: Bearer {token}` header
- Token extracted from `spark.connect.authenticate.token` config

## Database Schema

**application table:**
- id (int, PK)
- username (string)
- token (string, unique)
- address (string, nullable)
- state (enum: LAUNCHING, RUNNING, FAILED, COMPLETED)
- created_at, updated_at (timestamps)

## Build & Deploy

**Build:**
```bash
sbt package                                    # Plugin JAR
cargo build                                    # Server binary
cargo build --release --features embed-plugin  # With plugin embedded
```

**Run:**
```bash
cargo run -- --config-file conf/config.yaml
```

**Docker:**
```bash
docker build -t spark-connect-proxy .          # With plugin
docker build -t spark-connect-proxy --target base .  # Without
```

## Extending

**Add Auth Method:**
- Implement `UserAuthMethod` trait in `auth.rs`
- Add case in `UserAuth::from_config()`

**Add API Endpoint:**
- Handler function in `routes.rs`
- Add route to router with auth layer

**Add Spark Config:**
- Add field to `SparkVersion` in `config.rs`
- Inject in `launcher.rs` spark-submit builder

## Key Limitations

- In-memory session store (not HA without external DB)
- HTTP/2 only (gRPC requires HTTP/2)
- Plugin requires Spark modifications (Spark 4.0+ has native token auth)
- No session dashboard UI

## Security Notes

- **Tokens**: UUID v4 (cryptographically random), stored plaintext in DB → Secure DB access required
- **TLS**: Optional but recommended (rustls with modern ciphers)
- **Auth Bypass**: All requests must pass auth OR have valid token
- **Callback Trust**: Must include Bearer token, expected to come from spark-submit process
- **Plugin Privs**: Runs in Spark driver JVM with full privileges

## Testing

```bash
cargo test --lib # Unit tests
uv run cargo test --test test_integration # Integration test with default PySpark in venv
```

## Troubleshooting

| Issue | Cause | Fix |
|-------|-------|-----|
| 401 /apps | Auth failed | Check auth config, verify headers |
| gRPC timeout | App not launching | Check Spark home, launcher logs |
| Plugin not found | Missing JAR | Use `--features embed-plugin` |
| DB locked | SQLite concurrency | Switch to PostgreSQL |
| Session timeout | Inactivity | Increase `session_timeout` config |

## Key Dependencies

**Rust:** Tokio, Hyper, Axum, SeaORM, Rustls, Figment, jsonwebtoken, jwks
**Scala:** Spark 4.0.0, Java HTTP Client
**Python:** PySpark, gRPC, requests

---

**Last Updated**: November 2025
