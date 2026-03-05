# Authentication

vgi-rpc-go supports optional HTTP request authentication. When configured, each HTTP request passes through an authentication callback that extracts an `AuthContext`. The auth context is then available to method handlers via `CallContext.Auth` and to dispatch hooks via `DispatchInfo.Auth`.

## Overview

1. Register an `AuthenticateFunc` callback on the `HttpServer` via `SetAuthenticate`.
2. On each HTTP request, the callback receives the `*http.Request` and returns an `*AuthContext` or an error.
3. If the callback returns an error, the request is rejected (401 or 500). If it returns an `*AuthContext`, the context flows into `CallContext` and `DispatchInfo`.
4. When no callback is registered, all requests receive `Anonymous()`.

## AuthContext

```go
type AuthContext struct {
    Domain        string         // auth scheme: "bearer", "jwt", etc.
    Authenticated bool           // true when successfully authenticated
    Principal     string         // caller identity
    Claims        map[string]any // arbitrary claims from auth token
}
```

### Anonymous()

`vgirpc.Anonymous()` returns a shared `*AuthContext` with all zero values (`Authenticated: false`). It is used for unauthenticated requests and all stdio transport requests.

### RequireAuthenticated()

```go
func (a *AuthContext) RequireAuthenticated() error
```

Returns `nil` if `Authenticated` is true, or a `*RpcError{Type: "PermissionError", Message: "Authentication required"}` otherwise. Use this in handlers for a quick auth gate.

## Setting up authentication

```go
httpServer := vgirpc.NewHttpServer(server)

httpServer.SetAuthenticate(func(r *http.Request) (*vgirpc.AuthContext, error) {
    auth := r.Header.Get("Authorization")
    if !strings.HasPrefix(auth, "Bearer ") {
        return nil, &vgirpc.RpcError{
            Type:    "ValueError",
            Message: "Missing or invalid Authorization header",
        }
    }
    token := strings.TrimPrefix(auth, "Bearer ")

    // Validate the token (e.g. JWT verification, database lookup, etc.)
    claims, err := validateToken(token)
    if err != nil {
        return nil, &vgirpc.RpcError{
            Type:    "PermissionError",
            Message: "Invalid or expired token",
        }
    }

    return &vgirpc.AuthContext{
        Domain:        "bearer",
        Authenticated: true,
        Principal:     claims.Subject,
        Claims:        map[string]any{"roles": claims.Roles},
    }, nil
})
```

## Accessing auth in handlers

The `AuthContext` is available on `CallContext.Auth`:

```go
vgirpc.Unary(server, "get_profile", func(ctx context.Context, callCtx *vgirpc.CallContext, p ProfileParams) (Profile, error) {
    // Require authentication
    if err := callCtx.Auth.RequireAuthenticated(); err != nil {
        return Profile{}, err
    }

    // Use the principal identity
    user := callCtx.Auth.Principal

    // Check claims
    if roles, ok := callCtx.Auth.Claims["roles"].([]string); ok {
        // ...
    }

    return fetchProfile(user)
})
```

## Error handling

The `AuthenticateFunc` callback controls how auth failures are reported:

| Error type | HTTP status |
|---|---|
| `*RpcError{Type: "ValueError"}` | 401 Unauthorized |
| `*RpcError{Type: "PermissionError"}` | 401 Unauthorized |
| Any other error | 500 Internal Server Error |

This matches the Python vgi-rpc implementation where `ValueError` and `PermissionError` map to 401.

For 500 errors, only "Internal server error" is sent to the client; the actual error is logged server-side via `slog.Error`.

## Streaming

Authentication is performed once per HTTP request, not per streaming tick/exchange. The `AuthContext` from the initial request is available in:

- The **init handler** `CallContext.Auth` (via `/init` endpoint)
- Each **Produce/Exchange call** `CallContext.Auth` (via `/exchange` endpoint, re-authenticated on each HTTP request)

Auth state is **not** serialized into state tokens. Each HTTP request authenticates independently, which means exchange continuations re-authenticate on every round-trip.

## Stdio transport

The stdio transport (`server.RunStdio()` / `server.Serve()`) always sets `CallContext.Auth` to `Anonymous()`. Authentication over stdio is not supported since there is no HTTP request to inspect.

Transport metadata from IPC custom metadata (`req.Metadata`) is still available on `CallContext.TransportMetadata`.

## Observability

`DispatchInfo.Auth` carries the auth context for each dispatch. Use this in a `DispatchHook` to add auth-related attributes to traces or metrics:

```go
func (h *myHook) OnDispatchStart(ctx context.Context, info vgirpc.DispatchInfo) (context.Context, vgirpc.HookToken) {
    if info.Auth.Authenticated {
        // Add principal to span, metric labels, etc.
        _ = info.Auth.Principal
    }
    // ...
}
```
