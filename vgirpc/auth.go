// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import "net/http"

// AuthContext carries authentication information for the current request.
// It is available to method handlers via [CallContext.Auth] and to dispatch
// hooks via [DispatchInfo.Auth]. It is never nil — unauthenticated requests
// receive [Anonymous].
type AuthContext struct {
	// Domain is the authentication scheme (e.g. "bearer", "jwt").
	// Empty when unauthenticated.
	Domain string
	// Authenticated is true when the request was successfully authenticated.
	Authenticated bool
	// Principal is the caller identity (username, service account, token subject).
	Principal string
	// Claims holds arbitrary claims extracted from the auth token.
	Claims map[string]any
}

// anonymous is the package-level singleton returned by Anonymous().
var anonymous = &AuthContext{}

// Anonymous returns a shared AuthContext representing an unauthenticated caller.
func Anonymous() *AuthContext {
	return anonymous
}

// RequireAuthenticated returns nil if the caller is authenticated, or a
// PermissionError [RpcError] otherwise.
func (a *AuthContext) RequireAuthenticated() error {
	if a.Authenticated {
		return nil
	}
	return &RpcError{Type: "PermissionError", Message: "Authentication required"}
}

// AuthenticateFunc extracts an [AuthContext] from an HTTP request.
// Return a non-nil error to reject the request:
//   - *[RpcError] with Type "ValueError" or "PermissionError" → HTTP 401
//   - any other error → HTTP 500
type AuthenticateFunc func(r *http.Request) (*AuthContext, error)
