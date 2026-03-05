// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"net/http"
	"strings"
)

// BearerAuthenticate returns an [AuthenticateFunc] that extracts a Bearer token
// from the Authorization header and calls validate with the raw token string.
// The validate function should return an [AuthContext] on success or an error on
// failure.
//
// If the Authorization header is missing or does not use the Bearer scheme, the
// request is rejected with a ValueError [RpcError].
func BearerAuthenticate(validate func(token string) (*AuthContext, error)) AuthenticateFunc {
	return func(r *http.Request) (*AuthContext, error) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			return nil, &RpcError{
				Type:    "ValueError",
				Message: "Missing Authorization header",
			}
		}
		if !strings.HasPrefix(authHeader, "Bearer ") {
			return nil, &RpcError{
				Type:    "ValueError",
				Message: "Authorization header must use Bearer scheme",
			}
		}
		token := strings.TrimPrefix(authHeader, "Bearer ")
		return validate(token)
	}
}

// BearerAuthenticateStatic returns an [AuthenticateFunc] that validates Bearer
// tokens against a fixed map of token → [AuthContext]. Unknown tokens are
// rejected with a ValueError [RpcError].
func BearerAuthenticateStatic(tokens map[string]*AuthContext) AuthenticateFunc {
	return BearerAuthenticate(func(token string) (*AuthContext, error) {
		if ac, ok := tokens[token]; ok {
			return ac, nil
		}
		return nil, &RpcError{
			Type:    "ValueError",
			Message: "Unknown bearer token",
		}
	})
}

// ChainAuthenticate returns an [AuthenticateFunc] that tries each authenticator
// in order. A ValueError [RpcError] from one authenticator causes the chain to
// fall through to the next. A PermissionError [RpcError] or any non-RpcError
// propagates immediately. If no authenticator accepts the request, the chain
// returns a ValueError [RpcError].
//
// ChainAuthenticate panics if no authenticators are provided.
func ChainAuthenticate(authenticators ...AuthenticateFunc) AuthenticateFunc {
	if len(authenticators) == 0 {
		panic("vgirpc: ChainAuthenticate requires at least one authenticator")
	}
	return func(r *http.Request) (*AuthContext, error) {
		for _, auth := range authenticators {
			ac, err := auth(r)
			if err == nil {
				return ac, nil
			}
			// If it's a ValueError RpcError, try the next authenticator.
			if rpcErr, ok := err.(*RpcError); ok && rpcErr.Type == "ValueError" {
				continue
			}
			// PermissionError or non-RpcError: propagate immediately.
			return nil, err
		}
		return nil, &RpcError{
			Type:    "ValueError",
			Message: "No authenticator accepted the request",
		}
	}
}
