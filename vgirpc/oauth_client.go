// © Copyright 2025-2026, Query.Farm LLC - https://query.farm
// SPDX-License-Identifier: Apache-2.0

package vgirpc

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// FetchOAuthResourceMetadata fetches OAuth Protected Resource Metadata from the
// well-known URL derived from baseURL per RFC 9728. An optional http.Client may
// be provided; if nil, http.DefaultClient is used.
func FetchOAuthResourceMetadata(baseURL string, client ...*http.Client) (*OAuthResourceMetadata, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("parsing base URL: %w", err)
	}
	path := strings.TrimSuffix(u.Path, "/")
	u.Path = wellKnownURL(path)
	return FetchOAuthResourceMetadataFromURL(u.String(), client...)
}

// FetchOAuthResourceMetadataFromURL fetches OAuth Protected Resource Metadata
// from an explicit URL. An optional http.Client may be provided; if nil,
// http.DefaultClient is used.
func FetchOAuthResourceMetadataFromURL(metadataURL string, client ...*http.Client) (*OAuthResourceMetadata, error) {
	c := http.DefaultClient
	if len(client) > 0 && client[0] != nil {
		c = client[0]
	}

	resp, err := c.Get(metadataURL)
	if err != nil {
		return nil, fmt.Errorf("fetching metadata: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metadata endpoint returned status %d", resp.StatusCode)
	}

	const maxMetadataSize = 1 << 20 // 1 MB
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxMetadataSize))
	if err != nil {
		return nil, fmt.Errorf("reading metadata body: %w", err)
	}

	var meta OAuthResourceMetadata
	if err := json.Unmarshal(body, &meta); err != nil {
		return nil, fmt.Errorf("parsing metadata JSON: %w", err)
	}
	return &meta, nil
}

// ParseResourceMetadataURL extracts the resource_metadata URL from a
// WWW-Authenticate header value. Returns an empty string if not found.
func ParseResourceMetadataURL(wwwAuthenticate string) string {
	return parseQuotedParam(wwwAuthenticate, "resource_metadata")
}

// ParseClientID extracts the client_id from a WWW-Authenticate header value.
// Returns an empty string if not found.
func ParseClientID(wwwAuthenticate string) string {
	return parseQuotedParam(wwwAuthenticate, "client_id")
}

// ParseUseIDTokenAsBearer extracts the use_id_token_as_bearer flag from a
// WWW-Authenticate header value. Returns true when the value is "true".
func ParseUseIDTokenAsBearer(wwwAuthenticate string) bool {
	return parseQuotedParam(wwwAuthenticate, "use_id_token_as_bearer") == "true"
}

// ParseClientSecret extracts the client_secret from a WWW-Authenticate header
// value. Returns an empty string if not found.
func ParseClientSecret(wwwAuthenticate string) string {
	return parseQuotedParam(wwwAuthenticate, "client_secret")
}

// ParseDeviceCodeClientID extracts the device_code_client_id from a
// WWW-Authenticate header value. Returns an empty string if not found.
func ParseDeviceCodeClientID(wwwAuthenticate string) string {
	return parseQuotedParam(wwwAuthenticate, "device_code_client_id")
}

// ParseDeviceCodeClientSecret extracts the device_code_client_secret from a
// WWW-Authenticate header value. Returns an empty string if not found.
func ParseDeviceCodeClientSecret(wwwAuthenticate string) string {
	return parseQuotedParam(wwwAuthenticate, "device_code_client_secret")
}

// parseQuotedParam extracts a quoted parameter value (key="value") from a
// WWW-Authenticate header value. Returns an empty string if not found.
func parseQuotedParam(header, param string) string {
	key := param + `="`
	idx := strings.Index(header, key)
	if idx == -1 {
		return ""
	}
	rest := header[idx+len(key):]
	end := strings.Index(rest, `"`)
	if end == -1 {
		return ""
	}
	return rest[:end]
}
