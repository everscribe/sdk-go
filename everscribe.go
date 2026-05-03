// Package everscribe is the top-level entry point for the Everscribe
// Go SDK. It binds a project's credentials once and hands out
// per-surface clients (recorder for ingest, minter for read-side
// embed-token minting) that share the same auth.
//
// Typical usage:
//
//	import "github.com/everscribe/sdk-go"
//
//	es, err := everscribe.New(projectID, apiKey)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	rec := es.NewRecorder()
//	defer rec.Close()
//
//	m := es.NewMinter()
//	token, err := m.MintToken(ctx, minter.TokenOptions{...})
//
// For 12-factor / containerized deployments, NewFromEnv reads
// EVERSCRIBE_PROJECT_ID and EVERSCRIBE_API_KEY from the process
// environment.
//
// Customers who only need one surface can call its constructor
// directly — recorder.New(projectID, apiKey) and
// minter.New(projectID, apiKey) both still work and skip the
// SDK-client step.
package everscribe

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/everscribe/sdk-go/pkg/minter"
	"github.com/everscribe/sdk-go/pkg/recorder"
)

// Environment-variable names read by NewFromEnv.
const (
	envProjectID = "EVERSCRIBE_PROJECT_ID"
	envAPIKey    = "EVERSCRIBE_API_KEY"
)

// Client is a credential-bearing handle to an Everscribe project.
// It does not hold network state itself; subclient constructors
// (NewRecorder, NewMinter) build per-surface clients that own their
// own connections.
//
// Reuse a single Client for the lifetime of the process; safe for
// concurrent use.
type Client struct {
	projectID string
	apiKey    string
}

// New returns a Client bound to projectID, authenticating subclient
// requests with apiKey. Both arguments are trimmed; an empty or
// whitespace-only value returns an error so configuration bugs surface
// at construction rather than at the first network call.
func New(projectID, apiKey string) (*Client, error) {
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return nil, errors.New("everscribe: projectID is empty")
	}
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return nil, errors.New("everscribe: apiKey is empty")
	}
	return &Client{projectID: projectID, apiKey: apiKey}, nil
}

// NewFromEnv constructs a Client by reading EVERSCRIBE_PROJECT_ID and
// EVERSCRIBE_API_KEY from the process environment. Returns an error
// naming the missing variable if either is unset or empty after
// trimming.
//
// Use this in 12-factor app bootstraps so credentials never appear in
// source. For tests and CLIs that pass credentials explicitly, call
// New directly.
func NewFromEnv() (*Client, error) {
	projectID := strings.TrimSpace(os.Getenv(envProjectID))
	if projectID == "" {
		return nil, fmt.Errorf("everscribe: %s is not set or empty", envProjectID)
	}
	apiKey := strings.TrimSpace(os.Getenv(envAPIKey))
	if apiKey == "" {
		return nil, fmt.Errorf("everscribe: %s is not set or empty", envAPIKey)
	}
	return &Client{projectID: projectID, apiKey: apiKey}, nil
}

// NewRecorder returns a buffered recorder for the bound project.
// Forwarded options apply to the recorder; see the recorder package
// for the full list (WithBufferSize, WithFlushInterval, WithBaseURL,
// etc.).
func (c *Client) NewRecorder(opts ...recorder.RecorderOption) *recorder.BufferedRecorder {
	return recorder.New(c.projectID, c.apiKey, opts...)
}

// NewMinter returns a minter client for the bound project.
// Forwarded options apply to the minter; see the minter package
// for the full list (WithBaseURL, WithHTTPClient).
func (c *Client) NewMinter(opts ...minter.Option) *minter.Client {
	return minter.New(c.projectID, c.apiKey, opts...)
}
