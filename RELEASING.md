# Releasing

This repo is a single Go module, `github.com/everscribe/sdk-go`.

## Tagging

Tag the module directly:

    git tag v0.2.0

## Before tagging

    go build ./... && go vet ./... && go test ./... -race
