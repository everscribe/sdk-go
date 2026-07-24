# Releasing

This repo is six Go modules: the root `github.com/everscribe/sdk-go` plus
five adapters under `adapters/`.

## Tagging

Adapter tags are path-prefixed and independent of the root module's:

    git tag v0.2.0                        # root module
    git tag adapters/http/gin/v0.1.0      # gin adapter

## Order

**Release the core first, always.** An adapter's `go.mod` requires a
minimum core version, and adapters may not depend on unreleased core
symbols.

## The replace directives

While the root module is unpublished at `v0.0.0`, every adapter carries a
`replace` directive pointing back at the repo root, relative to the
adapter's own directory:

    replace github.com/everscribe/sdk-go => ../../..   # adapters/http/stdlib, gin, echo, fiber (3 levels deep)
    replace github.com/everscribe/sdk-go => ../..       # adapters/grpc (2 levels deep)

The rule is "however many `..` segments walk from the adapter's directory
back to the repo root," not a single literal path. Verify with `cd
<adapter> && go list -m github.com/everscribe/sdk-go` if unsure.

Remove all five and pin a real version in the same commit that tags the
first core release. `go.work` stays for local development either way.

## Before tagging

    for m in . adapters/http/stdlib adapters/http/gin adapters/http/echo \
             adapters/http/fiber adapters/grpc; do
      (cd "$m" && go build ./... && go vet ./... && go test ./... -race) || exit 1
    done
