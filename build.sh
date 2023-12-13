#!/bin/sh
# Compiles all tools
# Usage: build.sh [OS list]
# Valid OS: linux, darwin, windows
set -e

OS="${@:-linux windows}"
APPS="datasetArchiver datasetGetProposal datasetRetriever datasetIngestor datasetCleaner"
PREFIX="build"

for APP in $APPS; do
    for GOOS in $OS; do
        echo "Building $APP for $GOOS"

        GO_TOOL=cmd/${APP}
        OUT="$PREFIX/$GOOS"
        mkdir -p "$OUT"

        case $GOOS in
        windows)
            GOARCH=amd64 GOOS=$GOOS go build -o $OUT/${APP}.exe $GO_TOOL/main.go
            ;;
        darwin)
            GOARCH=amd64 GOOS=$GOOS go build -o $OUT/${APP}-darwin-amd64 $GO_TOOL/main.go
            GOARCH=arm64 GOOS=$GOOS go build -o $OUT/${APP}-darwin-arm64 $GO_TOOL/main.go
            # universal binary
            lipo -create -output $OUT/${APP} $OUT/${APP}-darwin-amd64 $OUT/${APP}-darwin-arm64
            rm $OUT/${APP}-darwin-amd64 $OUT/${APP}-darwin-arm64
            ;;
        linux)
            GOARCH=amd64 GOOS=$GOOS go build -o $OUT/${APP} $GO_TOOL/main.go
            ;;
        *)
            # Best effort for other OS versions
            echo "Warning: Unsupported OS $OS"
            GOARCH="${GOARCH:-amd64}" GOOS=$GOOS go build -o $OUT/${APP} $GO_TOOL/main.go
            ;;
        esac

    done
done 
