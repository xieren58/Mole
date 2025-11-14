#!/bin/bash
# Build Universal Binary for analyze-go
# Supports both Apple Silicon and Intel Macs

set -euo pipefail

cd "$(dirname "$0")/.."

echo "Building analyze-go for multiple architectures..."

# Build for arm64 (Apple Silicon)
echo "  → Building for arm64..."
GOARCH=arm64 go build -ldflags="-s -w" -o bin/analyze-go-arm64 cmd/analyze/main.go

# Build for amd64 (Intel)
echo "  → Building for amd64..."
GOARCH=amd64 go build -ldflags="-s -w" -o bin/analyze-go-amd64 cmd/analyze/main.go

# Create Universal Binary
echo "  → Creating Universal Binary..."
lipo -create bin/analyze-go-arm64 bin/analyze-go-amd64 -output bin/analyze-go

# Clean up temporary files
rm bin/analyze-go-arm64 bin/analyze-go-amd64

# Verify
echo ""
echo "✓ Build complete!"
echo ""
file bin/analyze-go
ls -lh bin/analyze-go | awk '{print "Size:", $5}'
echo ""
echo "Binary supports: arm64 (Apple Silicon) + x86_64 (Intel)"
