#!/usr/bin/env bash
set -euo pipefail

PLUGIN_DIR=$(cd "$(dirname "$0")/.." && pwd)/broker-plugin
BUILD_DIR="$PLUGIN_DIR/build"
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"
cmake ..
cmake --build . --config Release

echo "Plugin built to $BUILD_DIR"
