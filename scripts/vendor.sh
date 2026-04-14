#!/usr/bin/env bash
# scripts/vendor.sh
#
# Download third-party front-end assets into static/.
# Run this once after checkout (or after updating versions below).
#
# Requirements: curl
#
# Versions pinned here — bump and re-run to upgrade.
BOOTSTRAP_VERSION="5.3.3"
BOOTSWATCH_VERSION="5.3.3"
CODEMIRROR_VERSION="5.65.18"

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STATIC_DIR="$SCRIPT_DIR/../static"

# ── helpers ──────────────────────────────────────────────────────────────────

download() {
    local url="$1"
    local dest="$2"
    echo "  Downloading $dest"
    curl --silent --fail --location --output "$dest" "$url"
}

# ── Bootstrap ────────────────────────────────────────────────────────────────

echo "==> Bootstrap $BOOTSTRAP_VERSION"
mkdir -p "$STATIC_DIR/bootstrap"

download \
    "https://cdn.jsdelivr.net/npm/bootstrap@${BOOTSTRAP_VERSION}/dist/css/bootstrap.min.css" \
    "$STATIC_DIR/bootstrap/bootstrap.min.css"

download \
    "https://cdn.jsdelivr.net/npm/bootstrap@${BOOTSTRAP_VERSION}/dist/js/bootstrap.bundle.min.js" \
    "$STATIC_DIR/bootstrap/bootstrap.bundle.min.js"

# ── Bootswatch ───────────────────────────────────────────────────────────────

echo "==> Bootswatch $BOOTSWATCH_VERSION (Flatly + Darkly)"
mkdir -p "$STATIC_DIR/bootswatch"

download \
    "https://cdn.jsdelivr.net/npm/bootswatch@${BOOTSWATCH_VERSION}/dist/flatly/bootstrap.min.css" \
    "$STATIC_DIR/bootswatch/flatly.min.css"

download \
    "https://cdn.jsdelivr.net/npm/bootswatch@${BOOTSWATCH_VERSION}/dist/darkly/bootstrap.min.css" \
    "$STATIC_DIR/bootswatch/darkly.min.css"

# ── CodeMirror ───────────────────────────────────────────────────────────────

echo "==> CodeMirror $CODEMIRROR_VERSION"
mkdir -p "$STATIC_DIR/codemirror"

download \
    "https://cdnjs.cloudflare.com/ajax/libs/codemirror/${CODEMIRROR_VERSION}/codemirror.min.css" \
    "$STATIC_DIR/codemirror/codemirror.min.css"

download \
    "https://cdnjs.cloudflare.com/ajax/libs/codemirror/${CODEMIRROR_VERSION}/codemirror.min.js" \
    "$STATIC_DIR/codemirror/codemirror.min.js"

download \
    "https://cdnjs.cloudflare.com/ajax/libs/codemirror/${CODEMIRROR_VERSION}/mode/sql/sql.min.js" \
    "$STATIC_DIR/codemirror/sql.min.js"

# ── Perspective WASM ─────────────────────────────────────────────────────────
# TODO: vendor @finos/perspective WASM assets.
#
# Perspective's build output includes several large .wasm files and a
# JavaScript loader that must be served from the same origin.  The canonical
# approach is:
#
#   npm pack @finos/perspective@<version>
#   npm pack @finos/perspective-viewer@<version>
#   npm pack @finos/perspective-viewer-datagrid@<version>
#
# Then copy the dist/ tree into static/perspective/.  Because these packages
# are 10–30 MB each, they are excluded from version control (.gitignore) and
# must be fetched by this script or a CI step.
echo ""
echo "NOTE: Perspective WASM assets are NOT vendored yet."
echo "      See the TODO comment in this script for instructions."

echo ""
echo "Done. All assets written to $STATIC_DIR/"
