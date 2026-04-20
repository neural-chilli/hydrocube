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
PERSPECTIVE_VERSION="3.8.0"   # user confirmed latest is 4.4.1 but 3.8.0 has full CDN build

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
# The CDN JS files use `new URL("../wasm/...", import.meta.url)` to locate WASM
# binaries, so we must preserve the cdn/ and wasm/ subdirectory structure from
# the npm packages exactly.  The CSS themes live under css/.
#
# Total download: ~15–20 MB (WASM binaries dominate).
# These files are gitignored and must be fetched by this script or a CI step.

echo "==> @finos/perspective $PERSPECTIVE_VERSION (WASM — may take a moment)"
mkdir -p "$STATIC_DIR/perspective/cdn"
mkdir -p "$STATIC_DIR/perspective/wasm"
mkdir -p "$STATIC_DIR/perspective/css"

PSP_BASE="https://cdn.jsdelivr.net/npm/@finos/perspective@${PERSPECTIVE_VERSION}"
download "${PSP_BASE}/dist/cdn/perspective.js" \
         "$STATIC_DIR/perspective/cdn/perspective.js"
download "${PSP_BASE}/dist/cdn/perspective-server.worker.js" \
         "$STATIC_DIR/perspective/cdn/perspective-server.worker.js"
download "${PSP_BASE}/dist/wasm/perspective-server.wasm" \
         "$STATIC_DIR/perspective/wasm/perspective-server.wasm"

PSP_V_BASE="https://cdn.jsdelivr.net/npm/@finos/perspective-viewer@${PERSPECTIVE_VERSION}"
download "${PSP_V_BASE}/dist/cdn/perspective-viewer.js" \
         "$STATIC_DIR/perspective/cdn/perspective-viewer.js"
download "${PSP_V_BASE}/dist/wasm/perspective-viewer.wasm" \
         "$STATIC_DIR/perspective/wasm/perspective-viewer.wasm"
# Themes: "Pro Light" and "Pro Dark" match our Flatly/Darkly UI themes
download "${PSP_V_BASE}/dist/css/themes.css" \
         "$STATIC_DIR/perspective/css/themes.css"
download "${PSP_V_BASE}/dist/css/pro.css" \
         "$STATIC_DIR/perspective/css/pro.css"
download "${PSP_V_BASE}/dist/css/pro-dark.css" \
         "$STATIC_DIR/perspective/css/pro-dark.css"
download "${PSP_V_BASE}/dist/css/icons.css" \
         "$STATIC_DIR/perspective/css/icons.css"

PSP_DG_BASE="https://cdn.jsdelivr.net/npm/@finos/perspective-viewer-datagrid@${PERSPECTIVE_VERSION}"
download "${PSP_DG_BASE}/dist/cdn/perspective-viewer-datagrid.js" \
         "$STATIC_DIR/perspective/cdn/perspective-viewer-datagrid.js"

PSP_D3_BASE="https://cdn.jsdelivr.net/npm/@finos/perspective-viewer-d3fc@${PERSPECTIVE_VERSION}"
download "${PSP_D3_BASE}/dist/cdn/perspective-viewer-d3fc.js" \
         "$STATIC_DIR/perspective/cdn/perspective-viewer-d3fc.js"

echo ""
echo "Done. All assets written to $STATIC_DIR/"
echo ""
du -sh "$STATIC_DIR"/bootstrap "$STATIC_DIR"/bootswatch \
       "$STATIC_DIR"/codemirror "$STATIC_DIR"/perspective 2>/dev/null || true
echo ""
echo "Next: cargo build   (rust-embed will pick up the new files)"
