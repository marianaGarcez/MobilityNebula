#!/usr/bin/env bash
# Build a .deb package from compiled MobilityNebula binaries.
# Usage: ./build-deb.sh <version> <bin_dir> <lib_dir> <output_dir>
#   version  - Package version (e.g., 1.0.0)
#   bin_dir  - Directory containing compiled binaries
#   lib_dir  - Directory containing shared libraries (optional, can be empty)
#   output_dir - Where to write the .deb file
set -euo pipefail

VERSION="${1:?Usage: $0 <version> <bin_dir> <lib_dir> <output_dir>}"
BIN_DIR="${2:?}"
LIB_DIR="${3:?}"
OUTPUT_DIR="${4:?}"
ARCH="amd64"
PKG_NAME="mobility-nebula"
PKG_DIR=$(mktemp -d)

trap 'rm -rf "$PKG_DIR"' EXIT

# Create directory structure
mkdir -p "${PKG_DIR}/DEBIAN"
mkdir -p "${PKG_DIR}/usr/local/bin"
mkdir -p "${PKG_DIR}/usr/local/lib/mobility-nebula"
mkdir -p "${PKG_DIR}/etc/systemd/system"
mkdir -p "${PKG_DIR}/etc/mobility-nebula"

# Copy binaries
for bin in nes-single-node-worker nes-nebuli; do
    if [ -f "${BIN_DIR}/${bin}" ]; then
        install -Dm755 "${BIN_DIR}/${bin}" "${PKG_DIR}/usr/local/bin/${bin}"
    fi
done

# Copy shared libraries if present
if [ -d "${LIB_DIR}" ] && [ "$(ls -A "${LIB_DIR}" 2>/dev/null)" ]; then
    cp -a "${LIB_DIR}"/* "${PKG_DIR}/usr/local/lib/mobility-nebula/"
fi

# Copy systemd service
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -f "${SCRIPT_DIR}/mobility-nebula.service" ]; then
    install -Dm644 "${SCRIPT_DIR}/mobility-nebula.service" \
        "${PKG_DIR}/etc/systemd/system/mobility-nebula.service"
fi

# Default configuration
cat > "${PKG_DIR}/etc/mobility-nebula/worker.conf" << 'CONF'
# MobilityNebula Worker Configuration
# See: https://github.com/MobilityDB/MobilityNebula
NES_WORKER_THREADS=2
CONF

# DEBIAN/control
cat > "${PKG_DIR}/DEBIAN/control" << EOF
Package: ${PKG_NAME}
Version: ${VERSION}
Section: science
Priority: optional
Architecture: ${ARCH}
Depends: libc6 (>= 2.35), wget, ca-certificates
Recommends: docker.io
Maintainer: MobilityNebula <mobilitynebula@mobilitydb.com>
Description: MobilityNebula stream processing engine for railway edge gateways
 Real-time stream processing engine designed for NROK-1030 and similar
 railway edge computing platforms. Provides spatiotemporal data processing
 with MEOS integration for passenger information systems.
Homepage: https://github.com/MobilityDB/MobilityNebula
EOF

# DEBIAN/postinst
cat > "${PKG_DIR}/DEBIAN/postinst" << 'EOF'
#!/bin/bash
set -e

# Update library cache
if [ -d /usr/local/lib/mobility-nebula ]; then
    echo "/usr/local/lib/mobility-nebula" > /etc/ld.so.conf.d/mobility-nebula.conf
    ldconfig
fi

# Enable and start systemd service
if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload
    systemctl enable mobility-nebula.service
    echo "MobilityNebula service installed. Start with: systemctl start mobility-nebula"
fi

echo ""
echo "MobilityNebula installed successfully!"
echo "  Binary:  /usr/local/bin/nes-single-node-worker"
echo "  Config:  /etc/mobility-nebula/worker.conf"
echo "  Service: systemctl start mobility-nebula"
echo ""
EOF
chmod 755 "${PKG_DIR}/DEBIAN/postinst"

# DEBIAN/prerm
cat > "${PKG_DIR}/DEBIAN/prerm" << 'EOF'
#!/bin/bash
set -e
if command -v systemctl >/dev/null 2>&1; then
    systemctl stop mobility-nebula.service 2>/dev/null || true
    systemctl disable mobility-nebula.service 2>/dev/null || true
fi
EOF
chmod 755 "${PKG_DIR}/DEBIAN/prerm"

# DEBIAN/postrm
cat > "${PKG_DIR}/DEBIAN/postrm" << 'EOF'
#!/bin/bash
set -e
rm -f /etc/ld.so.conf.d/mobility-nebula.conf
ldconfig
if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload
fi
EOF
chmod 755 "${PKG_DIR}/DEBIAN/postrm"

# Build the .deb
mkdir -p "${OUTPUT_DIR}"
DEB_FILE="${OUTPUT_DIR}/${PKG_NAME}_${VERSION}_${ARCH}.deb"
dpkg-deb --root-owner-group --build "${PKG_DIR}" "${DEB_FILE}"

echo "Package built: ${DEB_FILE}"
echo "Size: $(du -h "${DEB_FILE}" | cut -f1)"
