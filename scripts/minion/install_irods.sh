#!/usr/bin/env bash

set -eou pipefail

set -x

TMPDIR=$PWD/
TMP=$(mktemp -d ${TMPDIR:-/tmp/}$(basename -- "$0").XXXXXXXXXX)

ARCH="x86_64"
PLATFORM="ubuntu14"

RENCI_RELEASE_URL="ftp://ftp.renci.org"
IRODS_VERSION="4.1.10"

IRODS_DEV=irods-dev-${IRODS_VERSION}-${PLATFORM}-${ARCH}.deb
IRODS_DEV_SHA256="62980d2bb222f314e10fc9f7f80fd7dca4b235988e72da017d8374f250170804"

IRODS_ICOMMANDS=irods-icommands-${IRODS_VERSION}-${PLATFORM}-${ARCH}.deb
IRODS_ICOMMANDS_SHA256="4f42477b32ae4a088dba9778e068b156e9a0db5675379c8b9f88254c51378cdb"

trap cleanup EXIT INT TERM

cleanup() {
    rm -rf "$TMP"
}

download_irods() {
    for pkg in "$IRODS_DEV" "$IRODS_ICOMMANDS"; do
        curl -sSL -O "${RENCI_RELEASE_URL}/pub/irods/releases/${IRODS_VERSION}/${PLATFORM}/$pkg"
    done
}

verify_irods_packages() {
    echo "$IRODS_DEV_SHA256 *$IRODS_DEV" | sha256sum -c -
    echo "$IRODS_ICOMMANDS_SHA256 *$IRODS_ICOMMANDS" | sha256sum -c -
}

install_irods() {
    sudo dpkg -i "$IRODS_DEV" "$IRODS_ICOMMANDS"
}

sudo apt-get install -y libssl-dev

pushd "$TMP"
download_irods
verify_irods_packages
install_irods
popd
