#!/usr/bin/env bash

set -eou pipefail

set -x

PREFIX=${PREFIX:=/usr/local/npg}

TMPDIR=$PWD/
TMP=$(mktemp -d ${TMPDIR:-/tmp/}$(basename -- "$0").XXXXXXXXXX)

JANSSON_RELEASE_URL=http://www.digip.org
JANSSON_RELEASE_KEY_ID="D058434C"
JANSSON_VERSION="2.10"

trap cleanup EXIT INT TERM

cleanup() {
    rm -rf "$TMP"
}

download_jansson_source() {
    curl -sSL ${JANSSON_RELEASE_URL}/jansson/releases/jansson-${JANSSON_VERSION}.tar.gz -o jansson-${JANSSON_VERSION}.tar.gz
    curl -sSL ${JANSSON_RELEASE_URL}/jansson/releases/jansson-${JANSSON_VERSION}.tar.gz.asc -o jansson-${JANSSON_VERSION}.tar.gz.asc
}

verify_jansson_source() {
    gpg --keyserver hkp://keys.gnupg.net --recv-keys ${JANSSON_RELEASE_KEY_ID}
    gpg --verify jansson-${JANSSON_VERSION}.tar.gz.asc
}

install_jansson() {
    tar xfz jansson-${JANSSON_VERSION}.tar.gz -C "$TMP"
    cd "$TMP/jansson-${JANSSON_VERSION}"
    ./configure --prefix="$PREFIX"
    make install
}

pushd "$TMP"
download_jansson_source
verify_jansson_source
install_jansson
popd
