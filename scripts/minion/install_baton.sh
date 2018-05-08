#!/usr/bin/env bash

set -eou pipefail

set -x

PREFIX=${PREFIX:=/usr/local/npg}

TMPDIR=$PWD/
TMP=$(mktemp -d ${TMPDIR:-/tmp/}$(basename -- "$0").XXXXXXXXXX)

GITHUB_URL=${GITHUB_URL:-https://github.com}
GITHUB_USER=${GITHUB_USER:=wtsi-npg}
BATON_VERSION="1.1.0"
BATON_SHA256="1b8f5711eb04dd2c6f2c423e441054b327138719299864827a6148f127508c5e"

trap cleanup EXIT INT TERM

cleanup() {
    rm -rf "$TMP"
}

download_baton_source() {
    curl -sSL -o baton-${BATON_VERSION}.tar.gz ${GITHUB_URL}/$GITHUB_USER/baton/releases/download/${BATON_VERSION}/baton-${BATON_VERSION}.tar.gz
}

verify_baton_source() {
    echo "$BATON_SHA256 *baton-${BATON_VERSION}.tar.gz" | sha256sum -c -
}

install_baton() {
    tar xfz baton-${BATON_VERSION}.tar.gz -C "$TMP"
    pushd "$TMP/baton-${BATON_VERSION}"

    ./configure --prefix="$PREFIX" --with-irods CPPFLAGS="-I/usr/include/irods -I$PREFIX/include" LDFLAGS="-L/usr/lib/irods/externals -L$PREFIX/lib"
    make install
    popd
}

pushd "$TMP"
download_baton_source
verify_baton_source
install_baton
popd
