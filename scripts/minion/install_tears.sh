#!/usr/bin/env bash

set -eou pipefail

set -x

PREFIX=${PREFIX:=/usr/local/npg}

TMPDIR=$PWD/
TMP=$(mktemp -d ${TMPDIR:-/tmp/}$(basename -- "$0").XXXXXXXXXX)

GITHUB_URL=${GITHUB_URL:-https://github.com}
GITHUB_USER=${GITHUB_USER:=whitwham}
TEARS_VERSION="1.2.3"
TEARS_SHA256="80fe8c0619f4d18b7fec5b7bb261b7cc51805c6141deeb0364695dbf2aaa2c33"

trap cleanup EXIT INT TERM

cleanup() {
    rm -rf "$TMP"
}

download_tears_source() {
    curl -sSL -O ${GITHUB_URL}/$GITHUB_USER/tears/archive/v${TEARS_VERSION}.tar.gz
}

verify_tears_source() {
    echo "$TEARS_SHA256 *v${TEARS_VERSION}.tar.gz" | sha256sum -c -
}

install_tears() {
    tar xfz v${TEARS_VERSION}.tar.gz -C "$TMP"
    pushd "$TMP/tears-${TEARS_VERSION}"
    autoreconf -fi
    ./configure --prefix="$PREFIX" --with-irods CPPFLAGS="-I/usr/include/irods -I$PREFIX/include" LDFLAGS="-L/usr/lib/irods/externals -L$PREFIX/lib"
    make install
    popd
}

pushd "$TMP"
download_tears_source
verify_tears_source
install_tears
popd
