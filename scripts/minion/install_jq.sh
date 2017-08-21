#!/usr/bin/env bash

set -eou pipefail

set -x

PREFIX=${PREFIX:=/usr/local/npg}

TMPDIR=$PWD/
TMP=$(mktemp -d ${TMPDIR:-/tmp/}$(basename -- "$0").XXXXXXXXXX)

GITHUB_URL=${GITHUB_URL:-https://github.com}
GITHUB_USER=${GITHUB_USER:=stedolan}
JQ_VERSION="1.5"
JQ_SHA256="c4d2bfec6436341113419debf479d833692cc5cdab7eb0326b5a4d4fbe9f493c"

trap cleanup EXIT INT TERM

cleanup() {
    rm -rf "$TMP"
}

download_jq_source() {
    curl -sSL -O ${GITHUB_URL}/$GITHUB_USER/jq/releases/download/jq-${JQ_VERSION}/jq-${JQ_VERSION}.tar.gz
}

verify_jq_source() {
    echo "$JQ_SHA256 *jq-${JQ_VERSION}.tar.gz" | sha256sum -c -
}

install_jq() {
    tar xfz jq-${JQ_VERSION}.tar.gz -C "$TMP"
    pushd "$TMP/jq-${JQ_VERSION}"
    ./configure --prefix="$PREFIX"
    make install
    popd
}

pushd "$TMP"
download_jq_source
verify_jq_source
install_jq
popd
