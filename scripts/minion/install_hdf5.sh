#!/usr/bin/env bash

set -eou pipefail

set -x

PREFIX=${PREFIX:=/usr/local/minion}

TMPDIR=$PWD/
TMP=$(mktemp -d ${TMPDIR:-/tmp/}$(basename -- "$0").XXXXXXXXXX)

HDF5_RELEASE_URL=https://support.hdfgroup.org/ftp/HDF5/current18/src
HDF5_VERSION="1.8.19"
HDF5_SHA256="a4335849f19fae88c264fd0df046bc321a78c536b2548fc508627a790564dc38"

trap cleanup EXIT INT TERM

cleanup() {
    rm -rf "$TMP"
}

download_hdf5_source() {
    curl -sSL -O ${HDF5_RELEASE_URL}/hdf5-${HDF5_VERSION}.tar.gz
}

verify_hdf5_source() {
    echo "$HDF5_SHA256 *hdf5-${HDF5_VERSION}.tar.gz" | sha256sum -c -
}

install_hdf5() {
    tar xfz hdf5-${HDF5_VERSION}.tar.gz -C "$TMP"
    pushd "$TMP/hdf5-${HDF5_VERSION}"
    ./configure --prefix="$PREFIX"
    make -j 2 install
    popd
}

pushd "$TMP"
download_hdf5_source
verify_hdf5_source
install_hdf5
popd
