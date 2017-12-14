#!/usr/bin/env bash

set -eou pipefail

set -x

PREFIX=${PREFIX:=/usr/local/npg}

TMPDIR=$PWD/
TMP=$(mktemp -d ${TMPDIR:-/tmp/}$(basename -- "$0").XXXXXXXXXX)

HDF5_RELEASE_URL=https://support.hdfgroup.org/ftp/HDF5/current18/src
HDF5_VERSION="1.8.20"
HDF5_SHA256="6ed660ccd2bc45aa808ea72e08f33cc64009e9dd4e3a372b53438b210312e8d9"

trap cleanup EXIT INT TERM

cleanup() {
    rm -rf "$TMP"
}

download_hdf5_source() {
    curl -sSL -o hdf5-${HDF5_VERSION}.tar.gz ${HDF5_RELEASE_URL}/hdf5-${HDF5_VERSION}.tar.gz
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
