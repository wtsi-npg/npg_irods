#!/usr/bin/env bash

set -eou pipefail

set -x

PREFIX=${PREFIX:=/usr/local/npg}

TMPDIR=$PWD/
TMP=$(mktemp -d ${TMPDIR:-/tmp/}$(basename -- "$0").XXXXXXXXXX)

GITHUB_URL=${GITHUB_URL:-https://github.com}
GITHUB_USER=${GITHUB_USER:=wtsi-npg}

PERL_DNAP_UTILITIES_VERSION="0.5.6"
PERL_IRODS_WRAP_VERSION="3.2.0"
NPG_IRODS_VERSION="2.8.1"

trap cleanup EXIT INT TERM

cleanup() {
    rm -rf "$TMP"
}

install_from_source() {
    local repo_name="$1"
    local repo_branch="$2"
    local build_script="$3"

    pushd "$TMP"
    git clone -b "$repo_branch" ${GITHUB_URL}/${GITHUB_USER}/${repo_name}.git
    pushd "$repo_name"

    perl "$build_script.PL"
    ./"$build_script" --install_base "$PREFIX" \
      --cpan_client "cpanm --notest -L '$PREFIX'" installdeps
    ./"$build_script" --install_base "$PREFIX" \
      --cpan_client "cpanm --notest -L '$PREFIX'" install

    popd
    popd
}

sudo apt-get install -y cpanminus liblocal-lib-perl uuid-dev
sudo apt-get install -y git

PERL_LOCAL_LIB_ROOT="$PREFIX"
PERL5LIB="$PREFIX"/lib/perl5/
eval $(perl -Mlocal::lib="$PREFIX")


export PERL_MM_USE_DEFAULT=1
# The following is not declared in the CPAN dependency graph
cpanm --notest -L "$PREFIX" Params::Util
# The following are not declared in our build file dependencies
cpanm --notest -L "$PREFIX" DateTime

install_from_source perl-dnap-utilities "$PERL_DNAP_UTILITIES_VERSION" Build
install_from_source perl-irods-wrap "$PERL_IRODS_WRAP_VERSION" Build

export HDF5_PATH="$PREFIX"
install_from_source npg_irods "$NPG_IRODS_VERSION" BuildONT
