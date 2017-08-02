#!/usr/bin/env bash

set -eou pipefail

set -x

PREFIX=${PREFIX:=/usr/local/minion}

USER=${USER:=minion}
IRODS_HOST=${IRODS_HOST:=localhost}
IRODS_ZONE=${IRODS_ZONE:=tempZone}
IRODS_USER=${IRODS_USER:=irods}
IRODS_HOME=${IRODS_HOME:=/$IRODS_ZONE/home/$IRODS_USER}
IRODS_DEFAULT_RESC=${IRODS_DEFAULT_RESC:=demoResc}

TMPDIR=$PWD/
TMP=$(mktemp -d ${TMPDIR:-/tmp/}$(basename -- "$0").XXXXXXXXXX)

export PREFIX TMPDIR

trap cleanup EXIT INT TERM

cleanup() {
    rm -rf "$TMP"
}

sudo apt-get install -y gcc g++ make autoconf libtool libc++-dev

./install_hdf5.sh
./install_irods.sh
./install_jansson.sh
./install_baton.sh
./install_tears.sh
./install_npg_irods.sh

for icommand in \
    ichksum \
    ichmod \
    icp \
    ienv \
    iget \
    iinit \
    ils \
    ilsresc \
    imeta \
    imkdir \
    imv \
    iput \
    irm ; do
    cp /usr/bin/$icommand "$PREFIX/bin/$icommand"
done

mkdir -p "$PREFIX/var/lib/irods/plugins"
cp -R /var/lib/irods/plugins/* "$PREFIX/var/lib/irods/plugins"

sudo apt-get install -y jq
mkdir -p "$PREFIX/etc"

jq '.' <<EOF > "$PREFIX/etc/irods_environment.json"
{
    "irods_host": "$IRODS_HOST",
    "irods_port": 1247,
    "irods_user_name": "$IRODS_USER",
    "irods_home": "$IRODS_HOME",
    "irods_zone_name": "$IRODS_ZONE",
    "irods_default_resource": "$IRODS_DEFAULT_RESC",
    "irods_authentication_scheme": "native",
    "irods_authentication_file": "/home/$USER/.irods/irods_environment.json",
    "irods_plugins_home": "$PREFIX/var/lib/irods/plugins/"
}
EOF

tar cvfz minion-tools-`date --iso-8601`.tar.gz "$PREFIX"

echo "Done!"
