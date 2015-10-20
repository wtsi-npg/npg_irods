#!/bin/bash

set -e -x

cd $TRAVIS_BUILD_DIR

source $TRAVIS_BUILD_DIR/travis_linux_env.sh
source $TRAVIS_BUILD_DIR/scripts/irods_paths.sh

sudo mkdir -p $IRODS_VAULT
sudo chown $USER:$USER $IRODS_VAULT

sudo mkdir -p $IRODS_TEST_VAULT
sudo chown $USER:$USER $IRODS_TEST_VAULT

sudo -E -u postgres $TRAVIS_BUILD_DIR/setup_pgusers.sh
sudo -E -u postgres $TRAVIS_BUILD_DIR/irodscontrol psetup
$TRAVIS_BUILD_DIR/irodscontrol istart ; sleep 10

echo irods | script -q -c "iinit" > /dev/null
iadmin mkresc testResc 'unix file system' cache `hostname --fqdn` $IRODS_TEST_VAULT
