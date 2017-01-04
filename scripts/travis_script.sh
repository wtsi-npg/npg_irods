#!/bin/bash

set -e -x

export TEST_AUTHOR=1
export WTSI_NPG_iRODS_Test_irodsEnvFile=$HOME/.irods/.irodsEnv
export WTSI_NPG_iRODS_Test_IRODS_ENVIRONMENT_FILE=$HOME/.irods/irods_environment.json

perl Build.PL
./Build clean
./Build test
