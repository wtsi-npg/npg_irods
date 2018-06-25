#!/bin/bash
#
# Copyright (C) 2017 Genome Research Limited. All Rights Reserved.

# This program is free software: you can redistribute it and/or modify
# it under the terms of the Perl Artistic License or the GNU General
# Public License as published by the Free Software Foundation, either
# version 3 of the License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

set -eo pipefail

# set -x

usage() {
    cat 1>&2 << 'EOF'
This script reads a stream from a data object in iRODS and writes to
STDOUT. The stream is tee'd through md5sum and the result compared to
the expected checksum returned by ichksum. The script will exit with
an error if these checksums do not concur.

Version: $VERSION
Author:  Keith James <kdj@sanger.ac.uk>

Usage: $0 [-h] <iRODS path>

Options:

 -h  Print usage and exit.

EOF
}

trap cleanup EXIT INT TERM

cleanup() {
    local exit_code=$?

    [ -d "$TMPD" ] && rm -rf "$TMPD"
    exit $exit_code
}

make_temp_dir() {
    echo $(mktemp -d ${TMPDIR:-/tmp/}$(basename -- "$0").XXXXXXXXXX)
}

# Non-core executables (i.e. exclusing awk, cat etc.)
ICHKSUM=ichksum
MD5SUM=md5sum
TEARS=tears

IRODS_PATH=

while getopts "ht:" option; do
    case "$option" in
        h)
            usage
            exit 0
            ;;
        *)
            usage
            echo "Invalid argument: $option"
            exit 1
            ;;
    esac
done

shift $((OPTIND-1))

IRODS_PATH="$1"

if [ -z "$IRODS_PATH" ] ; then
    usage
    echo -e "\nERROR:\n  An iRODS path argument is required"
    exit 2
fi

COLLECTION=$(dirname -- "$IRODS_PATH")
DATA_OBJECT=$(basename -- "$IRODS_PATH")
TIMESTAMP=$(date +'%Y:%m:%dT%H:%m:%S')

TMPDIR=/tmp/
TMPD=$(make_temp_dir)
MD5_FILE="$TMPD/$DATA_OBJECT.md5"

IRODS_MD5=$($ICHKSUM "$IRODS_PATH" | awk "/$DATA_OBJECT/ { print \$2 }")

# Send the data from iRODS to md5sum and to STDOUT
$TEARS -d -r "$IRODS_PATH" | tee >($MD5SUM - | awk '{print $1}' > "$MD5_FILE")

LOCAL_MD5=$(<$MD5_FILE)
if [ "$LOCAL_MD5" != "$IRODS_MD5" ]; then
    echo -e "\nERROR: local MD5 '$LOCAL_MD5'" \
         "did not match iRODS MD5 '$IRODS_MD5'"
    exit 3
fi
