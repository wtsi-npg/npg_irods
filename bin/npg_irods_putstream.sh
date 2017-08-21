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

This script creates a data object in iRODS from its STDIN. The data
object will be given certain basic metadata:

 type            <CLI option type argument>
 md5             <MD5 calculated from STDIN>
 dcterms:created <Time started>

baton-format JSON representing the data object is printed to STDOUT:

  $ echo foo | ./bin/npg_irods_putstream.sh -t txt \
     /irods-dev/home/irods/foo.txt
  {
     "data_object": "foo.txt",
     "collection": "/irods-dev/home/irods",
     "avus": [
        {
          "attribute": "type",
          "value": "txt"
        },
        {
          "attribute": "dcterms:created",
          "value": "2017:06:30T13:06:44"
        },
        {
           "attribute": "md5",
           "value": "d3b07384d113edec49eaa6238ad5ff00"
        }
     ]
   }


Version: $VERSION
Author:  Keith James <kdj@sanger.ac.uk>

Usage: $0 [-h] -t <type> <iRODS path> < data

Options:

 -h  Print usage and exit.
 -t  iRODS type metadata.

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
BATON_LIST=baton-list
BATON_METAMOD=baton-metamod
ICHKSUM=ichksum
IMKDIR=imkdir
IRM=irm
JQ=jq
MD5SUM=md5sum
TEARS=tears

IRODS_PATH=
IRODS_TYPE=

while getopts "ht:" option; do
    case "$option" in
        h)
            usage
            exit 0
            ;;
        t)
            IRODS_TYPE="$OPTARG"
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

if [ -z "$IRODS_TYPE" ] ; then
    usage
    echo -e "\nERROR:\n  A -t <iRODS type> argument is required"
    exit 4
fi

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

# Overwriting with tears is an error, so remove any file already
# present, detected with baton-list
$JQ . << EOF | $BATON_LIST >/dev/null 2>&1 && \
    $IRM "$IRODS_PATH" >/dev/null 2>&1
{
   "collection": "$COLLECTION",
   "data_object": "$DATA_OBJECT"
}
EOF

# Ensure that the leading path exists
$IMKDIR -p "$COLLECTION"

# Send this script's STDIN (the data to be sent to iRODS) to md5sum
# and to tears
tee >($MD5SUM - | awk '{print $1}' > "$MD5_FILE" ) </dev/stdin |\
    $TEARS "$IRODS_PATH" && $ICHKSUM -f "$IRODS_PATH" >/dev/null 2>&1

# Add minimal metadata
if [ -e "$MD5_FILE" ];
then
    MD5=$(cat "$MD5_FILE")
    $JQ . << EOF | $BATON_METAMOD --operation add | $JQ .
    {
        "collection": "$COLLECTION",
        "data_object": "$DATA_OBJECT",
        "avus": [
            {
                "attribute": "type",
                "value": "$IRODS_TYPE"
            },
            {
                "attribute": "dcterms:created",
                "value": "$TIMESTAMP"
            },
            {
                "attribute": "md5",
                "value": "$MD5"
            }
        ]
    }
EOF
fi
