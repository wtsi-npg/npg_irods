#!/bin/bash

set -euo pipefail
# set -x

trap cleanup EXIT INT TERM

cleanup() {
    local exit_code=$?

    [ -d "$TMPD" ] && rm -rf "$TMPD"
    exit $exit_code
}

make_temp_dir() {
    echo $(mktemp -d ${TMPDIR:-/tmp/}$(basename -- "$0").XXXXXXXXXX)
}

TEARS=tears
BATON_LIST=baton-list
BATON_METAMOD=baton-metamod

[ -z "$1" ] && exit 2

TIMESTAMP=$(date +'%Y:%m:%dT%H:%m:%S')

COL=$(dirname -- "$1")
OBJ=$(basename -- "$1")
SUFFIX="${OBJ##*.}"

TMPDIR=$PWD/
TMPD=$(make_temp_dir)
MD5_FILE="$TMPD/$OBJ.md5"

# Overwriting with tears is an error, so remove any file already
# present
jq . << EOF | "$BATON_LIST" >/dev/null 2>&1 && irm "$1"
{
   "collection": "$COL",
   "data_object": "$OBJ"
}
EOF

tee >(md5sum - | awk '{print $1}' > "$MD5_FILE" ) </dev/stdin |\
    "$TEARS" "$1" && ichksum "$1"

# Add minimal metadata
if [ -e "$MD5_FILE" ];
then
    MD5=$(cat "$MD5_FILE")
    jq . << EOF | "$BATON_METAMOD" --operation add | jq .
    {
        "collection": "$COL",
        "data_object": "$OBJ",
        "avus": [
            {
                "attribute": "type",
                "value": "$SUFFIX"
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
