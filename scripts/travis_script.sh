#!/bin/bash

set -e -x

perl Build.PL
./Build clean
./Build test
