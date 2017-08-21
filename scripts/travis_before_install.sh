#!/bin/bash

set -e -x

sudo apt-get update -qq
sudo apt-get install uuid-dev -qq # required for Perl UUID module

