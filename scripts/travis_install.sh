#!/bin/bash

set -e -x

sudo apt-get install -qq odbc-postgresql
sudo apt-get install libgd2-xpm-dev # For npg_tracking
sudo apt-get install liblzma-dev # For npg_qc

# iRODS 3.3.1
wget -q https://github.com/wtsi-npg/irods-legacy/releases/download/3.3.1-travis-bc85aa/irods.tar.gz -O /tmp/irods.tar.gz
tar xfz /tmp/irods.tar.gz
source $TRAVIS_BUILD_DIR/scripts/irods_paths.sh

# Jansson
wget -q https://github.com/akheron/jansson/archive/v${JANSSON_VERSION}.tar.gz -O /tmp/jansson-${JANSSON_VERSION}.tar.gz
tar xfz /tmp/jansson-${JANSSON_VERSION}.tar.gz -C /tmp
cd /tmp/jansson-${JANSSON_VERSION}
autoreconf -fi
./configure ; make ; sudo make install
sudo ldconfig

# baton
wget -q https://github.com/wtsi-npg/baton/releases/download/${BATON_VERSION}/baton-${BATON_VERSION}.tar.gz -O /tmp/baton-${BATON_VERSION}.tar.gz
tar xfz /tmp/baton-${BATON_VERSION}.tar.gz -C /tmp
cd /tmp/baton-${BATON_VERSION}
./configure --with-irods=$IRODS_HOME ; make ; sudo make install
sudo ldconfig

# htslib/ samtools
wget -q https://github.com/samtools/htslib/releases/download/${HTSLIB_VERSION}/htslib-${HTSLIB_VERSION}.tar.bz2 -O /tmp/htslib-${HTSLIB_VERSION}.tar.bz2
tar xfj /tmp/htslib-${HTSLIB_VERSION}.tar.bz2 -C /tmp
cd /tmp/htslib-${HTSLIB_VERSION}
./configure --with-irods=$IRODS_HOME
perl -i -ple 's{(\$\(CC\)) \s+ (\$\(LDFLAGS\) \s+ -o \s+ \$\@ \s+ test/hfile.o \s+ libhts.a \s+ \$\(LDLIBS\) \s+ -lz)}{$1 -pthread $2}smx' Makefile
make

wget -q https://github.com/samtools/samtools/releases/download/${SAMTOOLS_VERSION}/samtools-${SAMTOOLS_VERSION}.tar.bz2 -O /tmp/samtools-${SAMTOOLS_VERSION}.tar.bz2
tar xfj /tmp/samtools-${SAMTOOLS_VERSION}.tar.bz2 -C /tmp
cd /tmp/samtools-${SAMTOOLS_VERSION}
make HTSDIR=/tmp/htslib-${HTSLIB_VERSION} LDFLAGS="-L${IRODS_HOME}/lib/core/obj" LDLIBS="-lRodsAPIs -lgssapi_krb5"
sudo make HTSDIR=/tmp/htslib-${HTSLIB_VERSION} install

# CPAN
cpanm --quiet --notest Alien::Tidyp # For npg_tracking
cpanm --quiet --notest Module::Build

# WTSI NPG Perl repo dependencies
cd /tmp
git clone https://github.com/wtsi-npg/perl-dnap-utilities.git perl-dnap-utilities.git
git clone https://github.com/wtsi-npg/perl-irods-wrap.git perl-irods-wrap.git
git clone https://github.com/wtsi-npg/ml_warehouse.git ml_warehouse.git
git clone https://github.com/wtsi-npg/npg_ml_warehouse.git npg_ml_warehouse.git
git clone https://github.com/wtsi-npg/npg_tracking.git npg_tracking.git
git clone https://github.com/wtsi-npg/npg_seq_common.git npg_seq_common.git
git clone https://github.com/wtsi-npg/npg_qc.git npg_qc.git

cd /tmp/perl-dnap-utilities.git ; git checkout ${DNAP_UTILITIES_VERSION}
cd /tmp/perl-irods-wrap.git     ; git checkout ${IRODS_WRAP_VERSION}
cd /tmp/ml_warehouse.git        ; git checkout ${DNAP_WAREHOUSE_VERSION}
cd /tmp/npg_ml_warehouse.git    ; git checkout ${NPG_ML_WAREHOUSE_VERSION}
cd /tmp/npg_tracking.git        ; git checkout ${NPG_TRACKING_VERSION}
cd /tmp/npg_seq_common.git      ; git checkout ${NPG_SEQ_COMMON_VERSION}
cd /tmp/npg_qc.git              ; git checkout ${NPG_QC_VERSION}

repos="/tmp/perl-dnap-utilities.git /tmp/perl-irods-wrap.git /tmp/ml_warehouse.git /tmp/npg_ml_warehouse.git /tmp/npg_tracking.git /tmp/npg_seq_common.git /tmp/npg_qc.git"

# Install CPAN dependencies. The src libs are on PERL5LIB because of
# circular dependencies. The blibs are on PERL5LIB because the package
# version, which cpanm requires, is inserted at build time. They must
# be before the libs for cpanm to pick them up in preference.

for repo in $repos
do
    export PERL5LIB=$repo/blib/lib:$PERL5LIB:$repo/lib
done

for repo in $repos
do
    cd $repo
    cpanm --quiet --notest --installdeps .
    perl Build.PL
    ./Build
done

# Finally, bring any common dependencies up to the latest version and
# install
for repo in $repos
do
    cd $repo
    cpanm --quiet --notest --installdeps .
    ./Build install
done

cd $TRAVIS_BUILD_DIR
