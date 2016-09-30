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
./configure --with-irods=$IRODS_HOME --enable-plugins
make

wget -q https://github.com/samtools/samtools/releases/download/${SAMTOOLS_VERSION}/samtools-${SAMTOOLS_VERSION}.tar.bz2 -O /tmp/samtools-${SAMTOOLS_VERSION}.tar.bz2
tar xfj /tmp/samtools-${SAMTOOLS_VERSION}.tar.bz2 -C /tmp
cd /tmp/samtools-${SAMTOOLS_VERSION}
./configure --enable-plugins --with-plugin-path=/tmp/htslib-${HTSLIB_VERSION}
make all plugins-htslib
sudo make install
sudo ln -s samtools /usr/local/bin/samtools_irods

# CPAN
cpanm --quiet --notest Alien::Tidyp # For npg_tracking
cpanm --quiet --notest Module::Build

# WTSI NPG Perl repo dependencies
repos=""
for repo in perl-dnap-utilities perl-irods-wrap ml_warehouse npg_ml_warehouse npg_tracking npg_seq_common npg_qc; do
  cd /tmp
  git clone ${WTSI_NPG_GITHUB_URL}/${repo}.git ${repo}.git
  cd /tmp/${repo}.git
  git checkout ${TRAVIS_BRANCH} || git checkout master
  repos=$repos" /tmp/${repo}.git" 
done

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

cpanm --quiet --notest --installdeps .
