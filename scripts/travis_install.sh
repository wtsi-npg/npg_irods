#!/bin/bash

set -e -x

# The default build branch for all repositories. This defaults to
# TRAVIS_BRANCH unless set in the Travis build environment.
WTSI_NPG_BUILD_BRANCH=${WTSI_NPG_BUILD_BRANCH:=$TRAVIS_BRANCH}

sudo apt-get install uuid-dev # required for Perl UUID module
sudo apt-get install libgd2-xpm-dev # For npg_tracking
sudo apt-get install liblzma-dev # For npg_qc
sudo apt-get install pigz # for BioNano run publication in npg_irods

wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-4.6.14-Linux-x86_64.sh -O ~/miniconda.sh

/bin/bash ~/miniconda.sh -b -p ~/miniconda
~/miniconda/bin/conda clean -tipsy
. ~/miniconda/etc/profile.d/conda.sh

echo ". ~/miniconda/etc/profile.d/conda.sh" >> ~/.bashrc
echo "conda activate travis" >> ~/.bashrc

conda config --set auto_update_conda False
conda config --add channels https://dnap.cog.sanger.ac.uk/npg/conda/devel/generic/

conda create -y -n travis
conda activate travis
conda install -y baton="$BATON_VERSION"
conda install -y irods-icommands
conda install -y tears
conda install -y samtools

ln -s "$HOME/miniconda/envs/travis/bin/samtools" \
   "$HOME/miniconda/envs/travis/bin/samtools_irods"

mkdir -p ~/.irods
cat <<EOF > ~/.irods/irods_environment.json
{
    "irods_host": "localhost",
    "irods_port": 1247,
    "irods_user_name": "irods",
    "irods_zone_name": "testZone",
    "irods_home": "/testZone/home/irods",
    "irods_plugins_home": "$HOME/miniconda/envs/travis/lib/irods/plugins/",
    "irods_default_resource": "testResc"
}
EOF

# CPAN
cpanm --quiet --notest Alien::Tidyp # For npg_tracking
cpanm --quiet --notest Module::Build
cpanm --quiet --notest LWP::Protocol::https
cpanm --quiet --notest https://github.com/chapmanb/vcftools-cpan/archive/v0.953.tar.gz # for npg_qc

# WTSI NPG Perl repo dependencies
repos=""
for repo in perl-dnap-utilities perl-irods-wrap ml_warehouse npg_ml_warehouse npg_tracking npg_seq_common npg_qc; do
    cd /tmp
    # Clone deeper than depth 1 to get the tag even if something has been already
    # commited over the tag
    git clone --branch master --depth 3 "${WTSI_NPG_GITHUB_URL}/${repo}.git" "${repo}.git"
    cd /tmp/${repo}.git
    # Shift off master to appropriate branch (if possible)
    git ls-remote --heads --exit-code origin ${WTSI_NPG_BUILD_BRANCH} && git pull origin ${WTSI_NPG_BUILD_BRANCH} && echo "Switched to branch ${WTSI_NPG_BUILD_BRANCH}"
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
    cd "$repo"
    cpanm --notest --installdeps .
    perl Build.PL
    ./Build
done

# Finally, bring any common dependencies up to the latest version and
# install
for repo in $repos
do
    cd "$repo"
    cpanm --notest --installdeps .
    ./Build install
done

cd $TRAVIS_BUILD_DIR

cpanm --notest --installdeps . || cat /home/travis/.cpanm/work/*/build.log
