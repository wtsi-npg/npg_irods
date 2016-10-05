package WTSI::NPG::OM::BioNano::ResultSetTest;

use strict;
use warnings;

use Cwd qw(abs_path);

use base qw(WTSI::NPG::HTS::Test); # FIXME better path for shared base

use Test::More tests => 6;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::OM::BioNano::ResultSet'); }

use WTSI::NPG::OM::BioNano::ResultSet;

my $data_path = './t/data/bionano/';
my $run_path = $data_path.'/sample_barcode_01234_2016-10-04_09_00';

sub construction : Test(5) {

    my $resultset = WTSI::NPG::OM::BioNano::ResultSet->new(
        directory => $run_path,
    );
    ok($resultset, "ResultSet created");
    is(abs_path($resultset->data_directory),
       abs_path($run_path.'/Detect Molecules'),
       "Found expected data directory");

    my $bnx = $resultset->molecules_file;
    my $expected_bnx = $resultset->data_directory.'/Molecules.bnx';
    is(abs_path($bnx), abs_path($expected_bnx),
       "Found expected filtered molecules path");

    my $raw_bnx = $resultset->raw_molecules_file;
    my $expected_raw_bnx = $resultset->data_directory.'/RawMolecules.bnx';
    is(abs_path($raw_bnx), abs_path($expected_raw_bnx),
       "Found expected raw molecules path");

    my $ancillary = $resultset->ancillary_files;
    is(scalar @{$ancillary}, 6, "Found 6 ancillary files");
}
