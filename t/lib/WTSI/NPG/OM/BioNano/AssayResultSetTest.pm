package WTSI::NPG::OM::BioNano::AssayResultSetTest;

use strict;
use warnings;

use base qw(WTSI::NPG::HTS::Test); # FIXME better path for shared base

use Test::More tests => 2;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::OM::BioNano::AssayResultSet'); }

use WTSI::NPG::OM::BioNano::AssayResultSet;

my $data_path = './t/data/bionano/';
my $run_path = $data_path.'/sample_barcode_01234_2016-10-04_09_00';

sub assay_result_set : Test(1) {

    my $resultset = WTSI::NPG::OM::BioNano::AssayResultSet->new(
        directory => $run_path,
    );
    ok($resultset, "AssayResultSet created");

}
