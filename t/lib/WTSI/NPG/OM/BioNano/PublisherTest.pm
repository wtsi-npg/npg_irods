package WTSI::NPG::OM::BioNano::PublisherTest;

use strict;
use warnings;
use DateTime;

use base qw(WTSI::NPG::HTS::Test); # FIXME better path for shared base

use Test::More tests => 3;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::OM::BioNano::Publisher'); }

use WTSI::NPG::iRODS;
use WTSI::NPG::OM::BioNano::Publisher;
use WTSI::NPG::OM::BioNano::ResultSet;

my $data_path = './t/data/bionano/';
my $run_path = $data_path.'/sample_barcode_01234_2016-10-04_09_00';

my $irods_tmp_coll;

my $pid = $$;


sub make_fixture : Test(setup) {
    my $irods = WTSI::NPG::iRODS->new;

    $irods_tmp_coll = "BioNanoPublisherTest.$pid";
    $irods->add_collection($irods_tmp_coll);
}

sub teardown : Test(teardown) {
    my $irods = WTSI::NPG::iRODS->new;
    $irods->remove_collection($irods_tmp_coll);
}


sub publish : Test(2) {

    my $irods = WTSI::NPG::iRODS->new();
    my $resultset = WTSI::NPG::OM::BioNano::ResultSet->new(
        directory => $run_path,
    );
    my $publication_time = DateTime->now;
    my $publisher = WTSI::NPG::OM::BioNano::Publisher->new(
        resultset => $resultset,
        publication_time => $publication_time,
    );
    ok($publisher, "BioNano Publisher object created");

    ok($publisher->publish($irods_tmp_coll), "ResultSet published ok");

}

