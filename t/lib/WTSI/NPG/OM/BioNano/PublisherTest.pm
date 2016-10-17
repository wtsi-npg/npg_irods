package WTSI::NPG::OM::BioNano::PublisherTest;

use strict;
use warnings;
use DateTime;

use base qw(WTSI::NPG::HTS::Test); # FIXME better path for shared base

use Test::More tests => 3;
use Test::Exception;

use File::Temp qw(tempdir);

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::OM::BioNano::Publisher'); }

use WTSI::NPG::iRODS;
use WTSI::NPG::OM::BioNano::Publisher;
use WTSI::NPG::OM::BioNano::ResultSet;

my $data_path = './t/data/bionano/';
my $runfolder_name = 'sample_barcode_01234_2016-10-04_09_00';
my $test_run_path;
my $irods_tmp_coll;
my $pid = $$;

my $log = Log::Log4perl->get_logger();

sub make_fixture : Test(setup) {
    # set up iRODS test collection
    my $irods = WTSI::NPG::iRODS->new;
    $irods_tmp_coll = "BioNanoPublisherTest.$pid";
    $irods->add_collection($irods_tmp_coll);
    # create a temporary directory for test data
    # workaround for the space in BioNano's "Detect Molecules" directory,
    # because Build.PL does not work well with spaces in filenames
    my $tmp_data = tempdir('temp_bionano_data_XXXXXX', CLEANUP => 1);
    my $run_path = $data_path.$runfolder_name;
    system("cp -R $run_path $tmp_data") && $log->logcroak(
        q{Failed to copy '}, $run_path, q{' to '}, $tmp_data, q{'});
    $test_run_path = $tmp_data.'/'.$runfolder_name;;
    $test_run_path = $tmp_data.'/'.$runfolder_name;
    my $cmd = q{mv }.$test_run_path.q{/Detect_Molecules }.
        $test_run_path.q{/Detect\ Molecules};
    system($cmd) && $log->logcroak(
        q{Failed rename command '}, $cmd, q{'});
}

sub teardown : Test(teardown) {
    my $irods = WTSI::NPG::iRODS->new;
    $irods->remove_collection($irods_tmp_coll);
}


sub publish : Test(2) {

    my $irods = WTSI::NPG::iRODS->new();
    my $resultset = WTSI::NPG::OM::BioNano::ResultSet->new(
        directory => $test_run_path,
    );
    my $publication_time = DateTime->now;
    my $publisher = WTSI::NPG::OM::BioNano::Publisher->new(
        resultset => $resultset,
        publication_time => $publication_time,
    );
    ok($publisher, "BioNano Publisher object created");

    my $run_collection;
    lives_ok(
        sub { $run_collection = $publisher->publish($irods_tmp_coll); },
        'ResultSet published OK'
    );

}

