package WTSI::NPG::OM::BioNano::PublisherTest;

use strict;
use warnings;
use DateTime;

use Data::Dumper; # FIXME

use base qw(WTSI::NPG::HTS::Test); # FIXME better path for shared base

use Test::More tests => 7;
use Test::Exception;

use English qw(-no_match_vars);
use File::Spec;
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

sub metadata : Test(4) {

    my $irods = WTSI::NPG::iRODS->new();
    my $resultset = WTSI::NPG::OM::BioNano::ResultSet->new(
        directory => $test_run_path,
    );
    my $publication_time = DateTime->new(
        year       => 2016,
        month      => 1,
        day        => 1,
        hour       => 12,
        minute     => 00,
    );
    my $publisher = WTSI::NPG::OM::BioNano::Publisher->new(
        resultset => $resultset
    );
    my $bionano_coll = $publisher->publish($irods_tmp_coll,
                                           $publication_time);
    my @collection_meta = $irods->get_collection_meta($bionano_coll);

    is(scalar @collection_meta, 6,
       "Expected number of collection AVUs found");

    my ($user_name) = getpwuid $REAL_USER_ID;

    my @expected_meta = (
        {
            'attribute' => 'bnx_chip_id',
            'value' => '20000,10000,1/1/2015,987654321'
        },
        {
            'attribute' => 'bnx_flowcell',
            'value' => 1
        },
        {
            'attribute' => 'bnx_instrument',
            'value' => 'B001'
        },
        {
            'attribute' => 'dcterms:created',
            'value' => '2016-01-01T12:00:00'
        },
        {
            'attribute' => 'dcterms:creator',
            'value' => 'http://www.sanger.ac.uk'
        },
        {
            'attribute' => 'dcterms:publisher',
            'value' => 'ldap://ldap.internal.sanger.ac.uk/ou=people,dc=sanger,dc=ac,dc=uk?title?sub?(uid='.$user_name.')'
        },
    );

    is_deeply(\@collection_meta, \@expected_meta,
              "Collection metadata matches expected values");

    #print STDERR Dumper \@collection_meta;

    my $bnx_ipath =  File::Spec->catfile($bionano_coll,
                                         'Detect Molecules',
                                         'Molecules.bnx');
    my @file_meta = $irods->get_object_meta($bnx_ipath);

    my @additional_file_meta = (
        {
            'attribute' => 'md5',
            'value' => 'd50fb6797f561e74ae2a5ae6e0258d16'
        },
        {
            'attribute' => 'type',
            'value' => 'bnx'
        }
    );

    push @expected_meta, @additional_file_meta;

    is(scalar @file_meta, 8, "Expected number of BNX file AVUs found");

    is_deeply(\@file_meta, \@expected_meta,
              'BNX file metadata matches expected values');

    #print STDERR Dumper \@file_meta;
}


1;
