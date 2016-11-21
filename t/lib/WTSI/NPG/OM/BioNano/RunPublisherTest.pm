package WTSI::NPG::OM::BioNano::RunPublisherTest;

use strict;
use warnings;
use DateTime;
use URI;

use base qw[WTSI::NPG::HTS::Test]; # FIXME better path for shared base

use Test::More tests => 15;
use Test::Exception;

use English qw[-no_match_vars];
use File::Spec::Functions;
use File::Temp qw[tempdir];

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::OM::BioNano::RunPublisher'); }

use WTSI::NPG::iRODS;
use WTSI::NPG::OM::BioNano::RunPublisher;

my $data_path = './t/data/bionano/';
my $runfolder_name = 'sample_barcode_01234_2016-10-04_09_00';
my $tmp_data;
my $test_run_path;
my $irods_tmp_coll;
my $pid = $$;

my $log = Log::Log4perl->get_logger();

sub make_fixture : Test(setup) {
    # set up iRODS test collection
    my $irods = WTSI::NPG::iRODS->new;
    my $irods_cwd = $irods->working_collection;
    $irods_tmp_coll = catfile($irods_cwd, "BioNanoRunPublisherTest.$pid");
    $irods->add_collection($irods_tmp_coll);
    # create a temporary directory for test data
    # workaround for the space in BioNano's "Detect Molecules" directory,
    # because Build.PL does not work well with spaces in filenames
    $tmp_data = tempdir('temp_bionano_data_XXXXXX', CLEANUP => 1);
    my $run_path = $data_path.$runfolder_name;
    system("cp -R $run_path $tmp_data") && $log->logcroak(
        q[Failed to copy '], $run_path, q[' to '], $tmp_data, q[']);
    $test_run_path = $tmp_data.'/'.$runfolder_name;;
    $test_run_path = $tmp_data.'/'.$runfolder_name;
    my $cmd = q[mv ].$test_run_path.q[/Detect_Molecules ].
        $test_run_path.q[/Detect\ Molecules];
    system($cmd) && $log->logcroak(
        q[Failed rename command '], $cmd, q[']);
}

sub teardown : Test(teardown) {
    my $irods = WTSI::NPG::iRODS->new;
    $irods->remove_collection($irods_tmp_coll);
}

sub publish : Test(2) {
    my $irods = WTSI::NPG::iRODS->new();
    my $publication_time = DateTime->now;
    my $publisher = WTSI::NPG::OM::BioNano::RunPublisher->new(
        directory => $test_run_path,
        publication_time => $publication_time,
    );
    ok($publisher, "BioNano RunPublisher object created");

    my $run_collection;
    lives_ok(
        sub { $run_collection = $publisher->publish($irods_tmp_coll); },
        'ResultSet published OK'
    );
}

sub metadata : Test(4) {
    my $irods = WTSI::NPG::iRODS->new();
    my $publication_time = DateTime->new(
        year       => 2016,
        month      => 1,
        day        => 1,
        hour       => 12,
        minute     => 00,
    );
    my $user_name = getpwuid $REAL_USER_ID;
    my $affiliation_uri = URI->new('http://www.sanger.ac.uk');
    my $publisher = WTSI::NPG::OM::BioNano::RunPublisher->new(
        directory => $test_run_path
    );
    my $bionano_coll = $publisher->publish($irods_tmp_coll,
                                           $publication_time);
    my @collection_meta = $irods->get_collection_meta($bionano_coll);

    is(scalar @collection_meta, 7,
       "Expected number of collection AVUs found");

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
            'attribute' => 'bnx_uuid',
            'value' => $publisher->uuid
        },
        {
            'attribute' => 'dcterms:created',
            'value' => '2016-01-01T12:00:00'
        },
        {
            'attribute' => 'dcterms:creator',
            'value' => $affiliation_uri
        },
        {
            'attribute' => 'dcterms:publisher',
            'value' => 'ldap://ldap.internal.sanger.ac.uk/ou=people,dc=sanger,dc=ac,dc=uk?title?sub?(uid='.$user_name.')'
        },
    );

    is_deeply(\@collection_meta, \@expected_meta,
              "Collection metadata matches expected values");

    my $bnx_ipath = catfile($bionano_coll,
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

    is(scalar @file_meta, 9, "Expected number of BNX file AVUs found");

    is_deeply(\@file_meta, \@expected_meta,
              'BNX file metadata matches expected values');
}

sub script : Test(8) {

    my $irods = WTSI::NPG::iRODS->new();

    system("find $test_run_path -exec touch {} +") && $log->logcroak(
        "Failed to recursively update access time for $test_run_path"
    );

    my $script = "npg_publish_bionano_run.pl";

    my $cmd = "$script --collection $irods_tmp_coll --search_dir $tmp_data";

    ok(system($cmd)==0, "Publish script run successfully with search dir");

    my $expected_coll = $irods_tmp_coll."/d5/0f/b6/".$runfolder_name;
    ok($irods->is_collection($expected_coll),
       "Script publishes to expected iRODS collection");

    my $expected_bnx = $expected_coll."/Detect Molecules/Molecules.bnx";
    ok($irods->is_object($expected_bnx),
       "Script publishes expected filtered BNX file");

    $irods->remove_collection($expected_coll);

    $cmd = "$script --collection $irods_tmp_coll --search_dir $tmp_data ".
        "--runfolder_path $test_run_path 2> /dev/null";
    ok(system($cmd)!=0, "Publish script fails with incompatible arguments");
    ok(! $irods->is_collection($expected_coll),
       "No iRODS collection published by failed script");

    $cmd = "$script --collection $irods_tmp_coll ".
        "--runfolder_path $test_run_path";
    ok(system($cmd)==0, "Publish script run successfully with runfolder");
    ok($irods->is_collection($expected_coll),
       "Script publishes to expected iRODS collection");
    ok($irods->is_object($expected_bnx),
       "Script publishes expected filtered BNX file");
}


1;
