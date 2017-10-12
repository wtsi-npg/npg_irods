package WTSI::NPG::OM::BioNano::RunPublisherTest;

use strict;
use warnings;
use DateTime;
use URI;

use base qw[WTSI::NPG::HTS::Test]; # FIXME better path for shared base

use Test::More tests => 7;
use Test::Exception;

use English qw[-no_match_vars];
use File::Spec::Functions;
use File::Temp qw[tempdir];

Log::Log4perl::init('./etc/log4perl_tests.conf');

{
  package TestDBFactory;
  use Moose;

  with 'npg_testing::db';
}

BEGIN { use_ok('WTSI::NPG::OM::BioNano::RunPublisher'); }

use WTSI::NPG::iRODS;
use WTSI::NPG::OM::BioNano::RunPublisher;

my $data_path = './t/data/bionano';
my $runfolder_name = 'stock_barcode_01234_2016-10-04_09_00';
my $fixture_path = "t/fixtures";
my $tmp_data;
my $tmp_db;
my $test_run_path;
my $irods_tmp_coll;
my $pid = $$;
my $wh_schema;

my $log = Log::Log4perl->get_logger();


sub setup_databases : Test(startup) {
    $tmp_db = tempdir('temp_bionano_db_XXXXXX', CLEANUP => 1);
    my $wh_db_file = catfile($tmp_db, 'ml_wh.db');
    my $db_factory = TestDBFactory->new(
        sqlite_utf8_enabled => 1,
        verbose             => 0
    );
    $wh_schema = $db_factory->create_test_db(
        'WTSI::DNAP::Warehouse::Schema',
        "$fixture_path/ml_warehouse",
    );
}

sub teardown_databases : Test(shutdown) {
    $wh_schema->storage->disconnect;
}

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
    my $run_path = $data_path.'/'.$runfolder_name;
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
        irods            => $irods,
        mlwh_schema      => $wh_schema,
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
    #getpwuid $REAL_USER_ID used by NPG::Annotator, but doesn't work on Travis
    my $user_name = `whoami`;
    chomp $user_name;
    my $affiliation_uri = URI->new('http://www.sanger.ac.uk');
    my $publisher = WTSI::NPG::OM::BioNano::RunPublisher->new(
        directory => $test_run_path,
        irods => $irods,
        mlwh_schema => $wh_schema,
        irods     => $irods,
    );
    my $bionano_coll = $publisher->publish($irods_tmp_coll,
                                           $publication_time);
    my @collection_meta = $irods->get_collection_meta($bionano_coll);

    is(scalar @collection_meta, 21,
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
        {
            'attribute' => 'sample',
            'value' => '425STDY6079620'
        },
        {
            'value' => 'ERS1791246',
            'attribute' => 'sample_accession_number'
        },
        {
            'attribute' => 'sample_cohort',
            'value' => 'Virus_6'
        },
        {
            'attribute' => 'sample_common_name',
            'value' => 'Human herpesvirus 4'
        },
        {
            'attribute' => 'sample_donor_id',
            'value' => '425STDY6079620'
        },
        {
            'attribute' => 'sample_id',
            'value' => '2265577'
        },
        {
            'attribute' => 'sample_public_name',
            'value' => 'IMS Saliva 250'
        },
        {
            'attribute' => 'sample_supplier_name',
            'value' => '14751_IMS_Saliva_250'
        },
        {
            'attribute' => 'source',
            'value' => 'production'
        },
        {
            'attribute' => 'stock_id',
            'value' => 'stock_barcode_01234'
        },
        {
            'attribute' => 'study',
            'value' => 'Virus Genome Herpesvirus'
        },
        {
            'attribute' => 'study_accession_number',
            'value' => 'ERP001026'
        },
        {
            'attribute' => 'study_id',
            'value' => '425'
        },
        {
            'attribute' => 'study_title',
            'value' => "Herpesvirus whole genome sequencing[UTF-8 test: \x{3a4}\x{1f74} \x{3b3}\x{3bb}\x{1ff6}\x{3c3}\x{3c3}\x{3b1} \x{3bc}\x{3bf}\x{1fe6} \x{1f14}\x{3b4}\x{3c9}\x{3c3}\x{3b1}\x{3bd} \x{1f11}\x{3bb}\x{3bb}\x{3b7}\x{3bd}\x{3b9}\x{3ba}\x{1f74} \x{3c4}\x{3bf} \x{3c3}\x{3c0}\x{3af}\x{3c4}\x{3b9} \x{3c6}\x{3c4}\x{3c9}\x{3c7}\x{3b9}\x{3ba}\x{3cc} \x{3c3}\x{3c4}\x{3b9}\x{3c2} \x{3b1}\x{3bc}\x{3bc}\x{3bf}\x{3c5}\x{3b4}\x{3b9}\x{3ad}\x{3c2} \x{3c4}\x{3bf}\x{3c5} \x{39f}\x{3bc}\x{3ae}\x{3c1}\x{3bf}\x{3c5}.]"
        },
    );

    is_deeply(\@collection_meta, \@expected_meta,
              "Collection metadata matches expected values");

    my $bnx_ipath = catfile($bionano_coll,
                            'Detect Molecules',
                            'Molecules.bnx');
    my @file_meta = $irods->get_object_meta($bnx_ipath);

    my @expected_file_meta = (
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
            'value' => $publisher->uuid,
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
        {
            'attribute' => 'md5',
            'value' => 'd50fb6797f561e74ae2a5ae6e0258d16'
        },
        {
            'attribute' => 'sample',
            'value' => '425STDY6079620'
        },
        {
            'attribute' => 'sample_accession_number',
            'value' => 'ERS1791246'
        },
        {
            'attribute' => 'sample_cohort',
            'value' => 'Virus_6'
        },
        {
            'attribute' => 'sample_common_name',
            'value' => 'Human herpesvirus 4'
        },
        {
            'attribute' => 'sample_donor_id',
            'value' => '425STDY6079620'
        },
        {
            'attribute' => 'sample_id',
            'value' => 2265577
        },
        {
            'attribute' => 'sample_public_name',
            'value' => 'IMS Saliva 250'
        },
        {
            'attribute' => 'sample_supplier_name',
            'value' => '14751_IMS_Saliva_250'
        },
        {
            'attribute' => 'source',
            'value' => 'production'
        },
        {
            'attribute' => 'stock_id',
            'value' => 'stock_barcode_01234'
        },
        {
            'attribute' => 'study',
            'value' => 'Virus Genome Herpesvirus'
        },
        {
            'attribute' => 'study_accession_number',
            'value' => 'ERP001026'
        },
        {
            'attribute' => 'study_id',
            'value' => 425
        },
        {
            'attribute' => 'study_title',
            'value' => "Herpesvirus whole genome sequencing[UTF-8 test: \x{3a4}\x{1f74} \x{3b3}\x{3bb}\x{1ff6}\x{3c3}\x{3c3}\x{3b1} \x{3bc}\x{3bf}\x{1fe6} \x{1f14}\x{3b4}\x{3c9}\x{3c3}\x{3b1}\x{3bd} \x{1f11}\x{3bb}\x{3bb}\x{3b7}\x{3bd}\x{3b9}\x{3ba}\x{1f74} \x{3c4}\x{3bf} \x{3c3}\x{3c0}\x{3af}\x{3c4}\x{3b9} \x{3c6}\x{3c4}\x{3c9}\x{3c7}\x{3b9}\x{3ba}\x{3cc} \x{3c3}\x{3c4}\x{3b9}\x{3c2} \x{3b1}\x{3bc}\x{3bc}\x{3bf}\x{3c5}\x{3b4}\x{3b9}\x{3ad}\x{3c2} \x{3c4}\x{3bf}\x{3c5} \x{39f}\x{3bc}\x{3ae}\x{3c1}\x{3bf}\x{3c5}.]"
        },
        {
            'attribute' => 'type',
            'value' => 'bnx'
        },
    );

    is(scalar @file_meta, 23, "Expected number of BNX file AVUs found");

    is_deeply(\@file_meta, \@expected_file_meta,
              'BNX file metadata matches expected values');
}


1;
