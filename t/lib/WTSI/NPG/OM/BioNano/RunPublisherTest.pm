package WTSI::NPG::OM::BioNano::RunPublisherTest;

use strict;
use warnings;
use Archive::Tar;
use Cwd qw[abs_path];
use DateTime;
use Digest::MD5;
use File::Slurp qw[read_file];
use URI;

use base qw[WTSI::NPG::HTS::Test]; # FIXME better path for shared base

use Test::More tests => 16;
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

# $REAL_USER_ID used by NPG::Annotator, but causes Travis test to fail
my $user_name = getpwuid $EFFECTIVE_USER_ID;
my $affiliation_uri = URI->new('http://www.sanger.ac.uk');

my $data_path = './t/data/bionano';
my @runfolder_names = qw[stock_barcode_01234_2016-10-04_09_00
                         stock_barcode_56789_2016-10-05_12_00];

# expected contents of .tar.gz files
my @expected_contents = (
    [
        'stock_barcode_01234_2016-10-04_09_00/Detect Molecules/Molecules.bnx',
        'stock_barcode_01234_2016-10-04_09_00/Detect Molecules/RawMolecules.bnx',
        'stock_barcode_01234_2016-10-04_09_00/Detect Molecules/RawMolecules1.bnx',
        'stock_barcode_01234_2016-10-04_09_00/Detect Molecules/RawMolecules2.bnx',
        'stock_barcode_01234_2016-10-04_09_00/Detect Molecules/RawMolecules3.bnx',
        'stock_barcode_01234_2016-10-04_09_00/Detect Molecules/RawMolecules4.bnx',
        'stock_barcode_01234_2016-10-04_09_00/Metadata.xml',
        'stock_barcode_01234_2016-10-04_09_00/Detect Molecules/RunReport.txt',
        'stock_barcode_01234_2016-10-04_09_00/Detect Molecules/Stitch.fov',
        'stock_barcode_01234_2016-10-04_09_00/Detect Molecules/iovars.json',
        'stock_barcode_01234_2016-10-04_09_00/Detect Molecules/Stitch1.fov',
        'stock_barcode_01234_2016-10-04_09_00/Detect Molecules/Stitch2.fov',
        'stock_barcode_01234_2016-10-04_09_00/Detect Molecules/Stitch3.fov',
        'stock_barcode_01234_2016-10-04_09_00/Detect Molecules/Stitch4.fov',
        'stock_barcode_01234_2016-10-04_09_00/Detect Molecules/Molecules1.mol',
        'stock_barcode_01234_2016-10-04_09_00/Detect Molecules/Molecules2.mol',
        'stock_barcode_01234_2016-10-04_09_00/Detect Molecules/Molecules3.mol',
        'stock_barcode_01234_2016-10-04_09_00/Detect Molecules/Molecules4.mol',
        'stock_barcode_01234_2016-10-04_09_00/Detect Molecules/Labels1.lab',
        'stock_barcode_01234_2016-10-04_09_00/Detect Molecules/Labels2.lab',
        'stock_barcode_01234_2016-10-04_09_00/Detect Molecules/Labels3.lab',
        'stock_barcode_01234_2016-10-04_09_00/Detect Molecules/Labels4.lab',
        'stock_barcode_01234_2016-10-04_09_00/Detect Molecules/Molecules.mol',
        'stock_barcode_01234_2016-10-04_09_00/Detect Molecules/Labels.lab',
        'stock_barcode_01234_2016-10-04_09_00/Detect Molecules/iovars1.json',
        'stock_barcode_01234_2016-10-04_09_00/Detect Molecules/iovars2.json',
        'stock_barcode_01234_2016-10-04_09_00/Detect Molecules/iovars3.json',
        'stock_barcode_01234_2016-10-04_09_00/Detect Molecules/iovars4.json'
    ], [

        'stock_barcode_56789_2016-10-05_12_00/Detect Molecules/Molecules.bnx',
        'stock_barcode_56789_2016-10-05_12_00/Detect Molecules/RawMolecules.bnx',
        'stock_barcode_56789_2016-10-05_12_00/Detect Molecules/RawMolecules1.bnx',
        'stock_barcode_56789_2016-10-05_12_00/Detect Molecules/RawMolecules2.bnx',
        'stock_barcode_56789_2016-10-05_12_00/Detect Molecules/RawMolecules3.bnx',
        'stock_barcode_56789_2016-10-05_12_00/Detect Molecules/RawMolecules4.bnx',
        'stock_barcode_56789_2016-10-05_12_00/Metadata.xml',
        'stock_barcode_56789_2016-10-05_12_00/Detect Molecules/RunReport.txt',
        'stock_barcode_56789_2016-10-05_12_00/Detect Molecules/Stitch.fov',
        'stock_barcode_56789_2016-10-05_12_00/Detect Molecules/iovars.json',
        'stock_barcode_56789_2016-10-05_12_00/Detect Molecules/Stitch1.fov',
        'stock_barcode_56789_2016-10-05_12_00/Detect Molecules/Stitch2.fov',
        'stock_barcode_56789_2016-10-05_12_00/Detect Molecules/Stitch3.fov',
        'stock_barcode_56789_2016-10-05_12_00/Detect Molecules/Stitch4.fov',
        'stock_barcode_56789_2016-10-05_12_00/Detect Molecules/Molecules1.mol',
        'stock_barcode_56789_2016-10-05_12_00/Detect Molecules/Molecules2.mol',
        'stock_barcode_56789_2016-10-05_12_00/Detect Molecules/Molecules3.mol',
        'stock_barcode_56789_2016-10-05_12_00/Detect Molecules/Molecules4.mol',
        'stock_barcode_56789_2016-10-05_12_00/Detect Molecules/Labels1.lab',
        'stock_barcode_56789_2016-10-05_12_00/Detect Molecules/Labels2.lab',
        'stock_barcode_56789_2016-10-05_12_00/Detect Molecules/Labels3.lab',
        'stock_barcode_56789_2016-10-05_12_00/Detect Molecules/Labels4.lab',
        'stock_barcode_56789_2016-10-05_12_00/Detect Molecules/Molecules.mol',
        'stock_barcode_56789_2016-10-05_12_00/Detect Molecules/Labels.lab',
        'stock_barcode_56789_2016-10-05_12_00/Detect Molecules/iovars1.json',
        'stock_barcode_56789_2016-10-05_12_00/Detect Molecules/iovars2.json',
        'stock_barcode_56789_2016-10-05_12_00/Detect Molecules/iovars3.json',
        'stock_barcode_56789_2016-10-05_12_00/Detect Molecules/iovars4.json',
    ],
);

my @test_run_paths;
my $fixture_path = "t/fixtures";
my $tmp_data;
my $tmp_db;
my $irods_tmp_coll;
my $pid = $$;
my $wh_schema;

my $log = Log::Log4perl->get_logger();


sub setup_databases : Test(startup) {
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
    foreach my $runfolder_name (@runfolder_names) {
        my $run_path = $data_path.'/'.$runfolder_name;
        system("cp -R $run_path $tmp_data") && $log->logcroak(
            q[Failed to copy '], $run_path, q[' to '], $tmp_data, q[']);
        my $test_run_path = $tmp_data.'/'.$runfolder_name;
        my $cmd = q[mv ].$test_run_path.q[/Detect_Molecules ].
            $test_run_path.q[/Detect\ Molecules];
        system($cmd) && $log->logcroak(
            q[Failed rename command '], $cmd, q[']);
        push @test_run_paths, $test_run_path;
    }
}

sub teardown : Test(teardown) {
    my $irods = WTSI::NPG::iRODS->new;
    $irods->remove_collection($irods_tmp_coll);
}

sub publish : Test(2) {
    my $irods = WTSI::NPG::iRODS->new();
    my $publication_time = DateTime->now;
    my $publisher = WTSI::NPG::OM::BioNano::RunPublisher->new(
        directory => $test_run_paths[0],
        publication_time => $publication_time,
        mlwh_schema => $wh_schema,
        irods => $irods,
    );
    ok($publisher, "BioNano RunPublisher object created");

    my $run_collection;
    lives_ok(
        sub { $run_collection = $publisher->publish($irods_tmp_coll); },
        'ResultSet published OK'
    );
}

sub publication_results : Test(4) {
    my $irods = WTSI::NPG::iRODS->new();
    my $publication_time = DateTime->new(
        year       => 2016,
        month      => 11,
        day        => 1,
        hour       => 12,
        minute     => 00,
    );

    my $publisher = WTSI::NPG::OM::BioNano::RunPublisher->new(
        directory   => $test_run_paths[0],
        mlwh_schema => $wh_schema,
        irods       => $irods,
    );
    my $bionano_obj = $publisher->publish($irods_tmp_coll,
                                          $publication_time);
    my $bionano_copy = abs_path($tmp_data.'/bionano_published.tar.gz');
    $irods->get_object($bionano_obj, $bionano_copy);

    # check contents of tarfile
    my $tar = Archive::Tar->new;
    $tar->read($bionano_copy);
    my @contents = $tar->list_files();
    is(scalar @contents, 28, 'Expected number of files in .tar.gz archive');
    my @sorted_contents = sort @contents;
    my @sorted_expected = sort @{$expected_contents[0]};
    is_deeply(\@sorted_contents, \@sorted_expected,
              '.tar.gz archive contents match expected values');

    # check md5 in metadata
    my $md5 = Digest::MD5->new;
    $md5->add(read_file($bionano_copy));
    my $md5sum = $md5->hexdigest();

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
            'value' => '2016-11-01T12:00:00'
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
            'value' => $md5sum,
            'attribute' => 'md5'
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
        {
            'attribute' => 'type',
            'value' => 'tar'
        },
    );

    my @object_meta = $irods->get_object_meta($bionano_obj);
    is(scalar @object_meta, scalar @expected_meta,
       "Expected number of collection AVUs found");

    my @sorted_object_meta = $irods->sort_avus(@object_meta);
    my @sorted_expected_meta = $irods->sort_avus(@expected_meta);

    is_deeply(\@sorted_object_meta, \@sorted_expected_meta,
              "Object metadata matches expected values");

}

sub alternate_output_dir : Test(3) {
    # test the output_dir attribute
    my $outdir = $tmp_data.'/tgz_output';
    mkdir $outdir || $log->logcroak('Failed to create directory ', $outdir);
    my $irods = WTSI::NPG::iRODS->new;
    my $publisher = WTSI::NPG::OM::BioNano::RunPublisher->new(
        directory   => $test_run_paths[0],
        mlwh_schema => $wh_schema,
        irods       => $irods,
        output_dir  => $outdir,
    );
    $publisher->publish($irods_tmp_coll);
    my $expected_tgz = $outdir.'/stock_barcode_01234_2016-10-04_09_00.tar.gz';
    ok(-e $expected_tgz, 'Local .tar.gz file written');
    my $tar = Archive::Tar->new;
    $tar->read($expected_tgz);
    my @contents = $tar->list_files();
    is(scalar @contents, 28, 'Expected number of files in .tar.gz archive');
    my @sorted_contents = sort @contents;
    my @sorted_expected = sort @{$expected_contents[0]};
    is_deeply(\@sorted_contents, \@sorted_expected,
              '.tar.gz archive contents match expected values');

}

sub dies_with_malformed_runfolder_name : Test(1) {
    my $run_copy_dir = $tmp_data.'/bad_name';
    mkdir $run_copy_dir ||
        $log->logcroak('Failed to create directory ', $run_copy_dir);
    my $bad_name = 'not-a-well-formatted-bionano-name';
    my $dest = $run_copy_dir.'/'.$bad_name;
    system('cp -R '.$test_run_paths[0].' '.$dest) &&
        $log->logcroak('Failed to copy ', $test_run_paths[0],
                       ' to ', $dest);
    dies_ok {
        my $publisher = WTSI::NPG::OM::BioNano::RunPublisher->new(
            directory => $dest,
            mlwh_schema => $wh_schema,
        );
        $publisher->publish($irods_tmp_coll);
    }, 'Dies with malformed runfolder name';
}

sub dies_without_bnx : Test(1) {
    my $run_copy_dir = $tmp_data.'/no_bnx';
    mkdir $run_copy_dir ||
        $log->logcroak('Failed to create directory ', $run_copy_dir);
    system('cp -R '.$test_run_paths[0].' '.$run_copy_dir) &&
        $log->logcroak('Failed to copy ', $test_run_paths[0],
                       ' to ', $run_copy_dir);
    my $run_path = $run_copy_dir.'/'.$runfolder_names[0];
    unlink $run_path.'/Detect Molecules/Molecules.bnx' ||
        $log->logcroak('Failed to delete Molecules.bnx');
    dies_ok {
        my $irods = WTSI::NPG::iRODS->new;
        my $publisher = WTSI::NPG::OM::BioNano::RunPublisher->new(
            directory => $run_path,
            mlwh_schema => $wh_schema,
            irods       => $irods,
        );
        $publisher->publish($irods_tmp_coll);
    }, 'Dies without a Molecules.bnx file';
}

sub publish_without_stock_meta : Test(4) {
    # runfolder stock_barcode_56789_2016-10-05_09_00 has no dummy MLWH entry
    # check on publication results; sample/study metadata will be absent

    my $irods = WTSI::NPG::iRODS->new();
    my $publication_time = DateTime->new(
        year       => 2016,
        month      => 11,
        day        => 1,
        hour       => 12,
        minute     => 00,
    );

    my $bionano_obj;
    my $uuid;
    do {
        # object will warn about missing sample/study; suppress screen output
        local *STDERR;
        open STDERR, '>', '/dev/null' ||
            $log->logcroak("Failed to open /dev/null for STDERR");
        my $publisher = WTSI::NPG::OM::BioNano::RunPublisher->new(
            directory   => $test_run_paths[1],
            mlwh_schema => $wh_schema,
            irods       => $irods,
        );
        $bionano_obj = $publisher->publish($irods_tmp_coll,
                                           $publication_time);
        $uuid = $publisher->uuid;
        close STDERR ||
            $log->logcroak("Failed to close /dev/null for STDERR");
    };
    my $bionano_copy = abs_path($tmp_data.'/bionano_published.tar.gz');
    $irods->get_object($bionano_obj, $bionano_copy);

    # check contents of tarfile
    my $tar = Archive::Tar->new;
    $tar->read($bionano_copy);
    my @contents = $tar->list_files();
    is(scalar @contents, 28, 'Expected number of files in .tar.gz archive');
    my @sorted_contents = sort @contents;
    my @sorted_expected = sort @{$expected_contents[1]};
    is_deeply(\@sorted_contents, \@sorted_expected,
              '.tar.gz archive contents match expected values');

    # check md5 in metadata
    my $md5 = Digest::MD5->new;
    $md5->add(read_file($bionano_copy));
    my $md5sum = $md5->hexdigest();

    my @object_meta = $irods->get_object_meta($bionano_obj);

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
            'value' => $uuid,
        },
        {
            'attribute' => 'dcterms:created',
            'value' => '2016-11-01T12:00:00'
        },
        {
            'value' =>  $affiliation_uri,
            'attribute' => 'dcterms:creator'
        },
        {
            'attribute' => 'dcterms:publisher',
            'value' =>  'ldap://ldap.internal.sanger.ac.uk/ou=people,dc=sanger,dc=ac,dc=uk?title?sub?(uid='.$user_name.')',
        },
        {
            'attribute' => 'md5',
            'value' => $md5sum,
        },
        {
            'value' => 'production',
            'attribute' => 'source'
        },
        {
            'attribute' => 'stock_id',
            'value' => 'stock_barcode_56789'
        },
        {
            'attribute' => 'type',
            'value' => 'tar'
        }
    );

    is(scalar @object_meta, scalar @expected_meta,
       "Expected number of collection AVUs found");

    my @sorted_object_meta = $irods->sort_avus(@object_meta);
    my @sorted_expected_meta = $irods->sort_avus(@expected_meta);

    is_deeply(\@sorted_object_meta, \@sorted_expected_meta,
              "Object metadata matches expected values");

}


1;
