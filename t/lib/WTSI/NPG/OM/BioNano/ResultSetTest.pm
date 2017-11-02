package WTSI::NPG::OM::BioNano::ResultSetTest;

use strict;
use warnings;

use Cwd qw[abs_path];
use File::Temp qw[tempdir];

use base qw[WTSI::NPG::HTS::Test]; # FIXME better path for shared base

use Test::More tests => 11;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::OM::BioNano::ResultSet'); }

use WTSI::NPG::OM::BioNano::ResultSet;

my $data_path = './t/data/bionano/';
my $runfolder_name = 'stock_barcode_01234_2016-10-04_09_00';
my $test_run_path;

my $log = Log::Log4perl->get_logger();

sub make_fixture : Test(setup) {
    # create a temporary directory for test data
    # workaround for the space in BioNano's "Detect Molecules" directory,
    # because Build.PL does not work well with spaces in filenames
    my $tmp_data = tempdir('temp_bionano_data_XXXXXX', CLEANUP => 1);
    my $run_path = $data_path.$runfolder_name;
    system("cp -R $run_path $tmp_data") && $log->logcroak(
        q[Failed to copy '], $run_path, q[' to '], $tmp_data, q[']);
    $test_run_path = $tmp_data.'/'.$runfolder_name;
    my $cmd = q[mv ].$test_run_path.q[/Detect_Molecules ].
        $test_run_path.q[/Detect\ Molecules];
    system($cmd) && $log->logcroak(
        q[Failed rename command '], $cmd, q[']);
}

sub construction : Test(10) {

    my $resultset = WTSI::NPG::OM::BioNano::ResultSet->new(
        directory => $test_run_path,
    );

    ok($resultset, "ResultSet created");

    my $data_directory = $test_run_path.'/Detect Molecules';
    my $bnx_path = $resultset->filtered_bnx_path;
    my $expected_bnx = $data_directory.'/Molecules.bnx';
    is($bnx_path, $expected_bnx, "Found expected filtered molecules path");

    my @expected_bnx_paths = (
        $data_directory.'/Molecules.bnx',
        $data_directory.'/RawMolecules.bnx',
        $data_directory.'/RawMolecules1.bnx',
        $data_directory.'/RawMolecules2.bnx',
        $data_directory.'/RawMolecules3.bnx',
        $data_directory.'/RawMolecules4.bnx',
    );
    my @sorted_expected_paths = sort @expected_bnx_paths;
    my @sorted_result_paths = sort @{$resultset->bnx_paths};
    is_deeply(\@sorted_result_paths, \@sorted_expected_paths,
              'Found expected BNX paths');

    my $bnx;
    lives_ok(sub { $bnx = $resultset->bnx_file(); },
             'Found BNX file object');

    is($bnx->chip_id(), '20000,10000,1/1/2015,987654321',
       'Found expected chip ID from BNX file');

    my @sorted_ancillary = sort @{$resultset->ancillary_file_paths};
    my @expected_ancillary;
    my @filenames = qw(Labels.lab
                       Labels1.lab
                       Labels2.lab
                       Labels3.lab
                       Labels4.lab
                       Molecules.mol
                       Molecules1.mol
                       Molecules2.mol
                       Molecules3.mol
                       Molecules4.mol
                       RunReport.txt
                       Stitch.fov
                       Stitch1.fov
                       Stitch2.fov
                       Stitch3.fov
                       Stitch4.fov
                       iovars.json
                       iovars1.json
                       iovars2.json
                       iovars3.json
                       iovars4.json
                  );
    foreach my $filename (@filenames) {
        push @expected_ancillary, $data_directory.'/'.$filename;
    }
    push @expected_ancillary, $test_run_path.'/Metadata.xml';
    # note that .tiff files do not appear in the ancillary file list
    is_deeply(\@sorted_ancillary, \@expected_ancillary,
              'Found expected ancillary files');

    is($resultset->stock, 'stock_barcode_01234',
       'Found expected stock barcode');

    my $dt = DateTime->new(
        year   => 2016,
        month  => 10,
        day    => 4,
        hour   => 9,
        minute => 0,
    );
    is(DateTime->compare($resultset->run_date, $dt), 0,
       'Found expected run date');

    my $tmp = tempdir("BioNanoResultSetTest_XXXXXX", CLEANUP => 1);
    dies_ok(
        sub { WTSI::NPG::OM::BioNano::ResultSet->new( directory => $tmp) },
        "Dies with badly formatted directory name"
    );

    my $no_dir = $tmp."/foo";
    dies_ok(
        sub { WTSI::NPG::OM::BioNano::ResultSet->new( directory => $no_dir) },
        "Dies with nonexistent directory"
    );

}

1;
