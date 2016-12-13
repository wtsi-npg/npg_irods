package WTSI::NPG::OM::BioNano::ResultSetTest;

use strict;
use warnings;

use Cwd qw[abs_path];
use File::Temp qw[tempdir];

use base qw[WTSI::NPG::HTS::Test]; # FIXME better path for shared base

use Test::More tests => 12;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::OM::BioNano::ResultSet'); }

use WTSI::NPG::OM::BioNano::ResultSet;

my $data_path = './t/data/bionano/';
my $runfolder_name = 'sample_barcode_01234_2016-10-04_09_00';
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

sub construction : Test(11) {

    my $resultset = WTSI::NPG::OM::BioNano::ResultSet->new(
        directory => $test_run_path,
    );
    ok($resultset, "ResultSet created");
    is(abs_path($resultset->data_directory),
       abs_path($test_run_path.'/Detect Molecules'),
       "Found expected data directory");

    my $bnx_path = $resultset->bnx_path;
    my $expected_bnx = $resultset->data_directory.'/Molecules.bnx';
    is(abs_path($bnx_path), abs_path($expected_bnx),
       "Found expected filtered molecules path");

    my $raw_bnx_path = $resultset->raw_bnx_path;
    my $expected_raw_bnx = $resultset->data_directory.'/RawMolecules.bnx';
    is(abs_path($raw_bnx_path), abs_path($expected_raw_bnx),
       "Found expected raw molecules path");

    my $bnx;
    lives_ok(sub { $bnx = $resultset->bnx_file(); },
             'Found BNX file object');

    is($bnx->chip_id(), '20000,10000,1/1/2015,987654321',
       'Found expected chip ID from BNX file');

    my $ancillary = $resultset->ancillary_files;
    is(scalar @{$ancillary}, 6, "Found 6 ancillary files");

    is($resultset->sample, 'sample_barcode_01234',
       'Found expected sample barcode');

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
