package WTSI::NPG::OM::BioNano::RunFinderTest;

use strict;
use warnings;

use base qw[WTSI::NPG::HTS::Test]; # FIXME better path for shared base

use DateTime;
use Test::More tests => 2;
use Test::Exception;
use File::Temp qw[tempdir];
use File::Touch;
use File::Spec::Functions qw[catdir rel2abs];

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::OM::BioNano::RunFinder'); }

use WTSI::NPG::OM::BioNano::RunFinder;

my $tmp;

my $dir_blue = 'blue_9999-12-01_23_50';
my $dir_green = 'green_9999-12-02_00_04';
my $dir_purple = 'purple_1234567890';
my $path_blue;
my $path_green;
my $path_purple;

sub make_fixture : Test(setup) {
    $tmp = tempdir('temp_bionano_runfinder_XXXXXX', CLEANUP => 1);
    # create some folders and change modification times with touch()
    $path_blue = rel2abs(catdir($tmp, $dir_blue));
    $path_green = rel2abs(catdir($tmp, $dir_green));
    $path_purple = rel2abs(catdir($tmp, $dir_purple));
    foreach my $dir ($path_blue, $path_green, $path_purple) {
        mkdir $dir;
    }
    my $day = 24*60*60;
    my $now = time();
    my $time1 = $now - 60 * $day;
    my $ref1 = File::Touch->new(atime => $time1, mtime => $time1);
    $ref1->touch($path_green);
    my $time2 =  $now - 30 * $day;
    my $ref2 = File::Touch->new(atime => $time2, mtime => $time2);
    $ref2->touch($path_blue);
    $ref2->touch($path_purple);
}

sub find : Test(1) {
    my $days_ago = 10; # number of days ago that the publication window ends
    my $days = 30; # number of days in publication window
    # 'green' is outside publication window
    # 'purple' has wrongly formatted name
    # so we expect to return only 'blue'
    my $finder = WTSI::NPG::OM::BioNano::RunFinder->new();
    my @dirs = $finder->find($tmp, $days_ago, $days);
    my @abs_dirs;
    foreach my $dir (@dirs) {
        push @abs_dirs, rel2abs($dir);
    }
    is_deeply(\@abs_dirs, [$path_blue,], 'Found one runfolder: '.$path_blue);
}

1;
