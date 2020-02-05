package WTSI::NPG::HTS::PacBio::Sequel::RunDeleteTest;

use strict;
use warnings;

use English qw[-no_match_vars];
use File::Path qw[make_path];
use File::Temp;
use Log::Log4perl;
use Test::More;

use base qw[WTSI::NPG::HTS::Test];

BEGIN {
  Log::Log4perl->init('./etc/log4perl_tests.conf');
}

use WTSI::NPG::HTS::PacBio::Sequel::RunDelete;

my $pid           = $PID;
my $test_counter  = 0;
my $data_path     = 't/data/pacbio/sequel';
my $tmp_dir       = File::Temp->newdir;

sub require : Test(1) {
  require_ok('WTSI::NPG::HTS::PacBio::Sequel::RunDelete');
}

sub delete_run : Test(2) {
   
   my $tmp_output_dir     = "$tmp_dir/rundeletefolder.$pid";
   make_path($tmp_output_dir);

   my $deleter = WTSI::NPG::HTS::PacBio::Sequel::RunDelete->new
       (runfolder_path => $tmp_output_dir);

   isa_ok($deleter, 'WTSI::NPG::HTS::PacBio::Sequel::RunDelete');  
   ok($deleter->delete_run(), "Deleted runfolder directory");
}

1;
