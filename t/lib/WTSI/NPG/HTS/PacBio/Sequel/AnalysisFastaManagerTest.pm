package WTSI::NPG::HTS::PacBio::Sequel::AnalysisFastaManagerTest;

use strict;
use warnings;

use English qw[-no_match_vars];
use File::Copy::Recursive qw(dircopy);
use File::Find;
use File::Spec::Functions;
use File::Temp;
use Log::Log4perl;
use Test::More;
use Test::Exception;

use base qw[WTSI::NPG::HTS::Test];

use WTSI::NPG::HTS::PacBio::Sequel::AnalysisFastaManager;
use WTSI::NPG::HTS::PacBio::Sequel::MetaXMLParser;

BEGIN {
  Log::Log4perl->init('./etc/log4perl_tests.conf');
}

my $pid          = $PID;
my $test_counter = 0;
my $data_path    = 't/data/pacbio/sequel_analysis';

sub require : Test(1) {
  require_ok('WTSI::NPG::HTS::PacBio::Sequel::AnalysisFastaManager');
}

sub make_loadable_files : Test(2) {

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $analysis_path = catdir($tmpdir->dirname, '0000004117');
  dircopy("$data_path/0000004117",$analysis_path) or die $!;
  chmod (0770, "$analysis_path") or die "Chmod 0770 directory failed : $!";

  my $meta_file = "$analysis_path/entry-points/222a1be3-85b1-12b3-b3bc-9742f8e22f95.consensusreadset.xml";
  my $meta_data = WTSI::NPG::HTS::PacBio::Sequel::MetaXMLParser->new->parse_file
    ($meta_file, 'pbmeta:');
  
  my $runfolder_path = "$analysis_path/cromwell-job/call-lima_isoseq/execution";

  my @init_args = (analysis_path  => $analysis_path,
                   runfolder_path => $runfolder_path,
                   meta_data      => $meta_data);

  my $iso = WTSI::NPG::HTS::PacBio::Sequel::AnalysisFastaManager->new(@init_args);
  my $is_success = $iso->make_loadable_files;
  cmp_ok($is_success, '==', 1, "make loadadble files succeeded");

  my @fasta;
  find( sub { push @fasta, $File::Find::name if -f && /\.fasta\.gz$/ }, $runfolder_path );
  cmp_ok(scalar @fasta, '==', 16, "correct number of fasta files found ". @fasta);

}

1;
