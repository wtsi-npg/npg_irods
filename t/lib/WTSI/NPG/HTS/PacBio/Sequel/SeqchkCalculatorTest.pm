package WTSI::NPG::HTS::PacBio::Sequel::SeqchkCalculatorTest;

use strict;
use warnings;

use English qw[-no_match_vars];
use File::Copy::Recursive qw(dircopy);
use File::Spec::Functions qw[catdir catfile];
use File::Temp;
use Log::Log4perl;
use Readonly;
use Test::More;

use base qw[WTSI::NPG::HTS::Test];

Readonly::Scalar my $SEQUENCE_SEQCHKSUM =>
  $WTSI::NPG::HTS::PacBio::Sequel::SeqchkCalculator::SEQCHKSUM_SUFFIX;


BEGIN {
  Log::Log4perl->init('./etc/log4perl_tests.conf');
}

use WTSI::NPG::HTS::PacBio::Sequel::SeqchkCalculator;

my $pid           = $PID;
my $test_counter  = 0;
my $data_path     = 't/data/pacbio/sequel';
my $tmp_dir       = File::Temp->newdir;

my $bamseqchksum_available = `which bamseqchksum`;

sub require : Test(1) {
  require_ok('WTSI::NPG::HTS::PacBio::Sequel::SeqchkCalculator');
}

sub calculate_seqchksum : Test(2) {
  SKIP: {
    if (not $bamseqchksum_available) {
      skip 'bamseqchksum executable not on the PATH', 2;
    }

    ## create tmp runfolder which will be deleted later
    my $run_name   = 'r64016e_20220316_164414';
    my $well       = '1_A01';
    my $bamfile    = 'm64016e_220316_165505.reads.bam';
    my $seqchkfile = 'm64016e_220316_165505.' . $SEQUENCE_SEQCHKSUM;
 
    my $data_path  = catdir('t/data/pacbio/sequel', $run_name, $well);

    my $runfolder_path = catdir($tmp_dir,$run_name);
    mkdir $runfolder_path;
    my $runfolder_data = catdir($runfolder_path,$well);
    mkdir $runfolder_data;
    dircopy($data_path,$runfolder_data) or die $!;

    chmod (0770, $runfolder_path) or die "Chmod 0770 directory $runfolder_path failed : $!";
    chmod (0770, $runfolder_data) or die "Chmod 0770 directory $runfolder_data failed : $!";

    my $ifile = catfile($runfolder_data,$bamfile);
    my $ofile = catfile($runfolder_data,$seqchkfile);

    my $seqchk = WTSI::NPG::HTS::PacBio::Sequel::SeqchkCalculator->new
      (input_file => $ifile, output_file => $ofile);

    isa_ok($seqchk, 'WTSI::NPG::HTS::PacBio::Sequel::SeqchkCalculator');

    my $num_errors = $seqchk->calculate_seqchksum;
    cmp_ok($num_errors, '==', 0, 'No error calculating seqchksum');
    
  };
};

1;
