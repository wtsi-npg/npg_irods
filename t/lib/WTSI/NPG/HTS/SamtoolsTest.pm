package WTSI::NPG::HTS::SamtoolsTest;

use strict;
use warnings;

use Log::Log4perl;
use Test::More;

use base qw(WTSI::NPG::HTS::Test);

Log::Log4perl::init('./etc/log4perl_tests.conf');

use WTSI::NPG::HTS::Samtools;

my $fixture_counter = 0;
my $data_path = './t/data/samtools';
my $data_file = '7915_5#1';
my $reference_file = 'test_ref.fa';
my $samtools = `which samtools`;

my $pid = $$;

sub require : Test(1) {
  require_ok('WTSI::NPG::HTS::Samtools');
}

sub iterate : Test(1) {
 SKIP: {
    if (not $samtools) { skip 'samtools executable not on the PATH', 1 }

    my @expected =
      ('@SQ	SN:test_ref0	LN:1000',
       '@SQ	SN:test_ref1	LN:1000',
       '@SQ	SN:test_ref2	LN:1000',
       '@SQ	SN:test_ref3	LN:1000',
       '@SQ	SN:test_ref4	LN:1000',
       '@SQ	SN:test_ref5	LN:1000',
       '@SQ	SN:test_ref6	LN:1000',
       '@SQ	SN:test_ref7	LN:1000',
       '@SQ	SN:test_ref8	LN:1000',
       '@SQ	SN:test_ref9	LN:1000',
       '@PG	ID:bwa	PN:emacs	CL:emacs 7915_5#1.sam ' .
       "$data_path/test_ref.fa");

    my @records;
    WTSI::NPG::HTS::Samtools->new
        (arguments => [q{-H}],
         path      => "$data_path/$data_file.sam")->iterate
           (sub {
              my ($record) = @_;
              push @records, $record;
          });

    is_deeply(\@records, \@expected, 'Iterated successfully') or
      diag explain \@records;
  } # SKIP samtools
}

sub collect : Test(1) {
 SKIP: {
    if (not $samtools) { skip 'samtools executable not on the PATH', 1 }

    my @expected =
      ('@PG	ID:bwa	PN:emacs	CL:emacs 7915_5#1.sam '.
       "$data_path/test_ref.fa");

    my @collected = WTSI::NPG::HTS::Samtools->new
      (arguments => [q{-H}],
       path      => "$data_path/$data_file.sam")->collect
         (sub {
            my ($record) = @_;
            $record =~ /^\@PG/;
          });

    is_deeply(\@collected, \@expected, 'Collected successfully') or
      diag explain \@collected;
  } # SKIP samtools
}

1;
