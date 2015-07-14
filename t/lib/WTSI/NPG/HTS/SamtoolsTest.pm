
package WTSI::NPG::HTS::SamtoolsTest;

use strict;
use warnings;

use Log::Log4perl;
use Test::More tests => 4;

use base qw(Test::Class);

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::HTS::Samtools') }

use WTSI::NPG::HTS::Samtools;

my $fixture_counter = 0;
my $data_path = './t/data';
my $data_file = '1234_5#6';
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
       '@PG	ID:bwa	PN:emacs	CL:emacs 1234_5#6.sam ./t/data/test_ref.fa');

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
  };
}

sub collect : Test(1) {
 SKIP: {
    if (not $samtools) { skip 'samtools executable not on the PATH', 1 }

    my @expected =
      ('@PG	ID:bwa	PN:emacs	CL:emacs 1234_5#6.sam ./t/data/test_ref.fa');

    my @collected = WTSI::NPG::HTS::Samtools->new
      (arguments => [q{-H}],
       path      => "$data_path/$data_file.sam")->collect
         (sub {
            my ($record) = @_;
            $record =~ /^\@PG/;
          });

    is_deeply(\@collected, \@expected, 'Collected successfully') or
      diag explain \@collected;
  };
}

1;
