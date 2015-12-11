package WTSI::NPG::HTS::AncFileDataObjectTest;

use strict;
use warnings;

use File::Temp;
use List::AllUtils qw(each_array);
use Log::Log4perl;
use Test::More;

use base qw(WTSI::NPG::HTS::Test);

Log::Log4perl::init('./etc/log4perl_tests.conf');

use WTSI::NPG::HTS::AncFileDataObject;
use WTSI::NPG::iRODS;

{
  package TestDB;
  use Moose;

  with 'npg_testing::db';
}

my $test_counter = 0;
my $data_path = './t/data/anc_file_data_object';
my $fixture_path = "./t/fixtures";

my $formats = {bamcheck  => [q[]],
               bed       => ['.deletions', '.insertions', '.junctions'],
               seqchksum => [q[], '.sha512primesums512'],
               stats     => ['_F0x900', '_F0xB00'],
               txt       => ['_quality_cycle_caltable',
                             '_quality_cycle_surv',
                             '_quality_error']};

my @untagged_paths = ('/seq/17550/17550_3',
                      '/seq/17550/17550_3_human',
                      '/seq/17550/17550_3_nonhuman',
                      '/seq/17550/17550_3_yhuman',
                      '/seq/17550/17550_3_phix');
my @tagged_paths   = ('/seq/17550/17550_3#1',
                      '/seq/17550/17550_3#1_human',
                      '/seq/17550/17550_3#1_nonhuman',
                      '/seq/17550/17550_3#1_yhuman',
                      '/seq/17550/17550_3#1_phix');

sub id_run : Test(110) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  foreach my $format (sort keys %{$formats}) {
    foreach my $path (@tagged_paths, @untagged_paths) {
      foreach my $part (@{$formats->{$format}}) {
        my $full_path = $path . $part . ".$format";
        cmp_ok(WTSI::NPG::HTS::AncFileDataObject->new
               ($irods, $full_path)->id_run,
               '==', 17550, "$full_path id_run is correct");
      }
    }
  }
}

sub position : Test(110) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  foreach my $format (sort keys %{$formats}) {
    foreach my $path (@tagged_paths, @untagged_paths) {
      foreach my $part (@{$formats->{$format}}) {
        my $full_path = $path . $part . ".$format";
        cmp_ok(WTSI::NPG::HTS::AncFileDataObject->new
               ($irods, $full_path)->position,
               '==', 3, "$full_path position is correct");
      }
    }
  }
}

sub tag_index : Test(110) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  foreach my $format (sort keys %{$formats}) {
    foreach my $path (@tagged_paths) {
      foreach my $part (@{$formats->{$format}}) {
        my $full_path = $path . $part . ".$format";
        cmp_ok(WTSI::NPG::HTS::AncFileDataObject->new
               ($irods, $full_path)->tag_index,
               '==', 1, "$full_path tag_index is correct");
      }
    }
  }

  foreach my $format (sort keys %{$formats}) {
    foreach my $path (@untagged_paths) {
      foreach my $part (@{$formats->{$format}}) {
        my $full_path = $path . $part . ".$format";
        is(WTSI::NPG::HTS::AncFileDataObject->new
           ($irods, $full_path)->tag_index, undef,
           "$full_path tag_index 'undef' is correct");
      }
    }
  }
}

sub align_filter : Test(110) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  foreach my $format (sort keys %{$formats}) {
    foreach my $path (@tagged_paths, @untagged_paths) {
      foreach my $part (@{$formats->{$format}}) {
        my $full_path = $path . $part . ".$format";
        my ($expected) = $path =~ m{_((human|nonhuman|yhuman|phix))};
        my $exp_str = defined $expected ? $expected : 'undef';

        my $align_filter = WTSI::NPG::HTS::AncFileDataObject->new
          ($irods, $full_path)->align_filter;

        is($align_filter, $expected,
           "$full_path align filter '$exp_str' is correct");
      }
    }
  }
}

1;
