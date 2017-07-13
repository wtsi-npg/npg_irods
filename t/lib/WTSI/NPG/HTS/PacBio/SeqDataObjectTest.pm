package WTSI::NPG::HTS::PacBio::SeqDataObjectTest;

use strict;
use warnings;

use Carp;
use English qw[-no_match_vars];
use Log::Log4perl;
use Test::More;

use base qw[WTSI::NPG::HTS::Test];

Log::Log4perl::init('./etc/log4perl_tests.conf');

use WTSI::NPG::HTS::PacBio::SeqDataObject;
use WTSI::NPG::HTS::PacBio::Metadata;
use WTSI::NPG::iRODS;

{
  package TestAnnotator;
  use Moose;

  with 'WTSI::NPG::HTS::PacBio::Annotator';
}

my $pid          = $PID;
my $test_counter = 0;
my $data_path    = './t/data/pacbio_seq_data_object';

my $irods_tmp_coll;

my $file1 = 'm54097_161207_133626.subreads.bam'; ## restricted
my $file2 = 'm54097_161031_165824.subreads.bam'; ## not restricted

sub setup_test : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  $irods_tmp_coll =
    $irods->add_collection("PacBio::SeqDataObjectTest.$pid.$test_counter");
  $test_counter++;

  $irods->put_collection($data_path, $irods_tmp_coll);

  _setup_file($irods,"$irods_tmp_coll/pacbio_seq_data_object/$file1");
  _setup_file($irods,"$irods_tmp_coll/pacbio_seq_data_object/$file2",1);
}

sub _setup_file {
  my($irods,$file,$dont_restrict) = @_;

  my $obj = WTSI::NPG::HTS::PacBio::SeqDataObject->new($irods, "$file");
  
  my $meta = WTSI::NPG::HTS::PacBio::Metadata->new(
      run_name => 'test', well_name => 'A01', cell_index => 1,
      collection_number => 1, sample_name => 'test', 
      instrument_name => 'SQ54097', file_path => $file,);

  my @avus;
  push @avus, TestAnnotator->new->make_primary_metadata($meta,$dont_restrict);

  foreach my $avu (@avus) {
      my $attribute = $avu->{attribute};
      my $value     = $avu->{value};
      my $units     = $avu->{units};
      $obj->supersede_avus($attribute, $value, $units);
  }

  return();
}

sub teardown_test : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  $irods->remove_collection($irods_tmp_coll);

}

sub require : Test(1) {
  require_ok('WTSI::NPG::HTS::PacBio::SeqDataObject');

}

sub is_restricted_access : Test(2) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $path1 = "$irods_tmp_coll/pacbio_seq_data_object/$file1";
  my $obj1 = WTSI::NPG::HTS::PacBio::SeqDataObject->new($irods, $path1);    
  ok($obj1->is_restricted_access, "$path1 is restricted_access");
 
  my $path2 = "$irods_tmp_coll/pacbio_seq_data_object/$file2";
  my $obj2 = WTSI::NPG::HTS::PacBio::SeqDataObject->new($irods, $path2);    
  ok(! $obj2->is_restricted_access, "$path2 is not restricted_access");

}

1;
