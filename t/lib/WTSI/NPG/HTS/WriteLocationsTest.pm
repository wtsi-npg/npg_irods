package WTSI::NPG::HTS::WriteLocationsTest;

use strict;
use warnings;

use English qw[-no_match_vars];
use File::Temp;
use JSON;
use Test::Deep;
use Test::Exception;
use Test::More;

use base qw[WTSI::NPG::HTS::Test];

use WTSI::NPG::HTS::WriteLocations;

my $data_path = 't/data/mlwh_json';
my $new_path = $data_path . '/new.json';
my $existing_path = $data_path . '/illumina_existing.json';

sub require: Test(1){
  require_ok('WTSI::NPG::HTS::WriteLocations');
}

sub build_locations : Test(2){
  my $new_locations = WTSI::NPG::HTS::WriteLocations->new(
    path => $new_path, platform_name => 'platform');
  is(@{$new_locations->locations}, 0,
    'Arrayref is empty where file does not exist');

  my $existing_locations = WTSI::NPG::HTS::WriteLocations->new(
    path => $existing_path, platform_name => 'platform');
  is_deeply($existing_locations->locations, read_json_content($existing_path)->{products},
    'Arrayref populated from file if it exists');
}

sub add_location_new : Test(7){
  my $expected_location_original = {
    id_product  => 'abcde12345',
    irods_root_collection => '/testZone/test/',
    irods_data_relative_path => 'data.cram',
    seq_platform_name => 'platform',
    pipeline_name => 'npg-prod'
  };
  my $expected_location_2 ={
    id_product  => 'fghij67890',
    irods_root_collection => '/testZone/test/',
    irods_data_relative_path => 'data2.cram',
    irods_secondary_data_relative_path => 'data2.alt',
    seq_platform_name => 'platform',
    pipeline_name => 'npg-prod'
  };
  my $expected_location_updated = {
    id_product  => 'abcde12345',
    irods_root_collection => '/testZone/test/',
    irods_data_relative_path => 'data.bam',
    seq_platform_name => 'platform',
    pipeline_name => 'npg-prod'
  };

  my $new_locations = WTSI::NPG::HTS::WriteLocations->new(
    path => $new_path, platform_name => 'platform');

  lives_ok{$new_locations->add_location(
    pid  => 'abcde12345',
    coll => '/testZone/test/',
    path => 'data.cram'
  )} 'Add location method runs without errors';

  is_deeply($new_locations->locations, [$expected_location_original],
    'Location added to empty location array');

  $new_locations->add_location(
    pid => 'fghij67890',
    coll => '/testZone/test',
    path => 'data2.cram',
    secondary_path => 'data2.alt');

  cmp_deeply($new_locations->locations,
    bag($expected_location_original, $expected_location_2),
    'Location added to populated array, collection corrected to include trailing slash');

  $new_locations->add_location(
    pid  => 'abcde12345',
    coll => '/testZone/test/',
    path => 'data.cram'
  );

  is (scalar @{$new_locations->locations}, 2, 'Correct length after identical hash is added');

  cmp_deeply($new_locations->locations,
    bag($expected_location_original, $expected_location_2),
    'No change to locations array when an identical location hash is added');

  $new_locations->add_location(
    pid  => 'abcde12345',
    coll => '/testZone/test/',
    path => 'data.bam');

  is (scalar @{$new_locations->locations}, 2,
    'Correct length after hash is updated with new values for an existing hash');

  cmp_deeply($new_locations->locations,
    bag($expected_location_updated, $expected_location_2),
    'Locations array updated when a different location hash with the same ' .
    'id_product and collection name is added');

}

sub add_location_existing: Test(2){
  my $expected_locations = read_json_content($existing_path)->{products};
  push @{$expected_locations}, {
    id_product  => 'abcde12345',
    irods_root_collection => '/testZone/test/',
    irods_data_relative_path => 'data.cram',
    seq_platform_name => 'platform',
    pipeline_name => 'npg-prod'
  };
  my $existing_locations = WTSI::NPG::HTS::WriteLocations->new(
    path => $existing_path, platform_name => 'platform');

  lives_ok{$existing_locations->add_location(
    pid  => 'abcde12345',
    coll => '/testZone/test/',
    path => 'data.cram'
  )} 'Location added to an array read from file without error';

  cmp_deeply($existing_locations->locations, bag(@{$expected_locations}),
    'Location correctly added to locations read from file');
}

sub write_locations : Test(1){
  my $expected_json = $data_path . '/pacbio.json';
  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $tmp_path = $tmpdir->dirname . '/mlwh.json';
  my $new_locations = WTSI::NPG::HTS::WriteLocations->new(
    path => $tmp_path, platform_name => 'pacbio');

  $new_locations->add_location(
    pid  => '40ff7f2193f3b515a6c69ab284622c935097a63ccbf3eaf09cc39b5ff44468af',
    coll => '/testZone/home/irods/RunPublisherTest.XXXXX.0/publish_files/2_B01/',
    path => 'lima_output.lbc12--lbc12.bam'
  );

  $new_locations->add_location(
    pid  => '6948c60e9ee9117255f1123e2c403013596339eda68b6b8e8867bc132023955d',
    coll => '/testZone/home/irods/RunPublisherTest.XXXXX.0/publish_files/2_B01/',
    path => 'lima_output.lbc5--lbc5.bam'
  );

  $new_locations->write_locations;

  is_deeply(read_json_content($tmp_path), read_json_content($expected_json),
    'written file has correct contents');

}

sub read_json_content {
  my ($path) = @_;

  open my $mlwh_json_fh, '<:encoding(UTF-8)', $path or die qq[could not open $path];

  my $json = decode_json(<$mlwh_json_fh>);
  close $mlwh_json_fh;
  return $json
}

sub set_destination {
  my ($json_hash, $temp_coll) = @_;
  foreach my $product (@{$json_hash->{products}}){
    $product->{irods_root_collection} =~ s|/testZone/home/irods/RunPublisherTest.XXXXX.0/|$temp_coll/|xms;
  }
  return $json_hash;
}
