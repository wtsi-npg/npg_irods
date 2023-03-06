package WTSI::NPG::HTS::LocationWriterTest;

use strict;
use warnings;

use English qw[-no_match_vars];
use File::Temp;
use JSON;
use Log::Log4perl;
use Test::Deep;
use Test::Exception;
use Test::More;

use base qw[WTSI::NPG::HTS::Test];

use WTSI::NPG::HTS::LocationWriter;


Log::Log4perl::init('./etc/log4perl_tests.conf');

my $data_path = 't/data/mlwh_json';
my $new_path = $data_path . '/new.json';
my $existing_path = $data_path . '/illumina_existing.json';

my $expected_existing_locations = {
      "/testZone/home/irods/RunPublisherTest.XXXXX.0/publish_entire_mlwh/Data/Intensities/BAM_basecalls_20151214-085833/no_cal/archive/\0" .
        '7382ff198a7321eadcea98bb39ade23749b3bace2874bbaced29789dbcd987659' => 'test.cram',
      "/testZone/home/irods/RunPublisherTest.XXXXX.0/publish_entire_mlwh/Data/Intensities/BAM_basecalls_20151214-085833/no_cal/archive/\0" .
        '98441df9e535436533620dcba86eef653d5749c546eb218dc9e2f7c587cec272' => '18448_1.cram',
      "/testZone/home/irods/RunPublisherTest.XXXXX.0/publish_entire_mlwh/Data/Intensities/BAM_basecalls_20151214-085833/no_cal/archive/\0" .
        '976dc767037549da1c8cc66c56379ee5c04403212bf82353646cbd6f880b83e7' => '18448_2.cram',
      "/testZone/home/irods/RunPublisherTest.XXXXX.0/publish_entire_mlwh/Data/Intensities/BAM_basecalls_20151214-085833/no_cal/archive/\0" .
        '38fbf8d4e04c677233464d8927c204e174a01d8ed997f4d1590cf1747e3f8f4e' => '18448_3.cram',
      "/testZone/home/irods/RunPublisherTest.XXXXX.0/publish_entire_mlwh/Data/Intensities/BAM_basecalls_20151214-085833/no_cal/archive/\0" .
        '6b54ffeb474f22eaff2292f017b22061e0ca7a946590b39a0cc5be66e6b72492' => '18448_4.cram',
      "/testZone/home/irods/RunPublisherTest.XXXXX.0/publish_entire_mlwh/Data/Intensities/BAM_basecalls_20151214-085833/no_cal/archive/\0" .
        'cffb91c18f6cd7c0817390fb6abf2b29db813e44719022f590f20d773731224a' => '18448_5.cram',
      "/testZone/home/irods/RunPublisherTest.XXXXX.0/publish_entire_mlwh/Data/Intensities/BAM_basecalls_20151214-085833/no_cal/archive/\0" .
        '411f5591390f380585535a955fede5bfc92b7eed8d2d79fc8352599cf3b187b8' => '18448_6.cram',
      "/testZone/home/irods/RunPublisherTest.XXXXX.0/publish_entire_mlwh/Data/Intensities/BAM_basecalls_20151214-085833/no_cal/archive/\0" .
        'd030d4ca2ffdee37359911653f1ddfb593af48d8945d039e6d3af960cde882b7' => '18448_7.cram',
      "/testZone/home/irods/RunPublisherTest.XXXXX.0/publish_entire_mlwh/Data/Intensities/BAM_basecalls_20151214-085833/no_cal/archive/\0" .
        '9f5f842c658619a3c38e221eb71bb620fdd2a9a1220df7aae5902e804a83a08d' => '18448_8.cram'};

sub require: Test(1){
  require_ok('WTSI::NPG::HTS::LocationWriter');
}

sub build_locations : Test(2){
  my $new_locations = WTSI::NPG::HTS::LocationWriter->new(
    path => $new_path, platform_name => 'platform');
  is(%{$new_locations->locations}, 0,
    'Hashref is empty where file does not exist');

  my $existing_locations = WTSI::NPG::HTS::LocationWriter->new(
    path => $existing_path, platform_name => 'platform');
  is_deeply($existing_locations->locations, $expected_existing_locations,
    'Hashref populated from file if it exists');
}

sub add_location_new : Test(5){
  my $expected_location_original = {
    "/testZone/test/\0" . 'abcde12345' => 'data.cram'
  };
  my $expected_location_2 = {
    "/testZone/test/\0" . 'abcde12345' => 'data.cram',
    "/testZone/test/\0" . 'fghij67890' => "data2.cram\0" . 'data2.alt'
  };
  my $expected_location_updated = {
    "/testZone/test/\0" . 'abcde12345' => 'data.bam',
    "/testZone/test/\0" . 'fghij67890' => "data2.cram\0" . 'data2.alt'
  };

  my $new_locations = WTSI::NPG::HTS::LocationWriter->new(
    path => $new_path, platform_name => 'platform');

  lives_ok{$new_locations->add_location(
    pid  => 'abcde12345',
    coll => '/testZone/test/',
    path => 'data.cram'
  )} 'Add location method runs without errors';

  is_deeply($new_locations->locations, $expected_location_original,
    'Location added to empty location hash');

  $new_locations->add_location(
    pid => 'fghij67890',
    coll => '/testZone/test',
    path => 'data2.cram',
    secondary_path => 'data2.alt');

  is_deeply($new_locations->locations, $expected_location_2,
    'Location added to populated hash, collection corrected to include trailing slash');

  $new_locations->add_location(
    pid  => 'abcde12345',
    coll => '/testZone/test/',
    path => 'data.cram'
  );

  is_deeply($new_locations->locations, $expected_location_2,
    'No change to locations hash when an identical location hash is added');

  $new_locations->add_location(
    pid  => 'abcde12345',
    coll => '/testZone/test/',
    path => 'data.bam');

  is_deeply($new_locations->locations, $expected_location_updated,
    'Locations hash updated when a different location hash with the same ' .
    'id_product and collection name is added');

}

sub add_location_existing: Test(2){
  my %expected_locations = %{$expected_existing_locations};
  $expected_locations{"/testZone/test/\0" . 'abcde12345'} = 'data.cram';
  my $existing_locations = WTSI::NPG::HTS::LocationWriter->new(
    path => $existing_path, platform_name => 'platform');

  lives_ok{$existing_locations->add_location(
    pid  => 'abcde12345',
    coll => '/testZone/test/',
    path => 'data.cram'
  )} 'Location added to an hash read from file without error';

  is_deeply($existing_locations->locations, \%expected_locations,
    'Location correctly added to locations read from file');
}

sub write_locations : Test(1){
  my $expected_json = $data_path . '/pacbio.json';
  my $tmpdir = File::Temp->newdir(TEMPLATE => "./batch_tmp.XXXXXX");
  my $tmp_path = $tmpdir->dirname . '/mlwh.json';
  my $new_locations = WTSI::NPG::HTS::LocationWriter->new(
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

  is_deeply( read_json_content($tmp_path), read_json_content($expected_json), 'written file has correct contents');

}

sub read_json_content {
  my ($path) = @_;

  open my $mlwh_json_fh, '<:encoding(UTF-8)', $path or die qq[could not open $path];

  my $json = decode_json(<$mlwh_json_fh>);
  close $mlwh_json_fh;

  @{$json->{products}} = sort             # sort so that files with the same
  {$a->{id_product} cmp $b->{id_product}} # contents are always equal
    @{$json->{products}};
  return $json
}

sub set_destination {
  my ($json_hash, $temp_coll) = @_;
  foreach my $product (@{$json_hash->{products}}){
    $product->{irods_root_collection} =~ s|/testZone/home/irods/RunPublisherTest.XXXXX.0/|$temp_coll/|xms;
  }
  return $json_hash;
}

