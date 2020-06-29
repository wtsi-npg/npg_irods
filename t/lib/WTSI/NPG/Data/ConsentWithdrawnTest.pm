package WTSI::NPG::Data::ConsentWithdrawnTest;

use strict;
use warnings;
use English qw(-no_match_vars);
use Test::More;
use Test::Exception;
use File::Temp qw( tempdir );
use Log::Log4perl;
use Test::MockObject;

use WTSI::NPG::iRODS;

use base 'WTSI::NPG::HTS::Test';

Log::Log4perl::init('./etc/log4perl_tests.conf');

my $pid          = $PID;
my $test_counter = 0;
my $collection;

sub setup_test : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  $collection =
    $irods->add_collection("RunPublisherTest.$pid.$test_counter");
  $test_counter++;
}

sub teardown_test : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  $irods->remove_collection($collection);
}

sub object_and_query_creation : Test(6) {

  use_ok 'WTSI::NPG::Data::ConsentWithdrawn';

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $cw = WTSI::NPG::Data::ConsentWithdrawn->new(irods => $irods);
  isa_ok($cw, 'WTSI::NPG::Data::ConsentWithdrawn');

  throws_ok { WTSI::NPG::Data::ConsentWithdrawn->new() }
    qr/Attribute \(irods\) is required/,
    'constructor should have irods attr. defined';

  my $query = q{"%s/%s" "select COLL_NAME, DATA_NAME where } .
    q{META_DATA_ATTR_NAME = 'sample_consent_withdrawn' and } .
    q{META_DATA_ATTR_VALUE = '1' and DATA_NAME not like '%header.bam%'"};

  is($cw->_iquery, q{iquest --no-page } . $query,
    'default iquery, no zone');

  $cw = WTSI::NPG::Data::ConsentWithdrawn->new(irods => $irods);
  is($cw->_iquery, q{iquest --no-page } . $query,
    'iquery with zone set');

  $cw = WTSI::NPG::Data::ConsentWithdrawn->new(irods      => $irods,
                                               zone       => 'seq',
                                               collection => '/some/other');
  is($cw->_iquery, q{iquest --no-page -z seq } . substr($query, 0, -1) .q{ and COLL_NAME like '/some/other%'"},
    'iquery with both zone and collection set');
}

sub permissions : Test(21) {

  my $mock = Test::MockObject->new();
  $mock->fake_new( 'MIME::Lite' );
  $mock->set_true('send');

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my @data_files = qw /1.bam 2.bam 3.bam 5.cram 6.cram 7.cram 9.cram/;
  my @files = @data_files;
  push @files, qw /1.bai 3.bai 4.bai 5.cram.crai 7.cram.crai 9.cram.crai
                   10.fastq 11.cram.crai/;
  my $tdir = tempdir( CLEANUP => 1 );
  foreach my $file (@files) {
    my $source = "$tdir/$file";
    `touch $source`;
    my $target = "$collection/$file";
    $irods->add_object($source, $target);
  }

  my $cw = WTSI::NPG::Data::ConsentWithdrawn->new(
    irods => $irods, collection => $collection);
  is_deeply($cw->_files, [], 'no data files are found');
  is_deeply($cw->_new_files, [], 'nothing to do');
  lives_ok {$cw->process} q[process when nothing to do];

  shift @data_files;
  pop @data_files;

  for my $file ( (map {"$collection/$_"} (@data_files, qw/10.fastq 11.cram.crai/)) ) {
    $irods->add_object_avu($file, q{sample_consent_withdrawn}, 1);
  }
  for my $file ( (map {"$collection/$_"} qw/3.bam 7.cram/) ) {
    $irods->add_object_avu($file, q{sample_consent_withdrawn_email_sent}, 1);
  }

  my @files_with_extra_permissions = map {"$collection/$_"} qw/5.cram 5.cram.crai/;
  for my $file (@files_with_extra_permissions) {
    $irods->set_object_permissions('read', 'public', $file);
    is ($irods->get_object_permissions($file), 2, "two sets of permissions for $file");
  }

  my @to_do   = map { "$collection/$_" } qw/2.bam 5.cram 6.cram/;
  @data_files = map { "$collection/$_" } @data_files;

  $cw = WTSI::NPG::Data::ConsentWithdrawn->new(
    dry_run => 1, irods => $irods, collection => $collection);
  is_deeply($cw->_files, \@data_files, 'all data with consent withdrawn are found');
  is_deeply($cw->_new_files, \@to_do, 'pending processing files are found');
  lives_ok {$cw->process} 'dry run processing data';

  $cw = WTSI::NPG::Data::ConsentWithdrawn->new(irods => $irods, collection => $collection);
  is_deeply($cw->_files, \@data_files, 'all data with consent withdrawn are found');
  is_deeply($cw->_new_files, \@to_do, 'pending processing files are found');
  lives_ok {$cw->process} 'no error processing data';

  for my $file (@files_with_extra_permissions) {
    my @permissions = $irods->get_object_permissions($file);
    is (@permissions, 1, "only one set permissions is left for $file");
    is ($permissions[0]->{level}, 'own', 'the only permission is for the owner');
  }
  
  for my $file (@to_do) {
    my @meta =
      grep { $_->{value} == 1 }
      grep { $_->{attribute} eq q{sample_consent_withdrawn_email_sent} }
      $irods->get_object_meta($file);
    is (scalar @meta, 1, 'sample_consent_withdrawn_email_sent flag set');
  }
  
  $cw = WTSI::NPG::Data::ConsentWithdrawn->new(
    dry_run => 1, irods => $irods, collection => $collection);
  ok(!@{$cw->_new_files}, 'no files to process');
  lives_ok {$cw->process} 'dry run processing when no eligible files exist';

  $cw = WTSI::NPG::Data::ConsentWithdrawn->new(
    dry_run => 1, irods => $irods, collection => '/some/collection');
  ok(!@{$cw->_new_files}, 'no files to process in a collection that does not exist');
}

1;
