package WTSI::NPG::HTS::HTSFilePublisherTest;

use strict;
use warnings;

use File::Spec::Functions;
use File::Temp;
use Log::Log4perl;
use Test::More;

use base qw(WTSI::NPG::HTS::Test);

use WTSI::NPG::HTS::HTSFilePublisher;

Log::Log4perl::init('./etc/log4perl_tests.conf');

{
  package TestDB;
  use Moose;

  with 'npg_testing::db';
}

my $data_path = './t/data/htsfile_publisher';

my $qc_schema;
my $db_dir = File::Temp->newdir;
my $db_file = catfile($db_dir, 'npg_qc.db');
{
  # create_test_db produces warnings during expected use, which
  # appear mixed with test output in the terminal
  local $SIG{__WARN__} = sub { };
  $qc_schema = TestDB->new->create_test_db('npg_qc::Schema',
                                           './t/fixtures/npgqc', $db_file);
}

sub positions : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $archive_path =
    catfile($data_path, 'sequence/100818_IL32_05174/Latest_Summary/archive');
  my $pub = WTSI::NPG::HTS::HTSFilePublisher->new(archive_path => $archive_path,
                                                  file_format  => 'bam',
                                                  id_run       => 5174,
                                                  irods        => $irods,
                                                  npgqc_schema => $qc_schema);
  is_deeply($pub->positions, [1 .. 8], 'Found expected positions')
    or diag explain $pub->positions;
}

sub list_alignment_files : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $archive_path =
    catfile($data_path, 'sequence/100818_IL32_05174/Latest_Summary/archive');

  my $pub = WTSI::NPG::HTS::HTSFilePublisher->new(archive_path => $archive_path,
                                                  file_format  => 'bam',
                                                  id_run       => 5174,
                                                  irods        => $irods,
                                                  npgqc_schema => $qc_schema);
  my @expected_files =
    map { catfile($archive_path, $_) } ('5174_1.bam',
                                        '5174_1_human.bam',
                                        '5174_4.bam',
                                        'lane1/5174_1#0.bam',
                                        'lane2/5174_2#10.bam',
                                        'lane2/5174_2_nonhuman#9.bam');

  my $observed_files = $pub->list_alignment_files;
  is_deeply($observed_files, \@expected_files, "Found alignment files")
    or diag explain $observed_files;
}

sub collection : Test(2) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $archive_path =
    catfile($data_path, 'sequence/100818_IL32_05174/Latest_Summary/archive');

  my $pub1 = WTSI::NPG::HTS::HTSFilePublisher->new
    (archive_path => $archive_path,
     file_format  => 'bam',
     id_run       => 5174,
     irods        => $irods,
     npgqc_schema => $qc_schema);
  is($pub1->collection, '/seq/5174', 'Default collection');

  my $pub2 = WTSI::NPG::HTS::HTSFilePublisher->new
    (archive_path => $archive_path,
     collection   => '/a/b/c',
     file_format  => 'bam',
     id_run       => 5174,
     irods        => $irods,
     npgqc_schema => $qc_schema);
  is($pub2->collection, '/a/b/c', 'Custom collection');
}


# sub metadata : Test(1) {
#   my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);

#   my $archive_path =
#     catfile($data_path, 'sequence/100818_IL32_05174/Latest_Summary/archive');
#   my $pub = WTSI::NPG::HTS::HTSFilePublisher->new(archive_path => $archive_path,
#                                            id_run       => 5174,
#                                            irods        => $irods,
#                                            npgqc_schema => $qc_schema);

#   my @study_meta = $pub->make_study_metadata($pub->lims->children_ia->{1});
#   is_deeply(\@study_meta, [], 'Found expected study metadata')
#     or diag explain \@study_meta;
# }

1;
