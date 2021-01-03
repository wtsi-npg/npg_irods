package WTSI::NPG::HTS::PacBio::Sequel::ImageArchiveTest;

use strict;
use warnings;

use Data::Dump qw(pp);
use Digest::MD5;
use English qw[-no_match_vars];
use File::Spec::Functions;
use File::Temp qw[tempdir];
use Log::Log4perl;
use Test::More;
use Test::Exception;
use URI;

use base qw[WTSI::NPG::HTS::Test];

use WTSI::DNAP::Utilities::Runnable;
use WTSI::NPG::HTS::PacBio::Sequel::ImageArchive;
use WTSI::NPG::HTS::PacBio::Sequel::MetaXMLParser;

BEGIN {
  Log::Log4perl->init('./etc/log4perl_tests.conf');
}

my $pid          = $PID;
my $test_counter = 0;
my $data_path    = 't/data/pacbio/sequel_analysis';

{
  package TestAPIClient;
  use Moose;

  extends 'WTSI::NPG::HTS::PacBio::Sequel::APIClient';
  override 'query_dataset_reports' => sub { 
      my $test_response = 
[
  {
    reportTypeId=> 'sl_import_subreads.report_adapters',
    dataStoreFile=> {
      sourceId=> 'sl_import_subreads.report_adapters',
      isActive=> 'true',
      createdAt=> '2020-12-28T16::36::56.212Z',
      modifiedAt=> '2020-12-28T16::36::56.212Z',
      name=> 'Adapter Report',
      fileTypeId=> 'PacBio.FileTypes.JsonReport',
      path=> 't/data/pacbio/sequel_analysis/0000003462/cromwell-job/call-sl_dataset_reports/sl_dataset_reports/75cf695b/call-import_dataset_reports/execution/adapter.report.json',
      description=> 'PacBio Report adapter_xml_report (7e21ed06-4cf8-45f1-ba5c-e5f20c8d11c4)',
      uuid=> '7e21ed06-4cf8-45f1-ba5c-e5f20c8d11c4',
      fileSize=> 835,
      importedAt=> '2020-12-28T16::36::56.225Z'
    }
  },
  {
    reportTypeId=> 'sl_import_subreads.report_control',
    dataStoreFile=> {
      sourceId=> 'sl_import_subreads.report_control',
      isActive=> 'true',
      createdAt=> '2020-12-28T16::36::56.213Z',
      modifiedAt=> '2020-12-28T16::36::56.213Z',
      name=> 'Control Report',
      fileTypeId=> 'PacBio.FileTypes.JsonReport',
      path=> 't/data/pacbio/sequel_analysis/0000003462/cromwell-job/call-sl_dataset_reports/sl_dataset_reports/75cf695b/call-import_dataset_reports/execution/control.report.json',
      description=> 'PacBio Report control (07dcba58-8da1-462a-b6ce-f5647a85af99)',
      uuid=> '07dcba58-8da1-462a-b6ce-f5647a85af99',
      fileSize=> 2166,
      importedAt=> '2020-12-28T16::36::56.225Z'
    }
  }
];
      return $test_response; 
  }
}


sub require : Test(1) {
  require_ok('WTSI::NPG::HTS::PacBio::Sequel::ImageArchive');
}

sub create_image_archive : Test(2) {

  my $run_name  = 'r54097_20170727_165601';
  my $well      = '1_A02';

  my $data_path = catdir('t/data/pacbio/sequel', $run_name, $well);

  my $metafile  = catfile($data_path,'m54097_170727_170646.subreadset.xml');
  my $metadata  = WTSI::NPG::HTS::PacBio::Sequel::MetaXMLParser->new->parse_file($metafile);

  my $client    = TestAPIClient->new();
  my $tmpdir    = tempdir(CLEANUP => 1);

  my @init_args = (api_client   => $client,
                   archive_name => $metadata->movie_name,
                   dataset_id   => $metadata->subreads_uuid,
                   output_dir   => $tmpdir);

  my $ia = WTSI::NPG::HTS::PacBio::Sequel::ImageArchive->new(@init_args);
  my $archive_file = $ia->generate_image_archive;
  ok(-f $archive_file, "created archive file exists");

  my $cmd = qq[tar tvf $archive_file | wc -l];
  my $run =  WTSI::DNAP::Utilities::Runnable->new
      (executable => '/bin/bash', arguments  => ['-c', $cmd])->run;
  my $count = ${$run->stdout};
  ok(($count -1) == 6, "created archive file contains correct file count");
}

1;
