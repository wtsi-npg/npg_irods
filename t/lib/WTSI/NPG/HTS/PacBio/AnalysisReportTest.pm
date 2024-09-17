package WTSI::NPG::HTS::PacBio::AnalysisReportTest;

use strict;
use warnings;

use English qw[-no_match_vars];
use File::Spec::Functions;
use File::Temp;
use JSON;
use Log::Log4perl;
use Test::More;
use Test::Exception;

use base qw[WTSI::NPG::HTS::Test];

use WTSI::NPG::HTS::PacBio::AnalysisReport;
use WTSI::NPG::HTS::PacBio::MetaXMLParser;

BEGIN {
  Log::Log4perl->init('./etc/log4perl_tests.conf');
}
my $pid          = $PID;
my $test_counter = 0;
my $data_path    = 't/data/pacbio/analysis';

sub require : Test(1) {
  require_ok('WTSI::NPG::HTS::PacBio::AnalysisReport');
}

sub read_report_file : Test(2) {
  
  my $analysis_path  = "$data_path/0000003499";
  my $runfolder_path = "$analysis_path/cromwell-job/call-export_bam/execution";
  my $merged_file    = "$runfolder_path/merged_analysis_report.json";

  my $meta_file      = "$analysis_path/entry-points/2a711efd-0812-4a5f-b4a2-29714400611f.subreadset.xml";
  my $meta_data      = WTSI::NPG::HTS::PacBio::MetaXMLParser->new->parse_file
    ($meta_file, 'pbmeta:');

  my $rdo = WTSI::NPG::HTS::PacBio::Reportdata->load($merged_file);
  
  cmp_ok($rdo->created_at, 'eq', '2021-05-10T11:23:36', 'created_at is correct');
  is_deeply($rdo->meta_data, $meta_data, 'meta_data is correct');
}

sub create_report_file_1 : Test(3) {
  
  my $analysis_path  = "$data_path/0000002152";
  my $runfolder_path = "$analysis_path/cromwell-job/call-demultiplex_barcodes/demultiplex_barcodes/5df96c44/call-lima/execution/";
  my $report_file_1  = "$runfolder_path/merged_analysis_report.json";

  my $meta_file      = "$analysis_path/entry-points/7da072af-387d-49e8-8ee3-b0fa0c873fb7.subreadset.xml";
  my $meta_data      = WTSI::NPG::HTS::PacBio::MetaXMLParser->new->parse_file
    ($meta_file, 'pbmeta:');

  my $rdo1 = WTSI::NPG::HTS::PacBio::Reportdata->load($report_file_1);

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./report_tmp.XXXXXX");
  my @init_args = (
    analysis_path  => $analysis_path,
    runfolder_path => $runfolder_path,
    output_dir     => $tmpdir->dirname,
    meta_data      => $meta_data );

 
  my $report = WTSI::NPG::HTS::PacBio::AnalysisReport->new(@init_args);
  my $report_file_name_2 = $report->generate_analysis_report;
  my $report_file_2 = catfile($report->output_dir,$report_file_name_2);
  ok(-f $report_file_2, "[2] created report file exists");

  my $rdo2 = WTSI::NPG::HTS::PacBio::Reportdata->load($report_file_2);

  is_deeply($rdo1->meta_data, $rdo2->meta_data, '[1] created report meta_data is correct');
  is_deeply($rdo1->reports, $rdo2->reports, '[1] created report reports is correct');

}

sub create_report_file_2 : Test(3) {
  
  my $analysis_path  = "$data_path/0000003442";
  my $runfolder_path = "$analysis_path/cromwell-job/call-demultiplex_barcodes/call-lima/execution/";
  my $report_file_1  = "$runfolder_path/merged_analysis_report.json";

  my $meta_file      = "$analysis_path/entry-points/081746ec-5099-4efb-9702-0f97bf22dc59.subreadset.xml";
  my $meta_data      = WTSI::NPG::HTS::PacBio::MetaXMLParser->new->parse_file
    ($meta_file, 'pbmeta:');

  my $rdo1 = WTSI::NPG::HTS::PacBio::Reportdata->load($report_file_1);

  my $tmpdir = File::Temp->newdir(TEMPLATE => "./report_tmp.XXXXXX");
  my @init_args = (
    analysis_path  => $analysis_path,
    runfolder_path => $runfolder_path,
    output_dir     => $tmpdir->dirname,
    meta_data      => $meta_data );

 
  my $report = WTSI::NPG::HTS::PacBio::AnalysisReport->new(@init_args);
  my $report_file_name_2 = $report->generate_analysis_report;
  my $report_file_2 = catfile($report->output_dir,$report_file_name_2);
  ok(-f $report_file_2, "[3] created report file exists");

  my $rdo2 = WTSI::NPG::HTS::PacBio::Reportdata->load($report_file_2);

  is_deeply($rdo1->meta_data, $rdo2->meta_data, '[2] created report meta_data is correct');
  is_deeply($rdo1->reports, $rdo2->reports, '[2] created report reports is correct');

}

1;
