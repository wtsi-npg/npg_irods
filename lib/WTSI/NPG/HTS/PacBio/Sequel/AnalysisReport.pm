package WTSI::NPG::HTS::PacBio::Sequel::AnalysisReport;

use namespace::autoclean;
use File::Basename;
use File::Spec::Functions qw[catfile catdir];
use IO::File;
use JSON;
use Moose;
use MooseX::StrictConstructor;
use Perl6::Slurp;
use Readonly;
use XML::LibXML;

use WTSI::NPG::HTS::PacBio::Sequel::Reportdata;

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::HTS::PathLister
       ];

our $VERSION = '';

# Report 
our $REPORT_NAME = 'merged_analysis_report.json';

# Deplex files
our $LIMA        = q{(lima.guess.txt|lima.summary.txt|lima.counts)};

# Combined metadata file inputs and outputs
our $OUTPUT_DIR  = 'outputs';
our $CCS_REPORT  = 'ccs.report.json';
our $CCS_PROCESS = 'ccs_processing.report.json';
our $CCS_FILES   = qq{($CCS_REPORT|$CCS_PROCESS)};
our $REPORTS     = 'reports';

has 'analysis_path' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'PacBio root analysis job path');

has 'runfolder_path' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'Primary analysis output path');

has 'meta_data' =>
  (isa           => 'WTSI::NPG::HTS::PacBio::Metadata',
   is            => 'ro',
   required      => 1,
   documentation => 'Meta data from file');

has 'report_file_name' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 0,
   default       => $REPORT_NAME,
   documentation => 'File name for merged report');

has 'output_dir' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 0,
   predicate     => 'has_output_dir',
   documentation => 'Optional output directory for report file');


=head2 generate_analysis_report

  Arg [1]    : None
  Example    : my ($report_file_path) = $report->generate_analysis_report
  Description: Generate a combined file from various ccs and deplexing files 
  Returntype : Str

=cut

sub generate_analysis_report {
  my ($self) = @_;

  my $output_dir  = $self->has_output_dir ? $self->output_dir :
    $self->runfolder_path;
  my $report_file = catfile($output_dir, $self->report_file_name);

  if(! -f $report_file){
    $self->_read_deplex_files();
    $self->_read_ccs_files();
    $self->_save_results($report_file);
  }
  return $self->report_file_name;
}


sub _read_deplex_files {
  my ($self) = @_;

  my @files = $self->list_directory
    ($self->runfolder_path, filter => $LIMA .q[$]);

  foreach my $file (@files) {
    my $file_contents = slurp $file;
    my $file_name     = fileparse($file);
    $self->_set_result($file_name => $file_contents);
  }
  return;
}

sub _read_ccs_files {
  my ($self) = @_;

  my @files = $self->list_directory(catdir($self->analysis_path, $OUTPUT_DIR),
    filter => $CCS_FILES .q[$]);

  if(@files >= 1){
    foreach my $file (@files) {
      my $file_name     = fileparse($file);
      my $file_contents = slurp $file;
      my $decoded       = decode_json($file_contents);
      if (defined $decoded->{'attributes'} ) {
        $self->_set_result($file_name => $decoded->{'attributes'});
      }
    }
  }
  return;
}


has '_result' =>
  (traits  => ['Hash'],
   isa     => 'HashRef',
   is      => 'ro',
   default => sub { {} },
   handles => {
      _set_result      => 'set',
      _get_result      => 'get',
      _get_result_keys => 'keys'});


sub _save_results {
  my ($self, $file) = @_;

  my %files;
  if($self->_get_result_keys >= 1){
    %files = map { $_ => $self->_get_result($_) } $self->_get_result_keys;
  }

  my $data = WTSI::NPG::HTS::PacBio::Sequel::Reportdata->new
    (meta_data => $self->meta_data,
     $REPORTS  => \%files );

  $self->debug("Writing merged analysis report JSON to '$file'");
  my $fh = IO::File->new($file,'>') or $self->logcroak("cant open $file");
  print $fh $data->freeze or $self->logcroak("cant write to $file");
  $fh->close or $self->logcroak("cannot close file $file");

  return;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::Sequel::AnalysisReport

=head1 DESCRIPTION

Combine various small analysis output files if they exist into a single json 
output file.

=head1 AUTHOR

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2020 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
