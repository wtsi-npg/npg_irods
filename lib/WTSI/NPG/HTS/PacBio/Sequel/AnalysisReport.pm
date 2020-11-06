package WTSI::NPG::HTS::PacBio::Sequel::AnalysisReport;

use namespace::autoclean;
use File::Basename;
use File::Spec::Functions qw[catfile catdir];
use IO::File;
use Moose;
use MooseX::StrictConstructor;
use Perl6::Slurp;
use Readonly;
use XML::LibXML;

use WTSI::NPG::HTS::PacBio::Sequel::Reportdata;

Readonly::Scalar my $HUNDRED => 100;

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
our $FINAL_XML   = 'final.consensusreadset.xml';
our $EXT_RES     = 'pbbase:ExternalResource';
our $RES_VALUE   = 'PacBio.ConsensusReadFile.ConsensusReadBamFile';
our $RES_TYPE    = 'MetaType';
our $RES_PATH    = 'ResourceId';
our $CCS_REPORT  = 'ccs_report.txt';
our $MERGED_CCS  = 'merged_'. $CCS_REPORT;
our $CCS_AHEADER = 'ZMWs input          (A)';
our $CCS_CHEADER = 'ZMWs filtered       (C)';


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
  Description: Generate a combined report file with deplexing and ccs stats
               if available from up to ~100 small job output files.
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
    filter => $FINAL_XML .q[$]);

  if(@files == 1){
    my $ccs_counts;
    my @ccs_keys;

    my $dom = XML::LibXML->new->parse_file($files[0]);
    my $loc = $dom->getElementsByTagName($EXT_RES);
    foreach my $f (@{$loc}) {
      my $type = $f->getAttribute($RES_TYPE);
      my $file = $f->getAttribute($RES_PATH);
      if ($file && $type eq $RES_VALUE) {
        my $shard_dir = dirname($file);
        my @ccs = $self->list_directory($shard_dir, filter => $CCS_REPORT .q[$]);
        if (@ccs != 1) {
          $self->logcroak("Expect only one $CCS_REPORT file in $shard_dir");
        }
        my $fh = IO::File->new($ccs[0],'<') or $self->logcroak("cant open $ccs[0]");
        while (<$fh>) {
          if (m/\A([ \w \s \- ) (]+ \S) \s+ [:] \s+ (\d+)/mxs) {
              ! defined $ccs_counts->{$1} ? push @ccs_keys, $1 : undef;
              $ccs_counts->{$1} += $2;
          }
        }
        $fh->close or $self->logcroak("cannot close file $file");
      }
    }

    my $output;
    foreach my $key ( @ccs_keys ) {
      $output .=  sprintf q(%-25s), $key;
      if (exists $ccs_counts->{$key}) {
        my $value = $ccs_counts->{$key};
        if ($key eq $CCS_AHEADER) {
          $output .= qq(: $value\n);
        } else {
          my $denom = $key =~ /^ZMW/smx ? $ccs_counts->{$CCS_AHEADER} :
            $ccs_counts->{$CCS_CHEADER};
          $output .= sprintf qq(: %d (%0.2f%%)\n), $value, $HUNDRED * $value / $denom;
          if ($key eq $CCS_CHEADER) {
            $output .= qq(\nExclusive ZMW counts for (C):\n);
          }
        }
      }
    }
    $self->_set_result($MERGED_CCS => $output);
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
    (meta_data       => $self->meta_data,
     reports         => \%files );

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

CCS file merging based on a script from Shane McCarthy :
https://github.com/sanger-tol/mergePacBioCCSreports

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
