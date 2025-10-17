package WTSI::NPG::HTS::PacBio::AnalysisPublisher;

use namespace::autoclean;
use English qw[-no_match_vars];
use File::Basename;
use File::Spec::Functions qw[catdir];
use List::AllUtils qw[any];
use JSON;
use Moose;
use MooseX::StrictConstructor;
use Perl6::Slurp;
use Readonly;

use WTSI::NPG::HTS::PacBio::AnalysisReport;
use WTSI::NPG::HTS::PacBio::MetaXMLParser;
use WTSI::NPG::HTS::PacBio::Product;

with qw[
         WTSI::NPG::HTS::PacBio::PublisherBase
         WTSI::NPG::HTS::PacBio::AnalysisPublisherBase
       ];

our $VERSION = '';

# Sequence and index file suffixes
our $SEQUENCE_FILE_FORMAT   = 'bam';
our $SEQUENCE_FASTA_FORMAT  = 'fasta.gz';
our $SEQUENCE_INDEX_FORMAT  = 'pbi';

# Metadata related
our $METADATA_FORMAT = 'xml';
our $METADATA_SET    = q{(subreadset|consensusreadset)};

# Generic moviename file prefix
our $MOVIENAME_PATTERN = 'm[0-9a-z]+_\d+_\d+';

# Additional sequence filenames permitted for loading 
our @FNAME_PERMITTED    = qw[fail_reads removed ccs hifi_reads fl_transcripts sequencing_control.subreads unbarcoded];
our @FNAME_NON_DEPLEXED = qw[unassigned removed sequencing_control.subreads unbarcoded];
our @FNAME_FAILED       = qw[fail_reads];

# Data processing level
our $DATA_LEVEL = 'secondary';

# If deplexed - minimum deplexed percentage to load
Readonly::Scalar my $HUNDRED       => 100;
Readonly::Scalar my $MIN_BARCODED  => 0.8;
Readonly::Scalar my $BARCODE_FIELD => 'Percent Barcoded Reads';
Readonly::Scalar my $REPORT_TITLE  =>
  $WTSI::NPG::HTS::PacBio::AnalysisReport::REPORTS;
Readonly::Scalar my $LIMA_SUMMARY  => 'lima.summary.txt';


has 'movie_pattern' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 0,
   default       => $MOVIENAME_PATTERN,
   documentation => 'Set movie name pattern.',);


=head2 publish_files

  Example    : my ($num_files, $num_published, $num_errors) =
                 $pub->publish_files()
  Description: Publish all files for an analysis jobs to iRODS. Return
               the number of files, the number published and the number
               of errors.
  Returntype : Array[Int]

=cut

sub publish_files {
  my ($self) = @_;

  my ($num_files, $num_processed, $num_errors) = (0, 0, 0);

  my $seq_files = $self->list_files
    ($SEQUENCE_FILE_FORMAT . q[$], $self->is_oninstrument);

  if (defined $seq_files->[0] && @{$self->smrt_names} == 1) {

    my $qc_fail = $self->_basic_qc();
    if ($qc_fail) {
      $self->logcroak('Skipping ', $self->analysis_path,
                      ' : QC check failed');
    }

    my ($nfb, $npb, $neb) = $self->publish_sequence_files
        ($SEQUENCE_FILE_FORMAT);
    my ($nfp, $npp, $nep) = $self->publish_non_sequence_files
        ($SEQUENCE_INDEX_FORMAT, $self->is_oninstrument);
    my ($nfx, $npx, $nex) = $self->publish_non_sequence_files
        ($METADATA_SET . q[.] . $METADATA_FORMAT, $self->is_oninstrument);
    my ($nfr, $npr, $ner) = $self->publish_non_sequence_files
        ($self->_merged_report);

    $num_files     += ($nfx + $nfb + $nfp + $nfr);
    $num_processed += ($npx + $npb + $npp + $npr);
    $num_errors    += ($nex + $neb + $nep + $ner);
  }
  else {
    $self->warn('Skipping ', $self->analysis_path,
                ' : unexpected file issues');
  }

  if ($num_errors > 0) {
    $self->error("Encountered errors on $num_errors / ",
                 "$num_processed files processed");
  }

  $self->write_locations;

  return ($num_files, $num_processed, $num_errors);
};

=head2 publish_sequence_files

  Arg [1]    : File format match regex, Str. Required.
 
  Example    : my ($num_files, $num_published, $num_errors) =
                 $pub->publish_sequence_files($format)
  Description: Identify sequence files which match the required file 
               format regex. and publish those files to iRODS. Return 
               the number of files, the number published and the number 
               of errors. R&D data not supported - only files with 
               databased information.
  Returntype : Array[Int]

=cut

sub publish_sequence_files {
  my ($self, $format) = @_;

  defined $format or
    $self->logconfess('A defined file format argument is required');

  my $files = $self->list_files($format . q[$], $self->is_oninstrument);

  my ($num_files, $num_processed, $num_errors) = (0, 0, 0);

  my $product = WTSI::NPG::HTS::PacBio::Product->new();

  foreach my $file ( @{$files} ){
    my @tag_records;

    my $filename = fileparse($file);
    my $tag_id   = $self->_get_tag_from_fname($filename);

    if ($tag_id) {
        my @tag_id_records = $self->find_pacbio_runs
            ($self->_metadata->run_name, $self->_metadata->well_name,
             $tag_id, $self->_metadata->plate_number);

        @tag_records = (@tag_id_records == 1) ? @tag_id_records :
            $self->find_pacbio_runs($self->_metadata->run_name,
                                    $self->_metadata->well_name,
                                    $self->_get_tag_name_from_fname($filename),
                                    $self->_metadata->plate_number);

        if (@tag_records != 1) {
          $self->logcroak("Unexpected barcode from $file for SMRT cell ",
              $self->_metadata->well_name, ' run ', $self->_metadata->run_name);
        }
    } else {
        $self->_is_allowed_fname($filename, \@FNAME_PERMITTED) or
            $self->logcroak("Unexpected file name for $file");
    }

    my @all_records = $self->find_pacbio_runs($self->_metadata->run_name,
      $self->_metadata->well_name, undef, $self->_metadata->plate_number);

    my @records = (@tag_records == 1) ? @tag_records : @all_records;

    if (@records >= 1) {
      # Don't set target = 1 if more than 1 record
      #  or data is non deplexed leftovers on multiplexed run
      #  or data is for unexpected barcode
      #  or data is fasta.gz format
      my $is_target   = (@records > 1 ||
          $self->_is_allowed_fname($filename, \@FNAME_NON_DEPLEXED) ||
          $self->_is_allowed_fname($filename, \@FNAME_FAILED) ||
         ($tag_id && @tag_records != 1) ||
         ($format eq $SEQUENCE_FASTA_FORMAT))
          ? 0 : 1;

      my $tags;
      if ($is_target) {
        $tags = $records[0]->get_tags;
      }

      my $well_label = $self->remove_well_padding($self->_metadata->run_name,
                                                  $self->_metadata->well_name);
      my $id_product;

      if ($tags) {
        $id_product = $product->generate_product_id(
          $self->_metadata->run_name,
          $well_label,
          tags => $tags,
          plate_number => $self->_metadata->plate_number);
      } else {
        $id_product = $product->generate_product_id(
          $self->_metadata->run_name,
          $well_label,
          plate_number => $self->_metadata->plate_number);
      }

      my @primary_avus   = $self->make_primary_metadata
         ($self->_metadata,
          data_level => $DATA_LEVEL,
          id_product => $id_product,
          is_target  => $is_target);
      my @secondary_avus = $self->make_secondary_metadata(@records);

      my ($a_files, $a_processed, $a_errors) =
        $self->pb_publish_files([$file], $self->_dest_path,
                              \@primary_avus, \@secondary_avus);

      $num_files     += $a_files;
      $num_processed += $a_processed;
      $num_errors    += $a_errors;
    }
    else {
      $self->warn("Skipping publishing $file as no records found");
    }
  }
  $self->info("Published $num_processed / $num_files sequence files ",
              'for SMRT cell ', $self->_metadata->well_name, ' run ',
              $self->_metadata->run_name);
  return ($num_files, $num_processed, $num_errors);
}

=head2 publish_non_sequence_files

  Arg [1]    : File format match regex, Str. Required.
  Arg [2]    : On instrument analysis, Boolean. Optional.

  Example    : my ($num_files, $num_published, $num_errors) =
                 $pub->publish_non_sequence_files($format)
  Description: Identify non sequence files which match the required file 
               format regex and publish those files to iRODS. Return
               the number of files, the number published and the number
               of errors.
  Returntype : Array[Int]

=cut

sub publish_non_sequence_files {
  my ($self, $format, $on_instrument) = @_;

  defined $format or
    $self->logconfess('A defined file format argument is required');

  my $files = $self->list_files($format . q[$], $on_instrument);

  my ($num_files, $num_processed, $num_errors) =
    $self->pb_publish_files($files, $self->_dest_path);

  $self->info("Published $num_processed / $num_files $format files ",
              'for SMRT cell ', $self->_metadata->well_name, ' run ',
              $self->_metadata->run_name);

  return ($num_files, $num_processed, $num_errors);
}


=head2 list_files

  Arg [1]    : File type. Required.
  Arg [2]    : List files in sub-directories only, Boolean. Optional.

  Example    : $pub->list_files($type)
  Description: Return paths of all sequence files for the given analysis.
               Calling this method will access the file system.
  Returntype : ArrayRef[Str]

=cut

sub list_files {
  my ($self, $type, $subdironly) = @_;

  defined $type or
    $self->logconfess('A defined file type argument is required');

  my @files;
  if (defined $subdironly && $subdironly == 1) {
    # for analysis files produced on the instrument we are only looking
    # in subdirectories for files to load directly to iRODS
    my @allfiles = $self->list_directory
      ($self->runfolder_path, filter => $type, recurse => 1);
    foreach my $file (@allfiles) {
      my ($filename, $directory, $suffix) = fileparse($file);
      $directory =~ s/\/$//smx;
      if ($directory && ($directory ne $self->runfolder_path)){
        push @files, $file;
      }
    }
  } else {
    @files = $self->list_directory
      ($self->runfolder_path, filter => $type, recurse => 1);
  }

  return \@files;
}

has '_merged_report' =>
  (isa           => 'Str',
   is            => 'ro',
   builder       => '_build_merged_report',
   lazy          => 1,
   init_arg      => undef,
   documentation => 'Merged report file name.',);

sub _build_merged_report {
  my ($self) = @_;

  my @init_args = (analysis_path  => $self->analysis_path,
                   runfolder_path => $self->runfolder_path,
                   meta_data      => $self->_metadata);

  my $report = WTSI::NPG::HTS::PacBio::AnalysisReport->new(@init_args);
  return $report->generate_analysis_report;
}


sub _basic_qc {
  # common sense qc checks prior to loading
  my ($self) = @_;

  my $merged_report = $self->_merged_report;
  my $files         = $self->list_files($merged_report . q[$]);

  my $decoded;
  if (scalar @{$files} == 1) {
    my $file_contents = slurp $files->[0];
    $decoded = decode_json($file_contents);
  }

  my $qc_fail = 0;
  if ($decoded && $decoded->{$REPORT_TITLE}) {

    my $lima_text;
    foreach my $report (%{$decoded->{$REPORT_TITLE}}) {
      if ($report =~ m{$LIMA_SUMMARY$}smx &&
          !$self->_is_allowed_fname($report, \@FNAME_FAILED)) {
        $lima_text = $decoded->{$REPORT_TITLE}->{$report};
      }
      next if ref $decoded->{$REPORT_TITLE}->{$report}  ne 'ARRAY';
      foreach my $x (@{$decoded->{$REPORT_TITLE}->{$report}}) {
        if ($x->{'name'} eq $BARCODE_FIELD && $x->{'value'} < $MIN_BARCODED) {
          $qc_fail = 1;
        }
      }
    }

    if ($qc_fail == 0 && $lima_text) {
      if ($lima_text =~ m{thresholds \s+ [(]B [)] \s+ \: \s+ \d+ \s+ [(](\d+)}smx) {
        my $value = $1;
        if ($value < ($MIN_BARCODED * $HUNDRED)) {
          $qc_fail = 1;
        }
      }
    }
  }
  else {
    $self->warn('No qc check possible for ', $self->analysis_path);
  }

  return $qc_fail;
}

sub _get_tag_from_fname {
  # SequenceScape tag id is just the numeric part of the name 
  my ($self, $file) = @_;
  my $tag_id;

  if ($file =~ /bc\D*(\d+).*bc\D*(\d+)/smx){
    my ($bc1, $bc2) = ($1, $2);
    $tag_id = ($bc1 == $bc2) ? $bc1 : undef;
  } elsif ($self->is_smtwelve && $file =~ m{[.]bc\D*(\d+)\S*[.]bam}smx){
    $tag_id = $1;
  } elsif ($file =~ m{--([\S\_]+)[.]bam}smx){
    # assymetric tags recorded as symmetric in traction
    $tag_id = $1;
    $tag_id =~ s/\_\S$//g;
  }
  return $tag_id;
}

sub _get_tag_name_from_fname {
  # Traction tag id is the full tag name
  my ($self, $file) = @_;
  my $tag_name;
  if ($file =~ m{[.] (\w+\d+\S*) [-] [-]}smx){
    $tag_name = $1;
    # remove 5 or 3 prime suffix
    $tag_name =~ s/_\dp//smxg;
  } elsif ($self->is_smtwelve && $file =~ m{[.] (\w+\d+\S*) [.]bam}smx){
    $tag_name = $1;
  } elsif ($file =~ m{--([\S\_]+)[.]bam}smx){
    # assymetric tags recorded as symmetric in traction
    $tag_name = $1;
    $tag_name =~ s/\_\S$//g;
  }
  return $tag_name;
}

sub _is_allowed_fname {
  my ($self, $file, $fnames) = @_;
  return (any {  $file =~ m{[.] $_ [.]}smx  } @{ $fnames } );
}

sub _dest_path {
  my ($self) = @_;

  @{$self->smrt_names} == 1 or
        $self->logcroak('Error multiple smrt names found');

  return catdir($self->dest_collection, $self->smrt_names->[0]);
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::AnalysisPublisher

=head1 DESCRIPTION

Publishes relevant files to iRODS, adds metadata and sets permissions.

This module is suitable for loading auto secondary analysis output from 
demultiplex jobs, ccs analysis and combined demultiplex+css analysis.

Since SMRT Link v7 deplexing jobs have produced BAM files for identified
barcode tags and also files named removed.bam (equivalent to tag zero
in Illumina) which contain the reads not assigned to any tag. Expected
tags are entered with single sample meta data in iRODS whereas
unexpected tags and tag zero files are entered as multiplexed data
e.g. multiplex = 1 flag and all sample and tag data for that cell.

=head1 AUTHOR

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2018 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
