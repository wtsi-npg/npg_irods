package WTSI::NPG::HTS::PacBio::Sequel::AnalysisPublisher;

use namespace::autoclean;
use English qw[-no_match_vars];
use File::Basename;
use File::Spec::Functions qw[catdir];
use JSON;
use Moose;
use MooseX::StrictConstructor;
use Perl6::Slurp;
use Readonly;

use WTSI::NPG::HTS::PacBio::Sequel::AnalysisReport;
use WTSI::NPG::HTS::PacBio::Sequel::MetaXMLParser;

extends qw{WTSI::NPG::HTS::PacBio::RunPublisher};

our $VERSION = '';

# Sequence and index file suffixes
our $SEQUENCE_FILE_FORMAT  = 'bam';
our $SEQUENCE_INDEX_FORMAT = 'pbi';

# Metadata relatedist
our $METADATA_FORMAT = 'xml';
our $METADATA_PREFIX = 'pbmeta:';
our $METADATA_SET    = q{(subreadset|consensusreadset)};

# Location of source metadata file
our $ENTRY_DIR       = 'entry-points';

# Well directory pattern
our $WELL_DIRECTORY_PATTERN = '\d+_[A-Z]\d+$';

# Additional sequence filenames permitted for loading 
our @FNAME_PERMITTED    = qw[removed ccs hifi_reads fl_transcripts];
our @FNAME_NON_DEPLEXED = qw[removed];

# Data processing level
our $DATA_LEVEL = 'secondary';

# If deplexed - minimum deplexed percentage to load
Readonly::Scalar my $MIN_BARCODED  => 0.3;
Readonly::Scalar my $BARCODE_FIELD => 'Percent Barcoded Reads';
Readonly::Scalar my $REPORT_TITLE  =>
  $WTSI::NPG::HTS::PacBio::Sequel::AnalysisReport::REPORTS;


has 'analysis_path' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'PacBio root analysis job path');


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

  my $seq_files = $self->list_files($SEQUENCE_FILE_FORMAT . q[$]);

  if (defined $seq_files->[0] && @{$self->smrt_names} == 1) {

    my $qc_fail = $self->_basic_qc();
    if ($qc_fail) {
      $self->logcroak('Skipping ', $self->analysis_path,
                      ' : QC check failed');
    }

    my ($nfb, $npb, $neb) = $self->publish_sequence_files;
    my ($nfp, $npp, $nep) = $self->publish_non_sequence_files
        ($SEQUENCE_INDEX_FORMAT);
    my ($nfx, $npx, $nex) = $self->publish_non_sequence_files
        ($METADATA_SET . q[.] . $METADATA_FORMAT);
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

  return ($num_files, $num_processed, $num_errors);
};

=head2 publish_sequence_files

  Example    : my ($num_files, $num_published, $num_errors) =
                 $pub->publish_sequence_files
  Description: Publish sequence files to iRODS. Return the number of files,
               the number published and the number of errors. R&D data
               not supported - only files with databased information.
  Returntype : Array[Int]

=cut

sub publish_sequence_files {
  my ($self) = @_;

  my $files = $self->list_files($SEQUENCE_FILE_FORMAT . q[$]);

  my ($num_files, $num_processed, $num_errors) = (0, 0, 0);

  foreach my $file ( @{$files} ){
    my @tag_records;

    my $filename = fileparse($file);
    my $tag_id   = $self->_get_tag_from_fname($filename);

    if ($tag_id) {
        my @tag_id_records = $self->find_pacbio_runs
            ($self->_metadata->run_name, $self->_metadata->well_name, $tag_id);

        @tag_records = (@tag_id_records == 1) ? @tag_id_records :
            $self->find_pacbio_runs($self->_metadata->run_name,
                                    $self->_metadata->well_name,
                                    $self->_get_tag_name_from_fname($filename));

        if (@tag_records != 1) {
          $self->logcroak("Unexpected barcode from $file for SMRT cell ",
              $self->_metadata->well_name, ' run ', $self->_metadata->run_name);
        }
    } else {
        $self->_is_allowed_fname($filename, \@FNAME_PERMITTED) or
            $self->logcroak("Unexpected file name for $file");
    }

    my @all_records = $self->find_pacbio_runs($self->_metadata->run_name,
                                              $self->_metadata->well_name);

    my @records = (@tag_records == 1) ? @tag_records : @all_records;

    if (@records >= 1) {
      # Don't set target = 1 if more than 1 record 
      #  or data is non deplexed leftovers on multiplexed run
      #  or data is for unexpected barcode
      #  or data is single tag standard (non ccs) deplex 
      my $is_target   = (@records > 1 ||
          $self->_is_allowed_fname($filename, \@FNAME_NON_DEPLEXED) ||
         ($tag_id && @tag_records != 1) ||
         ($self->_metadata->is_ccs ne 'true' && $tag_id && @all_records == 1))
          ? 0 : 1;

      my @primary_avus   = $self->make_primary_metadata
         ($self->_metadata,
          data_level => $DATA_LEVEL,
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

  Arg [1]    : Format - which needs to be at the end. Required.

  Example    : my ($num_files, $num_published, $num_errors) =
                 $pub->publish_non_sequence_files($format)
  Description: Publish non sequence files by type to iRODS. Return
               the number of files, the number published and the number
               of errors.
  Returntype : Array[Int]

=cut

sub publish_non_sequence_files {
  my ($self, $format) = @_;

  defined $format or
    $self->logconfess('A defined file format argument is required');

  my $files = $self->list_files($format . q[$]);

  my ($num_files, $num_processed, $num_errors) =
    $self->pb_publish_files($files, $self->_dest_path);

  $self->info("Published $num_processed / $num_files $format files ",
              'for SMRT cell ', $self->_metadata->well_name, ' run ',
              $self->_metadata->run_name);

  return ($num_files, $num_processed, $num_errors);
}


=head2 list_files

  Arg [1]    : File type. Required.

  Example    : $pub->list_files($type)
  Description: Return paths of all sequence files for the given analysis.
               Calling this method will access the file system.
  Returntype : ArrayRef[Str]

=cut

sub list_files {
  my ($self, $type) = @_;

  defined $type or
    $self->logconfess('A defined file type argument is required');

  return [$self->list_directory($self->runfolder_path, filter => $type)];
}

override 'run_name'  => sub {
  my ($self) = @_;
  return $self->_metadata->ts_run_name;
};

override 'smrt_names'  => sub {
  my ($self)  = @_;

  ($self->_metadata->has_results_folder &&
      $self->_metadata->ts_run_name) or
      $self->logconfess('Error ts or results folder missing');

  my $rfolder = $self->_metadata->results_folder;
  my $ts_name = $self->_metadata->ts_run_name;

  $rfolder =~ /$ts_name/smx or
     $self->logconfess('Error ts name missing from results folder ', $rfolder);

  $rfolder =~ s/$ts_name//smx;
  $rfolder =~ s/\///gsmx;

  $rfolder =~ /$WELL_DIRECTORY_PATTERN/smx or
     $self->logconfess('Error derived folder name ', $rfolder,
     'does not match expected pattern');

  return [$rfolder];
};

has '_metadata' =>
  (isa           => 'WTSI::NPG::HTS::PacBio::Metadata',
   is            => 'ro',
   builder       => '_build_metadata',
   lazy          => 1,
   init_arg      => undef,
   documentation => 'Load source meta data from file.',);

sub _build_metadata{
  my ($self) = @_;

  my $entry_dir = catdir($self->analysis_path, $ENTRY_DIR);

  my @metafiles = $self->list_directory($entry_dir,
                                        filter => $METADATA_FORMAT . q[$]);
  if (@metafiles != 1) {
    $self->logcroak("Expect one $METADATA_FORMAT file in $entry_dir");
  }
  return  WTSI::NPG::HTS::PacBio::Sequel::MetaXMLParser->new->parse_file
                 ($metafiles[0], $METADATA_PREFIX);
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

  my $report = WTSI::NPG::HTS::PacBio::Sequel::AnalysisReport->new(@init_args);
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
    foreach my $report (%{$decoded->{$REPORT_TITLE}}) {
      next if ref $decoded->{$REPORT_TITLE}->{$report}  ne 'ARRAY';
      foreach my $x (@{$decoded->{$REPORT_TITLE}->{$report}}) {
        if ($x->{'name'} eq $BARCODE_FIELD && $x->{'value'} < $MIN_BARCODED) {
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
  if ($file =~ /bc(\d+).*bc(\d+)/smx){
    my ($bc1, $bc2) = ($1, $2);
    $tag_id = ($bc1 == $bc2) ? $bc1 : undef;
  }
  return $tag_id;
}

sub _get_tag_name_from_fname {
  # Traction tag id is the full tag name
  my ($self, $file) = @_;
  my $tag_name;
  if ($file =~ m{[.] (\w+\d+\S+) [-] [-]}smx){
    $tag_name = $1;
  }
  return $tag_name;
}

sub _is_allowed_fname {
  my ($self, $file, $fnames) = @_;
  my @exists = grep { $file =~ m{[.] $_ [.]}smx } @{ $fnames };
  return @exists == 1 ? 1 : 0;
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

WTSI::NPG::HTS::PacBio::Sequel::AnalysisPublisher

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
