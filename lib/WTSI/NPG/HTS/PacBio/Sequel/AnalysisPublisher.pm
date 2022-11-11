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
use WTSI::NPG::HTS::PacBio::Sequel::AnalysisFastaManager;
use WTSI::NPG::HTS::PacBio::Sequel::MetaXMLParser;

extends qw{WTSI::NPG::HTS::PacBio::RunPublisher};

our $VERSION = '';

# Sequence and index file suffixes
our $SEQUENCE_FILE_FORMAT   = 'bam';
our $SEQUENCE_FASTA_FORMAT  = 'fasta.gz';
our $SEQUENCE_INDEX_FORMAT  = 'pbi';

# Metadata relatedist
our $METADATA_FORMAT = 'xml';
our $METADATA_PREFIX = 'pbmeta:';
our $METADATA_SET    = q{(subreadset|consensusreadset)};

# Location of source metadata file
our $ENTRY_DIR       = 'entry-points';

# Generic moviename file prefix
our $MOVIENAME_PATTERN = 'm[0-9a-z]+_\d+_\d+';

# Well directory pattern
our $WELL_DIRECTORY_PATTERN = '\d+_[A-Z]\d+$';

# Additional sequence filenames permitted for loading 
our @FNAME_PERMITTED    = qw[removed ccs hifi_reads fl_transcripts];
our @FNAME_NON_DEPLEXED = qw[removed];

# Data processing level
our $DATA_LEVEL = 'secondary';

# If deplexed - minimum deplexed percentage to load
Readonly::Scalar my $HUNDRED       => 100;
Readonly::Scalar my $MIN_BARCODED  => 0.3;
Readonly::Scalar my $BARCODE_FIELD => 'Percent Barcoded Reads';
Readonly::Scalar my $REPORT_TITLE  =>
  $WTSI::NPG::HTS::PacBio::Sequel::AnalysisReport::REPORTS;
Readonly::Scalar my $LIMA_SUMMARY  => 'lima.summary.txt';

has 'analysis_path' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'PacBio root analysis job path');

has 'is_oninstrument' =>
  (isa           => 'Bool',
   is            => 'ro',
   required      => 0,
   default       => 0,
   documentation => 'Set if the analysis was done on the instrument or in SMRT Link where publishable files are in analysis sub-directories. Historically if analysis is done in SMRT Link then all standard publishable files will be found in the analysis directory whereas if the analysis is done on the instrument or in a post v11.0 version of SMRT Link publishable deplexed bam, index and xml files are to be found in one or more sub-directories of the specified analysis path.');


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

    my ($nff, $npf, $nef) = $self->_iso_fasta_files() ?
        $self->publish_sequence_files($SEQUENCE_FASTA_FORMAT) : (0,0,0);
    my ($nfb, $npb, $neb) = $self->publish_sequence_files
        ($SEQUENCE_FILE_FORMAT);
    my ($nfp, $npp, $nep) = $self->publish_non_sequence_files
        ($SEQUENCE_INDEX_FORMAT, $self->is_oninstrument);
    my ($nfx, $npx, $nex) = $self->publish_non_sequence_files
        ($METADATA_SET . q[.] . $METADATA_FORMAT, $self->is_oninstrument);
    my ($nfr, $npr, $ner) = $self->publish_non_sequence_files
        ($self->_merged_report);

    $num_files     += ($nfx + $nfb + $nff + $nfp + $nfr);
    $num_processed += ($npx + $npb + $npf + $npp + $npr);
    $num_errors    += ($nex + $neb + $nef + $nep + $ner);
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
      #  or data is fasta.gz format
      my $is_target   = (@records > 1 ||
          $self->_is_allowed_fname($filename, \@FNAME_NON_DEPLEXED) ||
         ($tag_id && @tag_records != 1) ||
         ($format eq $SEQUENCE_FASTA_FORMAT))
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
    @files = $self->list_directory($self->runfolder_path, filter => $type);
  }

  return \@files;
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

  my $entry_path = catdir($self->analysis_path, $ENTRY_DIR);

  my @metafiles;
  if ($self->is_oninstrument == 1 && ! -d $entry_path) {
    @metafiles = $self->list_directory
      ($self->analysis_path,
       filter => $MOVIENAME_PATTERN .q[.]. $METADATA_SET .q[.]. $METADATA_FORMAT .q[$])
  } else {
    @metafiles = $self->list_directory
      ($entry_path, filter => $METADATA_FORMAT . q[$], recurse => 1);
  }

  if (@metafiles != 1) {
    $self->logcroak('Expect one xml file in '. $self->analysis_path . ' (entry_dir)');
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

has '_iso_fasta_files' =>
  (isa           => 'Bool',
   is            => 'ro',
   builder       => '_build_iso_fasta_files',
   lazy          => 1,
   init_arg      => undef,
   documentation => 'Find, reformat and write any isoseq fasta files to the analysis directory.',);

sub _build_iso_fasta_files {
  my ($self) = @_;

  my @init_args  = (analysis_path  => $self->analysis_path,
                    runfolder_path => $self->runfolder_path,
                    meta_data      => $self->_metadata);

  my $iso = WTSI::NPG::HTS::PacBio::Sequel::AnalysisFastaManager->new(@init_args);
  my $is_success = $iso->make_loadable_files;

  return $is_success;
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
      if ($report =~ m{$LIMA_SUMMARY$}smx) {
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
  if ($file =~ m{[.] (\w+\d+\S*) [-] [-]}smx){
    $tag_name = $1;
    # remove 5 or 3 prime suffix
    $tag_name =~ s/_\dp//smxg;
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
