package WTSI::NPG::HTS::PacBio::Sequel::RunPublisher;

use namespace::autoclean;
use Data::Dump qw[pp];
use English qw[-no_match_vars];
use File::Spec::Functions qw[catdir splitdir];
use Moose;
use MooseX::StrictConstructor;
use Readonly;

use WTSI::NPG::HTS::PacBio::Sequel::ImageArchive;
use WTSI::NPG::HTS::PacBio::Sequel::MetaXMLParser;
use WTSI::NPG::HTS::PacBio::Sequel::AnalysisPublisher;

extends qw{WTSI::NPG::HTS::PacBio::RunPublisher};

our $VERSION = '';

# Sequence and index file suffixes
our $SEQUENCE_FILE_FORMAT   = 'bam';
our $SEQUENCE_INDEX_FORMAT  = 'pbi';

# Sequence file types
our $SEQUENCE_PRODUCT    = 'subreads';
our $SEQUENCE_AUXILIARY  = 'scraps';

# CCS Sequence file types
our $CCS_SEQUENCE_PRODUCT = 'reads';

## Processing types
our $OFFINSTRUMENT = 'OnInstrument';
our $ONINSTRUMENT  = 'OffInstrument';

# Generic file prefix
our $FILE_PREFIX_PATTERN = 'm[0-9a-z]+_\d+_\d+';

# Well directory pattern
our $WELL_DIRECTORY_PATTERN = '\d+_[A-Z]\d+$';

# Data processing level
our $DATA_LEVEL = 'primary';

# Image archive related
Readonly::Scalar my $PRIMARY_REPORT_COUNT   => 4;
Readonly::Scalar my $SECONDARY_REPORT_COUNT => 2;
Readonly::Scalar my $CCS_REPORT_COUNT       => 6;
Readonly::Scalar my $DATA_LEVEL_TWO         =>
  $WTSI::NPG::HTS::PacBio::Sequel::AnalysisPublisher::DATA_LEVEL;

Readonly::Scalar my $MODE_GROUP_WRITABLE => q(0020);

has 'api_client' =>
  (isa           => 'WTSI::NPG::HTS::PacBio::Sequel::APIClient',
   is            => 'ro',
   documentation => 'A PacBio Sequel API client used to fetch runs');


sub _build_directory_pattern{
   my ($self) = @_;

   return $WELL_DIRECTORY_PATTERN;
};


=head2 publish_files

  Arg [1]    : smrt_names, ArrayRef[Str]. Optional.

  Example    : my ($num_files, $num_published, $num_errors) =
                 $pub->publish_files(['1_A01', '2_B01'])
  Description: Publish all files to iRODS. If the smrt_names argument is
               supplied, only those SMRT cells will be published. The default
               is to publish all SMRT cells. Return the number of files,
               the number published and the number of errors.
  Returntype : Array[Int]

=cut

sub publish_files {
  my ($self, $smrt_names) = @_;

  if (!$smrt_names) {
    $smrt_names = [$self->smrt_names];
  }

  $self->info('Publishing files for SMRT cells: ', pp($smrt_names));

  my ($num_files, $num_processed, $num_errors) = (0, 0, 0);

  foreach my $smrt_name (@{$smrt_names}) {

    my $process_type = $self->_processing_type($smrt_name);

    my ($num_files_cell, $num_processed_cell, $num_errors_cell) = (0, 0, 0);

    if (! $self->_dir_group_writable($smrt_name) ) {
      $self->warn('Skipping '. $self->smrt_path($smrt_name) .' as dir not writable');
    }
    elsif ($process_type eq $OFFINSTRUMENT) {
      ($num_files_cell, $num_processed_cell, $num_errors_cell) =
        $self->_publish_off_instrument_cell($smrt_name);
    }
    elsif ($process_type eq $ONINSTRUMENT) {
      ($num_files_cell, $num_processed_cell, $num_errors_cell) =
        $self->_publish_on_instrument_cell($smrt_name);
    }
    else {
      $self->warn('Skipping '. $self->smrt_path($smrt_name) .' as no seq files found');
    }

    $num_files     += $num_files_cell;
    $num_processed += $num_processed_cell;
    $num_errors    += $num_errors_cell;

    if ($num_errors > 0) {
      $self->error("Encountered errors on $num_errors / ",
                   "$num_processed files processed");
    }
  }

  return ($num_files, $num_processed, $num_errors);
};

sub _processing_type {
   my ($self, $smrt_name) = @_;

   my $seq_files = $self->list_files($smrt_name,
      $SEQUENCE_PRODUCT .q{[.]}. $SEQUENCE_FILE_FORMAT .q{$});

   my $ccs_seq_files = $self->list_files($smrt_name,
      $CCS_SEQUENCE_PRODUCT  .q{[.]}. $SEQUENCE_FILE_FORMAT .q{$});

   return defined $seq_files->[0] ? $OFFINSTRUMENT :
       (defined $ccs_seq_files->[0] ? $ONINSTRUMENT : q[]);
}

sub _publish_off_instrument_cell {
  my ($self, $smrt_name) = @_;

  my ($meta_data) = $self->_read_metadata($smrt_name,q[subreadset]);

  my ($num_files, $num_processed, $num_errors) = (0, 0, 0);
  my ($nfx, $npx, $nex) = $self->publish_xml_files
    ($smrt_name, q[subreadset|sts]);
  my ($nfb, $npb, $neb) = $self->publish_sequence_files
    ($smrt_name, $SEQUENCE_PRODUCT, $meta_data);
  my ($nfs, $nps, $nes) = $self->publish_sequence_files
    ($smrt_name, $SEQUENCE_AUXILIARY, $meta_data);
  my ($nfp, $npp, $nep) = $self->publish_index_files
    ($smrt_name, qq{($SEQUENCE_PRODUCT|$SEQUENCE_AUXILIARY)});
  my ($nfi, $npi, $nei) = $self->publish_image_archive
    ($smrt_name,$meta_data);

  $num_files     += ($nfx + $nfb + $nfs + $nfp + $nfi);
  $num_processed += ($npx + $npb + $nps + $npp + $npi);
  $num_errors    += ($nex + $neb + $nes + $nep + $nei);

  return ($num_files,$num_processed,$num_errors);
}

sub _publish_on_instrument_cell {
  my ($self, $smrt_name) = @_;

  my ($meta_data) = $self->_read_metadata
    ($smrt_name, q[consensusreadset], q[pbmeta:]);

  my ($num_files, $num_processed, $num_errors) = (0, 0, 0);
  my ($nfx, $npx, $nex) = $self->publish_xml_files
    ($smrt_name, q[consensusreadset|sts]);
  my ($nfb, $npb, $neb) = $self->publish_sequence_files
    ($smrt_name, $CCS_SEQUENCE_PRODUCT, $meta_data);
  my ($nfp, $npp, $nep) = $self->publish_index_files
    ($smrt_name, $CCS_SEQUENCE_PRODUCT);
  my ($nfa, $npa, $nea) = $self->publish_aux_files
    ($smrt_name, 'zmw_metrics[.]json[.]gz');
  my ($nfi, $npi, $nei) = $self->publish_image_archive
    ($smrt_name, $meta_data);

  $num_files     += ($nfx + $nfb + $nfp + $nfa + $nfi);
  $num_processed += ($npx + $npb + $npp + $npa + $npi);
  $num_errors    += ($nex + $neb + $nep + $nea + $nei);

  return ($num_files,$num_processed,$num_errors);
}

=head2 publish_xml_files

  Arg [1]    : smrt_name,  Str. Required.
  Arg [2]    : File type regex. Str. Can be single regex or multiple 
               seperated by pipe. Required.
 
  Example    : my ($num_files, $num_published, $num_errors) =
                 $pub->publish_xml_files($smrt_name, $type)
  Description: Publish XML files for a SMRT cell to iRODS. Return
               the number of files, the number published and the number
               of errors.
  Returntype : Array[Int]

=cut

sub publish_xml_files {
  my ($self, $smrt_name, $type) = @_;

  defined $type or
      $self->logconfess('A defined file types argument is required');

  my $num  = scalar split m/[|]/msx, $type;

  my $file_pattern = $FILE_PREFIX_PATTERN .'[.]'. '(' . $type .')[.]xml$';

  my $files = $self->list_files($smrt_name, $file_pattern, $num);
  my $dest_coll = catdir($self->dest_collection, $smrt_name);

  my ($num_files, $num_processed, $num_errors) =
    $self->pb_publish_files($files, $dest_coll);

  $self->info("Published $num_processed / $num_files metadata XML files ",
              "in SMRT cell '$smrt_name'");

  return ($num_files, $num_processed, $num_errors);
}

=head2 publish_sequence_files

  Arg [1]    : smrt_name,  Str.
  Arg [2]    : File type regex. Str. Required.
  Arg [3]    : Metadata. Obj.

  Example    : my ($num_files, $num_published, $num_errors) =
                 $pub->publish_sequence_files($smrt_name, $type, $meta)
  Description: Publish sequence files for a SMRT cell to iRODS. Return
               the number of files, the number published and the number
               of errors.
  Returntype : Array[Int]

=cut

sub publish_sequence_files {
  my ($self, $smrt_name, $type, $metadata) = @_;

  defined $type or
      $self->logconfess('A defined file types argument is required');

  defined $metadata or
      $self->logconfess('A defined metadata argument is required');

  # There will be 1 record for a non-multiplexed SMRT cell and >1
  # record for a multiplexed (currently no uuids recorded in XML).
  my @run_records =
    $self->find_pacbio_runs($metadata->run_name, $metadata->well_name);

  # R & D runs have no records in the ML warehouse
  my $is_r_and_d = @run_records ? 0 : 1;

  if ($is_r_and_d) {
    $self->warn($metadata->run_name,
                ": publishing '$smrt_name' as R and D data");
  }

  # Auxiliary files (adapter and low quality data only)are kept for now but 
  # are not useful and so are not marked as target.
  my $is_aux = ($type eq $SEQUENCE_AUXILIARY) ? 1 : 0;

  # is_target is set to 0 where the bam file contains sample data but there
  # is another preferred file for the customer. The logic to set is_target = 0
  # is ;
  #  if the data is single tag and ccs either on or off instrument (as data will
  #    be deplexed. if single tag non ccs customer prefers non deplexed data as
  #    target = 1) 
  #  if the bam is not from on board processing and ccs execution mode is not
  #   None (as ccs analysis will be run and ccs data will be target = 1),
  #  if there is more than 1 sample in the pool (as the data will be deplexed),
  #  if the data is R&D (will be untracked in LIMs)
  #  if the bam is auxiliary.
  my $is_target =
    ((@run_records == 1 && $run_records[0]->tag_sequence &&
      $metadata->execution_mode ne 'None') ||
    ($type ne $CCS_SEQUENCE_PRODUCT && $metadata->execution_mode ne 'None') ||
     @run_records > 1 || $is_r_and_d || $is_aux) ? 0 : 1;

  my @primary_avus   = $self->make_primary_metadata
      ($metadata,
       data_level => $DATA_LEVEL,
       is_target  => $is_target,
       is_r_and_d => $is_r_and_d);
  my @secondary_avus = $self->make_secondary_metadata(@run_records);

  my $file_pattern = $FILE_PREFIX_PATTERN .q{[.]}. $type .q{[.]}.
        $SEQUENCE_FILE_FORMAT .q{$};

  my $files     = $self->list_files($smrt_name,$file_pattern);
  my $dest_coll = catdir($self->dest_collection, $smrt_name);

  my ($num_files, $num_processed, $num_errors) =
    $self->pb_publish_files($files, $dest_coll,
                          \@primary_avus, \@secondary_avus);

  $self->info("Published $num_processed / $num_files sequence files ",
              "in SMRT cell '$smrt_name'");

  return ($num_files, $num_processed, $num_errors);
}

=head2 publish_index_files

  Arg [1]    : smrt_name,  Str. Required.
  Arg [2]    : File type regex. Str. Can be single regex or multiple 
               seperated by pipe. Required.

  Example    : my ($num_files, $num_published, $num_errors) =
                 $pub->publish_index_files($smrt_name, $type)
  Description: Publish index files for a SMRT cell to iRODS. Return
               the number of files, the number published and the number
               of errors.
  Returntype : Array[Int]

=cut

sub publish_index_files {
  my ($self, $smrt_name, $type) = @_;

  defined $type or
    $self->logconfess('A defined file type argument is required');

  my $num  = scalar split m/[|]/msx, $type;

  my $file_pattern = $FILE_PREFIX_PATTERN .q{[.]}. $type . q{[.]}.
        $SEQUENCE_FILE_FORMAT .q{[.]}. $SEQUENCE_INDEX_FORMAT .q{$};

  my $files = $self->list_files($smrt_name, $file_pattern, $num);
  my $dest_coll = catdir($self->dest_collection, $smrt_name);

  my ($num_files, $num_processed, $num_errors) =
    $self->pb_publish_files($files, $dest_coll);

  $self->info("Published $num_processed / $num_files index files ",
              "in SMRT cell '$smrt_name'");

  return ($num_files, $num_processed, $num_errors);
}

=head2 publish_aux_files

  Arg [1]    : smrt_name,  Str. Required.
  Arg [2]    : File type regex. Str. Can be single regex or multiple 
               seperated by pipe. Required.

  Example    : my ($num_files, $num_published, $num_errors) =
                 $pub->publish_aux_files($smrt_name, $type)
  Description: Publish adapter files for a SMRT cell to iRODS. Return
               the number of files, the number published and the number
               of errors.
  Returntype : Array[Int]

=cut

sub publish_aux_files {
  my ($self, $smrt_name, $type) = @_;

  defined $type or
    $self->logconfess('A defined file type argument is required');

  my $num  = scalar split m/[|]/msx, $type;

  my $file_pattern = $FILE_PREFIX_PATTERN .q{[.]}. $type .q{$};

  my $files = $self->list_files($smrt_name,$file_pattern,$num);
  my $dest_coll = catdir($self->dest_collection, $smrt_name);

  my ($num_files, $num_processed, $num_errors) =
    $self->pb_publish_files($files, $dest_coll);

  $self->info("Published $num_processed / $num_files adapter files ",
              "in SMRT cell '$smrt_name'");

  return ($num_files, $num_processed, $num_errors);
}

=head2 publish_image_archive

  Arg [1]    : smrt_name,  Str. Required.
  Arg [2]    : Pacbio run metadata, WTSI::NPG::HTS::PacBio::Metadata.
               Required.

  Example    : my ($num_files, $num_published, $num_errors) =
                 $pub->publish_image_archive($smrt_name, $metadata)
  Description: Publish images archive from SMRT cell import to iRODS. 
               Return the number of files, the number published and
               the number of errors.
  Returntype : Array[Int]

=cut

sub publish_image_archive {
  my ($self, $smrt_name, $metadata) = @_;

  my $name  = $self->_check_smrt_name($smrt_name);

  defined $metadata or
      $self->logconfess('A defined metadata argument is required');

  my $files = [];
  if ($self->api_client) {
    my @init_args = (api_client   => $self->api_client,
                     output_dir   => $self->smrt_path($name));
    my @i_handles;
    ## OffInstrument processed data
    if ($metadata->has_subreads_uuid) {
      my @p_init = @init_args;
      push @p_init,
        dataset_id   => $metadata->subreads_uuid,
        report_count => $PRIMARY_REPORT_COUNT,
        archive_name => $metadata->movie_name .q[.]. $DATA_LEVEL .q[_qc];
      my $iap = WTSI::NPG::HTS::PacBio::Sequel::ImageArchive->new(@p_init);
      push @i_handles, $iap;

      if ($metadata->has_ccsreads_uuid) {
        my @s_init = @init_args;
        push @s_init,
          dataset_id   => $metadata->ccsreads_uuid,
          dataset_type => q[ccsreads],
          report_count => $SECONDARY_REPORT_COUNT,
          archive_name => $metadata->movie_name .q[.]. $DATA_LEVEL_TWO .q[_qc];
        my $ias = WTSI::NPG::HTS::PacBio::Sequel::ImageArchive->new(@s_init);
        push @i_handles, $ias;
      }
    }
    ## OnInstrument processed data
    elsif ($metadata->has_ccsreads_uuid) {
      my $file_pattern   = $FILE_PREFIX_PATTERN .q{.ccs_reports.json$};
      my $runfolder_file = $self->list_files($smrt_name,$file_pattern,1);

      my @s_init = @init_args;
      push @s_init,
        dataset_id      => $metadata->ccsreads_uuid,
        dataset_type    => q[ccsreads],
        report_count    => $CCS_REPORT_COUNT,
        archive_name    => $metadata->movie_name .q[.]. $DATA_LEVEL .q[_qc],
        specified_files => $runfolder_file;
      my $ias = WTSI::NPG::HTS::PacBio::Sequel::ImageArchive->new(@s_init);
      push @i_handles, $ias;
    }

    foreach my $i (@i_handles) {
      my $pattern = $i->generate_image_archive;
      my $files_i = $self->list_files($smrt_name, $pattern .q{$});
      push @{$files}, @{$files_i};
    }
  }

  my ($num_files, $num_processed, $num_errors) = (0,0,0);
  if ($files->[0]) {
    my $dest_coll = catdir($self->dest_collection, $smrt_name);

    ($num_files, $num_processed, $num_errors) =
      $self->pb_publish_files($files, $dest_coll);

    $self->info("Published $num_processed / $num_files image archive files ",
                "in SMRT cell '$smrt_name'");
  }
  return ($num_files, $num_processed, $num_errors);
}

=head2 list_files

  Arg [1]    : SMRT cell name, Str. Required.
  Arg [2]    : File type. Str. Required.
  Arg [3]    : Number of files expected. Optional.

  Example    : $pub->list_files('1_A01', $type)
  Description: Return paths of all files for the given type.
               Calling this method will access the file system.
  Returntype : ArrayRef[Str]

=cut

sub list_files {
  my ($self, $smrt_name, $type, $expect) = @_;

  my $name  = $self->_check_smrt_name($smrt_name);

  defined $type or
    $self->logconfess('A defined file type argument is required');

  my @files = $self->list_directory($self->smrt_path($name), filter => $type);

  my $num_files = scalar @files;
  if ($expect && $num_files != $expect) {
    $self->logconfess("Expected $expect but found $num_files ",
                      "for SMRT cell '$smrt_name' and type $type : ",
                      pp(\@files));
  }
  return \@files;
}


sub _read_metadata {
  my ($self, $smrt_name, $type, $prefix) = @_;

  defined $type or
    $self->logconfess('A defined file type argument is required');

  my $pattern = $FILE_PREFIX_PATTERN .'[.]'. $type .'[.]xml$';
  my $metadata_file = $self->list_files($smrt_name, $pattern, '1')->[0];
  $self->debug("Reading metadata from '$metadata_file'");

  my $metadata =
    WTSI::NPG::HTS::PacBio::Sequel::MetaXMLParser->new->parse_file
      ($metadata_file,$prefix);

  return $metadata;
}

sub _dir_group_writable {
  my ($self, $smrt_name) = @_;

  my $name  = $self->_check_smrt_name($smrt_name);
  my $mode = (stat $self->smrt_path($name))[2];

  return ($mode & $MODE_GROUP_WRITABLE) ? 1 : 0;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::Sequel::RunPublisher

=head1 DESCRIPTION

Publishes relevant files to iRODS, adds metadata and sets permissions.

An instance of RunPublisher is responsible for copying PacBio sequencing
data from the instrument run folder to a collection in iRODS for a
single, specific run.

Data files are divided into a number of categories:

 - sequence files; sequence files for sequence data
 - index files; index files for sequence data
 - XML files; stats and dataset xml
 - auxilliary files; requested available additional files which
   have changed over time.
 - image archive; tar archive of qc images

A RunPublisher provides methods to list the complement of these
categories and to copy ("publish") them. Each of these list or publish
operations may be restricted to a specific SMRT cell.

As part of the copying process, metadata are added to, or updated on,
the files in iRODS. Following the metadata update, access permissions
are set. The information to do both of these operations is provided by
an instance of WTSI::DNAP::Warehouse::Schema.

If a run is published multiple times to the same destination
collection, the following take place:

 - the RunPublisher checks local (run folder) file checksums against
   remote (iRODS) checksums and will not make unnecessary updates

 - if a local file has changed, the copy in iRODS will be overwritten
   and additional metadata indicating the time of the update will be
   added

 - the RunPublisher will proceed to make metadata and permissions
   changes to synchronise with the metadata supplied by
   WTSI::DNAP::Warehouse::Schema, even if no files have been modified

=head1 AUTHOR

Guoying Qi E<lt>gq1@sanger.ac.ukE<gt>
Keith James E<lt>kdj@sanger.ac.ukE<gt>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2011, 2016 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
