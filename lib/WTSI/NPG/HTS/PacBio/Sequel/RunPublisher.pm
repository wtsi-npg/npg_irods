package WTSI::NPG::HTS::PacBio::Sequel::RunPublisher;

use namespace::autoclean;
use Data::Dump qw[pp];
use English qw[-no_match_vars];
use File::Basename;
use File::Spec::Functions qw[catdir splitdir];
use Moose;
use MooseX::StrictConstructor;
use Readonly;
use Try::Tiny;

use WTSI::NPG::HTS::PacBio::Sequel::ImageArchive;
use WTSI::NPG::HTS::PacBio::Sequel::MetaXMLParser;
use WTSI::NPG::HTS::PacBio::Sequel::AnalysisPublisher;
use WTSI::NPG::HTS::PacBio::Sequel::Product;

extends qw{WTSI::NPG::HTS::PacBio::RunPublisher};

our $VERSION = '';

# Sequence and index file suffixes
our $SEQUENCE_FILE_FORMAT   = 'bam';
our $SEQUENCE_INDEX_FORMAT  = 'pbi';

# Sequence file types
our $SEQUENCE_PRODUCT    = 'subreads';
our $SEQUENCE_AUXILIARY  = 'scraps';

# CCS Sequence file types
our $CCS_SEQUENCE_PRODUCT    = 'reads';
our $HIFI_SEQUENCE_PRODUCT   = 'hifi_reads';
our $HIFIUB_SEQUENCE_PRODUCT = 'unbarcoded.hifi_reads';
our $REV_HIFIUB_SEQ_PRODUCT  = 'hifi_reads.unassigned';

## Processing types
our $OFFINSTRUMENT  = 'OffInstrument';
our $ONINSTRUMENT   = 'OnInstrument';
our $ONINSTRUMENTHO = 'OnInstrumentHifiOnly';
our $ONINSTRUMENTDP = 'OnInstrumentDeplex';
our $ONINSTRUMENTSR = 'OnInstrumentPlusSubreads';
our $ONINST_REVIO1  = 'OnInstrumentRevioOne';
our $ONINST_REVIO2  = 'OnInstrumentRevioTwo';

# Generic file prefix
our $FILE_PREFIX_PATTERN  = 'm[0-9a-z]+_\d+_\d+';
our $REVIO_PREFIX_PATTERN = 'm[0-9a-z]+_\d+_\d+_s[1-4]';

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

Readonly::Scalar my $ANALYSIS_ONBOARD    => 1;
Readonly::Scalar my $MODE_GROUP_WRITABLE => q(0020);

has 'api_client' =>
  (isa           => 'WTSI::NPG::HTS::PacBio::Sequel::APIClient',
   is            => 'ro',
   documentation => 'A PacBio Sequel API client used to fetch runs');


sub _build_directory_pattern{
   my ($self) = @_;

   return $WELL_DIRECTORY_PATTERN;
};


has '_movie_pattern' =>
  (isa           => 'Str',
   is            => 'ro',
   builder       => '_build_movie_pattern',
   lazy          => 1,
   init_arg      => undef,
   documentation => 'Set file prefix pattern based on whether run is Revio or Sequel IIe.',);

sub _build_movie_pattern {
  my ($self) = @_;

  my $smrt_names = [$self->smrt_names];

  ## run must be all revio - but some cells may have failed to produce data
  my $revio = 0;
  foreach my $smrt_name (@{$smrt_names}) {
    if ( defined $self->list_files($smrt_name, $REVIO_PREFIX_PATTERN
      .q{[.]}. $REV_HIFIUB_SEQ_PRODUCT .q{[.]}. $SEQUENCE_FILE_FORMAT
      .q{$}, undef, 1)->[0] ) {
        $revio++;
    } elsif ( defined $self->list_files($smrt_name, $REVIO_PREFIX_PATTERN
      .q{[.]}. $HIFI_SEQUENCE_PRODUCT .q{[.]}. $SEQUENCE_FILE_FORMAT
      .q{$}, undef, 1)->[0] ) {
        ## non deplexed data now supported for Revio
        $revio++;
    }
    last if $revio > 0;
  }
  return ($revio > 0) ? $REVIO_PREFIX_PATTERN : $FILE_PREFIX_PATTERN;
}


has '_is_onrevio' =>
  (isa           => 'Bool',
   is            => 'ro',
   builder       => '_build_is_onrevio',
   lazy          => 1,
   init_arg      => undef,
   documentation => 'Set to true if Revio as all files will be in runfolder cell subdirs.',);

sub _build_is_onrevio {
  my ($self) = @_;
  return ($self->_movie_pattern eq $REVIO_PREFIX_PATTERN) ? 1 : 0;
}


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
    elsif ($process_type eq $ONINSTRUMENTSR) {
      ($num_files_cell, $num_processed_cell, $num_errors_cell) =
        $self->_publish_on_instrument_sr_cell($smrt_name, $process_type);
    }
    elsif (($process_type eq $ONINSTRUMENT) ||
           ($process_type eq $ONINSTRUMENTHO) ||
           ($process_type eq $ONINSTRUMENTDP)) {
      ($num_files_cell, $num_processed_cell, $num_errors_cell) =
        $self->_publish_on_instrument_cell($smrt_name, $process_type);
    }
    elsif (($process_type eq $ONINST_REVIO1) ||
           ($process_type eq $ONINST_REVIO2)) {
      ($num_files_cell, $num_processed_cell, $num_errors_cell) =
        $self->_publish_revio_instrument_cell($smrt_name, $process_type);
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

  $self->write_locations;

  return ($num_files, $num_processed, $num_errors);
};

sub _processing_type {
   my ($self, $smrt_name) = @_;

   my $type;
   if (defined $self->list_files($smrt_name, $self->_movie_pattern .q{[.]}.
    $SEQUENCE_PRODUCT .q{[.]}. $SEQUENCE_FILE_FORMAT .q{$})->[0]) {
     if (defined $self->list_files($smrt_name, $self->_movie_pattern .q{[.]}.
      $CCS_SEQUENCE_PRODUCT .q{[.]}. $SEQUENCE_FILE_FORMAT .q{$})->[0]) {
        # special configuration v11+ resulting in subreads.bam (no scraps.bam)
        # & oninstrument CCS processed reads.bam (so including low qual reads)
        # to enable completion of a historic project.     
      $type = $ONINSTRUMENTSR;
     } else {
      $type = $OFFINSTRUMENT;
     }
   }
   elsif (defined $self->list_files($smrt_name, $self->_movie_pattern .q{[.]}.
    $CCS_SEQUENCE_PRODUCT .q{[.]}. $SEQUENCE_FILE_FORMAT .q{$})->[0]) {
     $type = $ONINSTRUMENT;
   }
   elsif (defined $self->list_files($smrt_name, $self->_movie_pattern .q{[.]}.
    $HIFI_SEQUENCE_PRODUCT .q{[.]}. $SEQUENCE_FILE_FORMAT .q{$})->[0]) {
     $type = ($self->_is_onrevio) ? $ONINST_REVIO2 : $ONINSTRUMENTHO;
   }
   elsif (defined $self->list_files($smrt_name, $self->_movie_pattern .q{[.]}.
    $HIFIUB_SEQUENCE_PRODUCT .q{[.]}. $SEQUENCE_FILE_FORMAT .q{$})->[0]) {
     $type = $ONINSTRUMENTDP;
   }
   elsif (defined $self->list_files($smrt_name, $self->_movie_pattern
    .q{[.]}. $REV_HIFIUB_SEQ_PRODUCT .q{[.]}. $SEQUENCE_FILE_FORMAT
     .q{$}, undef, 1)->[0]) {
     $type = $ONINST_REVIO1;
   }

   return $type;
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
    ($smrt_name, $meta_data, $OFFINSTRUMENT);

  $num_files     += ($nfx + $nfb + $nfs + $nfp + $nfi);
  $num_processed += ($npx + $npb + $nps + $npp + $npi);
  $num_errors    += ($nex + $neb + $nes + $nep + $nei);

  return ($num_files,$num_processed,$num_errors);
}

sub _publish_on_instrument_sr_cell {
  my ($self, $smrt_name, $process_type) = @_;

  my ($meta_data) = $self->_read_metadata
      ($smrt_name, q[consensusreadset], q[pbmeta:]);

  my ($num_files, $num_processed, $num_errors) = (0, 0, 0);
  my ($nfx, $npx, $nex) = $self->publish_xml_files
    ($smrt_name, q[consensusreadset|sts]);
  my ($nfb, $npb, $neb) = $self->publish_sequence_files
    ($smrt_name, $SEQUENCE_PRODUCT, $meta_data);
  my ($nfs, $nps, $nes) = $self->publish_sequence_files
    ($smrt_name, $CCS_SEQUENCE_PRODUCT, $meta_data);
  my ($nfp, $npp, $nep) = $self->publish_index_files
    ($smrt_name, qq{($SEQUENCE_PRODUCT|$CCS_SEQUENCE_PRODUCT)});
  my ($nfa, $npa, $nea) = $self->publish_aux_files
    ($smrt_name, 'zmw_metrics[.]json[.]gz');
  my ($nfi, $npi, $nei) = $self->publish_image_archive
    ($smrt_name, $meta_data, $process_type);

  $num_files     += ($nfx + $nfb + $nfs + $nfp + $nfa + $nfi);
  $num_processed += ($npx + $npb + $nps + $npp + $npa + $npi);
  $num_errors    += ($nex + $neb + $nes + $nep + $nea + $nei);

  return ($num_files,$num_processed,$num_errors);
}

sub _publish_on_instrument_cell {
  my ($self, $smrt_name, $process_type) = @_;

  my ($meta_data) = $self->_read_metadata
    ($smrt_name, q[consensusreadset], q[pbmeta:]);

  my $seqtype = ($process_type eq $ONINSTRUMENT) ? $CCS_SEQUENCE_PRODUCT :
    (($process_type eq $ONINSTRUMENTHO) ? $HIFI_SEQUENCE_PRODUCT :
     $HIFIUB_SEQUENCE_PRODUCT);

  my $pub_xml = ($process_type eq $ONINSTRUMENTDP) ?
      q[consensusreadset|sts|unbarcoded.consensusreadset] :
      q[consensusreadset|sts];

  my ($num_files, $num_processed, $num_errors) = (0, 0, 0);
  my ($nfx, $npx, $nex) = $self->publish_xml_files
    ($smrt_name, $pub_xml);
  my ($nfb, $npb, $neb) = $self->publish_sequence_files
    ($smrt_name, $seqtype, $meta_data);
  my ($nfp, $npp, $nep) = $self->publish_index_files
    ($smrt_name, $seqtype);
  my ($nfa, $npa, $nea) = $self->publish_aux_files
    ($smrt_name, 'zmw_metrics[.]json[.]gz');
  my ($nfi, $npi, $nei) = $self->publish_image_archive
    ($smrt_name, $meta_data, $process_type);

  my ($nfd, $npd, $ned) = (0, 0, 0);
  if ($process_type eq $ONINSTRUMENTDP) {
    ($nfd, $npd, $ned) = $self->_publish_deplexed_files($smrt_name);
  }

  $num_files     += ($nfx + $nfb + $nfp + $nfa + $nfi + $nfd);
  $num_processed += ($npx + $npb + $npp + $npa + $npi + $npd);
  $num_errors    += ($nex + $neb + $nep + $nea + $nei + $ned);

  return ($num_files,$num_processed,$num_errors);
}

sub _publish_revio_instrument_cell {
  my ($self, $smrt_name, $process_type) = @_;

  my ($meta_data) = $self->_read_metadata
    ($smrt_name, q[hifi_reads.consensusreadset], q[pbmeta:]);

  my ($num_files, $num_processed, $num_errors) = (0, 0, 0);

  my $pub_xml = q[sts];

  my ($nfb, $npb, $neb) = $self->_publish_deplexed_files($smrt_name);
  my ($nfx, $npx, $nex) = $self->publish_xml_files
    ($smrt_name, $pub_xml);
  my ($nfa, $npa, $nea) = $self->publish_aux_files
    ($smrt_name, q{(zmw_metrics[.]json[.]gz|reports.zip)});
  my ($nfi, $npi, $nei) = $self->publish_image_archive
    ($smrt_name, $meta_data, $process_type);

  $num_files     += ($nfb + $nfx + $nfa + $nfi);
  $num_processed += ($npb + $npx + $npa + $npi);
  $num_errors    += ($neb + $nex + $nea + $nei);

  return ($num_files,$num_processed,$num_errors);
}

sub _publish_deplexed_files {
  my ($self, $smrt_name) = @_;

  my ($num_files, $num_processed, $num_errors) = (0, 0, 0);

  my @init_args =
    (is_oninstrument => $ANALYSIS_ONBOARD,
     is_smtwelve     => $self->_is_onrevio,
     movie_pattern   => $self->_movie_pattern,
     irods           => $self->irods,
     analysis_path   => $self->smrt_path($smrt_name),
     runfolder_path  => $self->smrt_path($smrt_name),
     mlwh_schema     => $self->mlwh_schema,
     dest_collection => $self->dest_collection);

  my $publisher =
    WTSI::NPG::HTS::PacBio::Sequel::AnalysisPublisher->new(@init_args);

  try {
    ($num_files, $num_processed, $num_errors) = $publisher->publish_files();
  } catch {
    $num_errors++;
    $self->error('Failed to process deplexed files for : ',
                 $self->smrt_path($smrt_name), $_);
  };

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

  my $file_pattern = $self->_movie_pattern .'[.]'. '(' . $type .')[.]xml$';

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
    $self->find_pacbio_runs($metadata->run_name, $metadata->well_name,
      undef, $metadata->plate_number);

  # R & D runs have no records in the ML warehouse
  my $is_r_and_d = @run_records ? 0 : 1;

  if ($is_r_and_d) {
    $self->warn($metadata->run_name,
                ": publishing '$smrt_name' as R and D data");
  }

  # Auxiliary files (scraps - adapter and low quality data only) are kept for 
  # now but are not useful and so are not marked as target.
  my $is_aux = ($type eq $SEQUENCE_AUXILIARY) ? 1 : 0;

  # is_target is set to 0 where the bam file contains sample data but there
  # is another preferred file for the customer. The logic to set is_target = 0
  # is ;
  #  if the data is single tag (as data will be deplexed). 
  #  if the ccs execution mode is OffInstrument (as ccs analysis will be run and 
  #    ccs data will be target = 1),
  #  if there is more than 1 sample in the pool (as the data will be deplexed),
  #  if the data is R&D (will be untracked in LIMs)
  #  if the bam is auxiliary.
  my $is_target =
    ((@run_records == 1 && $run_records[0]->tag_sequence) ||
     ($metadata->execution_mode eq 'OffInstrument') ||
     @run_records > 1 || $is_r_and_d || $is_aux) ? 0 : 1;

  my $product = WTSI::NPG::HTS::PacBio::Sequel::Product->new();

  my $id_product = $product->generate_product_id(
      $metadata->run_name,
      $self->remove_well_padding($metadata->run_name, $metadata->well_name),
      plate_number => $metadata->plate_number
  );

  my @primary_avus   = $self->make_primary_metadata
      ($metadata,
       data_level => $DATA_LEVEL,
       id_product => $id_product,
       is_target  => $is_target,
       is_r_and_d => $is_r_and_d);
  my @secondary_avus = $self->make_secondary_metadata(@run_records);

  my $file_pattern = $self->_movie_pattern .q{[.]}. $type .q{[.]}.
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

  my $file_pattern = $self->_movie_pattern .q{[.]}. $type . q{[.]}.
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

  my $file_pattern = $self->_movie_pattern .q{[.]}. $type .q{$};

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
  Arg [3]    : Processing type. Required.

  Example    : my ($num_files, $num_published, $num_errors) =
                 $pub->publish_image_archive($smrt_name, $metadata)
  Description: Publish images archive from SMRT cell import to iRODS. 
               Return the number of files, the number published and
               the number of errors.
  Returntype : Array[Int]

=cut

sub publish_image_archive {
  my ($self, $smrt_name, $metadata, $process_type) = @_;

  my $name  = $self->_check_smrt_name($smrt_name);

  defined $metadata or
      $self->logconfess('A defined metadata argument is required');
  defined $process_type or
      $self->logconfess('A defined process_type argument is required');

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
      my(@runfolder_files,$runfolder_file1,$file_types);
      ## OnInstrument processed data - CCS or CCS HiFi only
      if(($process_type eq $ONINSTRUMENT) || ($process_type eq $ONINSTRUMENTHO) ||
         ($process_type eq $ONINSTRUMENTSR)){
        $file_types  = q{ccs_reports.json|ccs_reports.txt};
      }
      elsif ($process_type eq $ONINSTRUMENTDP) {
        $file_types  = q{ccs_reports.json|lima_guess.json|lima_guess.txt|}.
          q{lima_counts.txt|lima_summary.txt|ccs_reports.txt};
      }
      elsif ($process_type eq $ONINST_REVIO1) {
        $file_types  = q{ccs_report.txt|fail_reads.lima_counts.txt|}.
          q{fail_reads.lima_summary.txt|hifi_reads.lima_counts.txt|}.
          q{hifi_reads.lima_summary.txt|summary.json|fail_reads.json|hifi_reads.json|}.
          q{ccs_report.json|fail_reads.unassigned.json|hifi_reads.unassigned.json};
      }
      elsif ($process_type eq $ONINST_REVIO2) {
        $file_types  = q{ccs_report.txt|summary.json|ccs_report.json};
      }

      my $file_pattern1 = $self->_movie_pattern .q{.}. $file_types . q{$};
      my $file_count = scalar split m/[|]/msx, $file_types;

      $runfolder_file1 = $self->list_files($smrt_name,$file_pattern1,$file_count);
      push @runfolder_files, @{$runfolder_file1};

      # Optional 5mC report file
      my $fmc_pattern =
        (($process_type eq $ONINST_REVIO1) || ($process_type eq $ONINST_REVIO2)) ?
        q{fail_reads.5mc_report.json|hifi_reads.5mc_report.json} : q{5mc_report.json};
      my $file_pattern2   = $self->_movie_pattern .q{.}. $fmc_pattern .q{$};
      my $runfolder_file2 = $self->list_files($smrt_name,$file_pattern2);
      if(defined $runfolder_file2->[0]){
        push @runfolder_files, @{$runfolder_file2};
      }

      my @s_init = @init_args;
      push @s_init,
        dataset_id      => $metadata->ccsreads_uuid,
        dataset_type    => q[ccsreads],
        report_count    => $CCS_REPORT_COUNT,
        archive_name    => $metadata->movie_name .q[.]. $DATA_LEVEL .q[_qc],
        specified_files => \@runfolder_files;
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
  Arg [4]    : List files in sub-directories only, Boolean. Optional

  Example    : $pub->list_files('1_A01', $type)
  Description: Return paths of all files for the given type.
               Calling this method will access the file system.
  Returntype : ArrayRef[Str]

=cut

sub list_files {
  my ($self, $smrt_name, $type, $expect, $subdir) = @_;

  my $name  = $self->_check_smrt_name($smrt_name);

  defined $type or
    $self->logconfess('A defined file type argument is required');

  my @files;
  if ((defined $subdir && $subdir == 1) ||
      (defined $self->_is_onrevio && $self->_is_onrevio == 1)) {
    # only look in subdirectories for files to load
    my @allfiles = $self->list_directory
      ($self->smrt_path($name), filter => $type, recurse => 1);
    foreach my $file (@allfiles) {
      my ($filename, $directory, $suffix) = fileparse($file);
      $directory =~ s/\/$//smx;
      if ($directory && ($directory ne $self->runfolder_path)){
        push @files, $file;
      }
    }
  } else {
    @files = $self->list_directory($self->smrt_path($name), filter => $type);
  }

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

  my $pattern = $self->_movie_pattern .'[.]'. $type .'[.]xml$';
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
 - deplexed files (if deplexing was run on the instrument)

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
