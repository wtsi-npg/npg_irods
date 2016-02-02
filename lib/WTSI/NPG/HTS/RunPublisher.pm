package WTSI::NPG::HTS::RunPublisher;

use namespace::autoclean;
use Data::Dump qw(pp);
use English qw(-no_match_vars);
use File::Basename;
use List::AllUtils qw(any first none);
use File::Spec::Functions qw(catdir catfile splitdir);
use Moose;
use Try::Tiny;

use WTSI::NPG::HTS::AlMapFileDataObject;
use WTSI::NPG::HTS::AncFileDataObject;
use WTSI::NPG::HTS::LIMSFactory;
use WTSI::NPG::HTS::Publisher;
use WTSI::NPG::HTS::Types qw(AlMapFileFormat);
use WTSI::NPG::iRODS::Metadata;
use WTSI::NPG::iRODS;

with 'WTSI::DNAP::Utilities::Loggable',
     'WTSI::DNAP::Utilities::JSONCodec',
     'WTSI::NPG::HTS::Annotator',
     'npg_tracking::illumina::run::short_info',
     'npg_tracking::illumina::run::folder';

with 'npg_tracking::illumina::run::long_info';

our $VERSION = '';

# Default 
our $DEFAULT_ROOT_COLL = '/seq';
our $DEFAULT_QC_COLL   = 'qc';

# Alignemtne and index file suffixes
our $BAM_FILE_FORMAT   = 'bam';
our $BAM_INDEX_FORMAT  = 'bai';
our $CRAM_FILE_FORMAT  = 'cram';
our $CRAM_INDEX_FORMAT = 'crai';

# Cateories of file to be published
our $ALIGNMENT_CATEGORY = 'alignment';
our $ANCILLARY_CATEGORY = 'ancillary';
our $INDEX_CATEGORY     = 'index';
our $QC_CATEGORY        = 'qc';

our $NUM_READS_JSON_PROPERTY = 'num_total_reads';

has 'irods' =>
  (is            => 'ro',
   isa           => 'WTSI::NPG::iRODS',
   required      => 1,
   documentation => 'An iRODS handle to run searches and perform updates');

has 'lims_factory' =>
  (is            => 'ro',
   isa           => 'WTSI::NPG::HTS::LIMSFactory',
   required      => 1,
   documentation => 'A factory providing st:api::lims objects');

has 'file_format' =>
  (isa           => AlMapFileFormat,
   is            => 'ro',
   required      => 1,
   lazy          => 1,
   default       => 'cram',
   documentation => 'The format of the file to be published');

has 'ancillary_formats' =>
  (isa           => 'ArrayRef',
   is            => 'ro',
   required      => 1,
   lazy          => 1,
   default       => sub {
     return [qw(bed bamcheck flagstat stats txt seqchksum)];
   },
   documentation => 'The ancillary file formats to be published');

has 'dest_collection' =>
  (isa           => 'Str',
   is            => 'ro',
   lazy          => 1,
   builder       => '_build_dest_collection',
   documentation => 'The destination collection within iRODS to store data');

has 'qc_dest_collection' =>
  (isa           => 'Str',
   is            => 'ro',
   lazy          => 1,
   builder       => '_build_qc_dest_collection',
   documentation => 'The destination collection within iRODS to store QC data');

has 'alt_process' =>
  (isa           => 'Maybe[Str]',
   is            => 'ro',
   required      => 0,
   documentation => 'Non-standard process used');

sub BUILD {
  my ($self) = @_;

  # Use our logger to log activity in attributes.
  $self->irods->logger($self->logger);
  $self->lims_factory->logger($self->logger);
  return;
}

# The list_*_files methods are uncached. The verb in their name
# suggests activity. The corresponding methods generated here without
# the list_ prefix are caching. We are not using attributes here
# because the plex-level accessors have a position parameter.
my @CACHING_LANE_METHOD_NAMES = qw(lane_alignment_files
                                   lane_index_files
                                   lane_qc_files
                                   lane_ancillary_files);
my @CACHING_PLEX_METHOD_NAMES = qw(plex_alignment_files
                                   plex_index_files
                                   plex_qc_files
                                   plex_ancillary_files);

# Cache of lane-level file lists keyed on method name
my $LANE_FILES_CACHE = {};
# Cache of plex-level file lists keyed on method name
my $PLEX_FILES_CACHE = {};

sub _make_caching_method {
  my ($method_name, $cache) = @_;

  __PACKAGE__->meta->add_method
    ($method_name,
     sub {
       my ($self, $position) = @_;
       $position = $self->_check_position($position);
       return $cache->{$method_name}->{$position}
     });

  around $method_name => sub {
    my ($orig, $self, $position) = @_;

    my $uncached_method_name = "list_$method_name";
    if (exists $cache->{$method_name}->{$position}) {
      $self->debug('Using cached result for ', __PACKAGE__,
                   "::$uncached_method_name($position)");
    }
    else {
      $cache->{$method_name}->{$position} =
        $self->$uncached_method_name($position);
    }

    return $self->$orig($position);
  };

  return;
}

foreach my $method_name (@CACHING_LANE_METHOD_NAMES) {
  _make_caching_method($method_name, $LANE_FILES_CACHE);
}

foreach my $method_name (@CACHING_PLEX_METHOD_NAMES) {
  _make_caching_method($method_name, $PLEX_FILES_CACHE);
}

=head2 positions

  Arg [1]    : None

  Example    : $pub->positions
  Description: Return a sorted array of lane position numbers.
  Returntype : Array

=cut

sub positions {
  my ($self) = @_;

  return $self->lims_factory->positions($self->id_run);
}

=head2 is_plexed

  Arg [1]    : Lane position, Int.

  Example    : $pub->is_plexed($position)
  Description: Return true if the lane position contains plexed data.
  Returntype : Bool

=cut


sub is_plexed {
  my ($self, $position) = @_;

  $position = $self->_check_position($position);

  return -d $self->lane_archive_path($position);
}

=head2 num_reads

  Arg [1]    : Lane position, Int.
  Arg [2]    : Tag index, Int. Required for plexed positions.

  Example    : my $num_lane_reads = $pub->num_reads($position1)
               my $num_plex_reads = $pub->num_reads($position2)
  Description: Return the total number of primary, non-supplementary
               reads.
  Returntype : Int

=cut

sub num_reads {
  my ($self, $position, $tag_index) = @_;

  $position = $self->_check_position($position);

  my $qc_file;
  if ($self->is_plexed($position)) {
    defined $tag_index or
      $self->logconfess('A defined tag_index argument is required');
    $qc_file = $self->_plex_qc_stats_file($position, $tag_index);

  }
  else {
    $qc_file = $self->_lane_qc_stats_file($position);
  }

  my $num_reads;
  if ($qc_file) {
    my $flag_stats = $self->_parse_json_file($qc_file);
    if ($flag_stats) {
      $num_reads = $flag_stats->{$NUM_READS_JSON_PROPERTY};
    }
  }

  return $num_reads
}

sub index_format {
  my ($self) = @_;

  my $index_format;
  if ($self->file_format eq $BAM_FILE_FORMAT) {
    $index_format = $BAM_INDEX_FORMAT;
  }
  elsif ($self->file_format eq $CRAM_FILE_FORMAT) {
    $index_format = $CRAM_INDEX_FORMAT;
  }
  else {
    $self->logconfess('The index format corresponding to HTS file format ',
                      $self->file_format, ' is unknown');
  }

  return $index_format;
}

=head2 list_lane_alignment_files

  Arg [1]    : None

  Example    : $pub->list_lane_alignment_files;
  Description: Return paths of all lane-level alignment files for the run.
               Calling this method will access the file system. For
               cached access to the list, use the lane_alignment_files
               method.
  Returntype : ArrayRef[Str]

=cut

sub list_lane_alignment_files {
  my ($self, $position) = @_;

  if (defined $position) {
    $position = $self->_check_position($position);
  }

  my $id_run        = $self->id_run;
  my $archive_path = $self->archive_path;

  my $positions_pattern = $self->_positions_pattern($position);
  my $lane_file_pattern = sprintf '^%d_%s.*[.]%s$',
    $id_run, $positions_pattern, $self->file_format;

  $self->debug("Finding lane alignment files for run '$id_run' ",
               "in '$archive_path matching pattern '$lane_file_pattern'");

  my @file_list = $self->_list_directory($archive_path, $lane_file_pattern);
  @file_list = sort @file_list;

  $self->debug("Found lane alignment files for run '$id_run' ",
               "in '$archive_path': ", pp(\@file_list));

  return \@file_list;
}

=head2 list_plex_alignment_files

  Arg [1]    : Lane position, Int.

  Example    : $pub->list_plex_alignment_files($position);
  Description: Return paths of all plex-level alignment files for the
               given lane. Calling this method will access the file
               system. For cached access to the list, use the
               plex_alignment_files method.
  Returntype : ArrayRef[Str]

=cut

sub list_plex_alignment_files {
  my ($self, $position) = @_;

  $position = $self->_check_position($position);

  my $id_run       = $self->id_run;
  my $archive_path = $self->lane_archive_path($position);

  my $plex_file_pattern = sprintf '^%d_%d.*[.]%s$',
    $id_run, $position, $self->file_format;

  $self->debug("Finding plex alignment files for run '$id_run' position ",
               "'$position' in '$archive_path' ",
               "matching pattern '$plex_file_pattern'");

  my @file_list = $self->_list_directory($archive_path, $plex_file_pattern);
  @file_list = sort @file_list;

  $self->debug("Found plex alignment files for run '$id_run' position ",
               "'$position' in '$archive_path': ", pp(\@file_list));

  return \@file_list;
}

=head2 list_lane_index_files

  Arg [1]    : None

  Example    : $pub->list_lane_index_files;
  Description: Return paths of all lane-level index files for the run.
               Calling this method will access the file system. For
               cached access to the list, use the lane_index_files
               method.
  Returntype : ArrayRef[Str]

=cut

sub list_lane_index_files {
  my ($self, $position) = @_;

  if (defined $position) {
    $position = $self->_check_position($position);
  }

  my $id_run       = $self->id_run;
  my $archive_path = $self->archive_path;
  my $file_format  = $self->file_format;

  my $positions_pattern = $self->_positions_pattern($position);
  my $lane_file_pattern;
  if ($file_format eq $BAM_FILE_FORMAT) {
    $lane_file_pattern = sprintf '^%d_%s.*[.]%s$',
      $id_run, $positions_pattern, $self->index_format;
  }
  elsif ($file_format eq $CRAM_FILE_FORMAT) {
    $lane_file_pattern = sprintf '^%d_%s.*[.]%s\.%s$',
      $id_run, $positions_pattern, $file_format, $self->index_format;
  }
  else {
    $self->logconfess("Invalid HTS file format for indexing '$file_format'");
  }

  $self->debug("Finding lane index files for run '$id_run' ",
               "in '$archive_path matching pattern '$lane_file_pattern'");

  my @file_list = $self->_list_directory($archive_path, $lane_file_pattern);
  @file_list = sort @file_list;

  $self->debug("Found lane index files for run '$id_run' ",
               "in '$archive_path': ", pp(\@file_list));

  return \@file_list;
}

=head2 list_plex_index_files

  Arg [1]    : Lane position, Int.

  Example    : $pub->list_plex_index_files($position);
  Description: Return paths of all plex-level index files for the
               given lane. Calling this method will access the file
               system. For cached access to the list, use the
               plex_index_files method.
  Returntype : ArrayRef[Str]

=cut

sub list_plex_index_files {
  my ($self, $position) = @_;

  $position = $self->_check_position($position);

  my $id_run       = $self->id_run;
  my $archive_path = $self->lane_archive_path($position);
  my $file_format  = $self->file_format;

  my $plex_file_pattern;
  if ($file_format eq $BAM_FILE_FORMAT) {
    $plex_file_pattern = sprintf '^%d_%d.*[.]%s$',
      $id_run, $position, $self->index_format;
  }
  elsif ($file_format eq $CRAM_FILE_FORMAT) {
    $plex_file_pattern = sprintf '^%d_%d.*[.]%s[.]%s$',
      $id_run, $position, $file_format, $self->index_format;
  }
  else {
    $self->logconfess("Invalid HTS file format for indexing '$file_format'");
  }

  my @file_list = $self->_list_directory($archive_path, $plex_file_pattern);
  @file_list = sort @file_list;

  $self->debug("Found plex index files for run '$id_run' position ",
               "'$position' in '$archive_path': ", pp(\@file_list));

  return \@file_list;
}

=head2 list_lane_qc_files

  Arg [1]    : None

  Example    : $pub->list_qc_alignment_files;
  Description: Return paths of all lane-level qc files for the run.
               Calling this method will access the file system. For
               cached access to the list, use the lane_qc_files
               method.
  Returntype : ArrayRef[Str]

=cut

sub list_lane_qc_files {
  my ($self, $position) = @_;

  if (defined $position) {
    $position = $self->_check_position($position);
  }

  my $id_run      = $self->id_run;
  my $qc_path     = $self->qc_path;
  my $file_format = 'json';

  my $positions_pattern = $self->_positions_pattern($position);
  my $lane_file_pattern = sprintf '^%d_%s.*[.]%s$',
    $id_run, $positions_pattern, $file_format;

  $self->debug("Finding lane QC files for run '$id_run' in '$qc_path' ",
               "matching pattern '$lane_file_pattern'");

  my @file_list = $self->_list_directory($qc_path, $lane_file_pattern);
  @file_list = sort @file_list;

  $self->debug("Found lane QC files for run '$id_run' in '$qc_path': ",
               pp(\@file_list));

  return \@file_list;
}

=head2 list_plex_qc_files

  Arg [1]    : Lane position, Int.

  Example    : $pub->list_plex_qc_files($position);
  Description: Return paths of all plex-level qc files for the
               given lane. Calling this method will access the file
               system. For cached access to the list, use the
               plex_qc_files method.
  Returntype : ArrayRef[Str]

=cut

sub list_plex_qc_files {
  my ($self, $position) = @_;

  $position = $self->_check_position($position);

  my $id_run      = $self->id_run;
  my $qc_path     = $self->lane_qc_path($position);
  my $file_format = 'json';

  my $plex_file_pattern = sprintf '^%d_%d.*[.]%s$',
    $id_run, $position, $file_format;

  $self->debug("Finding plex QC files for run '$id_run' position ",
               "'$position' in '$qc_path' matching pattern ",
               "'$plex_file_pattern'");

  my @file_list = $self->_list_directory($qc_path, $plex_file_pattern);
  @file_list = sort @file_list;

  $self->debug("Found plex QC files for run '$id_run' position ",
               "'$position' in '$qc_path': ", pp(\@file_list));

  return \@file_list;
}

=head2 list_lane_ancillary_files

  Arg [1]    : Lane position, Int.

  Example    : $pub->list_lane_ancillary_files($position);
  Description: Return paths of all lane-level ancillary files for the
               given lane. Calling this method will access the file
               system. For cached access to the list, use the
               lane_ancillary_files method.
  Returntype : ArrayRef[Str]

=cut

sub list_lane_ancillary_files {
  my ($self, $position) = @_;

  if (defined $position) {
    $position = $self->_check_position($position);
  }

  my $id_run = $self->id_run;
  my $archive_path = $self->archive_path;

  my $positions_pattern = $self->_positions_pattern($position);
  my $suffix_pattern    = sprintf '(%s)',
    join q[|], @{$self->ancillary_formats};
  my $lane_file_pattern = sprintf '^%d_%s.*[.]%s$',
    $id_run, $positions_pattern, $suffix_pattern;

  $self->debug("Finding lane ancillary files for run '$id_run' in ",
               "'$archive_path' matching pattern '$lane_file_pattern'");

  my @file_list = $self->_list_directory($archive_path, $lane_file_pattern);
  # The file pattern match is deliberately kept simple. The downside
  # is that it matches one file that we do not want.
  @file_list = grep { ! m{markdups_metrics}msx } @file_list;
  @file_list = sort @file_list;

  $self->debug("Found lane ancillary files for run '$id_run' in ",
               "in '$archive_path': ", pp(\@file_list));

  return \@file_list;
}

sub list_plex_ancillary_files {
  my ($self, $position) = @_;

  $position = $self->_check_position($position);

  my $id_run       = $self->id_run;
  my $archive_path = $self->lane_archive_path($position);

  my $suffix_pattern    = sprintf '(%s)',
    join q[|], @{$self->ancillary_formats};
  my $plex_file_pattern = sprintf '^%d_%d.*[.]%s$',
    $id_run, $position, $suffix_pattern;

  $self->debug("Finding plex ancillary files for run '$id_run' position ",
               "'$position' in '$archive_path' ",
               "matching pattern '$plex_file_pattern'");

  my @file_list = $self->_list_directory($archive_path, $plex_file_pattern);
  # The file pattern match is deliberately kept simple. The downside
  # is that it matches one file that we do not want.
  @file_list = grep { ! m{markdups_metrics}msx } @file_list;
  @file_list = sort @file_list;

  $self->debug("Found plex ancillary files for run '$id_run' position ",
               "'$position' in '$archive_path': ", pp(\@file_list));

  return \@file_list;
}

=head2 publish_alignment_files

  Arg [1]    : HTS data has spiked control, Bool. Optional.

  Example    : my $num_published = $pub->publish_alignment_files
  Description: Publish all the alignment files (lane- or plex-level) to
               iRODS. Return the number of files published without error.
  Returntype : Int

=cut

sub publish_alignment_files {
  my ($self, $with_spiked_control) = @_;

  return $self->_publish_file_category($ALIGNMENT_CATEGORY,
                                       $with_spiked_control);
}

=head2 publish_lane_alignment_files

  Arg [1]    : HTS data has spiked control, Bool. Optional.

  Example    : my $num_published = $pub->publish_lane_alignment_files
  Description: Publish all the lane-level alignment files to iRODS.
               Return the number of files published without error.
  Returntype : Int

=cut

sub publish_lane_alignment_files {
  my ($self, $position, $with_spiked_control) = @_;

  $position = $self->_check_position($position);

  my $id_run        = $self->id_run;
  my $num_published = 0;

  if (not $self->is_plexed($position)) {
    my ($np, $num_files) =
      $self->_publish_alignment_files($position,
                                      $self->lane_alignment_files($position),
                                      $self->dest_collection,
                                      $with_spiked_control);
    $num_published = $np;
    $self->info("Published $num_published / $num_files lane-level ",
                "alignment files in run '$id_run'");
  }
  else {
    $self->logconfess("Attempted to publish position $position plex-level ",
                      "alignment files in run '$id_run'; ",
                      'the position is not plexed');
  }

  return $num_published;
}

=head2 publish_plex_alignment_files

  Arg [1]    : Lane position, Int.
  Arg [2]    : HTS data has spiked control, Bool. Optional.

  Example    : my $num_published = $pub->publish_plex_alignment_files(8)
  Description: Publish all the plex-level alignment files in the
               specified lane to iRODS.  Return the number of files
               published without error.
  Returntype : Int

=cut

sub publish_plex_alignment_files {
  my ($self, $position, $with_spiked_control) = @_;

  $position = $self->_check_position($position);

  my $id_run        = $self->id_run;
  my $num_published = 0;

  if ($self->is_plexed($position)) {
    my ($np, $num_files) =
      $self->_publish_alignment_files($position,
                                      $self->plex_alignment_files($position),
                                      $self->dest_collection,
                                      $with_spiked_control);
    $num_published = $np;
    $self->info("Published $num_published / $num_files plex-level ",
                "alignment files in run '$id_run' position '$position'");
  }
  else {
    $self->logconfess("Attempted to publish position $position lane-level ",
                      "alignment files in run '$id_run'; ",
                      'the position is plexed');
  }

  return $num_published;
}

=head2 publish_index_files

  Arg [1]    : HTS data has spiked control, Bool. Optional.

  Example    : my $num_published = $pub->publish_index_files
  Description: Publish all the index files to iRODS. Return the
               number of files published without error.
  Returntype : Int

=cut

sub publish_index_files {
  my ($self, $with_spiked_control) = @_;

  return $self->_publish_file_category($INDEX_CATEGORY, $with_spiked_control);
}

=head2 publish_lane_ancillary_files

  Arg [1]    : HTS data has spiked control, Bool. Optional.

  Example    : my $num_published = $pub->publish_plex_ancillary_files
  Description: Publish all the lane-level ancillary files to iRODS. Return
               the number of files published without error.
  Returntype : Int

=cut

sub publish_lane_index_files {
  my ($self, $position, $with_spiked_control) = @_;

  return $self->_publish_lane_support_files($position,
                                            $self->lane_index_files($position),
                                            $self->dest_collection,
                                            $INDEX_CATEGORY,
                                            $with_spiked_control);
}

=head2 publish_plex_ancillary_files

  Arg [1]    : HTS data has spiked control, Bool. Optional.

  Example    : my $num_published = $pub->publish_plex_ancillary_files
  Description: Publish all the plex-level ancillary files to iRODS. Return
               the number of files published without error.
  Returntype : Int

=cut

sub publish_plex_index_files {
  my ($self, $position, $with_spiked_control) = @_;

  return  $self->_publish_plex_support_files($position,
                                             $self->plex_index_files($position),
                                             $self->dest_collection,
                                             $INDEX_CATEGORY,
                                             $with_spiked_control);
}

=head2 publish_ancillary_files

  Arg [1]    : HTS data has spiked control, Bool. Optional.

  Example    : my $num_published = $pub->publish_ancillary_files
  Description: Publish all the ancillary files to iRODS. Return the
               number of files published without error.
  Returntype : Int

=cut

sub publish_ancillary_files {
  my ($self, $with_spiked_control) = @_;

  return $self->_publish_file_category($ANCILLARY_CATEGORY,
                                       $with_spiked_control);
}

=head2 publish_lane_ancillary_files

  Arg [1]    : Lane position, Int.
  Arg [2]    : HTS data has spiked control, Bool. Optional.

  Example    : my $num_published = $pub->publish_lane_ancillary_files(8)
  Description: Publish all the lane-level ancillary files in the
               specified lane to iRODS.  Return the number of files
               published without error.
  Returntype : Int

=cut

sub publish_lane_ancillary_files {
  my ($self, $position, $with_spiked_control) = @_;

  return $self->_publish_lane_support_files
    ($position,
     $self->lane_ancillary_files($position),
     $self->dest_collection,
     $ANCILLARY_CATEGORY,
     $with_spiked_control);
}

=head2 publish_plex_ancillary_files

  Arg [1]    : Lane position, Int.
  Arg [2]    : HTS data has spiked control, Bool. Optional.

  Example    : my $num_published = $pub->publish_plex_ancillary_files(8)
  Description: Publish all the plex-level ancillary files in the
               specified lane to iRODS.  Return the number of files
               published without error.
  Returntype : Int

=cut

sub publish_plex_ancillary_files {
  my ($self, $position, $with_spiked_control) = @_;

  return $self->_publish_plex_support_files
    ($position,
     $self->plex_ancillary_files($position),
     $self->dest_collection,
     $ANCILLARY_CATEGORY,
     $with_spiked_control);
}

=head2 publish_qc_files

  Arg [1]    : HTS data has spiked control, Bool. Optional.

  Example    : my $num_published = $pub->publish_qc_files
  Description: Publish all the QC files to iRODS. Return the
               number of files published without error.
  Returntype : Int

=cut

sub publish_qc_files {
  my ($self, $with_spiked_control) = @_;

  return $self->_publish_file_category($QC_CATEGORY, $with_spiked_control);
}

=head2 publish_lane_qc_files

  Arg [1]    : Lane position, Int.
  Arg [2]    : HTS data has spiked control, Bool. Optional.

  Example    : my $num_published = $pub->publish_lane_qc_files(8)
  Description: Publish all the lane-level QC files in the
               specified lane to iRODS.  Return the number of files
               published without error.
  Returntype : Int

=cut

sub publish_lane_qc_files {
  my ($self, $position, $with_spiked_control) = @_;

  return $self->_publish_lane_support_files($position,
                                            $self->lane_qc_files($position),
                                            $self->qc_dest_collection,
                                            $QC_CATEGORY,
                                            $with_spiked_control);
}

=head2 publish_plex_qc_files

  Arg [1]    : Lane position, Int.
  Arg [2]    : HTS data has spiked control, Bool. Optional.

  Example    : my $num_published = $pub->publish_plex_qc_files(8)
  Description: Publish all the plex-level QC files in the
               specified lane to iRODS.  Return the number of files
               published without error.
  Returntype : Int

=cut

sub publish_plex_qc_files {
  my ($self, $position, $with_spiked_control) = @_;

  return  $self->_publish_plex_support_files($position,
                                             $self->plex_qc_files($position),
                                             $self->qc_dest_collection,
                                             $QC_CATEGORY,
                                             $with_spiked_control);
}

# Check that a position argument is valid
sub _check_position {
  my ($self, $position) = @_;

  defined $position or
    $self->logconfess('A defined position argument is required');
  any { $position } $self->positions or
    $self->logconfess("Invalid position argument '$position'");

  return $position;
}

# Create a pattern to match file of one position, or all positions
sub _positions_pattern {
  my ($self, $position) = @_;

  $position = $self->_check_position($position);
  return defined $position ? "$position" :
    join q[], q([), $self->positions, q(]);
}

# A dispatcher to call the correct method for a given file category
# and lane plex state
sub _publish_file_category {
  my ($self, $category, $with_spiked_control) = @_;

  defined $category or
    $self->logconfess('A defined category argument is required');
  any { $category eq $_ } ($ALIGNMENT_CATEGORY, $ANCILLARY_CATEGORY,
                           $INDEX_CATEGORY, $QC_CATEGORY) or
    $self->logconfess("Unknown file category '$category'");

  my $num_published = 0;
  my $lane_method = sprintf 'publish_lane_%s_files', $category;
  my $plex_method = sprintf 'publish_plex_%s_files', $category;

  foreach my $position ($self->positions) {
    if ($self->is_plexed($position)) {
      $num_published += $self->$plex_method($position, $with_spiked_control);
    }
    else {
      $num_published += $self->$lane_method($position, $with_spiked_control);
    }
  }

  return $num_published;
}

# Backend alignment file publisher
sub _publish_alignment_files {
  my ($self, $position, $files, $dest_coll, $with_spiked_control) = @_;

  my $publisher = WTSI::NPG::HTS::Publisher->new(irods  => $self->irods,
                                                 logger => $self->logger);

  my $num_files     = scalar @{$files};
  my $num_processed = 0;
  my $num_errors    = 0;
  foreach my $file (@{$files}) {
    my $dest = q[];

    try {
      $num_processed++;
      my $obj = WTSI::NPG::HTS::AlMapFileDataObject->new
        (collection  => $dest_coll,
         data_object => fileparse($file),
         irods       => $self->irods);

      $dest = $obj->str;
      $dest = $publisher->publish($file, $dest);

      # FIXME -- can remove the is_plexed check?
      my $num_reads;
      if ($self->is_plexed($position)) {
        $num_reads = $self->num_reads($position, $obj->tag_index);
      }
      else {
        $num_reads = $self->num_reads($position);
      }

      # FIXME -- break primary metadata setup out into a new method
      my @avus = $self->make_primary_metadata
        ($self->id_run, $position, $num_reads,
         tag_index      => $obj->tag_index,
         is_paired_read => $self->is_paired_read,
         is_aligned     => $obj->is_aligned,
         reference      => $obj->reference,
         align_filter   => $obj->align_filter,
         alt_process    => $self->alt_process);
      $self->_set_metadata($obj, @avus);

      $obj->update_secondary_metadata($self->lims_factory,
                                      $with_spiked_control);
      $self->info("Published '$dest' [$num_processed / $num_files]");
    } catch {
      $num_errors++;

      ## no critic (RegularExpressions::RequireDotMatchAnything)
      my ($msg) = m{^(.*)$}mx;
      ## use critic
      $self->error("Failed to publish '$file' to '$dest' ",
                   "[$num_processed / $num_files]: ", $msg);
    };
  }

  if ($num_errors > 0) {
    $self->error("Encountered errors on $num_errors / ",
                 "$num_processed alignment files processed");
  }

  return ($num_processed - $num_errors, $num_files);
}

# Backend ancillary, index and qc file publisher for lane-level positions
## no critic (Subroutines::ProhibitManyArgs)
sub _publish_lane_support_files {
  my ($self, $position, $files, $dest_collection, $description,
      $with_spiked_control) = @_;

  $position = $self->_check_position($position);

  my $id_run        = $self->id_run;
  my $num_published = 0;

  if (not $self->is_plexed($position)) {
    my ($np, $num_files) = $self->_publish_support_files($files,
                                                         $dest_collection,
                                                         $with_spiked_control);
    $num_published = $np;
    $self->info("Published $num_published / $num_files lane-level ",
                "$description files in run '$id_run' position '$position'");
  }
  else {
    $self->logconfess("Attempted to publish position $position lane-level ",
                      "$description files in run '$id_run'; ",
                      'the position is plexed');
  }

  return $num_published;
}
## use critic

# Backend ancillary, index and qc file publisher for plex-level positions
## no critic (Subroutines::ProhibitManyArgs)
sub _publish_plex_support_files {
  my ($self, $position, $files, $dest_collection, $description,
      $with_spiked_control) = @_;

  $position = $self->_check_position($position);

  my $id_run        = $self->id_run;
  my $num_published = 0;

  if ($self->is_plexed($position)) {
    my ($np, $num_files) = $self->_publish_support_files($files,
                                                         $dest_collection,
                                                         $with_spiked_control);
    $num_published = $np;
    $self->info("Published $num_published / $num_files plex-level ",
                "$description files in run '$id_run' position '$position'");
  }
  else {
    $self->logconfess("Attempted to publish position $position lane-level ",
                      "$description files in run '$id_run'; ",
                      'the position is not plexed');
  }

  return $num_published;
}
## use critic

# Backend publisher for qc, index and ancillary files
sub _publish_support_files {
  my ($self, $files, $dest_coll, $with_spiked_control) = @_;

  my $publisher = WTSI::NPG::HTS::Publisher->new(irods  => $self->irods,
                                                 logger => $self->logger);

  my $num_files     = scalar @{$files};
  my $num_processed = 0;
  my $num_errors    = 0;
  foreach my $file (@{$files}) {
    my $dest = q[];

    try {
      $num_processed++;
      my $obj = WTSI::NPG::HTS::AncFileDataObject->new
        (collection  => $dest_coll,
         data_object => fileparse($file),
         irods       => $self->irods);

      $dest = $obj->str;
      $dest = $publisher->publish($file, $dest);

      if (defined $self->alt_process) {
        my @avus = $self->make_alt_metadata($self->alt_process);
        $self->_set_metadata($obj, @avus);
      }

      $obj->update_secondary_metadata($self->lims_factory,
                                      $with_spiked_control);
      $self->info("Published '$dest' [$num_processed / $num_files]");
    } catch {
      $num_errors++;

      ## no critic (RegularExpressions::RequireDotMatchAnything)
      my ($msg) = m{^(.*)$}mx;
      ## use critic
      $self->error("Failed to publish '$file' to '$dest' ",
                   "[$num_processed / $num_files]: ", $msg);
    };
  }

  if ($num_errors > 0) {
    $self->error("Encountered errors on $num_errors / ",
                 "$num_processed files processed");
  }

  return ($num_processed - $num_errors, $num_files);
}

# We are required by npg_tracking::illumina::run::short_info to
# implment this method
sub _build_run_folder {
  my ($self) = @_;

  if (! ($self->_given_path or $self->has_id_run or $self->has_name)){
    $self->logconfess('The run folder cannot be determined because ',
                      'it was not supplied to the constructor and ',
                      'no path, id_run or run name were available, ',
                      'from which it could be inferred');
  }

  return first { $_ ne q[] } reverse splitdir($self->runfolder_path);
}

sub _build_dest_collection  {
  my ($self) = @_;

  my @colls = ($DEFAULT_ROOT_COLL, $self->id_run);
  if (defined $self->alt_process) {
    push @colls, $self->alt_process
  }

  return catdir(@colls);
}

sub _build_qc_dest_collection  {
  my ($self) = @_;

  return catdir($self->dest_collection, $DEFAULT_QC_COLL);
}

sub _set_metadata {
  my ($self, $target, @avus) = @_;

  foreach my $avu (@avus) {
    try {
      my $attribute = $avu->{attribute};
      my $value     = $avu->{value};
      my $units     = $avu->{units};
      $target->supersede_avus($attribute, $value, $units);
    } catch {
      $self->error('Failed to set AVU ', pp($avu), ' on ',  $target->str);
    };
  }

  return $target;
}

# Return a sorted array of file paths, filtered by a regex.
sub _list_directory {
  my ($self, $path, $filter_pattern) = @_;

  my @file_list;
  opendir my $dh, $path or $self->logcroak("Failed to opendir '$path': $!");
  @file_list = grep { m{$filter_pattern}msx } readdir $dh;
  closedir $dh;

  @file_list = sort map { catfile($path, $_) } @file_list;

  return @file_list;
}

sub _lane_qc_stats_file {
  my ($self, $position) = @_;

  my $id_run = $self->id_run;
  my $qc_file_pattern = sprintf '%s_%d.bam_flagstats.json$',
    $id_run, $position;

  my @files = grep { m{$qc_file_pattern}msx }
    @{$self->list_lane_qc_files($position)};
  my $num_files = scalar @files;

  if ($num_files != 1) {
    $self->logcroak("Found $num_files QC files for id_run: $id_run, ",
                    "position: $position; ", pp(\@files));
  }

  return shift @files;
}

sub _plex_qc_stats_file {
  my ($self, $position, $tag_index) = @_;

  my $id_run = $self->id_run;
  my $qc_file_pattern = sprintf '%s_%d\#%d.bam_flagstats.json$',
    $id_run, $position, $tag_index;

  my @files = grep { m{$qc_file_pattern}msx }
    @{$self->list_plex_qc_files($position)};
  my $num_files = scalar @files;

  if ($num_files != 1) {
    $self->logcroak("Found $num_files QC files for id_run: $id_run, ",
                    "position: $position, tag_index: $tag_index; ",
                    pp(\@files));
  }

  return shift @files;
}

# Cache of JSON strings read from files
my $JSON_VALUE_CACHE;

sub _parse_json_file {
  my ($self, $file) = @_;

  if (exists $JSON_VALUE_CACHE->{$file}) {
    $self->debug("Returning cached JSON value for '$file'");
  }
  else {
    local $INPUT_RECORD_SEPARATOR = undef;

    $self->debug("Parsing JSON value from '$file'");

    open my $fh, '<', $file or
      $self->error("Failed to open '$file' for reading: ", $ERRNO);
    my $octets = <$fh>;
    close $fh or $self->warn("Failed to close '$file'");

    my $json = Encode::decode('UTF-8', $octets, Encode::FB_CROAK);
    $JSON_VALUE_CACHE->{$file} = $self->decode($json);
  }

  return $JSON_VALUE_CACHE->{$file};
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::RunPublisher

=head1 DESCRIPTION

Publishes alignment, QC and ancillary files to iRODS, adds metadata and
sets permissions.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
