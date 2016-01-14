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

our $DEFAULT_ROOT_COLL = '/seq';
our $DEFAULT_QC_COLL   = 'qc';

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
# the list_ prefix are caching.
my @CACHING_LANE_METHOD_NAMES = qw(lane_alignment_files lane_qc_files);
my @CACHING_PLEX_METHOD_NAMES = qw(plex_alignment_files
                                   plex_qc_files
                                   plex_ancillary_files);

# Cache of lane-level file lists keyed on method name
my $LANE_FILES_CACHE = {};
# Cache of plex-level file lists keyed on method name
my $PLEX_FILES_CACHE = {};

foreach my $method_name (@CACHING_LANE_METHOD_NAMES) {

  __PACKAGE__->meta->add_method
    ($method_name,
     sub { return $LANE_FILES_CACHE->{$method_name} });

  around $method_name => sub {
    my ($orig, $self) = @_;

    my $uncached_method_name = "list_$method_name";
    if (exists $LANE_FILES_CACHE->{$method_name}) {
      $self->debug('Using cached result for ', __PACKAGE__,
                   "::$uncached_method_name");
    }
    else {
      $LANE_FILES_CACHE->{$method_name} = $self->$uncached_method_name;
    }

    return $self->$orig;
  };
}

foreach my $method_name (@CACHING_PLEX_METHOD_NAMES) {

  __PACKAGE__->meta->add_method
    ($method_name,
     sub {
       my ($self, $position) = @_;
       return $PLEX_FILES_CACHE->{$method_name}->{$position}
     });

  around $method_name => sub {
    my ($orig, $self, $position) = @_;

    my $uncached_method_name = "list_$method_name";
    if (exists $PLEX_FILES_CACHE->{$method_name}->{$position}) {
      $self->debug('Using cached result for ', __PACKAGE__,
                   "::$uncached_method_name($position)");
    }
    else {
      $PLEX_FILES_CACHE->{$method_name}->{$position} =
        $self->$uncached_method_name($position);
    }

    return $self->$orig($position);
  };
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

=head2 num_plex_reads

  Arg [1]    : Lane position, Int.
  Arg [1]    : Tag index, Int.

  Example    : $pub->num_plex_reads($position, $tag_index);
  Description: Return the total number of primary, non-supplementary
               reads.
  Returntype : Int

=cut

sub num_plex_reads {
  my ($self, $position, $tag_index) = @_;

  defined $position or
    $self->logconfess('A defined position argument is required');
  any { $position } $self->positions or
    $self->logconfess("Invalid position argument '$position'");

  my $qc_file = $self->_plex_qc_stats_file($position, $tag_index);
  my $flag_stats = $self->_parse_json_file($qc_file);

  return $flag_stats->{num_total_reads};
}

=head2 list_lane_alignment_files

  Arg [1]    : None

  Example    : $pub->list_lane_alignment_files;
  Description: Return paths of all lane-level alignment files for the run.
               Calling this method will access the file system. For
               cached access to the list, use the lane_alignment_files
               attribute.
  Returntype : ArrayRef[Str]

=cut

sub list_lane_alignment_files {
  my ($self) = @_;

  my $id_run = $self->id_run;
  my $archive_path = $self->archive_path;

  my $positions_pattern = sprintf '[%s]', join q[], $self->positions;
  my $lane_file_pattern = sprintf '^%d_%s\.%s$',
    $id_run, $positions_pattern, $self->file_format;

  $self->debug("Finding lane alignment files for run '$id_run' ",
               "in '$archive_path matching pattern '$lane_file_pattern'");
  my @file_list = $self->_list_directory($archive_path, $lane_file_pattern);
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
               plex_alignment_files attribute.
  Returntype : ArrayRef[Str]

=cut

sub list_plex_alignment_files {
  my ($self, $position) = @_;

  defined $position or
    $self->logconfess('A defined position argument is required');
  any { $position } $self->positions or
    $self->logconfess("Invalid position argument '$position'");

  my $id_run = $self->id_run;
  my $archive_path = $self->lane_archive_path($position);
  my $plex_file_pattern = sprintf '^%d_%d.*\.%s$',
    $id_run, $position, $self->file_format;

  $self->debug("Finding plex alignment files for run '$id_run' position ",
               "'$position' in '$archive_path' ",
               "matching pattern '$plex_file_pattern'");
  my @file_list = $self->_list_directory($archive_path, $plex_file_pattern);
  $self->debug("Found plex alignment files for run '$id_run' position ",
               "'$position' in '$archive_path': ", pp(\@file_list));

  return \@file_list;
}

=head2 list_lane_qc_files

  Arg [1]    : None

  Example    : $pub->list_qc_alignment_files;
  Description: Return paths of all lane-level qc files for the run.
               Calling this method will access the file system. For
               cached access to the list, use the lane_qc_files
               attribute.
  Returntype : ArrayRef[Str]

=cut

sub list_lane_qc_files {
  my ($self) = @_;

  my $id_run  = $self->id_run;
  my $qc_path = $self->qc_path;
  my $file_format = 'json';
  my $positions_pattern = sprintf '[%s]', join q[], $self->positions;
  my $lane_file_pattern = sprintf '^%d_%s.*\.%s$',
    $id_run, $positions_pattern, $file_format;

  $self->debug("Finding lane QC files for run '$id_run' in '$qc_path' ",
               "matching pattern '$lane_file_pattern'");
  my @file_list = $self->_list_directory($qc_path, $lane_file_pattern);
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
               plex_qc_files attribute.
  Returntype : ArrayRef[Str]

=cut

sub list_plex_qc_files {
  my ($self, $position) = @_;

  defined $position or
    $self->logconfess('A defined position argument is required');
  any { $position } $self->positions or
    $self->logconfess("Invalid position argument '$position'");

  my $id_run  = $self->id_run;
  my $qc_path = $self->lane_qc_path($position);
  my $file_format = 'json';

  my $plex_file_pattern = sprintf '^%d_%d.*\.%s$',
    $id_run, $position, $file_format;

  $self->debug("Finding plex QC files for run '$id_run' position ",
               "'$position' in '$qc_path' matching pattern ",
               "'$plex_file_pattern'");
  my @file_list = $self->_list_directory($qc_path, $plex_file_pattern);
  $self->debug("Found plex QC files for run '$id_run' position ",
               "'$position' in '$qc_path': ", pp(\@file_list));

  return \@file_list;
}

sub list_plex_ancillary_files {
  my ($self, $position) = @_;

  defined $position or
    $self->logconfess('A defined position argument is required');
  any { $position } $self->positions or
    $self->logconfess("Invalid position argument '$position'");

  my $id_run = $self->id_run;
  my $archive_path = $self->lane_archive_path($position);

  my $suffix_pattern = sprintf '(%s)', join q[|], @{$self->ancillary_formats};
  my $plex_file_pattern = sprintf '^%d_%d.*\.%s$',
    $id_run, $position, $suffix_pattern;

  $self->debug("Finding plex ancillary files for run '$id_run' position ",
               "'$position' in '$archive_path' ",
               "matching pattern '$plex_file_pattern'");
  my @file_list = $self->_list_directory($archive_path, $plex_file_pattern);
  # The file pattern match is deliberately kept simple. The downside
  # is that it matches one file that we do not want.
  @file_list = grep { ! m{markdups_metrics}msx } @file_list;

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

  my $num_published = $self->publish_lane_alignment_files($with_spiked_control);
  foreach my $position ($self->positions) {
    $num_published += $self->publish_plex_alignment_files($position,
                                                          $with_spiked_control);
  }

  return $num_published;
}

=head2 publish_lane_alignment_files

  Arg [1]    : HTS data has spiked control, Bool. Optional.

  Example    : my $num_published = $pub->publish_lane_alignment_files
  Description: Publish all the lane-level alignment files to iRODS.
               Return the number of files published without error.
  Returntype : Int

=cut

sub publish_lane_alignment_files {
  my ($self, $with_spiked_control) = @_;

  my $id_run        = $self->id_run;
  my @files         = @{$self->lane_alignment_files};
  my $num_files     = scalar @files;
  $self->info("Run '$id_run' has $num_files lane-level alignment files");

  my $publisher = WTSI::NPG::HTS::Publisher->new(irods  => $self->irods,
                                                 logger => $self->logger);
  my $num_published = $self->_publish_alignment_files($publisher, \@files,
                                                      $with_spiked_control);
  $self->info("Published $num_published / $num_files lane-level ",
              "alignment files in run '$id_run'");

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

  my $id_run = $self->id_run;
  my @files  = @{$self->plex_alignment_files($position)};
  my $num_files = scalar @files;
  $self->info("Run '$id_run' position '$position' ",
              "has $num_files plex-level alignment files");

  my $publisher = WTSI::NPG::HTS::Publisher->new(irods  => $self->irods,
                                                 logger => $self->logger);
  my $num_published = $self->_publish_alignment_files($publisher, \@files,
                                                      $self->dest_collection,
                                                      $with_spiked_control);
  $self->info("Published $num_published / $num_files plex-level ",
              "alignment files in run '$id_run' position '$position'");

  return $num_published;
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

  my $num_published = 0;

  # FIXME -- publish_lane_ancillary_files

  foreach my $position ($self->positions) {
    $num_published += $self->publish_plex_ancillary_files
      ($position, $with_spiked_control);
  }

  return $num_published;
}

sub publish_lane_ancillary_files {
  my ($self, $with_spiked_control) = @_;

  $self->logconfess('Not implemented');

  return;
}

=head2 publish_plex_alignment_files

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

  my $id_run = $self->id_run;
  my @files  = @{$self->plex_ancillary_files($position)};
  my $num_files = scalar @files;
  $self->info("Run '$id_run' position '$position' ",
              "has $num_files plex-level ancillary files");

  my $publisher = WTSI::NPG::HTS::Publisher->new(irods  => $self->irods,
                                                 logger => $self->logger);
  my $num_published = $self->_publish_support_files($publisher, \@files,
                                                    $self->dest_collection,
                                                    $with_spiked_control);
  $self->info("Published $num_published / $num_files plex-level ",
              "ancillary files in run '$id_run' position '$position'");

  return $num_published;
}

sub publish_qc_files {
  my ($self, $with_spiked_control) = @_;

  my $num_published = 0;

  # FIXME -- publish_lane_qc_files

  foreach my $position ($self->positions) {
    $num_published += $self->publish_plex_qc_files
      ($position, $with_spiked_control);
  }

  return $num_published;
}

sub publish_lane_qc_files {
  my ($self, $with_spiked_control) = @_;

  $self->logconfess('Not implemented');

  return;
}

sub publish_plex_qc_files {
  my ($self, $position, $with_spiked_control) = @_;

  my $id_run = $self->id_run;
  my @files  = @{$self->plex_qc_files($position)};
  my $num_files = scalar @files;
  $self->info("Run '$id_run' position '$position' ",
              "has $num_files plex-level JSON QC files");

  my $publisher = WTSI::NPG::HTS::Publisher->new(irods  => $self->irods,
                                                 logger => $self->logger);
  my $num_published = $self->_publish_support_files($publisher, \@files,
                                                    $self->qc_dest_collection,
                                                    $with_spiked_control);
  $self->info("Published $num_published / $num_files plex-level ",
              "JSON QC files in run '$id_run' position '$position'");

  return $num_published;
}

sub _publish_alignment_files {
  my ($self, $publisher, $files, $dest_coll, $with_spiked_control) = @_;

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

      $dest = $publisher->publish($file, $obj->str);

      # FIXME -- break primary metadata setup out into a new method
      $obj->set_primary_metadata;
      $obj->add_avu($IS_PAIRED_READ, $self->is_paired_read);

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

  return $num_processed - $num_errors;
}

sub _publish_support_files {
  my ($self, $publisher, $files, $dest_coll, $with_spiked_control) = @_;

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

      $dest = $publisher->publish($file, $obj->str);
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
                 "$num_processed ancillary files processed");
  }

  return $num_processed - $num_errors;
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
  if ($self->alt_process) {
    push @colls, $self->alt_process
  }

  return catdir(@colls);
}

sub _build_qc_dest_collection  {
  my ($self) = @_;

  return catdir($self->dest_collection, $DEFAULT_QC_COLL);
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

sub _plex_qc_stats_file {
  my ($self, $position, $tag_index) = @_;

  my $id_run = $self->id_run;
  my $qc_file_pattern = sprintf '%s_%s\#%d.bam_flagstats.json$',
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
