package WTSI::NPG::HTS::10x::RunPublisher;

use namespace::autoclean;
use Data::Dump qw[pp];
use English qw[-no_match_vars];
use File::Basename;
use List::AllUtils qw[any first none];
use File::Spec::Functions qw[catdir catfile splitdir];
use Moose;
use MooseX::StrictConstructor;
use Try::Tiny;

use WTSI::DNAP::Utilities::Params qw[function_params];
use WTSI::NPG::HTS::10x::DataObjectFactory;
use WTSI::NPG::HTS::LIMSFactory;
use WTSI::NPG::HTS::Publisher;
use WTSI::NPG::HTS::Seqchksum;
use WTSI::NPG::HTS::Types qw[AlnFormat];
use WTSI::NPG::iRODS::Metadata;
use WTSI::NPG::iRODS;
use npg_tracking::util::types;

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::DNAP::Utilities::JSONCodec
         WTSI::NPG::HTS::PathLister
         WTSI::NPG::HTS::10x::Annotator
         npg_tracking::illumina::run::short_info
         npg_tracking::illumina::run::folder
       ];

with qw[npg_tracking::illumina::run::long_info];

our $VERSION = '';

# Default 
our $DEFAULT_ROOT_COLL    = '/seq';

# Cateories of file to be published
our $FASTQ_CATEGORY = 'fastq';

our @FILE_CATEGORIES = ($FASTQ_CATEGORY);

has 'irods' =>
  (isa           => 'WTSI::NPG::iRODS',
   is            => 'ro',
   required      => 1,
   documentation => 'An iRODS handle to run searches and perform updates');

has 'tenx_fastq_path' =>
#  (isa           => 'npg_tracking::util::types::NpgTrackingDirectory',
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   builder       => '_build_tenx_fastq_path',
   documentation => 'tenx fastq path');

has 'obj_factory' =>
  (isa           => 'WTSI::NPG::HTS::DataObjectFactory',
   is            => 'ro',
   required      => 1,
   builder       => '_build_obj_factory',
   documentation => 'A factory building data objects from files');

has 'lims_factory' =>
  (is            => 'ro',
   isa           => 'WTSI::NPG::HTS::LIMSFactory',
   required      => 1,
   documentation => 'A factory providing st:api::lims objects');

has 'dest_collection' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   lazy          => 1,
   builder       => '_build_dest_collection',
   documentation => 'The destination collection within iRODS to store data');

has 'alt_process' =>
  (isa           => 'Maybe[Str]',
   is            => 'ro',
   required      => 0,
   documentation => 'Non-standard process used');

has '_path_cache' =>
  (isa           => 'HashRef',
   is            => 'ro',
   required      => 1,
   default       => sub { return {} },
   init_arg      => undef,
   documentation => 'Caches of file paths read from disk, indexed by method');

# The list_*_files methods are uncached. The verb in their name
# suggests activity. The corresponding methods generated here without
# the list_ prefix are caching. We are not using attributes here
# because the plex-level accessors have a position parameter.
my @CACHING_PLEX_METHOD_NAMES = qw[plex_fastq_files];

foreach my $method_name (@CACHING_PLEX_METHOD_NAMES) {
  __PACKAGE__->meta->add_method
    ($method_name,
     sub {
       my ($self, $position) = @_;

       my $cache = $self->_path_cache;
       if (exists $cache->{$method_name}->{$position}) {
         $self->debug('Using cached result for ', __PACKAGE__,
                      "::$method_name($position)");
       }
       else {
         $self->debug('Caching result for ', __PACKAGE__,
                      "::$method_name($position)");

         my $uncached_method_name = "list_$method_name";
         $cache->{$method_name}->{$position} =
           $self->$uncached_method_name($position);
       }

       return $cache->{$method_name}->{$position};
     });
}

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

  my $pos = $self->_check_position($position);

  return -d $self->lane_archive_path($pos);
}

=head2 list_plex_fastq_files

  Arg [1]    : Lane position, Int.

  Example    : $pub->list_plex_fastq_files($position);
  Description: Return paths of all plex-level fastq files for the
               given lane. Calling this method will access the file
               system. For cached access to the list, use the
               plex_fastq_files method.
  Returntype : ArrayRef[Str]

=cut

sub list_plex_fastq_files {
  my ($self, $position) = @_;

  my $pos = $self->_check_position($position);

  my $plex_file_pattern = sprintf '^.*_lane\-00%d\-chunk-00[\d][.]fastq[.]gz$',
    $pos;

  return [$self->list_directory($self->tenx_fastq_path,
                                $plex_file_pattern)];
}

=head2 publish_files

  Arg [1]    : None

  Named args : positions            ArrayRef[Int]. Optional.

  Example    : my ($num_files, $num_published, $num_errors) =
                 $pub->publish_files
  Description: Publish all files (lane- or plex-level) to iRODS. If the
               positions argument is supplied, only those positions will be
               published. The default is to publish all positions. Return
               the number of files, the number published and the number of
               errors.
  Returntype : Array[Int]

=cut

{
  my $positional = 1;
  my @named      = qw[positions];
  my $params = function_params($positional, @named);

  sub publish_files {
    my ($self) = $params->parse(@_);

    my $positions = $params->positions || [$self->positions];

    my ($num_files, $num_processed, $num_errors) = (0, 0, 0);

    foreach my $category (@FILE_CATEGORIES) {
      my ($nf, $np, $ne) =
        $self->_publish_file_category($category,
                                      $positions);
      $num_files     += $nf;
      $num_processed += $np;
      $num_errors    += $ne;
    }

    return ($num_files, $num_processed, $num_errors);
  }
}

=head2 publish_fastq_files

  Arg [1]    : None

  Named args : positions            ArrayRef[Int]. Optional.

  Example    : my ($num_files, $num_published, $num_errors) =
                 $pub->publish_fastq_files
  Description: Publish fastq files (lane- or plex-level) to
               iRODS. If the positions argument is supplied, only those
               positions will be published. The default is to publish all
               positions. Return the number of files, the number published
               and the number of errors.
  Returntype : Array[Int]

=cut

{
  my $positional = 1;
  my @named      = qw[positions];
  my $params = function_params($positional, @named);

  sub publish_fastq_files {
    my ($self) = $params->parse(@_);

    my $positions = $params->positions || [$self->positions];

    return $self->_publish_file_category($FASTQ_CATEGORY,
                                         $positions);
  }
}

# Check that a position argument is given and valid
sub _check_position {
  my ($self, $position) = @_;

  defined $position or
    $self->logconfess('A defined position argument is required');
  any { $position == $_ } $self->positions or
    $self->logconfess("Invalid position argument '$position'");

  return $position;
}

# A dispatcher to call the correct method for a given file category
# and lane plex state
sub _publish_file_category {
  my ($self, $category, $positions) = @_;

  defined $positions or
    $self->logconfess('A defined positions argument is required');
  ref $positions eq 'ARRAY' or
    $self->logconfess('The positions argument is required to be an ArrayRef');

  defined $category or
    $self->logconfess('A defined category argument is required');
  any { $category eq $_ } @FILE_CATEGORIES or
    $self->logconfess("Unknown file category '$category'");

  my $plex_method = sprintf 'publish_plex_%s_files', $category;

  my $num_files     = 0;
  my $num_processed = 0;
  my $num_errors    = 0;

  $self->info("Publishing $category files for positions: ", pp($positions));

  foreach my $position (@{$positions}) {
    my $pos = $self->_check_position($position);

    my ($nf, $np, $ne);
    if ($self->is_plexed($pos)) {
      ($nf, $np, $ne) = $self->$plex_method($pos);
    }

    $num_files     += $nf;
    $num_processed += $np;
    $num_errors    += $ne;
  }

  return ($num_files, $num_processed, $num_errors);
}

=head2 publish_plex_fastq_files

  Arg [1]    : Lane position, Int.

  Example    : my ($num_files, $num_published, $num_errors) =
                 $pub->publish_plex_fastq_files(8)
  Description: Publish plex-level fastq files in the
               specified lane to iRODS.  Return the number of files,
               the number published and the number of errors.
  Returntype : Array[Int]

=cut

sub publish_plex_fastq_files {
  my ($self, $position) = @_;

  my $pos = $self->_check_position($position);
  my $id_run = $self->id_run;

  if (not $self->is_plexed($pos)) {
    $self->logconfess("Attempted to publish position '$pos' lane-level ",
                      "fastq files in run '$id_run'; ",
                      'the position is plexed');
  }

  return $self->_publish_fastq_files($self->plex_fastq_files($pos),
                                         $self->dest_collection);
}

# Backend fastq file publisher
sub _publish_fastq_files {
  my ($self, $files, $dest_coll) = @_;

  my $primary_avus_callback = sub {
    return $self->_make_fastq_primary_meta(shift);
  };

  my $secondary_avus_callback = sub {
    $self->_make_fastq_secondary_meta(shift);
  };

  return $self->_safe_publish_files($files, $dest_coll,
                                    $primary_avus_callback,
                                    $secondary_avus_callback);
}

# Backend publisher for all files which handles errors and logging
sub _safe_publish_files {
  my ($self, $files, $dest_coll, $primary_avus_callback,
      $secondary_avus_callback) = @_;

  defined $files or
    $self->logconfess('A defined files argument is required');
  ref $files eq 'ARRAY' or
    $self->logconfess('The files argument must be an ArrayRef');
  defined $dest_coll or
    $self->logconfess('A defined dest_coll argument is required');

  my $publisher = WTSI::NPG::HTS::Publisher->new(irods => $self->irods);

  my $num_files     = scalar @{$files};
  my $num_processed = 0;
  my $num_errors    = 0;
  foreach my $file (@{$files}) {
    my $dest = q[];

    try {
      $num_processed++;
      my $obj = $self->_make_obj($file, $dest_coll);
      $dest = $obj->str;
      $dest = $publisher->publish($file, $dest);

      my @primary_avus = $primary_avus_callback->($obj);
      my ($num_pattr, $num_pproc, $num_perr) =
        $obj->set_primary_metadata(@primary_avus);

      my @secondary_avus = $secondary_avus_callback->($obj);
      my ($num_sattr, $num_sproc, $num_serr) =
        $obj->update_secondary_metadata(@secondary_avus);

      # Test metadata at the end
      if ($num_perr > 0) {
        $self->logcroak("Failed to set primary metadata cleanly on '$dest'");
      }
      if ($num_serr > 0) {
        $self->logcroak("Failed to set secondary metadata cleanly on '$dest'");
      }

      $self->info("Published '$dest' [$num_processed / $num_files]");
    } catch {
      $num_errors++;
      my @stack = split /\n/msx;  # Chop up the stack trace
      $self->error("Failed to publish '$file' to '$dest' cleanly ",
                   "[$num_processed / $num_files]: ", pop @stack);
    };
  }

  if ($num_errors > 0) {
    $self->error("Encountered errors on $num_errors / ",
                 "$num_processed files processed");
  }

  return ($num_files, $num_processed, $num_errors);
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

sub _build_tenx_fastq_path  {
  my ($self) = @_;

  my @n = split /_/smx, $self->run_folder;
  ## no critic (Variables::RequireNegativeIndices)
  my $flowcell = $n[@n-1];
  my $path = sprintf q[%s/%s/outs/fastq_path],dirname($self->recalibrated_path),$flowcell;

  return $path;
}

sub _build_dest_collection  {
  my ($self) = @_;

  my @colls = ($DEFAULT_ROOT_COLL, $self->id_run);
  if (defined $self->alt_process) {
    push @colls, $self->alt_process
  }

  return catdir(@colls);
}

sub _build_obj_factory {
  my ($self) = @_;

  return WTSI::NPG::HTS::10x::DataObjectFactory->new
    (irods => $self->irods);
}

sub _make_obj {
  my ($self, $file, $dest_coll) = @_;

  my ($filename, $directories, $suffix) = fileparse($file);

  my $obj = $self->obj_factory->make_data_object
    (catfile($dest_coll, $filename), id_run => $self->id_run);

  if (not $obj) {
    $self->logconfess("Failed to parse and make an object from '$file'");
  }

  return $obj;
}

sub _make_fastq_primary_meta {
  my ($self, $obj) = @_;

  my @pri = $self->make_primary_metadata
    ($self->id_run, $obj->position, $obj->read, $obj->tag,
     alt_process      => $self->alt_process);

  $self->debug(q[Created primary metadata AVUs for '], $obj->str,
               q[': ], pp(\@pri));

  return @pri;
}

sub _make_fastq_secondary_meta {
  my ($self, $obj) = @_;

  my @sec = $self->make_secondary_metadata
    ($self->lims_factory, $self->id_run, $obj->position, $obj->read, $obj->tag);

  $self->debug(q[Created secondary metadata AVUs for '], $obj->str,
               q[': ], pp(\@sec));

  return @sec;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::10x::RunPublisher

=head1 DESCRIPTION

Publishes fastq files to iRODS, adds metadata and
sets permissions.

An instance of RunPublisher is responsible for copying 10x
fastq files from the instrument run folder to a collection in
iRODS for a single, specific run

A RunPublisher provides methods to list the files and to copy ("publish")
them. The list or publish operations may be restricted to a specific
instrument lane position.

As part of the copying process, metadata are added to, or updated on,
the files in iRODS. Following the metadata update, access permissions
are set. The information to do both of these operations is provided by
an instance of st::api::lims.

If a run is published multiple times to the same destination
collection, the following take place:

 - the RunPublisher checks local (run folder) file checksums against
   remote (iRODS) checksums and will not make unnecessary updates

 - if a local file has changed, the copy in iRODS will be overwritten
   and additional metadata indicating the time of the update will be
   added

 - the RunPublisher will proceed to make metadata and permissions
   changes to synchronise with the metadata supplied by st::api::lims,
   even if no files have been modified

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2015, 2016 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
