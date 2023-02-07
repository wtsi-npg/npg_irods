package WTSI::NPG::HTS::PacBio::RunPublisher;

use namespace::autoclean;
use Data::Dump qw[pp];
use English qw[-no_match_vars];
use File::Basename;
use File::Spec::Functions qw[catdir catfile splitdir];
use List::AllUtils qw[any first];
use Moose;
use MooseX::StrictConstructor;
use Try::Tiny;

use WTSI::DNAP::Utilities::Params qw[function_params];
use WTSI::NPG::HTS::BatchPublisher;
use WTSI::NPG::HTS::WriteLocations;
use WTSI::NPG::HTS::PacBio::DataObjectFactory;
use WTSI::NPG::iRODS::Metadata;
use WTSI::NPG::iRODS::Publisher;
use WTSI::NPG::iRODS;

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::HTS::PathLister
         WTSI::NPG::HTS::PacBio::Annotator
         WTSI::NPG::HTS::PacBio::MetaQuery
       ];

our $VERSION = '';


# Default
our $DEFAULT_ROOT_COLL    = '/seq/pacbio';

my $MLWH_JSON_PATH = 'mlwh_locations.json'; # Move this?
my $PACBIO = 'pacbio';

has 'irods' =>
  (isa           => 'WTSI::NPG::iRODS',
   is            => 'ro',
   required      => 1,
   documentation => 'An iRODS handle to run searches and perform updates');

has 'obj_factory' =>
  (does          => 'WTSI::NPG::HTS::DataObjectFactory',
   is            => 'ro',
   required      => 1,
   lazy          => 1,
   builder       => '_build_obj_factory',
   documentation => 'A factory building data objects from files');

has 'runfolder_path' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'PacBio runfolder path');

has 'batch_publisher' =>
  (is            => 'ro',
   isa           => 'WTSI::NPG::HTS::BatchPublisher',
   required      => 1,
   lazy          => 1,
   builder       => '_build_batch_publisher',
   documentation => 'A publisher implementation capable to handling errors');

has 'restart_file' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   lazy          => 1,
   builder       => '_build_restart_file',
   documentation => 'A file containing a list of files for which ' .
                    'publication failed');

has 'mlwh_locations' =>
  (isa           => 'WTSI::NPG::HTS::WriteLocations',
   is            => 'ro',
   required      => 1,
   lazy          => 1,
   builder       => '_build_locations',
   documentation => 'An object used to build and write information to be ' .
                    'loaded into the seq_product_irods_locations table.');

has 'dest_collection' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   lazy          => 1,
   builder       => '_build_dest_collection',
   documentation => 'The destination collection within iRODS to store data');

has 'directory_pattern' =>
  (isa           => 'Str',
   is            => 'ro',
   init_arg      => undef,
   lazy          => 1,
   builder       => '_build_directory_pattern',
   documentation => 'Well directory pattern');

has 'force' =>
  (isa           => 'Bool',
   is            => 'ro',
   required      => 0,
   default       => 0,
   documentation => 'Force re-publication of files that have been published');

sub run_name {
  my ($self) = @_;

  return first { $_ ne q[] } reverse splitdir($self->runfolder_path);
}

=head2 smrt_names

  Arg [1]    : None

  Example    : my @names = $pub->smrt_names;
  Description: Return the SMRT cell names within a run, sorted lexically.
  Returntype : Array[Str]

=cut

sub smrt_names {
  my ($self) = @_;

  my $dir_pattern = $self->directory_pattern;
  my @dirs = grep { -d } $self->list_directory($self->runfolder_path,
                                               filter => $dir_pattern);
  my @names = sort map { first { $_ ne q[] } reverse splitdir($_) } @dirs;

  return @names;
}

=head2 smrt_path

  Arg [1]    : SMRT cell name, Str.

  Example    : my $path = $pub->smrt_path('1_A01');
  Description: Return the path to SMRT cell data within a run, given
               the cell name.
  Returntype : Str

=cut

sub smrt_path {
  my ($self, $smrt_name) = @_;

  my $name = $self->_check_smrt_name($smrt_name);
  return catdir($self->runfolder_path, $name);
}

sub read_restart_file {
  my ($self) = @_;

  $self->batch_publisher->read_state;
  return;
}


sub write_restart_file {
  my ($self) = @_;

  $self->batch_publisher->write_state;
  return
}

sub write_locations{
  my ($self) = @_;

  $self->mlwh_locations->write_locations;
  return;
}

## no critic (ProhibitManyArgs)
sub pb_publish_files {
  my ($self, $files, $dest_coll, $primary_avus, $secondary_avus,
      $extra_avus) = @_;

  $primary_avus   ||= [];
  $secondary_avus ||= [];
  $extra_avus     ||= [];

  ref $primary_avus eq 'ARRAY' or
    $self->logconfess('The primary_avus argument must be an ArrayRef');
  ref $secondary_avus eq 'ARRAY' or
    $self->logconfess('The secondary_avus argument must be an ArrayRef');

  my $primary_avus_callback = sub {
    return @{$primary_avus};
  };

  my $secondary_avus_callback = sub {
    return @{$secondary_avus};
  };

  my $extra_avus_callback = sub {
    return @{$extra_avus};
  };

  return $self->batch_publisher->publish_file_batch
    ($files, $dest_coll,
     primary_cb   => $primary_avus_callback,
     secondary_cb => $secondary_avus_callback,
     extra_cb     => $extra_avus_callback);
}
## use critic

sub _build_dest_collection  {
  my ($self) = @_;

  return catdir($DEFAULT_ROOT_COLL, $self->run_name);
}

sub _build_batch_publisher {
  my ($self) = @_;

  return WTSI::NPG::HTS::BatchPublisher->new
    (force                  => $self->force,
     irods                  => $self->irods,
     obj_factory            => $self->obj_factory,
     state_file             => $self->restart_file,
     mlwh_locations         => $self->mlwh_locations,
     require_checksum_cache => []); ## no md5s precreated for PacBio
}

sub _build_restart_file {
  my ($self) = @_;

  return catfile($self->runfolder_path, 'published.json');
}

sub _build_locations {
  my ($self) = @_;

  return WTSI::NPG::HTS::WriteLocations->new(path=> $self->runfolder_path . $MLWH_JSON_PATH, platform_name=> $PACBIO);
}

sub _build_obj_factory {
  my ($self) = @_;

  return WTSI::NPG::HTS::PacBio::DataObjectFactory->new(irods => $self->irods);
}

# Check that a SMRT cell name argument is given and valid
sub _check_smrt_name {
  my ($self, $smrt_name) = @_;

  defined $smrt_name or
    $self->logconfess('A defined smrt_name argument is required');
  any { $smrt_name eq $_ } $self->smrt_names or
    $self->logconfess("Invalid smrt_name argument '$smrt_name'");

  return $smrt_name;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::RunPublisher

=head1 DESCRIPTION

Attributes and methods used by PacBio publisher modules

=head1 AUTHOR

Guoying Qi E<lt>gq1@sanger.ac.ukE<gt>
Keith James E<lt>kdj@sanger.ac.ukE<gt>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2011, 2016, 2017, 2021 Genome Research Limited. All Rights
Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
