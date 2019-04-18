package WTSI::NPG::HTS::10X::RunPublisher;

use namespace::autoclean;
use Data::Dump qw[pp];
use File::Basename;
use File::Spec::Functions qw[catfile abs2rel];

use Moose;
use MooseX::StrictConstructor;
use Try::Tiny;

use WTSI::NPG::HTS::10X::DataObjectFactory;
use WTSI::NPG::HTS::10X::ResultSet;
use WTSI::NPG::HTS::TreePublisher;

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::HTS::Illumina::Annotator
         WTSI::NPG::HTS::Illumina::CompositionFileParser
         WTSI::NPG::HTS::RunPublisher
        ];

our $VERSION = '';

has 'lims_factory' =>
  (is            => 'ro',
   isa           => 'WTSI::NPG::HTS::LIMSFactory',
   required      => 1,
   documentation => 'A factory providing st:api::lims objects');

has 'publish_state' =>
  (isa           => 'WTSI::NPG::HTS::PublishState',
   is            => 'ro',
   required      => 1,
   builder       => '_build_publish_state',
   lazy          => 1,
   documentation => 'State of all files published, across all batches');

has 'restart_file' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   lazy          => 1,
   builder       => '_build_restart_file',
   documentation => 'A file containing a record of files successfully ' .
                    'published');

has 'run_files' =>
  (isa           => 'ArrayRef[Str]',
   is            => 'ro',
   required      => 1,
   builder       => '_build_run_files',
   lazy          => 1,
   documentation => 'All of the files in the dataset, some or all of which ' .
                    'will be published');

has 'force' =>
  (isa           => 'Bool',
   is            => 'ro',
   required      => 0,
   default       => 0,
   documentation => 'Force re-publication of files that have been published');

has 'max_errors' =>
  (isa           => 'Int',
   is            => 'ro',
   required      => 0,
   predicate     => 'has_max_errors',
   documentation => 'The maximum number of errors permitted before ' .
                    'the remainder of a publishing process is aborted');

has 'result_set' =>
  (isa           => 'WTSI::NPG::HTS::10X::ResultSet',
   is            => 'ro',
   required      => 1,
   builder       => '_build_result_set',
   lazy          => 1,
   documentation => 'The set of results files in the run');

sub publish_files {
  my ($self) = @_;

  my ($num_files, $num_processed, $num_errors) = (0, 0, 0);

  my $primary_avus = sub {
    my ($obj) = @_;

    return $self->make_primary_metadata($obj->composition);
  };

  my $secondary_avus = sub {
    my ($obj) = @_;

    return $self->make_secondary_metadata($obj->composition,
                                          $self->lims_factory);
  };

  my @cfiles = $self->result_set->composition_files;
  $self->debug('Found 10X composition files: ', pp(\@cfiles));

  foreach my $cfile (@cfiles) {
     my ($name, $directory, $suffix) =
       $self->parse_composition_filename($cfile);
     my $composition = $self->read_composition_file($cfile);

     my $obj_factory = WTSI::NPG::HTS::10X::DataObjectFactory->new
       (composition => $composition,
        irods       => $self->irods);

     my $tree_publisher = $self->_make_tree_publisher;

     my @files;
     push @files, $self->result_set->alignment_files($name);
     push @files, $self->result_set->index_files($name);
     push @files, $self->result_set->matrix_files($name);
     push @files, $self->result_set->ancillary_files($name);

     my ($nf, $np, $ne) =
       $tree_publisher->publish_tree(\@files, $obj_factory,
                                     $primary_avus,
                                     $secondary_avus);
     $num_files     += $nf;
     $num_processed += $np;
     $num_errors    += $ne;
   }

  return ($num_files, $num_processed, $num_errors);
}

sub read_restart_file {
  my ($self) = @_;

  $self->publish_state->read_state($self->restart_file);
  return;
}

sub write_restart_file {
  my ($self) = @_;

  $self->publish_state->write_state($self->restart_file);
  return;
}

sub _make_tree_publisher {
  my ($self) = @_;

  my @init_args = (dest_collection  => $self->dest_collection,
                   force            => $self->force,
                   irods            => $self->irods,
                   publish_state    => $self->publish_state,
                   source_directory => $self->source_directory);
  if ($self->has_max_errors) {
    if ($self->num_errors >= $self->max_errors) {
      $self->logconfess(sprintf q[Internal error: the number of publish ].
                        q[errors (%d) reached the maximum permitted (%d) ] .
                        q[while working],
                        $self->num_errors, $self->max_errors);
    }

    my $max_errors = $self->max_errors - $self->num_errors;
    push @init_args, max_errors => $max_errors;
  }

  return WTSI::NPG::HTS::TreePublisher->new(@init_args);
}

sub _dest_coll {
  my ($self, $path) = @_;

  my $local_rel  = abs2rel($path, $self->source_directory);
  my $remote_abs = catfile($self->dest_collection, $local_rel);
  my ($obj_name, $dest_coll) = fileparse($remote_abs);
  $self->debug("Destination collection of '$path' is '$dest_coll'");

  return $dest_coll;
}

sub _build_run_files {
  my ($self) = @_;

  my $dir = $self->source_directory;
  -d $dir or $self->logcroak("Source directory '$dir' is not a directory");

  $self->info("Finding files under '$dir', recursively");

  my @files = grep { -f } $self->list_directory($dir, recurse => 1);

  return \@files;
}

sub _build_restart_file {
  my ($self) = @_;

  return catfile($self->source_directory, 'published.json');
}

sub _build_publish_state {
  my ($self) = @_;

  return WTSI::NPG::HTS::PublishState->new;
}

sub _build_result_set {
  my ($self) = @_;

  return WTSI::NPG::HTS::10X::ResultSet->new
    (result_files => $self->run_files)
}

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::10X::RunPublisher

=head1 DESCRIPTION


=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>, Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2019 Genome Research Limited.  All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
