package WTSI::NPG::HTS::ONT::GridIONMetaUpdater;

use namespace::autoclean;
use Moose;
use MooseX::StrictConstructor;
use Try::Tiny;

use WTSI::NPG::HTS::DataObject;
use WTSI::NPG::HTS::DefaultDataObjectFactory;
use WTSI::NPG::HTS::Metadata;
use WTSI::NPG::iRODS;

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::HTS::ONT::MetaQuery
         WTSI::NPG::HTS::ONT::Annotator
       ];

our $VERSION = '';

has 'irods' =>
  (is            => 'ro',
   isa           => 'WTSI::NPG::iRODS',
   required      => 1,
   documentation => 'An iRODS handle to run searches and perform updates');

has 'obj_factory' =>
  (is            => 'ro',
   isa           => 'WTSI::NPG::HTS::DataObjectFactory',
   required      => 1,
   lazy          => 1,
   builder       => '_build_obj_factory',
   documentation => 'A factory building data objects from files');

sub update_secondary_metadata {
  my ($self, $paths) = @_;

  defined $paths or
    $self->logconfess('A paths argument is required');
  ref $paths eq 'ARRAY' or
    $self->logconfess('The paths argument must be an array reference');

  my $num_paths = scalar @{$paths};
  $self->info("Updating metadata on $num_paths files");

  my $num_processed = 0;
  my $num_errors    = 0;
  foreach my $path (@{$paths}) {
    $self->info("Updating metadata on '$path' [$num_processed / $num_paths]");

    my $obj = $self->obj_factory->make_data_object($path);

    my $experiment_name = $obj->get_avu($EXPERIMENT_NAME)->{value};
    my $device_id       = $obj->get_avu($GRIDION_DEVICE_ID)->{value};

    my @run_records = $self->find_oseq_flowcells($experiment_name, $device_id);

    try {
      my @secondary_avus = $self->make_secondary_metadata(@run_records);
      $obj->update_secondary_metadata(@secondary_avus);

      $self->info("Updated metadata on '$path' ",
                  "[$num_processed / $num_paths]");
    } catch {
      $num_errors++;
      $self->error("Failed to update metadata on '$path' ",
                   "[$num_processed / $num_paths]: ", $_);
    };

    $num_processed++;
  }

  return $num_processed - $num_errors;
}

sub _build_obj_factory {
  my ($self) = @_;

  return WTSI::NPG::HTS::DefaultDataObjectFactory->new(irods => $self->irods);
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::ONT::GridIONMetaUpdater

=head1 DESCRIPTION

Updates secondary metadata and consequent permissions on ONT HTS data
files in iRODS. The information to do both of these operations is
provided by WTSI::DNAP::Warehouse::Schema. Any errors encountered on
each file are trapped and logged.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2017 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
