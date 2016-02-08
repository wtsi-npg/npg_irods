package WTSI::NPG::HTS::MetaUpdater;

use namespace::autoclean;
use Moose;
use Try::Tiny;

use WTSI::NPG::HTS::AlMapFileDataObject;
use WTSI::NPG::iRODS;

with qw[WTSI::DNAP::Utilities::Loggable];

our $VERSION = '';

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

sub BUILD {
  my ($self) = @_;

  # Use our logger to log activity in attributes.
  $self->lims_factory->logger($self->logger);
  $self->irods->logger($self->logger);
  return;
}

=head2 update_secondary_metadata

  Arg [1]    : iRODS data objects to update, ArrayRef.
  Arg [2]    : HTS data has spiked control, Bool. Optional.

  Example    : $updater->update_secondary_metadata(['/path/to/file.cram']);
  Description: Update all secondary (LIMS-supplied) metadata and set data
               access permissions on the given files in iRODS. Return the
               number of files updated without error.
  Returntype : Int

=cut

sub update_secondary_metadata {
  my ($self, $files, $with_spiked_control) = @_;

  defined $files or
    $self->logconfess('A files argument is required');
  ref $files eq 'ARRAY' or
    $self->logconfess('The files argument must be an array reference');

  my $num_files = scalar @{$files};

  $self->info("Updating metadata on $num_files files");

  my $num_processed = 0;
  my $num_errors    = 0;
  foreach my $file (@{$files}) {
    $self->info("Updating metadata on '$file' [$num_processed / $num_files]");

    my $obj = WTSI::NPG::HTS::AlMapFileDataObject->new($self->irods, $file);

    try {
      $obj->update_secondary_metadata($self->lims_factory,
                                      $with_spiked_control);
      $self->info("Updated metadata on '$file' ",
                  "[$num_processed / $num_files]");
    } catch {
      $num_errors++;
      $self->error("Failed to update metadata on '$file' ",
                   "[$num_processed / $num_files]: ", $_);
    };

    $num_processed++;
  }

  $self->info("Updated metadata on $num_processed / $num_files files");

  if ($num_errors > 0) {
    $self->error("Failed to update cleanly metadata on $num_files files. ",
                 "$num_errors errors were recorded. See logs for details.")
  }

  return $num_processed - $num_errors;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::MetaUpdater

=head1 DESCRIPTION

Updates secondary metadata on HTS data files in iRODS. Any errors
encountered on each file are trapped and logged.

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
