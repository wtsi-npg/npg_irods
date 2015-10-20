package WTSI::NPG::HTS::MetaUpdater;

use namespace::autoclean;
use Moose;
use Try::Tiny;

use WTSI::DNAP::Warehouse::Schema;
use WTSI::NPG::HTS::HTSFileDataObject;
use WTSI::NPG::iRODS;

our $VERSION = '';

has 'irods' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::iRODS',
   required => 1);

has 'schema' =>
  (is       => 'ro',
   isa      => 'WTSI::DNAP::Warehouse::Schema',
   required => 1);

sub run {
  my ($self, $files) = @_;

  defined $files or
    $self->logconfess('A files argument is required');
  ref $files eq 'ARRAY' or
    $self->logconfess('The files argument must be an array reference');

  my $num_files = scalar @{$files};

  $self->info("Updating metadata on $num_files files");

  my $num_processed = 0;
  my $num_errors = 0;
  foreach my $file (@{$files}) {
    $self->debug("Updating metadata on '$file' [$num_processed / $num_files]");

    my $obj = WTSI::NPG::HTS::HTSFileDataObject->new($irods, $file);

    try {
      $obj->update_secondary_metadata($schema);
      $self->debug("Updated metadata on '$file' [$num_processed / $num_files]");
    } catch {
      $num_error++;
      $self->error("Failed to update metadata on '$file' ",
                   "[$num_processed / $num_files]: ", $_);
    };
  }

  $self->info("Updated metadata on $num_updated / $num_files files");

  if ($num_errors > 0) {
    $self->error("Failed to update cleanly metadata on $num_files files. ",
                 "$num_errors errors were recorded. See logs for details.")
  }

  return $num_errors == 0;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::MetaUpdater

=head1 DESCRIPTION



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
