package WTSI::NPG::HTS::Illumina::Merged::MetaUpdater;

use namespace::autoclean;
use File::Basename;
use Moose;
use MooseX::StrictConstructor;
use Try::Tiny;

use WTSI::NPG::HTS::Illumina::Merged::DataObjectFactory;
use WTSI::NPG::iRODS;

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::HTS::Illumina::Merged::Annotator
       ];

extends qw[WTSI::NPG::HTS::Illumina::MetaUpdater];

our $VERSION = '';


=head2 update_secondary_metadata

  Arg [1]    : iRODS data objects to update, ArrayRef.

  Example    : $updater->update_secondary_metadata(['/path/to/file.cram']);
  Description: Update all secondary (LIMS-supplied) metadata and set data
               access permissions on the given files in iRODS. Return the
               number of files updated without error.
  Returntype : Int

=cut

override 'update_secondary_metadata' => sub {
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

    if ($obj and defined $obj->composition) {
      try {
        my @secondary_avus = $self->make_secondary_metadata
          ($self->lims_factory, $obj->rpt_list);

        $obj->update_secondary_metadata(@secondary_avus);

        $self->info("Updated metadata on '$path' ",
                    "[$num_processed / $num_paths]");
      } catch {
        $num_errors++;
        $self->error("Failed to update metadata on '$path' ",
                     "[$num_processed / $num_paths]: ", $_);
      };

    }

    $num_processed++;

  }

  $self->info("Updated metadata on $num_processed / $num_paths files");

  if ($num_errors > 0) {
    $self->error("Failed to update cleanly metadata on $num_paths files. ",
                 "$num_errors errors were recorded. See logs for details.")
  }

  return $num_processed - $num_errors;
};


__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::Illumina::Merged::MetaUpdater

=head1 DESCRIPTION

Updates secondary metadata and consequent permissions on Illumina merged
data files in iRODS. The information to do both of these operations is
provided by st::api::lims. Any errors encountered on each file are
trapped and logged.

=head1 AUTHOR

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2018 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
