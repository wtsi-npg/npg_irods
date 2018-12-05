package WTSI::NPG::HTS::Illumina::MetaUpdater;

use namespace::autoclean;
use Data::Dump qw[pp];
use File::Basename;
use Moose;
use MooseX::StrictConstructor;
use Try::Tiny;

use WTSI::NPG::HTS::Illumina::DataObjectFactory;
use WTSI::NPG::HTS::Illumina::ResultSet;
use WTSI::NPG::iRODS;

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::HTS::Illumina::Annotator
         WTSI::NPG::HTS::Illumina::CompositionFileParser
       ];

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

=head2 update_secondary_metadata

  Arg [1]    : Composition file data objects to, whose corresponding data
               are to be updated, ArrayRef.
  Arg [2]    : HTS data has spiked control, Bool. Optional.

  Example    : $updater->update_secondary_metadata
                 (['/path/to/<name>.composition.json']);
  Description: Update all secondary (LIMS-supplied) metadata and set data
               access permissions on data files corresponding the given
               composition files, in iRODS. Return the number of files
               updated without error.

               To correspond to a composition file, a data file must be
               directly or indirectly within (a subcollection of) the
               composition file and match the "name" prefix of the
               composition file.

               E.g. All of

               /seq/12345/<name>.composition.json
               /seq/12345/<name>.cram
               /seq/12345/qc/<name>.genotype.json

              correspond to <name>.composition.json

  Returntype : Int

=cut

sub update_secondary_metadata {
  my ($self, $paths, $with_spiked_control) = @_;

  defined $paths or
    $self->logconfess('A paths argument is required');
  ref $paths eq 'ARRAY' or
    $self->logconfess('The paths argument must be an array reference');

  my $num_paths = scalar @{$paths};
  $self->info("Updating metadata for files related to $num_paths ",
              'composition files');

  my $num_processed = 0;
  my $num_errors    = 0;
  foreach my $composition_file (@{$paths}) {
    $self->info("Updating metadata for files related to '$composition_file'");
    my $num_objs = 0;

    try {
      my ($name, $collection, $suffix) =
        $self->parse_composition_filename($composition_file);
      my $composition =
        $self->make_composition($self->irods->read_object($composition_file));

      my $obj_factory = WTSI::NPG::HTS::Illumina::DataObjectFactory->new
        (ancillary_formats => [$self->hts_ancillary_suffixes],
         composition       => $composition,
         genotype_formats  => [$self->hts_genotype_suffixes],
         irods             => $self->irods);

      my ($objs, $colls) =
        $self->irods->list_collection($collection, 'RECURSE');

      my $result_set =
        WTSI::NPG::HTS::Illumina::ResultSet->new(result_files => $objs);

      my @aln = $result_set->alignment_files($name);
      $self->debug('Updating alignment files: ', pp(\@aln));

      my @anc = $result_set->ancillary_files($name);
      $self->debug('Updating ancillary files: ', pp(\@anc));

      my @gen = $result_set->genotype_files($name);
      $self->debug('Updating genotype files: ', pp(\@gen));

      my @qc  = $result_set->qc_files($name);
      $self->debug('Updating QC files: ', pp(\@qc));

      my @objs = (@aln, @anc, @gen, @qc);
      $num_objs = scalar @objs;
      $self->info("Updating metadata on a total of $num_objs data objects");

      foreach my $obj_path (@objs) {
        $self->info("Updating metadata on '$obj_path'");
        my $obj = $obj_factory->make_data_object($obj_path);

        if ($obj and defined $obj->id_run) {
          try {
            my @secondary_avus = $self->make_secondary_metadata
              ($composition, $self->lims_factory,
               with_spiked_control => $with_spiked_control);
            $obj->update_secondary_metadata(@secondary_avus);

            $self->info("Updated metadata on '$obj_path'");
          } catch {
            $num_errors++;
            $self->error("Failed to update metadata on '$obj_path': ", $_);
          };
        }

        $num_processed++;
      }
    } catch {
      $num_errors++;
      $self->error("Failed to update metadata for '$composition_file' ",
                   "[$num_processed / $num_objs]: ", $_);
    };
  }

  $self->info('Updated metadata for files related to ',
              " $num_processed / $num_paths files");

  if ($num_errors > 0) {
    $self->error('Failed to update cleanly metadata on files related to ',
                 "$num_paths composition files. $num_errors errors ",
                 'were recorded.');
  }

  return $num_processed - $num_errors;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::Illumina::MetaUpdater

=head1 DESCRIPTION

Updates secondary metadata and consequent permissions on Illumina HTS
data files in iRODS. The information to do both of these operations is
provided by st::api::lims. Any errors encountered on each file are
trapped and logged.

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
