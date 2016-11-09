package WTSI::NPG::DataSub::MetaUpdater;

use namespace::autoclean;
use Data::Dump qw[pp];
use Moose;
use Try::Tiny;

use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::DataObject;
use WTSI::NPG::iRODS::Metadata qw[$FILE_MD5];

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::iRODS::Utilities
       ];

our $VERSION = '';

# To be moved to WTSI::NPG::iRODS::Metadata
our $EBI_RUN_ACC  = 'ebi_run_acc';
our $EBI_SUB_ACC  = 'ebi_sub_acc';
our $EBI_SUB_DATE = 'ebi_sub_date';
our $EBI_SUB_MD5  = 'ebi_sub_md5';

has 'irods' =>
  (is            => 'ro',
   isa           => 'WTSI::NPG::iRODS',
   required      => 1,
   documentation => 'An iRODS handle to run searches and perform updates');


=head2 update_submission_metadata

  Arg [1]    : iRODS collection under which to search for data objects
  Arg [2]    : Files to update, ArrayRef[WTSI::NPG::DataSub::File]

  Example    : my $num_updated = $obj->

  Description: Update all EBI submission (subtrack-supplied) metadata on the
               data objects in iRODS corresponding to the supplied files.
               Return the number of data objects updated without error.
  Returntype : Int

=cut

sub update_submission_metadata {
  my ($self, $root, $files) = @_;

  defined $root or $self->logconfess('A defined root argument is required');
  $root eq q[] and $self->logconfess('A non-empty root argument is required');

  defined $files or
    $self->logconfess('A files argument is required');
  ref $files eq 'ARRAY' or
    $self->logconfess('The files argument must be an array reference');

  my ($num_files, $num_processed, $num_errors) = (scalar @{$files}, 0, 0);

  foreach my $file (@{$files}) {
    my $md5       = $file->submission_md5;
    my $file_name = $file->file_name;

    try {
      $num_processed++;

      my @objs = grep { $_->data_object eq $file_name }
                 map  { WTSI::NPG::iRODS::DataObject->new($self->irods, $_) }
                 $self->irods->find_objects_by_meta($root,
                                                    [$FILE_MD5 => $md5]);

      my $num_objs = scalar @objs;
      if ($num_objs > 1) {
        $self->logcroak("Found $num_objs data objects with MD5 '$md5' and ",
                        "file name '$file_name' in '$root': ",
                        pp(map { $_->str } @objs));
      }

      if ($num_objs == 0) {
        $self->logwarn("No data data object exists with MD5 '$md5' and ",
                       "file name '$file_name' in '$root'");
      }
      else {
        my $obj = shift @objs;
        my $path = $obj->str;
        $self->debug("Found one data object with MD5 '$md5' and ",
                     "file name '$file_name' in '$root': '$path'");

        my @datasub_metadata = $self->_make_datasub_metadata($file);
        $self->_set_metadata($obj, \@datasub_metadata);
      }
    } catch {
      $num_errors++;
      my @stack = split /\n/msx;   # Chop up the stack trace
      $self->error('Failed to update submission metadata for submitted file ',
                   "'$file_name' with MD5 '$md5' ",
                   "[$num_processed / $num_files]: ", pop @stack);
    };
  }

  $self->info("Processed $num_processed / $num_files files");

  if ($num_errors > 0) {
    $self->error('Failed to update submission metadata cleanly; ',
                 "errors were recorded on $num_errors / $num_processed ",
                 'processed. See logs for details.')
  }

  my $num_updated = $num_processed - $num_errors;
  $self->info("Updated metadata on $num_updated / $num_files files");

  return $num_updated;
}

sub _make_datasub_metadata {
  my ($self, $file) = @_;

  my @avus;
  push @avus, $self->make_avu($EBI_RUN_ACC,  $file->run_accession);
  push @avus, $self->make_avu($EBI_SUB_ACC,  $file->submission_accession);
  push @avus, $self->make_avu($EBI_SUB_DATE, $file->submission_date->ymd);
  push @avus, $self->make_avu($EBI_SUB_MD5,  $file->submission_md5);

  return @avus;
}

sub _set_metadata {
  my ($self, $obj, $avus) = @_;

  my @avus = @{$avus};
  my ($num_attributes, $num_processed, $num_errors) = (scalar @avus, 0, 0);

  foreach my $avu (@avus) {
    my $attr  = $avu->{attribute};
    my $value = $avu->{value};

    try {
      $obj->supersede_avus($attr, $value);
      $num_processed++;
    } catch {
      $num_errors++;
      $self->error("Failed to supersede with attribute '$attr' and value ",
                   "'$value'", q[: ], $_);
    };
  }

  if ($num_errors > 0) {
    my $path = $obj->str;
    $self->logcroak("Failed to update cleanly metadata on '$path'");
  }

  return;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__


=head1 NAME

WTSI::NPG::DataSub::MetaUpdater

=head1 DESCRIPTION

Updates EBI submission metadata on data objects in iRODS.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2016 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
