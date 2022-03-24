package WTSI::NPG::Data::SingleReplicaMetaUpdater;

use namespace::autoclean;
use Carp;
use Data::Dump qw[pp];
use DateTime;
use DateTime::Duration;
use Moose;
use Try::Tiny;

use WTSI::DNAP::Utilities::Params qw[function_params];

use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::DataObject;

with qw[
  WTSI::DNAP::Utilities::Loggable
];

our $VERSION = '';

# The name of an iRODS specific query (i.e. SQl query) to be called via iquest,
# which will find a data objects that are candidates for moving to a single-replica
# iRODS resource.
our $SPECIFIC_QUERY_NAME = 'find_single_replica_targets';

# The iRODS metadata attribute that, when added to a data object, triggers migration
# to single-replica iRODS resource. The migration is carried out by iRODS rules on
# the server. Its AVU value is "1", when present.
our $SINGLE_REPLICA_ATTR = 'tier:single-copy';

# The iRODS metadata attribute holding the MD5 data from a data object that was
#  submitted to the EBI.
our $EBI_SUB_MD5_ATTR = 'ebi_sub_md5';

has 'irods' =>
    (is            => 'ro',
     isa           => 'WTSI::NPG::iRODS',
     required      => 1,
     documentation => 'An iRODS handle to run searches and perform updates');

has 'default_grace_period' =>
    (isa           => 'Int',
     is            => 'ro',
     required      => 1,
     default       => 2,
     documentation => 'The default grace period where data submitted to ' .
                      'the EBI are retain as multiple replicas, in years');

=head2 update_single_replica_metadata

  Args       : None

  Named args : begin_date
               Include only data objects with a dcterms:created later than
               this value, DateTime.

               end_date
               Include only data objects with a dcterms:created earlier than
               this value, DateTime.

  Example    : $meta->update_single_replica_metadata(begin_date => $begin,
                                                     end_date   => $end);

  Description: Add a metadata AVU to data objects created between the specified
               dates that will trigger their migration to a single-replica
               resource.  Return the number of data objects found, the number
               updated and the number of errors.

               Errors are raised in the following circumstances:

               - An object's ebi_sub_md5 value (i.e. the MD5 of the data submitted
                 to the EBI) differs from the object's current MD5. i.e. The object
                 has been updated since submission and is therefore in an unknown
                 state.

               - An object's current size is inconsistent with its MD5 i.e. it
                 has a size >0 bytes and does not have the MD5 of any empty file.
                 i.e. The object is in an inconsistent state.

               - An object does not have any valid replicas. i.e. The object is in
                 a state where it apparently has no valid data.

               Encountering an error on a subset of data objects will not prevent
               others being processed.

  Returntype : Array[Int]
=cut

{
  my $positional = 1;
  my @named      = qw[begin_date end_date zone];
  my $params     = function_params($positional, @named);

  sub update_single_replica_metadata {
    my ($self) = $params->parse(@_);

    my $grace = DateTime::Duration->new(years => $self->default_grace_period);
    my $default_begin = DateTime->from_epoch(epoch => 0);
    my $default_end   = DateTime->now->subtract($grace);

    my $begin_date = $params->begin_date ? $params->begin_date :
                     $default_begin;
    my $end_date   = $params->end_date ? $params->end_date :
                     $default_end;

    $begin_date->isa('DateTime') or
      $self->logconfess('The begin_date argument must be a DateTime');
    $end_date->isa('DateTime') or
      $self->logconfess('The end_date argument must be a DateTime');

    my $paths = $self->_find_candidate_objects($begin_date, $end_date,
                                               $params->zone);
    return $self->_do_update_metadata($paths);
  }
}

sub _do_update_metadata {
  my ($self, $paths) = @_;

  my ($num_objs, $num_processed, $num_errors) = (scalar @{$paths}, 0, 0);

  foreach my $path (@{$paths}) {
    try {
      $num_processed++;

      my $obj = WTSI::NPG::iRODS::DataObject->new($self->irods, $path);
      $self->debug(sprintf q[Found %s, size: %d, ] .
                           q[valid replicates: %d, ] .
                           q[invalid replicates: %d, ] .
                           q[checksum: %s, ] .
                           q[consistent size: %s],
                           $obj->str, $obj->size,
                           scalar $obj->valid_replicates,
                           scalar $obj->invalid_replicates,
                           $obj->checksum,
                           $obj->is_consistent_size);

      if (not $obj->find_in_metadata($EBI_SUB_MD5_ATTR)) {
        croak sprintf q[missing %s in metadata], $EBI_SUB_MD5_ATTR;
      }

      my $ebi_sub_md5 = $obj->get_avu($EBI_SUB_MD5_ATTR)->{value};
      if (not $ebi_sub_md5 eq $obj->checksum) {
        croak sprintf q[object's ebi_sub_md5 %s is not equal to ] .
                      q[the current checksum %s], $ebi_sub_md5, $obj->checksum;
      }

      if (not $obj->is_consistent_size) {
        croak sprintf q[object size %d is not consistent with its checksum %s],
          $obj->size, $obj->has_checksum ? $obj->checksum : 'undef';
      }

      if (not $obj->valid_replicates) {
        croak sprintf q[object does not have any valid replicas: %s],
          scalar $obj->valid_replicates;
      }

      $self->info(sprintf q[Marking %s for migration to single replica],
                          $obj->str);
      $obj->add_avu($SINGLE_REPLICA_ATTR, 1);
    } catch {
      $num_errors++;
      $self->error(sprintf q[Failed to mark '%s' for migration ] .
                           q[to single replica [%d / %d]: %s],
                           $path, $num_processed, $num_objs, $_);
    };
  }

  return ($num_objs, $num_processed, $num_errors);
}

sub _find_candidate_objects {
  my ($self, $begin_date, $end_date, $zone) = @_;

  $self->debug(sprintf q[Running query %s %s %s], $SPECIFIC_QUERY_NAME,
                       $begin_date->iso8601, $end_date->iso8601);

  my @args = ('--no-page');
  if ($zone) {
    push @args, '-z', $zone;
  }

  my @records = WTSI::DNAP::Utilities::Runnable->new
    (executable => 'iquest',
     arguments  => [@args, '--sql', $SPECIFIC_QUERY_NAME,
                    $begin_date->iso8601,
                    $end_date->iso8601])->run->split_stdout;

  my $paths = $self->_parse_iquest_records(@records);
  $self->debug(sprintf q[Found %d paths], scalar @{$paths});

  return $paths;
}

sub _parse_iquest_records {
  my ($self, @records) = @_;

  # iquest does not add a trailing record separator. Having one makes
  # processing easier because each set of 3 lines may be treated the same
  # way and there is no special case for having a single record.
  my $record_separator = '----';
  push @records, $record_separator;

  my @paths;
  my @path_elements;
  foreach my $line (@records) {
    chomp $line;
    next if $line =~ m{^\s*$}msx;

    # Work around the iquest bug/misfeature where it mixes its logging output
    # with its data output
    next if $line =~ m{^Zone is}msx;
    next if $line =~ m{^No rows found}msx;

    $self->debug("iquest: $line");
    if ($line eq $record_separator and @path_elements) {
      push @paths, join q[/], @path_elements;
      @path_elements = ();
      next;
    }

    push @path_elements, $line;
  }

  return \@paths;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::Data::SingleReplicaMetaUpdater

=head1 DESCRIPTION

Updates metadata on the set of data objects eligible to be migrated to
a single-replica resource by adding the AVU tier:single-copy = 1. Data
objects having this AVU will be migrated automatically by the iRODS
server.

The trigger AVU may only be added by using this API, not removed. This
class requires that its supporting iRODS specific query be installed
on the server by an administrator.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2022 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
