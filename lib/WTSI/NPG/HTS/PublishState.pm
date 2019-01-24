package WTSI::NPG::HTS::PublishState;

use namespace::autoclean;

use Data::Dump qw[pp];
use English qw[-no_match_vars];
use Moose;
use Try::Tiny;

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::DNAP::Utilities::JSONCodec
       ];

our $VERSION = '';

has 'state' =>
  (isa           => 'HashRef',
   is            => 'rw',
   required      => 1,
   default       => sub { return {} },
   init_arg      => undef,
   documentation => 'State of all files published, across all batches. The ' .
                    'keys are local file paths and values are 1 if the ' .
                    'file has been published or 0 if it has not');

=head2 num_published

  Arg [1]    : None.

  Example    : my $i = $obj->num_published
  Description: Return the number of files published.
  Returntype : Int

=cut

sub num_published {
  my ($self) = @_;

  return scalar keys %{$self->state};
}

=head2 is_published

  Arg [1]    : Path, Str.

  Example    : $obj->is_published("/local/path")
  Description: Return true if the specified local file has been published.
  Returntype : Bool

=cut

sub is_published {
  my ($self, $path) = @_;

  return $self->state->{$path};
}

=head2 set_published

  Arg [1]    : Path, Str.

  Example    : $obj->set_published("/local/path")
  Description: Record the specified local file as published, updating
               this PublishState.
  Returntype : Void

=cut

sub set_published {
  my ($self, $path) = @_;

  $self->state->{$path} = 1;

  return;
}

=head2 merge_state

  Arg [1]    : Other state, WTSI::NPG::HTS::PublishState.

  Example    : $obj->merge_state($other_state)
  Description: Merge the state of another WTSI::NPG::HTS::PublishState
               with this PublishState. All the files published by the
               argument PublishState will be recorded as published,
               this PublishState.
  Returntype : Void

=cut

sub merge_state {
  my ($self, $state) = @_;

  ref $state and ref $state eq 'WTSI::NPG::HTS::PublishState' or
    $self->logconfess('The state argument must be a ',
                      'WTSI::NPG::HTS::PublishState');

  $self->debug('Merging new state ', pp($state->state));

  my $num_before = $self->num_published;

  my $num_duplicates = 0;
  my $num_added      = 0;
  my $current_state = $self->state;
  foreach my $key (keys %{$state->state}) {
    if (exists $current_state->{$key}) {
      $num_duplicates++;
    }

    $current_state->{$key} = $state->{$key};
    $num_added++;
  }

  my $num_after = $self->num_published;

  $self->debug("Before merge: $num_before items, ",
               "after merge: $num_after items, ",
               "added $num_added ($num_duplicates were duplicates)");

  return;
}

=head2 read_state

  Arg [1]    : State file path, Str.

  Example    : my $state = $obj->read_state('path/to/state.json')
  Description: Read state from a file and use it to set this PublishState's
               state. Return the state hat was read.
  Returntype : HashRef

=cut

sub read_state {
  my ($self, $file) = @_;

  defined $file or $self->logconfess('A defined file argument is required');

  local $INPUT_RECORD_SEPARATOR = undef;
  if (-e $file) {
    open my $fh, '<', $file or
      $self->logcroak("Failed to open '$file' for reading: ", $ERRNO);
    my $octets = <$fh>;
    close $fh or $self->warn("Failed to close '$file'");

    try {
      my $state = $self->decode($octets);
      $self->debug("Read from state file '$file': ", pp($state));
      $self->state($state);
    } catch {
      $self->logcroak('Failed to a parse JSON value from ',
                      "state file '$file': ", $_);
    };
  }

  return $self->state;
}

=head2 write_state

  Arg [1]    : State file path, Str.

  Example    : $obj->wriate_state('path/to/state.json')
  Description: Write this PublishState's state to a file.
  Returntype : Void

=cut

sub write_state {
  my ($self, $file) = @_;

  defined $file or $self->logconfess('A defined file argument is required');

  $self->debug("Writing to state file '$file':", pp($self->state));

  my $octets;
  try {
    $octets = $self->encode($self->state);
  } catch {
    $self->logcroak('Failed to a encode JSON value to ',
                    "state file '$file'. State was: ",
                    pp($self->state));
  };

  open my $fh, '>', $file or
    $self->logcroak("Failed to open '$file' for writing: ", $ERRNO);
  print $fh $octets or
    $self->logcroak("Failed to write to '$file': ", $ERRNO);
  close $fh or $self->warn("Failed to close '$file'");

  return;
}

no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PublishState

=head1 DESCRIPTION

A record of a set of local files that have been published to iRODS,
providing methods to store its contents in a JSON file.

=head1 AUTHOR

Keith James E<lt>kdj@sanger.ac.ukE<gt>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2019 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
