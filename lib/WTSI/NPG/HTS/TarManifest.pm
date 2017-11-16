package WTSI::NPG::HTS::TarManifest;

use namespace::autoclean;

use English qw[-no_match_vars];
use List::AllUtils qw[uniq];
use Moose;
use MooseX::StrictConstructor;

our $VERSION= '';

with qw[
         WTSI::DNAP::Utilities::Loggable
       ];

has 'manifest_index' =>
  (isa           => 'HashRef',
   is            => 'ro',
   required      => 1,
   default       => sub { return {} },
   init_arg      => undef,
   documentation => 'Maps each file path input to publish_file to the tar ' .
                    'file storing that file.');

has 'manifest_path' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'A manifest of published read files');

has 'manifest_updates' =>
  (isa           => 'ArrayRef',
   is            => 'ro',
   required      => 1,
   default       => sub { return [] },
   init_arg      => undef,
   documentation => 'An array of items added to this manifest since the ' .
                    'manifest file was last written');

=head2 add_items

  Arg [1]    : Path of tar file to which items have been added, Str.
  Arg [2]    : Paths within tar file of items added, ArrayRef[Str].

  Example    : $obj->add_items('/tmp/foo.tar', ['./1.txt', './2.txt'])
  Description: Update the manifest with information describing new items
               added to a tar file.
  Returntype : Undef

=cut

sub add_items {
  my ($self, $tar_path, $item_paths) = @_;

  my $updates = $self->manifest_updates;
  my $index   = $self->manifest_index;

  foreach my $item_path (sort @{$item_paths}) {
    $self->debug("Adding to manifest '$item_path' => '$tar_path'");

    if ($self->contains_item($item_path)) {
      my $mpath             = $self->manifest_path;
      my $existing_location = $index->{$item_path};

      if ($tar_path eq $existing_location) {
        $self->logwarn("In manifest '$mpath', '$item_path' added to ",
                       "'$tar_path' is already present");
      }
      else {
        $self->logwarn("In manifest '$mpath', '$item_path' added to ",
                       "'$tar_path' is already present in another ",
                       "tar file '$existing_location'");
      }
    }

    push @{$updates}, $item_path;
    $index->{$item_path} = $tar_path;
  }

  return;
}

=head2 contains_item

  Arg [1]    : None

  Example    : $obj->contains_item('./1.txt')
  Description: Return true if the manifest contains a record for
               the specified item path, indicating that the item
               has been archived.
  Returntype : Bool

=cut

sub contains_item {
  my ($self, $item_path) = @_;

  defined $item_path or
    $self->logconfess('A defined item_path argument is required');
  $item_path ne q[] or
    $self->logconfess('A non-empty item_path argument is required');

  return exists $self->manifest_index->{$item_path} ? 1 : 0;
}

=head2 file_exists

  Arg [1]    : None

  Example    : $obj->file_exists
  Description: Return true if the manifest file exists.
  Returntype : Undef

=cut

sub file_exists {
  my ($self) = @_;

  return -e $self->manifest_path;
}

sub items {
  my ($self) = @_;

  my @items = sort keys %{$self->manifest_index};

  return @items;
}

=head2 read_file

  Arg [1]    : None

  Example    : $obj->read_manifest_file
  Description: Read a manifest from manifest_path and update the index.
  Returntype : Undef

=cut

sub read_file {
  my ($self) = @_;

  my $manifest_path = $self->manifest_path;
  my $index         = $self->manifest_index;
  my $updates       = $self->manifest_updates;

  %{$index}   = (); # Clear existing entries
  @{$updates} = ();

  if ($self->file_exists) {
    open my $fh, '<', $manifest_path or
      $self->logcroak("Failed to open manifest '$manifest_path' ",
                      "for reading: $ERRNO");

    while (my $line = <$fh>) {
      chomp $line;
      my ($tar_path, $item_path) = split /\t/msx, $line;
      $self->debug("Read '$item_path' from existing manifest into index");
      $index->{$item_path} = $tar_path;
    }

    close $fh or $self->logcroak("Failed to close '$manifest_path': $ERRNO");
  }

  return;
}

sub tar_files {
  my ($self) = @_;

  my @tar_files = uniq values %{$self->manifest_index};
  @tar_files = sort @tar_files;

  return @tar_files;
}

=head2 update_file

  Arg [1]    : None

  Example    : $obj->update_file
  Description: Append to the manifest on disk information describing new items
               added to tar files.
  Returntype : Undef

=cut

sub update_file {
  my ($self) = @_;

  my $index         = $self->manifest_index;
  my $manifest_path = $self->manifest_path;

  open my $fh, '>>', $manifest_path
    or $self->logcroak("Failed to open manifest '$manifest_path' ",
                       "for appending: $ERRNO");

  foreach my $item_path (@{$self->manifest_updates}) {
    my $tar_path = $index->{$item_path};
    $self->debug("Adding to manifest file '$item_path' => '$tar_path'");
    print $fh "$tar_path\t$item_path\n" or
      $self->logcroak("Failed to write to filehandle of '$manifest_path'");
  }

  close $fh or $self->logcroak("Failed to close '$manifest_path': $ERRNO");

  @{$self->manifest_updates} = (); # Clear pending updates

  return;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::TarManifest

=head1 DESCRIPTION

A file containing a record, for a number of tar files, which files
have been stored in them. The manifest is a text file containing one
row per file archived, associating that file with a tar file:

<Tar file path><tab><Archived file path>

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
