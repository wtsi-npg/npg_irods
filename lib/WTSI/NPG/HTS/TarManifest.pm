package WTSI::NPG::HTS::TarManifest;

use namespace::autoclean;

use English qw[-no_match_vars];
use List::AllUtils qw[uniq];
use Moose;
use MooseX::StrictConstructor;

use WTSI::NPG::HTS::TarItem;

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
   documentation => 'Maps each file path input to publish_file to an ' .
                    'array of tar file(s)');

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

=head2 add_item

  Arg [1]    : Path of tar file to which the item has been added, Str.
  Arg [2]    : Path within tar file of item added, Str.
  Arg [2]    : Checksum of the item added, Str.

  Example    : $obj->add_item('/tmp/foo.tar', '1.txt', $md5)
  Description: Update the manifest with information describing a file
               added to a tar file.
  Returntype : Undef

=cut

sub add_item {
  my ($self, $tar_path, $item_path, $item_checksum) = @_;

  my $mpath   = $self->manifest_path;
  my $index   = $self->manifest_index;
  my $updates = $self->manifest_updates;

  if ($self->contains_item($item_path)) {
    my $existing_item = $index->{$item_path};

    foreach my $elt (@{$existing_item}) {
      my $existing_tar  = $elt->tar_path;

      if ($tar_path eq $existing_tar) {
        $self->warn("In manifest '$mpath', '$item_path' added to ",
                    "'$tar_path' is already present");
      }
      else {
        $self->warn("In manifest '$mpath', '$item_path' added to ",
                    "'$tar_path' is already present in another ",
                    "tar file '$existing_tar'");
      }
    }
  }

  my $item = WTSI::NPG::HTS::TarItem->new(checksum  => $item_checksum,
                                          item_path => $item_path,
                                          tar_path  => $tar_path);
  push @{$updates}, $item;

  $index->{$item_path} ||= [];
  push @{$index->{$item_path}}, $item;

  return;
}

=head2 contains_item

  Arg [1]    : Item path, Str.

  Example    : $obj->contains_item('1.txt')
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

=head2 get_item

  Arg [1]    : Item path, Str.

  Example    : $obj->get_item('1.txt');
  Description: Return a TarItem describing the last occurrence of
               the named item in a tar file described by the manifest.
  Returntype : WTSI::NPG::HTS::TarItem

=cut

sub get_item {
  my ($self, $item_path) = @_;

  my @items = $self->item_history($item_path);
  return $items[-1];
}

=head2 item_history

  Arg [1]    : Item path, Str.

  Example    : $obj->item_history('1.txt');
  Description: Return an array of TarItems describing all occurrences
               the named item in a tar files described by the manifest.
  Returntype : ArrayRef[WTSI::NPG::HTS::TarItem]

=cut

sub item_history {
  my ($self, $item_path) = @_;

  my $mpath = $self->manifest_path;
  $self->contains_item($item_path) or
    $self->logcroak("Manifest '$mpath' does not contain item '$item_path'");

  return @{$self->manifest_index->{$item_path}};
}

=head2 item_paths

  Arg [1]    : None

  Example    : $obj->item_paths
  Description: Return an array of all the item paths described by the
               manifest;
  Returntype : Array[Str]

=cut

sub item_paths {
  my ($self) = @_;

  my @item_paths = sort keys %{$self->manifest_index};

  return @item_paths;
}

sub contains_tar {
  my ($self, $tar_path) = @_;

  defined $tar_path or
    $self->logconfess('A defined tar_path argument is required');
  $tar_path eq q[] and
    $self->logconfess('A non-empty tar_path argument is required');

  my %tar_index = map { $_ => 1 } $self->tar_paths;

  return exists $tar_index{$tar_path};
}

sub tar_paths {
  my ($self) = @_;

  my @tar_paths;
  foreach my $item_path ($self->item_paths) {
    my $item = $self->get_item($item_path);
    push @tar_paths, $item->tar_path;
  }
  @tar_paths = uniq @tar_paths;
  @tar_paths = sort @tar_paths;

  return @tar_paths;
}

sub tar_items {
  my ($self, $tar_path) = @_;

  my $mpath = $self->manifest_path;
  $self->contains_tar($tar_path) or
    $self->logcroak("Manifest '$mpath' does not contain tar '$tar_path'");

  my @items;

  foreach my $item_path ($self->item_paths) {
    push @items, @{$self->manifest_index->{$item_path}};
  }
  @items = grep { $_->tar_path eq $tar_path } @items;
  @items = sort { $a->item_path cmp $b->item_path } @items;

  return @items;
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
      my ($tar_path, $item_path, $item_checksum) = split /\t/msx, $line;
      $self->debug("Read '$item_path' with checksum '$item_checksum' ",
                   'from existing manifest into index');

      my $item = WTSI::NPG::HTS::TarItem->new(checksum  => $item_checksum,
                                              item_path => $item_path,
                                              tar_path  => $tar_path);
      $index->{$item_path} ||= [];
      push @{$index->{$item_path}}, $item;
    }

    close $fh or $self->logcroak("Failed to close '$manifest_path': $ERRNO");
  }

  return;
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

  foreach my $item (@{$self->manifest_updates}) {
    my $checksum  = $item->checksum;
    my $item_path = $item->item_path;
    my $tar_path  = $item->tar_path;
    my $row       = "$tar_path\t$item_path\t$checksum";
    $self->debug("Adding to manifest file '$manifest_path': $row");
    print $fh "$row\n" or
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

<Tar file path><tab><Archived file path><tab><Archived file checksum>

The nomenclature used in the API is that an archived "file" is an
absolute path to the original file passed to tar and an "item" is
a relative path of the same within the tar archive.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2017, 2018 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
