package WTSI::NPG::HTS::Illumina::AncDataObject;

use namespace::autoclean;
use Data::Dump qw[pp];
use List::AllUtils qw[none];
use Moose;
use MooseX::StrictConstructor;
use Try::Tiny;

use WTSI::NPG::iRODS::Metadata;

our $VERSION = '';

extends 'WTSI::NPG::HTS::Illumina::DataObject';

has '+is_restricted_access' =>
  (is => 'ro');

has '+primary_metadata' =>
  (is => 'ro');

sub BUILD {
  my ($self) = @_;

  # Modifying read-only attribute
  push @{$self->primary_metadata}, $ALT_PROCESS;

  # Restricted access files will get some secondary metadata, but
  # limited to $STUDY_ID
  if ($self->is_restricted_access) {
    $self->secondary_metadata([$STUDY_ID]);
  }

  return;
}

=head2 is_restricted_access

  Arg [1]      None

  Example    : $obj->is_restricted_access
  Description: Return true if the file contains or may contain sensitive
               information and is not for unrestricted public access.
               This true for bed, tab, quant.zip and JSON files.
  Returntype : Bool

=cut

# Only apply secondary metadata (which should contain study_id) on
# those files that are to have their access restricted.
override 'update_secondary_metadata' => sub {
  my ($self, @avus) = @_;

  my $path = $self->str;

  # No attributes, none processed, no errors
  my @counts = (0, 0, 0);
  if ($self->is_restricted_access) {
    @counts = super();
  }
  else {
    $self->debug("Skipping secondary metadata update for '$path'");
  }

  return @counts;
};

sub _build_is_restricted_access {
  my ($self) = @_;

  my $format = lc $self->file_format;
  # The contents of BED, TAB, ZIP and JSON formatted files is assumed to be
  # sensitive and thus they are given restricted access
  return ($format eq 'bed'  or
          $format eq 'json' or
          $format eq 'tab'  or
          $format eq 'zip');
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::Illumina::AncDataObject

=head1 DESCRIPTION

Represents Illumina ancillary files in iRODS. This class overrides
some base class behaviour to introduce:

 Custom primary metadata restrictions.

 Custom secondary metadata restrictions.

 Custom behaviour with respect to the file having restricted access
 (i.e. for BED and JSON files only).

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
