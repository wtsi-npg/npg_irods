package WTSI::NPG::HTS::PacBio::Reportdata;

use namespace::autoclean;
use DateTime;
use Moose;
use MooseX::StrictConstructor;
use MooseX::Storage;

use WTSI::NPG::HTS::PacBio::Metadata;

with Storage( 'traits' => ['OnlyWhenBuilt'],
              'format' => 'JSON',
              'io'     => 'File' );

our $VERSION = '';

has 'meta_data' =>
  (isa           => 'WTSI::NPG::HTS::PacBio::Metadata',
   is            => 'ro',
   required      => 1,
   documentation => 'Meta data from file');

has 'created_at' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   default       => DateTime->now->stringify,
   documentation => 'The creation time');

has 'reports'  =>
  (isa           => 'HashRef',
   is            => 'ro',
   required      => 0,
   predicate     => 'has_reports',
   documentation => 'Report info');

=head2 thaw

  Arg [1]    : None
  Example    : my ($ob) = $report->thaw($json)
  Description: Extends thaw method provided by the MooseX::Storage to disable
               module version checking when deseralizing JSON string.
  Returntype : Str

=cut

around 'thaw' => sub {
  my $orig = shift;
  my $self = shift;
  return $self->$orig(@_, 'check_version' => 0);
};

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::Reportdata

=head1 DESCRIPTION

Represents components of the merged report

=head1 AUTHOR

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2020 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
