package WTSI::NPG::HTS::10X::Annotator;

use Data::Dump qw[pp];
use Moose::Role;

use WTSI::NPG::HTS::Metadata;
use WTSI::NPG::iRODS::Metadata;

use npg_tracking::glossary::composition;

our $VERSION = '';

with qw[
         WTSI::NPG::iRODS::Annotator
         WTSI::NPG::HTS::CompositionAnnotator
         WTSI::NPG::HTS::Illumina::LIMSAnnotator
       ];

sub make_primary_metadata {
  my ($self, $composition) = @_;

  $composition or $self->logconfess('A composition argument is required');

  my @avus;
  push @avus, $self->make_composition_metadata($composition);


  return @avus;
}

sub make_secondary_metadata {
  my ($self, $composition, $factory) = @_;

  $composition or $self->logconfess('A composition argument is required');
  defined $factory or
    $self->logconfess('A defined factory argument is required');

  my $lims = $factory->make_lims($composition);

  my @avus;
  push @avus, $self->make_plex_metadata($lims);
  push @avus, $self->make_consent_metadata($lims);
  push @avus, $self->make_study_metadata($lims);
  push @avus, $self->make_study_id_metadata($lims);
  push @avus, $self->make_sample_metadata($lims);
  push @avus, $self->make_library_metadata($lims);

  my $rpt = $composition->freeze2rpt;
  $self->debug("Created metadata for '$rpt': ", pp(\@avus));

  return @avus;
}

no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::10X::Annotator

=head1 DESCRIPTION


=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>, Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2019 Genome Research Limited.  All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
