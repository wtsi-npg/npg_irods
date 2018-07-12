package WTSI::NPG::HTS::Illumina::Merged::Annotator;

use Moose::Role;

with qw[ 
         WTSI::DNAP::Utilities::Loggable 
         WTSI::NPG::HTS::Illumina::Annotator
       ] => {
  -excludes    => [qw(make_secondary_metadata)]
};

our $VERSION = '';

=head2 make_secondary_metadata

  Arg [1]    : Factory for st:api::lims objects, WTSI::NPG::HTS::LIMSFactory.
  Arg [2]    : rpt_list.

  Example    : my @avus = $ann->make_secondary_metadata($factory,$rpt_list);

  Description: Return an array of metadata AVUs describing the data

  Returntype : Array[HashRef]

=cut

sub make_secondary_metadata {
  my ($self, $factory, $rpt_list) = @_;

  defined $factory or
    $self->logconfess('A defined factory argument is required');
  defined $rpt_list or
    $self->logconfess('A defined rpt_list argument is required');

  my $lims = $factory->make_merged_lims($rpt_list);

  my @avus;
  push @avus, $self->make_consent_metadata($lims);
  push @avus, $self->make_study_metadata($lims);
  push @avus, $self->make_sample_metadata($lims);
  push @avus, $self->make_library_metadata($lims);

  return @avus;
}

no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::Illumina::Merged::Annotator

=head1 DESCRIPTION

A role providing methods to calculate metadata for Illumina
merged data.

=head1 AUTHOR

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2018 Genome Research Limited. All Rights
Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
