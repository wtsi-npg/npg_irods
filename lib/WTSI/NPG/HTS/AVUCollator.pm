package WTSI::NPG::HTS::AVUCollator;

use Data::Dump qw(pp);
use Moose::Role;

our $VERSION = '';

with 'WTSI::DNAP::Utilities::Loggable';

sub collate_avus {
  my ($self, @avus) = @_;

  # Collate into lists of values per attribute
  my %collated_avus;
  foreach my $avu (@avus) {
    my $avu_str = pp($avu);
    if (not ref $avu eq 'HASH') {
      $self->logconfess("Failed to collate AVU $avu_str : it is not a HashRef");
    }
    if (not exists $avu->{attribute}) {
      $self->logconfess("Failed to collate AVU $avu_str : missing attribute");
    }
    if (not exists $avu->{value}) {
      $self->logconfess("Failed to collate AVU $avu_str : missing value");
    }

    my $attr  = $avu->{attribute};
    my $value = $avu->{value};
    if (exists $collated_avus{$attr}) {
      push @{$collated_avus{$attr}}, $value;
    }
    else {
      $collated_avus{$attr} = [$value];
    }
  }

  $self->debug('Collated ', scalar @avus, ' AVUs into ',
               scalar keys %collated_avus, ' lists');

  return \%collated_avus;
}

no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::AVUCollator

=head1 DESCRIPTION

A role providing methods to collate metadata for WTSI HTS runs. This
could be pushed back into WTSI::NPG::iRODS, removing the need for this
role.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
