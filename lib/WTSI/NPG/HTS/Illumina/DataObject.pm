package WTSI::NPG::HTS::Illumina::DataObject;

use namespace::autoclean;
use Data::Dump qw[pp];
use List::AllUtils qw[uniq];
use Moose;
use MooseX::StrictConstructor;

use npg_tracking::glossary::composition::component::illumina;
use npg_tracking::glossary::composition::factory;

our $VERSION = '';

extends 'WTSI::NPG::HTS::ComposedDataObject';

sub BUILD {
  my ($self, $args) = @_;

  # Only use the backward compatibility parameters
  if (not exists $args->{composition}) {
    my $id_run    = delete $args->{id_run};
    my $tag_index = delete $args->{tag_index};
    my $position  = delete $args->{position};
    my $subset    = delete $args->{subset};

    ## no critic(ControlStructures::ProhibitPostfixControls)
    my @buildargs;
    push @buildargs, id_run    => $id_run    if defined $id_run;
    push @buildargs, position  => $position  if defined $position;
    push @buildargs, tag_index => $tag_index if defined $tag_index;
    push @buildargs, subset    => $subset    if defined $subset;
    ## use critic

    if (@buildargs) {
      my $pkg = 'npg_tracking::glossary::composition::component::illumina';
      my $factory = npg_tracking::glossary::composition::factory->new;

      $factory->add_component($pkg->new(@buildargs));
      $self->composition($factory->create_composition);
    }
  }

  return;
};

sub id_run {
  my ($self) = @_;

  return $self->_unique_value('id_run');
}

sub position {
  my ($self) = @_;

  return $self->_unique_value('position');
}

sub tag_index {
  my ($self) = @_;

  return $self->_unique_value('tag_index');
}

sub subset {
  my ($self) = @_;

  return $self->_unique_value('subset');
}

sub _unique_value {
  my ($self, $attribute) = @_;

  my @vals = uniq map { $_->$attribute } $self->composition->components_list;
  my $num_vals = scalar @vals;
  if ($num_vals == 1) {
    return $vals[0];
  }
  else {
    $self->logconfess("Failed to get an unique '$attribute' value for '",
                      $self->str, " which is composed of $num_vals of these: ",
                      pp(\@vals))
  }

  return;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::Illumina::DataObject

=head1 DESCRIPTION

A data object whose contents are Illumina platform data and supports
the id_run, position, tag_index and subset methods for backwards
compatibility.

The preferred way to make an instance is to pass a composition to the
constructor. However, for the sake of backward compatibility, the
constructor parameters 'id_run', 'position', 'tag_index' and 'subset'
are supported. If no composition is provided, the values of these
arguments, if provided, will be used to make an appropriate new
composition during the BUILD method.

=head1 AUTHOR

Keith James E<lt>kdj@sanger.ac.ukE<gt>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2018 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
