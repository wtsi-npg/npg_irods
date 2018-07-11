package WTSI::NPG::HTS::Illumina::Merged::AlnDataObject;

use namespace::autoclean;
use Moose;
use MooseX::StrictConstructor;
use npg_tracking::glossary::composition::factory::rpt_list;
#use WTSI::NPG::iRODS::Metadata qw[$COMPOSITION];
use JSON;

extends qw[WTSI::NPG::HTS::DataObject];

our $COMPOSITION = 'composition'; ## temporary

our $VERSION = '';

has '+is_restricted_access' =>
  (is        => 'ro');

sub _build_is_restricted_access {
  my ($self) = @_;
  return 1;
}


=head2 composition

  Arg [1]      None

  Example    : $obj->composition
  Description: Returns composition json string if defined
  Returntype : Str

=cut

sub composition {
  my ($self) = @_;

  my $composition;
  if($self->find_in_metadata($COMPOSITION)){
      $composition = $self->get_avu($COMPOSITION)->{value};
  }else{
      $self->logcroak(qq[No $COMPOSITION field set on ], $self->str);
  }
  return $composition;
}

=head2 rpt_list

  Arg [1]      None

  Example    : $obj->rpt_list
  Description: Returns rpt list from the composition
  Returntype : rpt_list

=cut

sub rpt_list {
   my ($self) = @_;

   my @components;
   for my $component( @{decode_json($self->composition)->{components}} ){
     my $rpt = $component->{id_run} . q[:];
     if($component->{position}) { $rpt .=  $component->{position}};
     if($component->{tag_index}){ $rpt .=  q[:]. $component->{tag_index}};
     push @components, $rpt;
   }
   my $composition = _rpt_list2composition(join q[;], @components);

   return $composition->freeze2rpt;
}

sub _rpt_list2composition {
  my $rpt_list = shift;
  return npg_tracking::glossary::composition::factory::rpt_list
         ->new(rpt_list => $rpt_list)->create_composition();
}



__PACKAGE__->meta->make_immutable;

no Moose;

1;

=head1 NAME

WTSI::NPG::HTS::Illumina::Merged::AlnDataObject

=head1 DESCRIPTION

Represents an merged alignment/map (CRAM) file in iRODS.

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
