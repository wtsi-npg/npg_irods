package WTSI::NPG::HTS::Illumina::DataObjectFactory;

use namespace::autoclean;
use File::Basename;
use File::Slurp;
use File::Spec::Functions;
use Moose;
use MooseX::StrictConstructor;

use WTSI::DNAP::Utilities::Params qw[function_params];
use WTSI::NPG::HTS::Illumina::AlnDataObject;
use WTSI::NPG::HTS::Illumina::AgfDataObject;
use WTSI::NPG::HTS::Illumina::AncDataObject;
use WTSI::NPG::HTS::Illumina::IndexDataObject;
use WTSI::NPG::HTS::Illumina::InterOpDataObject;
use WTSI::NPG::HTS::Illumina::XMLDataObject;

use npg_tracking::glossary::composition;

our $VERSION = '';

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::HTS::DataObjectFactory
       ];

has 'irods' =>
  (is            => 'ro',
   isa           => 'WTSI::NPG::iRODS',
   required      => 1,
   default       => sub { return WTSI::NPG::iRODS->new },
   documentation => 'The iRODS connection handle');

has 'composition' =>
  (isa           => 'npg_tracking::glossary::composition',
   is            => 'ro',
   required      => 0,
   predicate     => 'has_composition',
   documentation => 'The composition describing the composed data');

=head2 make_data_object

  Arg [1]      Data object path, Str.

  Example    : my $obj = $factory->make_data_object($remote_path);
  Description: Return a new data object for a path. If the factory cannot
               construct a suitable object for the given path, it may
               return undef.

               If the factory's composition attribute is set, it will
               use that value when constructing data objects requiring
               a composition.

               If the attribute is not set, constructing data objects
               requiring a composition must be able to source one by
               another means, typically by inspecting their own
               iRODS metadata for the relevant values.

  Returntype : WTSI::NPG::HTS::DataObject or undef

=cut

sub make_data_object {
  my ($self, $remote_path) = @_;

  defined $remote_path or
    $self->logconfess('A defined remote_path argument is required');
  length $remote_path or
    $self->logconfess('A non-empty remote_path argument is required');

  my ($objname, $collection, $ignore) = fileparse($remote_path);
  my @init_args = (collection  => $collection,
                   data_object => $objname,
                   irods       => $self->irods);
  if ($self->has_composition) {
    push @init_args, composition => $self->composition;
  }

  my $align_regex   = $WTSI::NPG::HTS::Illumina::RegEx::ILLUMINA_REGEX_PATTERNS{'alignment_regex'};
  my $index_regex   = $WTSI::NPG::HTS::Illumina::RegEx::ILLUMINA_REGEX_PATTERNS{'index_regex'};

  my $xml_regex     = $WTSI::NPG::HTS::Illumina::RegEx::ILLUMINA_REGEX_PATTERNS{'xml_regex'};
  my $interop_regex = $WTSI::NPG::HTS::Illumina::RegEx::ILLUMINA_REGEX_PATTERNS{'interop_regex'};

  my $anc_regex = $WTSI::NPG::HTS::Illumina::RegEx::ILLUMINA_REGEX_PATTERNS{'ancillary_regex'};
  my $agf_regex = $WTSI::NPG::HTS::Illumina::RegEx::ILLUMINA_REGEX_PATTERNS{'genotype_regex'};

  my $obj;
  ## no critic (ControlStructures::ProhibitCascadingIfElse)
  if ($objname =~ m{$align_regex}msxi) {
    $self->debug('Making WTSI::NPG::HTS::Illumina::AlnDataObject from ',
                 "'$remote_path' matching $align_regex");
    $obj = WTSI::NPG::HTS::Illumina::AlnDataObject->new(@init_args);
  }
  elsif ($objname =~ m{$index_regex}msxi) {
    $self->debug('Making WTSI::NPG::HTS::Illumina::IndexDataObject from ',
                 "'$remote_path' matching $index_regex");
    $obj = WTSI::NPG::HTS::Illumina::IndexDataObject->new(@init_args);
  }
  elsif ($objname =~ m{$interop_regex}msxi) {
    $self->debug('Making WTSI::NPG::HTS::Illumina::InterOpDataObject from ',
                 "'$remote_path' matching $interop_regex");
    $obj = WTSI::NPG::HTS::Illumina::InterOpDataObject->new(@init_args);
  }
  elsif ($objname =~ m{$xml_regex}msxi) {
    $self->debug('Making WTSI::NPG::HTS::Illumina::XMLDataObject from ',
                 "'$remote_path' matching $xml_regex");
    $obj = WTSI::NPG::HTS::Illumina::XMLDataObject->new(@init_args);
  }
  elsif ($anc_regex && $objname =~ m{$anc_regex}msxi) {
    $self->debug('Making WTSI::NPG::HTS::Illumina::AncDataObject from ',
                 "'$remote_path' matching $anc_regex");
    $obj = WTSI::NPG::HTS::Illumina::AncDataObject->new(@init_args);
  }
  elsif ($agf_regex && $objname =~ m{$agf_regex}msxi) {
    $self->debug('Making WTSI::NPG::HTS::Illumina::AgfDataObject from ',
                 "'$remote_path' matching $agf_regex");
    $obj = WTSI::NPG::HTS::Illumina::AgfDataObject->new(@init_args);
  }
  else {
    $self->error('Failed to find suitable data object class ',
                 "for '$remote_path'");
    # return undef
  }
  ## use critic

  return $obj;
}

__PACKAGE__->meta->make_immutable;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::Illumina::DataObjectFactory

=head1 DESCRIPTION

A factory for creating iRODS data objects given local files from an
Illumina sequencing run. Different types of local file may require
data objects of different classes and an object of this class will
construct the appropriate one.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2016, 2017, 2018 Genome Research Limited. All Rights
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
