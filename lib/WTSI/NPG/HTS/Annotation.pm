package WTSI::NPG::HTS::Annotation;

use Moose::Role;

use WTSI::NPG::HTS::Types qw(ArrayRefOfHTSMetaAttr);

our $VERSION = '';

our %HTS_METADATA_ATTR =
  (
   alignment                => 'alignment',
   control                  => 'control',
   id_run                   => 'id_run',
   is_paired_read           => 'is_paired_read',
   library                  => 'library',
   library_id               => 'library_id',
   manual_qc                => 'manual_qc',
   position                 => 'lane',
   reference                => 'reference',
   sample_consent_withdrawn => 'consent_withdrawn',
   sample_common_name       => 'sample_common_name',
   sample_public_name       => 'sample_public_name',
   study                    => 'study',
   study_id                 => 'study_id',
   study_title              => 'study_title',
   study_accession_number   => 'study_accession_number',
   tag                      => 'tag',
   tag_index                => 'tag_index',
   target                   => 'target',
   total_reads              => 'total_reads',
  );

has 'metadata_attrs' =>
  (is            => 'ro',
   isa           => ArrayRefOfHTSMetaAttr,
   required      => 1,
   default       => sub { return [sort keys %HTS_METADATA_ATTR] },
   init_arg      => undef,
   documentation => 'Permitted metadata attributes.');


=head2 

  Arg [1]    : None

  Example    : 
  Description: 
  Returntype : 

=cut

sub is_metadata_attr {
  my ($self, $name) = @_;

  return exists $HTS_METADATA_ATTR{$name};
}

=head2 

  Arg [1]    : None

  Example    : 
  Description: 
  Returntype : 

=cut

sub metadata_attr {
  my ($self, $name) = @_;

  if (not $self->is_metadata_attr($name)) {
    $self->logconfess("There is no metadata attribute to store a '$name'");
  }

  return $HTS_METADATA_ATTR{$name};
}

1;


__END__

=head1 NAME

WTSI::NPG::HTS::Annotation

=head1 DESCRIPTION

A role providing methods to calculate metadata for WTSI HTS runs.

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
