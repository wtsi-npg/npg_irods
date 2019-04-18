package WTSI::NPG::HTS::10X::ResultSet;

use namespace::autoclean;
use Data::Dump qw[pp];
use File::Basename;
use Moose;
use MooseX::StrictConstructor;

with qw[
         WTSI::DNAP::Utilities::Loggable
       ];

our $VERSION = '';

has 'result_files' =>
  (isa           => 'ArrayRef[Str]',
   is            => 'ro',
   required      => 1,
   builder       => '_build_result_files',
   lazy          => 1,
   documentation => 'The files in the result set');


our %TENX_PART_PATTERNS =
  (alignment_regex => sub {
     return q[[.]bam$];
   },
   index_regex     => sub {
     return q[[.]bai$];
   },
   matrix_regex    => sub {
     return q[[.]h5$];
   },
   ancillary_regex => sub {
     return sprintf q[(%s)$],
       join q[|],
       '[.]cloupe',
       '[.]csv',
       '[.]tsv',
       '[.]mtx',
       '[.]html';
   });

=head2 composition_files

  Arg [1]    : None

  Example    : $set->composition_files
  Description: Return a sorted array of composition JSON files.
  Returntype : Array

=cut

sub composition_files {
  my ($self) = @_;

  return grep { m{[.]composition[.]json$}msx } @{$self->result_files};
}

sub alignment_files {
  my ($self, $name) = @_;

  return $self->_filter_files('alignment_regex', $name);
}

sub index_files {
  my ($self, $name) = @_;

  return $self->_filter_files('index_regex', $name);
}

sub matrix_files {
  my ($self, $name) = @_;

  return $self->_filter_files('matrix_regex', $name);
}

sub ancillary_files {
  my ($self, $name) = @_;

  return $self->_filter_files('ancillary_regex', $name);
}

sub _build_result_files {
  my ($sel, $name) = @_;

  return [];
}

sub _make_filter_regex {
  my ($self, $category, $name) = @_;

  exists $TENX_PART_PATTERNS{$category} or
    $self->logconfess("Invalid file category '$category'. Expected one of : ",
                      pp([sort keys %TENX_PART_PATTERNS]));

  my $regex = $TENX_PART_PATTERNS{$category}->($name);

  return qr{$regex}msx;
}

sub _filter_files {
  my ($self, $category, $name) = @_;

  my $regex = $self->_make_filter_regex($category);
  $self->debug("$category filter regex: '$regex'");

  my @cfiles = grep { m{\/\Q$name\E[.composition.json]}msx }
    $self->composition_files;
  if (scalar @cfiles > 1) {
    $self->logconfess('Found multiple composition files with the same name. ',
                      'Expected one: ', pp(\@cfiles));
  }

  my $cfile = shift @cfiles;
  my ($filename, $directories, $suffix) = fileparse($cfile);
  # Accept only files in or below the directory containing the
  # composition file

  return grep { m{$regex}msx }
         grep { m{\Q$directories\E} } @{$self->result_files};
}

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::10X::ResultSet

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
