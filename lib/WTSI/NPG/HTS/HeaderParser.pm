package WTSI::NPG::HTS::HeaderParser;

use Moose::Role;

with qw[WTSI::DNAP::Utilities::Loggable];

our $VERSION = '';

=head2 get_records

  Arg [1]      Entire header. This may be provided as a Str or ArrayRef[Str]
               where each header row is an element of the array.
  Arg [2]      Header tag whose records are to be returned, Str.

  Example    : my @pg_records = $obj->get_records($header, 'PG');
  Description: Return an array of records for the given tag.
  Returntype : Array[Str]

=cut

sub get_records {
  my ($self, $header, $tag) = @_;

  defined $header or $self->logconfess('A defined header argument is required');

  my @records;
  if (not ref $header) {
    @records = split m{\r\n?||\n}msx, $header;
  }
  elsif (ref $header eq 'ARRAY') {
    @records = @{$header};
  }
  else {
    $self->logconfess('The header argument must be a scalar or an ArrayRef: ',
                      $header);
  }

  if (defined $tag) {
    @records = grep { m{^[@]$tag}msx } @records;
  }

  return @records;
}

=head2 get_tag_values

  Arg [1]      Header record, Str.
  Arg [2]      Header tag whose tag-values are to be returned, Str.

  Example    : foreach my $pg_record ($obj->get_records($header, 'PG') {
                 my @tag_values = $obj->get_tag_values($pg_record, 'CL');
               }
  Description: Return an array of tag-values for the given tag.
  Returntype : Array[Str]

=cut

sub get_tag_values {
  my ($self, $header_record, $tag) = @_;

  defined $header_record or
    $self->logconfess('A defined header_record argument is required');

  my @tag_values = split m{\t}msx, $header_record;
  if (defined $tag) {
    @tag_values = grep { m{^$tag:}msx } @tag_values;
  }

  return @tag_values;
}

=head2 get_values

  Arg [1]      Header record, Str.
  Arg [2]      Header tag whose values are to be returned, Str.

  Example    : foreach my $pg_record ($obj->get_records($header, 'PG') {
                 my @values = $obj->get_values($pg_record, 'CL');
               }
  Description: Return an array of values for the given tag.
  Returntype : Array[Str]

=cut

sub get_values {
  my ($self, $header_record, $tag) = @_;

  defined $header_record or
    $self->logconfess('A defined header_record argument is required');
  defined $tag or $self->logconfess('A defined tag argument is required');

  my @values;
  foreach my $tag_value ($self->get_tag_values($header_record, $tag)) {
    my ($value) = $tag_value =~ m{^$tag:(.*)$}msx;
    if (not defined $value or not length $value) {
      $self->logconfess('Failed to parse a value from tag:value pair ',
                        "'$tag_value'");
    }
    push @values, $value;
  }

  return @values;
}

no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::HeaderParser

=head1 DESCRIPTION

A basic parser for SAM/BAM/CRAM file headers. This may be used to
discover whether the reads have been aligned and if so, to what
reference, for eaxmple.

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
