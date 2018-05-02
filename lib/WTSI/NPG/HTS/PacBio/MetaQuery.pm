package WTSI::NPG::HTS::PacBio::MetaQuery;

use namespace::autoclean;
use File::Basename;
use Moose::Role;
use MooseX::StrictConstructor;

with qw[
         WTSI::DNAP::Utilities::Loggable
       ];

our $VERSION = '';

has 'mlwh_schema' =>
  (is            => 'ro',
   isa           => 'WTSI::DNAP::Warehouse::Schema',
   required      => 1,
   documentation => 'A ML warehouse handle to obtain secondary metadata');


=head2 find_pacbio_runs

  Arg [1]    : PacBio run ID, Str.
  Arg [2]    : PacBio plate well, zero-padded form, Str. E.g. 'A01'.
  Arg [3]    : Tag identifier, Optional.

  Example    : @run_records - $obj->find_runs($id, 'A01');
  Description: Returns run records for a PabcBio run. Pre-fetches related
               sample and study information.
  Returntype : Array[WTSI::DNAP::Warehouse::Schema::Result::PacBioRun]

=cut

sub find_pacbio_runs {
  my ($self, $run_id, $well, $tag_id) = @_;

  defined $run_id or
    $self->logconfess('A defined run_id argument is required');
  defined $well or
    $self->logconfess('A defined well argument is required');

  # Well addresses are unpadded in the ML warehouse
  my ($row, $col) = $well =~ m{^([[:upper:]])([[:digit:]]+)$}msx;
  if ($row and $col) {
    $col =~ s/^0+//msx; # Remove leading zeroes
  }
  else {
    $self->logcroak("Failed to match a plate row and column in well '$well' ",
                    "of PacBio run '$run_id'");
  }

  my $well_label = "$row$col";

  my $query      = {id_pac_bio_run_lims => $run_id,
                    well_label          => $well_label};

  if (defined $tag_id){
      $query->{tag_identifier} = $tag_id;
  }

  my @run_records = $self->mlwh_schema->resultset('PacBioRun')->search
    ($query,  {prefetch => ['sample', 'study']});

  my $num_records = scalar @run_records;
  $self->debug("Found $num_records records for PacBio ",
               "run $run_id, well $well_label");

  return @run_records;
}

no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::MetaQuery

=head1 DESCRIPTION

Queries WTSI::DNAP::Warehouse::Schema for secondary metadata in order
to update PacBio HTS data files in iRODS.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2016 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
