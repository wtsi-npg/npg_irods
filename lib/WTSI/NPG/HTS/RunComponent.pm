package WTSI::NPG::HTS::RunComponent;

use strict;
use warnings;
use Moose::Role;

use npg_tracking::util::types qw[
                                  NpgTrackingRunId
                                  NpgTrackingLaneNumber
                                  NpgTrackingTagIndex
                                ];

our $VERSION = '';

has 'id_run' =>
  (isa           => 'NpgTrackingRunId',
   is            => 'ro',
   required      => 0,
   documentation => 'The run identifier');

has 'position' =>
  (isa           => 'NpgTrackingLaneNumber',
   is            => 'ro',
   required      => 0,
   documentation => 'The position (i.e. sequencing lane)');

has 'tag_index' =>
  (isa           => 'Maybe[NpgTrackingTagIndex]',
   is            => 'ro',
   required      => 0,
   documentation => 'The tag_index');

no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::RunComponent

=head1 DESCRIPTION

A run component is a subset of an Illumina sequencing run's alignment
or ancillary data, identified by run, lane position and (optionally)
sequencing tag index.

This roles exists because We can't extend npg_tracking::glossary::run
or npg_tracking::glossary::lane to make objects that do not require
id_run or position; that would violate Liskov.

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
