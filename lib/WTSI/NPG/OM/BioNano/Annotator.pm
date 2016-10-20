package WTSI::NPG::OM::BioNano::Annotator;

use DateTime;
use Moose::Role;

our $VERSION = '';




# Based on genotyping WTSI::NPG::Annotator, and WTSI::NPG::HTS::Annotator
# TODO consolidate Annotator functionality in one place where possible






no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::OM::BioNano::Annotator

=head1 DESCRIPTION

A role providing methods to generate metadata for WTSI Optical Mapping
runs.

=head1 AUTHOR

Iain Bancarz <ib5@sanger.ac.uk>

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
