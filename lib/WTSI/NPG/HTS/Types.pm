package WTSI::NPG::HTS::Types;

use strict;
use warnings;
use MooseX::Types::Moose qw(Str);

use MooseX::Types -declare =>
  [qw(
       HTSFileFormat
    )];

our $VERSION = '';

subtype HTSFileFormat,
  as Str,
  where { m{^(cram|bam)$}msx },
  message { "'$_' is not a valid HTS file format" };

1;

__END__

=head1 NAME

WTSI::NPG::HTS::Types - Moose types for high-throughput sequencing

=head1 DESCRIPTION

The non-core Moose types for high-throughput sequencing are all
defined here.

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
