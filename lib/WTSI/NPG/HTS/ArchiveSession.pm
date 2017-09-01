package WTSI::NPG::HTS::ArchiveSession;

use Moose::Role;

our $VERSION = '';

## no critic (ValuesAndExpressions::ProhibitMagicNumbers)
has 'arch_capacity' =>
  (isa           => 'Int',
   is            => 'ro',
   required      => 1,
   default       => 10_000,
   documentation => 'The maximum number of files that will be added to any ' .
                    'archive file');

has 'arch_bytes' =>
  (isa           => 'Int',
   is            => 'ro',
   required      => 1,
   default       => 10_000_000,
   documentation => 'The maximum number of bytes that will be added to any ' .
                    'archive file');

has 'arch_timeout' =>
  (isa           => 'Int',
   is            => 'ro',
   required      => 1,
   default       => 60 * 5,
   documentation => 'The number of seconds idle time since the previous ' .
                    'file was added to an open archive file, after which ' .
                    'the archive will be closed will be closed, even if ' .
                    'not at capacity');

has 'session_begin' =>
  (isa           => 'Int',
   is            => 'rw',
   required      => 0,
   documentation => 'The epoch time the archive session started');

has 'session_timeout' =>
  (isa           => 'Int',
   is            => 'ro',
   required      => 1,
   default       => 60 * 20,
   documentation => 'The number of seconds idle time (no files added) ' .
                    'after which it will be ended automatically');
## use critic

no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::ArchiveSession

=head1 DESCRIPTION

A role providing methods to describe parameters for a data-archiving
session e.g. where files are incrementally added to a tar archive.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2017 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
