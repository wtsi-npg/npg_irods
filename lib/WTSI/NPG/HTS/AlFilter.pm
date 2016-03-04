package WTSI::NPG::HTS::AlFilter;

use Moose::Role;

our $VERSION = '';

has 'alignment_filter' =>
  (isa           => 'Maybe[Str]',
   is            => 'ro',
   required      => 0,
   writer        => 'set_alignment_filter',
   documentation => 'The align filter, parsed from the iRODS path');
no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::AlFilter

=head1 DESCRIPTION

A synonym role for npg_tracking::glossary::subset

A 'subset' is the new term for 'alignment_filter'. There will be a
global replacement of the latter for the former, at which point this
role will be removed.

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
