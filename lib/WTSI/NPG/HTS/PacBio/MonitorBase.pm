package WTSI::NPG::HTS::PacBio::MonitorBase;

use Moose::Role;
use WTSI::NPG::iRODS;
use WTSI::DNAP::Warehouse::Schema;


our $VERSION = '';

has 'irods' =>
  (isa           => 'WTSI::NPG::iRODS',
   is            => 'ro',
   required      => 1,
   documentation => 'An iRODS handle to run searches and perform updates');

has 'dest_collection' =>
  (isa           => 'Maybe[Str]',
   is            => 'ro',
   required      => 1,
   default       => undef,
   documentation => 'The destination collection within iRODS to store data');

has 'local_staging_area' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'A local filesystem path acting as a root path for ' .
                    'runfolders');

has 'mlwh_schema' =>
  (is            => 'ro',
   isa           => 'WTSI::DNAP::Warehouse::Schema',
   required      => 1,
   documentation => 'A ML warehouse handle to obtain secondary metadata');


no Moose::Role;

1;

__END__


=head1 NAME

WTSI::NPG::HTS::PacBio::MonitorBase

=head1 DESCRIPTION

Base for Monitors.

=head1 AUTHOR

Guoying Qi E<lt>gq1@sanger.ac.ukE<gt>
Keith James E<lt>kdj@sanger.ac.ukE<gt>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2011, 2016 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
