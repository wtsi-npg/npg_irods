package WTSI::NPG::DataSub::File;

use namespace::autoclean;

use DateTime;
use Moose;

our $VERSION = '';

has 'run_accession' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'EBI run accession number');

has 'submission_accession' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'EBI submission accession number');

has 'file_name' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The file name');

has 'submission_md5' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The submitted file MD5 checksum');

has 'submission_date' =>
  (isa           => 'DateTime',
   is            => 'ro',
   required      => 1,
   documentation => 'The submission timestamp (time at which the submission ' .
                    'status changed)');

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__


=head1 NAME

WTSI::NPG::DataSub::File

=head1 DESCRIPTION

A file submitted to the EBI.

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
