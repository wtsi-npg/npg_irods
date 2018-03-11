package WTSI::NPG::HTS::ChecksumCalculator;

use English qw[-no_match_vars];
use Moose::Role;

our $VERSION = '';

with qw[WTSI::DNAP::Utilities::Loggable];

=head2 calculate_checksum

  Arg [1]    : Path to a local file, Str.

  Example    : my $checksum = $self->calculate_checksum('1.txt')
  Description: Return the checksum of a file as a hexdigest.
  Returntype : Str

=cut

sub calculate_checksum {
  my ($self, $path) = @_;

  open my $in, '<', $path or
    $self->logcroak("Failed to open '$path' for checksum calculation: $ERRNO");
  binmode $in;

  my $checksum = Digest::MD5->new->addfile($in)->hexdigest;

  close $in or
    $self->warn("Failed to close '$path': $ERRNO");

  return $checksum;
}

no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::ChecksumCalculator

=head1 DESCRIPTION

This role provides a method to calculate a checksum of a file.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2018 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
