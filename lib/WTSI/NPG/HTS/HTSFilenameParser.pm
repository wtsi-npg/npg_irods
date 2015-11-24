package WTSI::NPG::HTS::HTSFilenameParser;

use strict;
use warnings;
use Moose::Role;

our $VERSION = '';

sub parse_file_name {
  my ($self, $name) = @_;

  my ($id_run, $position, $align_filter, $align_filter2,
      $tag_index, $tag_index2, $align_filter3, $align_filter4, $format) =
        $name =~ m{\/
                   (\d+)        # Run ID
                   _            # Separator
                   (\d)         # Position
                   (_(\w+))?    # Align filter 1/2
                   (\#(\d+))?   # Tag index
                   (_(\w+))?    # Align filter 3/4
                   [.](\S+)$    # File format
                }mxs;

  $align_filter2 ||= $align_filter4;

  return ($id_run, $position, $tag_index2, $align_filter2, $format);
}

no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::HTSFilenameParser

=head1 DESCRIPTION



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
