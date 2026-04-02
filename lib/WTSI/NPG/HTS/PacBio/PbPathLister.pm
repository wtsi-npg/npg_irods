package WTSI::NPG::HTS::PacBio::PbPathLister;

use Moose::Role;

use WTSI::DNAP::Utilities::Params qw[function_params];

our $VERSION = '';

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::HTS::PathLister
       ];

# Exclude all pacbio internal files
our $PATH_EXCLUDE = '^(?!.*pb_internal)';

=head2 pb_list_directory

  Arg [1]      Directory path, Str.
  Arg [2]      Item match regex, Str. Optional.

  Example    : my @entries = $obj->pb_list_directory('/tmp', '^foo');
  Description: Return entries in a directory, optionally filtered to match
               match a regex. The regex is applied to the directory entry
               with its leading path. Return entries with directories
               sorted first.
  Returntype : Array

=cut

{
   my $positional = 2;
   my @named      = qw[recurse filter];
   my $params = function_params($positional, @named);

   sub pb_list_directory {
     my ($self, $path) = $params->parse(@_);

     my @files = $self->list_directory
       ($path, filter => $params->filter, recurse => $params->recurse);

     @files = grep { m{$PATH_EXCLUDE}msx } @files;

     return @files;
   }

}


no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::PbPathLister

=head1 DESCRIPTION

Always exclude duplicate files in certain named directories. These
directories may be beta and not permanent.

=head1 AUTHOR

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2026 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
