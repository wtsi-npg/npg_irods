package WTSI::NPG::HTS::PathLister;

use Data::Dump qw[pp];
use File::Find;
use Moose::Role;

use WTSI::DNAP::Utilities::Params qw[function_params];

our $VERSION = '';

with qw[
         WTSI::DNAP::Utilities::Loggable
       ];

=head2 list_directory

  Arg [1]      Directory path, Str.
  Arg [2]      Item match regex, Str. Optional.

  Example    : my @entries = $obj->list_directory('/tmp', '^foo');
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

   sub list_directory {
     my ($self, $path) = $params->parse(@_);

     my @entries;
     my @dirs;
     my @files;

     if (-e $path) {
       if (-d $path) {
         if ($params->recurse) {
           find(sub {
                  if (-d) {
                    push @dirs, $File::Find::dir
                  }
                  if (-f) {
                    push @files, $File::Find::name
                  }
                },
                $path);
         }
         else {
           opendir my $dh, $path or
             $self->logcroak("Failed to opendir '$path': $!");
           my @dirents = map { "$path/$_" }
                        grep { $_ ne q[.] and $_ ne q[..] } readdir $dh;
           closedir $dh;

           push @dirs,  grep { -d } @dirents;
           push @files, grep { -f } @dirents;
         }

         @dirs  = sort @dirs;
         @files = sort @files;
         $self->debug("Found directories in '$path': ", pp(\@dirs));
         $self->debug("Found files in '$path': ",       pp(\@files));

         push @entries, @dirs, @files;

         if (defined $params->filter) {
           my $regex = $params->filter;
           @entries = grep { m{$regex}msx } @entries;
           $self->debug('Filtered result: ', pp(\@entries));
         }
       }
       else {
         $self->error("Path '$path' is not a directory; ",
                      'unable to scan it for contents');
       }
     }
     else {
       $self->info("Path '$path' does not exist locally; ignoring");
     }

     return @entries;
   }
}


no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PathLister

=head1 DESCRIPTION


=head1 AUTHOR

Keith James E<lt>kdj@sanger.ac.ukE<gt>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2016, 2018 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
