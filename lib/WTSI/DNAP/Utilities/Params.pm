package WTSI::DNAP::Utilities::Params;

use strict;
use warnings;

use Exporter qw[import];
our @EXPORT_OK = qw[function_params method_params];

use WTSI::DNAP::Utilities::ParamsParser;

our $VERSION = '';

=head2 function_params

  Arg [1]      Number of positional parameters, Int.
  Arg [2]      Named parameters, Array.

  Example    : my $params = function_params(2, qw[foo bar]);
  Description: Return a new argument list parser recognising the
               specified positional and named parameters.
  Returntype : WTSI::DNAP::Utilities::ParamsParser

=cut

sub function_params {
  my ($positional, @names) = @_;

  $positional ||= 0;
  @names = sort @names;

  my $class = Moose::Meta::Class->create_anon_class
    (roles => ['WTSI::DNAP::Utilities::ParamsParser'],
     cache => 1);

  foreach my $name (@names) {
    $class->add_method($name, sub { return shift->named->{$name} });
  }

  my $num_names = scalar @names;
  my $names     = join q[, ], @names;

  my $params = $class->new_object(positional => $positional,
                                  names      => \@names);
  return $params;
}

1;

__END__

=head1 NAME

WTSI::DNAP::Utilities::Params

=head1 DESCRIPTION

A named parameter parser interface for functions.

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
