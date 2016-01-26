package WTSI::DNAP::Utilities::ParamsParser;

use namespace::autoclean;
use Carp;
use Data::Dump qw(pp);
use List::AllUtils;
use Moose::Role;

our $VERSION = '';

with 'WTSI::DNAP::Utilities::Loggable';

has 'arguments' =>
  (is            => 'rw',
   isa           => 'ArrayRef',
   required      => 1,
   default       => sub { [] },
   init_arg      => undef,
   documentation => 'The unmodified caller arguments');

has 'named' =>
  (is            => 'rw',
   isa           => 'HashRef',
   required      => 1,
   default       => sub { {} },
   init_arg      => undef,
   documentation => 'The parsed named arguments');

has 'names' =>
  (is            => 'ro',
   isa           => 'ArrayRef',
   required      => 1,
   documentation => 'The permitted parameter names');

has 'positional' =>
  (is            => 'ro',
   isa           => 'Int',
   required      => 1,
   default       => 0,
   documentation => 'The number of positional parameters');


=head2 arg_at

  Arg [1]      Position, Int.

  Example    : my $nth_arg = $params->arg_at($n)
  Description: Return the nth positional argument. Raise an error if
               n is out of bounds.
  Returntype : Any

=cut

sub arg_at {
  my ($self, $position) = @_;

  my $max = $self->positional - 1;
  if ($position < 0 or $position > $max) {
    $self->logconfess("Positional argument index $position out of bounds; ",
                      "must be in the range 0 - $max, inclusive");
  }

  return $self->arguments->[$position];
}

=head2 has_named_params

  Arg [1]      None.

  Example    : $params->has_named_params
  Description: Return true if any named parameters are available.
  Returntype : Bool

=cut

sub has_named_params {
  my ($self) = @_;

  return scalar @{$self->names} > 0;
}

=head2 has_positional_params

  Arg [1]      None.

  Example    : $params->has_positional_params
  Description: Return true if any positional parameters are available.
  Returntype : Bool

=cut

sub has_positional_params {
  my ($self) = @_;

  return $self->positional > 0;
}

=head2 parse

  Arg [1]      Function argument list.

  Example    : my ($a, $b, $c) $params->parse(1, 2, 3, foo => 'bar')
  Description: Parse an argument list, extract the positional and named
               arguments and return the positional arguments as an array.
               The named arguments are made available through
               correspondingly named methods, one per parameter.

               Raise a warning if there are any extra positional or named
               arguments. Raise an error if there are an odd number of
               elements in the named arguments array (signifying a missing
               parameter name or value).
  Returntype : Array

=cut

sub parse {
  my ($self, @args) = @_;

  $self->arguments(\@args);
  my $num_args = scalar @args;

  if ($self->has_named_params) {
    my @named_args = @args[$self->positional .. $num_args - 1];
    my $num_named = scalar @named_args;

    if (@named_args and not $num_named % 2 == 0) {
      $self->logconfess('Name or value missing (odd number of elements) ',
                        'in named arguments: ', pp(\@named_args));
    }
    else {
      my %named = @named_args;

      my %valid_name_table = map { $_ => 1 } @{$self->names};
      foreach my $name (sort keys %named) {
        if (not exists $valid_name_table{$name}) {
          $self->warn("Ignoring unknown name '$name' in named arguments: ",
                      pp(\@named_args));
          delete $named{$name};
        }
      }

      $self->named(\%named);
    }
  }
  elsif ($self->has_positional_params) {
    # Warn of extra positinal args if we know they are not named ones
    if ($self->positional < $num_args) {
      my @extra_args = @args[$self->positional .. $num_args - 1];
      my $num_extra = scalar @extra_args;

      $self->warn("Ignoring $num_extra extra arguments ", pp(\@extra_args),
                  ' in positional arguments: ', pp(\@args));
    }
  }

  my @positional;
  if ($self->has_positional_params) {
    push @positional, @args[0 .. $self->positional - 1];
  }

  return @positional;
}

no Moose::Role;

1;

__END__

=head1 NAME

WTSI::DNAP::Utilities::ParamsParser

=head1 DESCRIPTION

A role providing simple function argument parsing for positional
and/or named parameters.

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
