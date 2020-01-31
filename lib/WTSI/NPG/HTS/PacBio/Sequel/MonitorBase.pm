package WTSI::NPG::HTS::PacBio::Sequel::MonitorBase;

use Moose::Role;
use WTSI::NPG::HTS::PacBio::Sequel::APIClient;

our $VERSION = '';

has 'api_client' =>
  (isa           => 'WTSI::NPG::HTS::PacBio::Sequel::APIClient',
   is            => 'ro',
   lazy_build    => 1,
   builder       => q[_build_api_client],
   documentation => 'A PacBio Sequel API client used to fetch runs');

sub _build_api_client {
    my $self = shift;
    my @init_args = $self->api_uri ? ('api_uri' => $self->api_uri) : ();
    if($self->interval) { push @init_args, ('default_interval' => $self->interval) };
    if($self->older_than) { push @init_args, ('default_end' => $self->older_than) };
    return WTSI::NPG::HTS::PacBio::Sequel::APIClient->new(@init_args);
}

has 'api_uri' =>
  (isa           => 'Str',
   is            => 'ro',
   documentation => 'PacBio root API URL');

has 'interval' =>
  (isa           => 'Str',
   is            => 'ro',
   documentation => 'Interval of time in days');

has 'older_than' =>
  (isa           => 'Str',
   is            => 'ro',
   documentation => 'Time in days to remove from end date');

no Moose::Role;

1;

__END__


=head1 NAME

WTSI::NPG::HTS::PacBio::Sequel::MonitorBase

=head1 DESCRIPTION

Base for Sequel Monitors.

=head1 AUTHOR

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2019 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
