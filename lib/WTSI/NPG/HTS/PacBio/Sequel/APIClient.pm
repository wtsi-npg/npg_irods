package WTSI::NPG::HTS::PacBio::Sequel::APIClient;

use namespace::autoclean;
use DateTime;
use English qw[-no_match_vars];
use LWP::UserAgent;
use Moose;
use MooseX::StrictConstructor;
use URI;
use URI::Split qw(uri_join);

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::DNAP::Utilities::JSONCodec
       ];

our $VERSION  = '';
our $PROTOCOL = 'http';


has 'api_uri' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   default       => sub {
       return('sf2-farm-srv1.internal.sanger.ac.uk:8071');
   },
   documentation => 'PacBio root API URL');


has 'runs_api_uri' =>
  (isa           => 'URI',
   is            => 'ro',
   lazy_build    => 1,
   documentation => 'PacBio API URI to return runs list');

sub _build_runs_api_uri {
  my ($self) = @_;

  my $uri    = uri_join($PROTOCOL, $self->api_uri, 'smrt-link/runs');
  return URI->new($uri);
}


has 'default_interval' =>
  (isa           => 'Int',
   is            => 'ro',
   required      => 1,
   default       => 14,
   documentation => 'The default number of days activity to report');



=head2 query_runs

  Arg [1]    : Start date. Optional.
  Arg [2]    : End date. Optional.

  Example    : my $runs = $client->query_runs
  Description: Return runs. Optionally restrict to runs completed within
               a specific date range, otherwise runs completed in the 
               last 2 weeks will be returned. A completedAt date is 
               available for runs using primary analysis version 4+ 
               (pre V4 runs will never have a completedAt date).
  Returntype : ArrayRef[HashRef]

=cut

sub query_runs {
  my ($self, $begin_date, $end_date) = @_;

  my $end   = $end_date   ? $end_date   : DateTime->now;
  my $begin = $begin_date ? $begin_date :
    DateTime->from_epoch(epoch => $end->epoch)->subtract
    (days => $self->default_interval);

  my $query = $self->runs_api_uri->clone;

  $self->debug("Getting query URI '$query'");
  my $ua = LWP::UserAgent->new;
  my $response = $ua->get($query);

  my $msg = sprintf 'code %d, %s', $response->code, $response->message;
  $self->debug('Query URI returned ', $msg);

  my $content;
  if ($response->is_success) {
    $content = $self->decode($response->content);
  }
  else {
    $self->logcroak("Failed to get results from URI '$query': ",$msg);
  }

  my @runs;
  if (ref $content eq 'ARRAY') {
      foreach my $run (@{$content}) {
          if($run->{completedAt}                      &&
             ($run->{completedAt} gt $begin->iso8601) &&
             ($run->{completedAt} lt $end->iso8601)){
              push @runs, $run;
          }
      }
  }

  return [@runs];
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::Sequel::APIClient

=head1 DESCRIPTION

A client for the PacBio SMRT Link services API which provides
information about sequencing runs.

=head1 AUTHOR

Keith James E<lt>kdj@sanger.ac.ukE<gt>
Guoying Qi E<lt>gq1@sanger.ac.ukE<gt>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2017 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
