package WTSI::NPG::HTS::PacBio::APIClient;

use namespace::autoclean;
use DateTime;
use English qw[-no_match_vars];
use LWP::UserAgent;
use Moose;
use MooseX::StrictConstructor;

use WTSI::DNAP::Utilities::Params qw[function_params];

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::DNAP::Utilities::JSONCodec
       ];

our $VERSION = '';

our $STATUS_COMPLETE = 'Complete';

has 'api_uri' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   default       => 'http://pacbio1-1:8081/Jobs/PrimaryAnalysis/Query',
   documentation => 'PacBio API URI to check Primary Analysis status');

has 'default_interval' =>
  (isa           => 'Int',
   is            => 'ro',
   required      => 1,
   default       => 14,
   documentation => 'The default number of days activity to report');

{
  my $positional = 1;
  my @named      = qw[job_status begin_date end_date];
  my $params = function_params($positional, @named);

  sub query_jobs {
    my ($self) =  $params->parse(@_);

    my $status = $params->job_status ? $params->job_status : $STATUS_COMPLETE;

    my $end   = $params->end_date    ? $params->end_date   : DateTime->now;
    my $begin = $params->begin_date  ? $params->begin_date :
      DateTime->from_epoch(epoch => $end->epoch)->subtract
      (days => $self->default_interval);

    my $query_uri = sprintf '%s?status=%s&after=%s',
      $self->api_uri, $status, $begin->iso8601, $end->iso8601;

    $self->debug("Connecting to query URI '$query_uri'");
    my $ua = LWP::UserAgent->new;
    my $response = $ua->get($query_uri);

    my $msg = sprintf 'code %d, %s', $response->code, $response->message;
    $self->debug('Query URI returned ', $msg);

    my $content;
    if ($response->is_success) {
      $content = $self->decode($response->content);
    }
    else {
      $self->logcroak("Failed to fetch query results from URI '$query_uri': ",
                      $msg);
    }

    return $content;
  }
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::APIClient

=head1 DESCRIPTION

A client for the PacBio instrument web service which provides
information about sequencing job status. It is used to identify
completed jobs.

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
