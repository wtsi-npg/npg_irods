package WTSI::NPG::HTS::PacBio::Sequel::APIClient;

use namespace::autoclean;
use DateTime;
use English qw[-no_match_vars];
use LWP::UserAgent;
use Moose;
use MooseX::StrictConstructor;
use URI;
use URI::Split qw(uri_join);
use Readonly;
use JSON;

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::DNAP::Utilities::JSONCodec
       ];

our $VERSION  = '';
our $PROTOCOL = 'http';

our $SUCCESS_STATE  = q[SUCCESSFUL];


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


has 'jobs_api_uri' =>
  (isa           => 'URI',
   is            => 'ro',
   lazy_build    => 1,
   documentation => 'PacBio API URI to return a jobs list');

sub _build_jobs_api_uri {
  my ($self) = @_;

  my $path   = join q[/], q[secondary-analysis/job-manager/jobs], $self->job_type;
  my $uri    = uri_join($PROTOCOL, $self->api_uri, $path);
  return URI->new($uri);
}

has 'job_type' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   default       => 'pbsmrtpipe',
   documentation => 'The job type');


has 'default_interval' =>
  (isa           => 'Int',
   is            => 'ro',
   required      => 1,
   default       => 14,
   documentation => 'The default number of days activity to report');

has 'begin_date' =>
  (isa           => 'DateTime',
   is            => 'ro',
   lazy          => 1,
   builder       => q[_build_begin_date],
   documentation => 'The default begin date');

sub _build_begin_date {
    my ($self) = shift;
    return DateTime->from_epoch(epoch => $self->end_date->epoch)->subtract
           (days => $self->default_interval);
}

has 'end_date' =>
  (isa           => 'DateTime',
   is            => 'ro',
   lazy          => 1,
   builder       => q[_build_end_date],
   documentation => 'The default end date');

sub _build_end_date {
    my ($self) = shift;
    return DateTime->now;
}


=head2 query_runs

  Example    : my $runs = $client->query_runs
  Description: Return runs. Optionally restrict to runs completed within
               a specific date range, otherwise runs completed in the 
               last 2 weeks will be returned. A completedAt date is 
               available for runs using primary analysis version 4+ 
               (pre V4 runs will never have a completedAt date).
  Returntype : ArrayRef[HashRef]

=cut

sub query_runs {
  my ($self) = @_;

  my $end   = $self->end_date;
  my $begin = $self->begin_date;

  my ($content) = $self->_get_content($self->runs_api_uri->clone);

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

=head2 query_analysis_jobs

  Arg [1]    : Pipeline id. Optional.

  Example    : my $jobs = $client->query_analysis_jobs($job_type)
  Description: Query for successful analysis jobs within a specific
               time frame in 
  Returntype : ArrayRef[HashRef]

=cut

sub query_analysis_jobs {
  my($self, $pipeline_id) = @_;

  my $end   = $self->end_date;
  my $begin = $self->begin_date;

  my ($content) = $self->_get_content($self->jobs_api_uri->clone);

  my @jobs;
  if(ref $content eq 'ARRAY') {
      foreach my $job (@{$content}) {
          if($job->{createdAt}                      &&
             ($job->{createdAt} gt $begin->iso8601) &&
             ($job->{createdAt} lt $end->iso8601)   &&
             $job->{state}                          &&
             ($job->{state} eq $SUCCESS_STATE)      &&
             $job->{jsonSettings}                   &&
             $self->_check_pid($job->{jsonSettings},$pipeline_id)
             ){
              push @jobs, $job;
          }
      }
  }
  return [@jobs];
}


sub _get_content{
  my($self, $query) = @_;

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
  return $content;
}

sub _check_pid {
    my($self,$json_settings,$pipeline_id) = @_;

    my $usejob = 1;
    my $settings = decode_json($json_settings);

    if($settings->{pipelineId}                   &&
       $pipeline_id                              &&
       ($settings->{pipelineId} ne $pipeline_id)
       ){
        $usejob = 0;
    }
    return $usejob;
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
