package WTSI::NPG::HTS::PacBio::Sequel::APIClient;

use namespace::autoclean;
use DateTime;
use English qw[-no_match_vars];
use LWP::UserAgent;
use Moose;
use MooseX::StrictConstructor;
use URI;
use URI::Split qw(uri_join);
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
       return('localhost:8071');
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

  my $path   = join q[/], q[smrt-link/job-manager/jobs], $self->job_type;
  my $uri    = uri_join($PROTOCOL, $self->api_uri, $path);
  return URI->new($uri);
}

has 'user_agent' =>
  (isa           => 'LWP::UserAgent',
   is            => 'ro',
   required      => 1,
   default       => sub {
       return LWP::UserAgent->new;
   },
   documentation => 'Web user agent handle');

has 'job_type' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   default       => 'analysis',
   documentation => 'The job type');

has 'job_status' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   default       => 'completedAt',
   documentation => 'The required job status');

has 'default_interval' =>
  (isa           => 'Int',
   is            => 'ro',
   required      => 1,
   default       => 14,
   documentation => 'The default number of days activity to report');

has 'default_end' =>
  (isa           => 'Int',
   is            => 'ro',
   required      => 1,
   default       => 0,
   documentation => 'The number of days to subtract from the end date');

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
    return DateTime->now->subtract(days => $self->default_end);
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
  my $status = $self->job_status;

  my ($content) = $self->_get_content($self->runs_api_uri->clone);

  my @runs;
  if (ref $content eq 'ARRAY') {
      foreach my $run (@{$content}) {
          if($run->{$status}                      &&
             ($run->{$status} gt $begin->iso8601) &&
             ($run->{$status} lt $end->iso8601)){
              push @runs, $run;
          }
      }
  }

  return [@runs];
}


=head2 query_run

  Arg [1]    : Run id. Required.

  Example    : my $run = $client->query_run($run_id)
  Description: Query for a specific run by run_id.
  Returntype : HashRef

=cut

sub query_run {
  my($self, $run_id) = @_;

  defined $run_id or
      $self->logconfess('A defined run_id is required');

  my $path  = join q[/], q[smrt-link/runs], $run_id;
  my ($run) = $self->_get_content($self->_get_uri($path)->clone);
  return $run;
}

=head2 query_run_collections

  Arg [1]    : Run id. Optional.

  Example    : my $collections = $client->query_run_collections($run_id)
  Description: Query for collections via runs within a specific
               time frame. Optionally specify a single run id, but note
               there is no time frame restriction applied when a single
               run id is defined.
  Returntype : ArrayRef[HashRef]

=cut

sub query_run_collections {
  my($self, $run_id) = @_;

  my $run_data = length $run_id ? [$self->query_run($run_id)] : $self->query_runs;
  my @collections;
  if(ref $run_data eq 'ARRAY') {
    foreach my $run (@{$run_data}) {
      my $path = join q[/], q[smrt-link/runs], $run->{uniqueId}, q[collections];
      my ($col) = $self->_get_content($self->_get_uri($path)->clone);
      if(ref $col eq 'ARRAY') {
        push @collections, @{$col};
      }
    }
  }
  return[@collections];
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
          if($job->{jobCompletedAt}                      &&
             ($job->{jobCompletedAt} gt $begin->iso8601) &&
             ($job->{jobCompletedAt} lt $end->iso8601)   &&
             $job->{state}                               &&
             ($job->{state} eq $SUCCESS_STATE)           &&
             $job->{subJobTypeId}                        &&
             ($job->{subJobTypeId} eq $pipeline_id)
             ){
              push @jobs, $job;
          }
      }
  }
  return [@jobs];
}


=head2 query_datasets

  Arg [1]    : Dataset type. Required.

  Example    : my $reports = $client->query_datasets($type)
  Description: Query for successfully generated datasets by type.
  Returntype : ArrayRef[HashRef]

=cut

sub query_datasets {
  my($self, $dataset_type) = @_;

  defined $dataset_type or
      $self->logconfess('A defined dataset_type is required');

  my $path = join q[/], q[smrt-link/datasets], $dataset_type;
  my ($content) = $self->_get_content($self->_get_uri($path)->clone);

  my @datasets;
  if(ref $content eq 'ARRAY') {
    push @datasets, @{$content};
  }
  return [@datasets];
}

=head2 query_dataset_reports

  Arg [1]    : Dataset type. Required.
  Arg [2]    : Dataset id. Required.

  Example    : my $reports = $client->query_dataset_reports($type, $id)
  Description: Query for successfully generated QC reports for a dataset.
  Returntype : ArrayRef[HashRef]

=cut

sub query_dataset_reports {
  my($self, $dataset_type, $dataset_id) = @_;

  defined $dataset_id or
      $self->logconfess('A defined dataset_id is required');

  defined $dataset_type or
      $self->logconfess('A defined dataset_type is required');

  my $path = join q[/], q[smrt-link/datasets], $dataset_type, $dataset_id, q[reports];
  my ($content) = $self->_get_content($self->_get_uri($path)->clone);

  my @reports;
  if(ref $content eq 'ARRAY') {
    foreach my $rep (@{$content}) {
      if($rep->{dataStoreFile}->{isActive} == 1 &&
         $rep->{dataStoreFile}->{path} ) {
         push @reports, $rep;
      }
    }
  }
  return [@reports];
}

sub _get_uri {
  my($self, $path) = @_;
  my $uri    = uri_join($PROTOCOL, $self->api_uri, $path);
  return URI->new($uri);
}

sub _get_content{
  my($self, $query) = @_;

  $self->debug("Getting query URI '$query'");
  my $ua = $self->user_agent;
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
