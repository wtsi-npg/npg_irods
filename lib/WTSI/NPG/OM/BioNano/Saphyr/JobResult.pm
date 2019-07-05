package WTSI::NPG::OM::BioNano::Saphyr::JobResult;

use namespace::autoclean;

use Carp;
use Data::Dump qw[pp];
use DateTime::Format::ISO8601;
use DateTime;
use JSON;
use Moose;
use MooseX::StrictConstructor;
use Try::Tiny;

our $VERSION = '';

with qw[
         WTSI::DNAP::Utilities::Loggable
       ];

has 'chip_name' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The chip name');

has 'chip_run_operator' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The user operating the instrument');

has 'chip_run_uid' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The unique ID given to the run by the Saphyr software');

has 'chip_serialnumber' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The serial number of the chip');

has 'enzyme_name' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The name of the recognition enzyme used');

has 'experiment_name' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The name of the run experiment in the Saphyr Access ' .
                    'database');

has 'flowcell' =>
  (isa           => 'Int',
   is            => 'ro',
   required      => 1,
   documentation => 'The flowcell number (1 or 2)');

has 'json' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The original JSON obtained when querying the ' .
                    'Saphyr Access database');

has 'job_command' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The command line run by the Saphyr software');

has 'job_id' =>
  (isa           => 'Int',
   is            => 'ro',
   required      => 1,
   documentation => 'The unique ID of the job');

has 'job_info' =>
  (isa           => 'HashRef',
   is            => 'ro',
   required      => 1,
   documentation => 'The JSON from the describing the job ');

has 'job_state' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The job state in the Saphyr Access database');

has 'job_updated' =>
  (isa           => 'DateTime',
   is            => 'ro',
   required      => 1,
   documentation => 'The time at which the job was updated in the ' .
                    'Saphyr Access database');

has 'project_name' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The name of the run project in the Saphyr Access ' .
                    'database');

has 'sample_name' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The name of the run sample in the Saphyr Access ' .
                    'database');

around BUILDARGS => sub {
  my ($orig, $class, @args) = @_;

  if (@args == 1 and not ref $args[0]) {
    my ($json) = @args;

    my @attrs = qw[
                    chip_name
                    chip_run_operator
                    chip_run_uid
                    chip_serialnumber
                    enzyme_name
                    experiment_name
                    flowcell
                    job_command
                    job_id
                    job_info
                    job_state
                    job_updated
                    project_name
                    sample_name
                 ];

    my $decoded = decode_json($json);
    my %init_args = map { $_ => $decoded->{$_} } @attrs;
    $init_args{json} = $json;

    my $date_str = $init_args{job_updated};
    try {
      my $timestamp = DateTime::Format::ISO8601->parse_datetime($date_str);
      $init_args{job_updated} = $timestamp;
    } catch {
      croak "Failed to parse '$date_str' as an ISO8601 timestamp: $_";
    };

    return $class->$orig(%init_args);
  }
  else {
    return $class->$orig(@_);
  }
};

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::OM::BioNano::Saphyr::JobResult

=head1 DESCRIPTION

Describes the result of an analysis job run by the Saphyr software.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

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
