package WTSI::NPG::OM::BioNano::Saphyr::SSHAccessClient;

use namespace::autoclean;

use Data::Dump qw[pp];
use DateTime;
use File::Spec::Functions;
use File::Path qw[make_path];
use Moose;
use MooseX::StrictConstructor;

use WTSI::DNAP::Utilities::Params qw[function_params];
use WTSI::DNAP::Utilities::Runnable;

use WTSI::NPG::OM::BioNano::Saphyr::JobResult;

our $VERSION = '';

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::OM::BioNano::Saphyr::AccessClient
       ];

has 'user' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The user to connect as');

has 'host' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The host to connect to');

has 'psql_executable' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The path to the psql executable');

has 'database_name' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The database name');

has 'default_interval' =>
  (isa           => 'Int',
   is            => 'ro',
   required      => 1,
   default       => 7,
   documentation => 'The default number of days activity to report');

has 'data_directory' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The directory on the Saphyr Access host where the ' .
                    'per-job subdirectories containing job data are ' .
                    'located');

sub BUILD {
  my ($self) = @_;

  $self->host =~ m{^(\w|[.])+$}msx or
    $self->logconfess(sprintf q[Invalid host '%s'], $self->host);

  $self->user =~ m{^\w+$}msx or
    $self->logconfess(sprintf q[Invalid user '%s'], $self->user);

  $self->database_name =~ m{^\w+$}msx or
    $self->logconfess(sprintf q[Invalid database name '%s'],
                      $self->database_name);

  $self->psql_executable =~ m{^(\w|[./-])+$}msxi or
    $self->logconfess(sprintf q[Invalid psql executable '%s'],
                      $self->psql_executable);

  return;
}

=head2 get_bnx_file

  Arg [1]    : Job ID, Int.
  Arg [2]    : Local directory path, Str.

  Example    : my $local_path = $obj->get_bnx_file(1234, '/tmp')

  Description: Download the RawMolecules.bnx.gz file for job with ID
               1234 to /tmp and return the path to the newly created
               file.
  Returntype : Str

=cut

sub get_bnx_file {
  my ($self, $job_id, $local_directory) = @_;

  make_path($local_directory);

  my $remote_path = catfile($self->data_directory, $job_id,
                            'RawMolecules.bnx.gz');
  $self->info("Fetching $remote_path from server");

  my $rsync_source = sprintf '%s@%s:%s',
    $self->user, $self->host, $remote_path;
  my $rsync_dest = catfile($local_directory, 'RawMolecules.bnx.gz');

  $self->debug("rsyncing '$rsync_source' to '$rsync_dest'");
  my $ssh = WTSI::DNAP::Utilities::Runnable->new
    (executable => '/usr/bin/rsync',
     arguments  => ['-q',
                    '-t',
                    $rsync_source,
                    $rsync_dest])->run;

  return $rsync_dest;
}

=head2 find_bnx_results

  Arg [1]    : Earliest date of completion, DateTime. Optional,
               defaults to 7 days ago.
  Arg [2]    : Latest date of completion, DateTime. Optional,
               defaults to the current time.

  Example    : my @runs = $db->find_completed_analysis_jobs
               my @runs = $db->find_completed_analysis_jobs
                 (begin_date => $begin)
               my @runs = $db->find_completed_analysis_jobs
                 (begin_date => $begin,
                  end_date   => $end)
  Description: Return information about Saphyr analysis jobs.
  Returntype : Array[HashRef]

=cut

{
  my $positional = 1;
  my @named      = qw[begin_date end_date];
  my $params = function_params($positional, @named);

  sub find_bnx_results {
    my ($self) = $params->parse(@_);

    my $end   = $params->end_date    ? $params->end_date   : DateTime->now;
    my $begin = $params->begin_date  ? $params->begin_date :
      DateTime->from_epoch(epoch => $end->epoch)->subtract
      (days => $self->default_interval);

    $begin->isa('DateTime') or
      $self->logconfess('The begin_date argument must be a DateTime');
    $end->isa('DateTime') or
      $self->logconfess('The end_date argument must be a DateTime');

    return $self->_do_query($begin, $end);
  }
}

sub _do_query {
  my ($self, $begin_date, $end_date) = @_;

  my $sql = <<'SQL';
SELECT to_json(result)
FROM (
  SELECT
    chip_run.chiprunuid       AS chip_run_uid,
    chip_run.operator         AS chip_run_operator,
    chip.serialnumber         AS chip_serialnumber,
    chipsetup.chipname        AS chip_name,
    flowcellsetup.location    AS flowcell,
    experiment.experimentname AS experiment_name,
    project.name              AS project_name,
    job.updated_at            AS job_updated,
    job.command               AS job_command,
    job.jobpk                 AS job_id,
    run_job_state.state       AS job_state,
    operation.operationname   AS operation_name,
    object_type.objecttype    AS object_type,
    object_state.objectstate  AS object_state,

    sample.samplename         AS sample_name,
    recognitionenzyme.name    AS enzyme_name,
    cmap.name                 AS cmap_name,
    job.json                  AS job_info

   FROM object
     JOIN object_type    ON object.objecttypefk =
                            object_type.objecttypepk
     JOIN project_object ON object.objectpk =
                            project_object.objectfk
     JOIN object_state   ON object_state.objectstatepk =
                            project_object.objectstatefk
     JOIN project        ON project_object.projectfk =
                            project.projectpk

     JOIN sample         ON sample.samplepk =
                            project_object.samplefk
     JOIN population     ON population.samplefk =
                            sample.samplepk

     JOIN recenzymelabelmapping ON recenzymelabelmapping.populationfk =
                                   population.populationpk
     JOIN recognitionenzyme     ON recognitionenzyme.recognitionenzymepk =
                                   recenzymelabelmapping.recognitionenzymefk

     JOIN experiment     ON experiment.projectfk =
                            project.projectpk

     JOIN chipsetup      ON chipsetup.experimentfk =
                            experiment.experimentpk
     JOIN flowcellsetup  ON flowcellsetup.chipsetupfk =
                            chipsetup.chipsetuppk
     JOIN chip           ON chip.chippk =
                            chipsetup.chipfk
     JOIN chip_run       ON chip_run.chipfk =
                            chip.chippk
     JOIN run            ON run.chiprunfk =
                            chip_run.chiprunpk
                        AND run.flowcellsetupfk =
                            flowcellsetup.flowcellsetuppk
     JOIN run_job        ON run_job.runfk =
                            run.runpk
     JOIN run_job_state  ON run_job_state.runjobstatepk =
                            run.runjobstatefk
     JOIN job            ON job.jobpk =
                            run_job.jobfk
                        AND object.jobfk =
                            job.jobpk
     JOIN operation      ON operation.operationpk =
                            job.operationfk
     JOIN prep           ON prep.populationfk =
                            population.populationpk
                        AND prep.flowcellsetupfk =
                            flowcellsetup.flowcellsetuppk

     LEFT JOIN cmap      ON cmap.cmappk =
                            prep.cmapfk

     WHERE job.updated_at BETWEEN '%s' and '%s'
) result
SQL

  my $begin_str = $begin_date->strftime('%F %T');
  my $end_str   = $end_date->strftime('%F %T');

  my $query = sprintf $sql, $begin_str, $end_str;
  $self->info("Finding jobs between '$begin_str' and '$end_str'");

  my $host_address = sprintf '%s@%s', $self->user, $self->host;

  my $ssh = WTSI::DNAP::Utilities::Runnable->new
    (executable => '/usr/bin/ssh',
     arguments  => [$host_address,
                    $self->psql_executable,
                    '--tuples-only',
                    '--dbname', $self->database_name,
                    '--command', sprintf q["%s"], qq[$query]])->run;

  my @records = $ssh->split_stdout;

  my @job_results;
  foreach my $record (@records) {
    push @job_results,
      WTSI::NPG::OM::BioNano::Saphyr::JobResult->new($record);
  }

  $self->info(sprintf q[Found %d jobs], scalar @job_results);

  return @job_results;
}

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::OM::BioNano::Saphyr::SSHAccessClient

=head1 DESCRIPTION

An interface for the backend database and analysis job filesystem of
the BioNano Saphyr Access application.

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
