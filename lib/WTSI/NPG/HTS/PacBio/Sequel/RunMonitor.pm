package WTSI::NPG::HTS::PacBio::Sequel::RunMonitor;

use namespace::autoclean;
use DateTime;
use File::Spec::Functions qw[catdir];
use Moose;
use MooseX::StrictConstructor;
use Readonly;
use Try::Tiny;

use WTSI::NPG::HTS::PacBio::Sequel::JobExecute::WrJob;

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::HTS::PacBio::MonitorBase
         WTSI::NPG::HTS::PacBio::Sequel::MonitorBase
         WTSI::NPG::HTS::PacBio::Sequel::RunPublisherBase
       ];

our $VERSION = '';

Readonly::Scalar my $PUBLISH_SCRIPT => q[npg_publish_pacbio_run.pl];


has 'execute' => (
  isa           => 'Bool',
  is            => 'ro',
  default       => 1,
  documentation => 'A flag turning on/off execution, true by default');

has 'log_dir' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 0,
   predicate     => 'has_log_dir',
   documentation => 'The directory for log files for wr jobs');

has 'submit_wr' =>
 (isa        => 'Bool',
  is         => 'ro',
  required   => 0,
  default    => 0,
  documentation => 'Submit publisher jobs to wr, false by default');


=head2 publish_completed_runs

  Arg [1]    : None

  Example    : my ($num_files, $num_published, $num_errors) =
                 $monitor->publish_completed_runs
  Description: Publish all completed runs to iRODS. Return the number of
               files, the number published and the number of errors. A very
               basic lock file is used to prevent clashing load jobs as
               the risk is low and the risk of difficult to recover from harm 
               caused by clashing is low.
  Returntype : Array[Int]

=cut

sub publish_completed_runs {
  my ($self) = @_;

  my $runs_to_process = $self->api_client->query_runs;

  my ($num_runs, $num_processed, $num_errors) = (0, 0, 0);
  my ($processed, $errors) = (0, 0);

  if (ref $runs_to_process eq 'ARRAY') {
    $num_runs = scalar @{$runs_to_process};

    if ( $self->submit_wr ) {
      ($processed,$errors) = $self->_submit_runs($runs_to_process);
    } else {
      ($processed,$errors) = $self->_process_runs($runs_to_process);
    }
  }

  $num_processed += $processed;
  $num_errors    += $errors;

  if ($num_errors > 0) {
    $self->error("Encountered errors on $num_errors / ",
                 "$num_processed runs processed");
  }

  return ($num_runs, $num_processed, $num_errors);
}

sub _process_runs {
  my ($self,$runs_to_process) = @_;

  my ($processed,$errors) = (0,0);

  foreach my $run (@{$runs_to_process}) {

    my($runfolder_path,$mark,$unmark);
    try {
      $runfolder_path = $self->get_runfolder_path($run);
      if ($runfolder_path) {
        my $publisher = $self->run_publisher_handle($runfolder_path);
        my ($nf, $np, $ne) = (0,0,0);
        if ($self->has_log_dir) {
          $mark = $self->mark_folder($self->log_dir,$run->{name});
          if ($mark) {
            ($nf, $np, $ne) = $publisher->publish_files();
            $unmark = $self->unmark_folder($self->log_dir,$run->{name});
          }
          else {
            $self->info("Cant load '$runfolder_path' as marked in progress");
          }
        }
        else {
          ($nf, $np, $ne) = $publisher->publish_files();
        }
        $self->debug("Processed [$np / $nf] files in ",
                     "'$runfolder_path' with $ne errors");

        if ($ne > 0) {
          $self->logcroak("Encountered $ne errors while processing ",
                          "[$np / $nf] files in '$runfolder_path'");
        }
        $processed++;
      }
    } catch {
      $errors++;
      $self->error('Failed to process ',$run->{context},' cleanly',
                   "[processed count: $processed]: ", $_);
    };
    if ($mark && !$unmark) {
      $self->unmark_folder($self->log_dir,$run->{name});
    }
  }
  return($processed,$errors);
}

sub _submit_runs {
  my ($self,$runs_to_process) = @_;

  my ($processed,$errors) = (0,0);

  foreach my $run (@{$runs_to_process}) {

    try {
      my $runfolder_path = $self->get_runfolder_path($run);

      if ($runfolder_path) {

        my $publisher = $self->run_publisher_handle($runfolder_path);
        my $script    = $PUBLISH_SCRIPT;
        $script       =~ s/[.]\w+$//msx;
        my $pdir      = catdir($self->log_dir,$run->{name});

        my @cmds;
        push @cmds, $PUBLISH_SCRIPT. q( --verbose --api_url ). $self->api_uri .
          qq( --runfolder-path $runfolder_path --collection ). $publisher->dest_collection;

        my @init_args  = (commands4jobs => \@cmds,
                          created_on    => DateTime->now(),
                          identifier    => $run->{name} .q[_]. $script,
                          working_dir   => $pdir,);

        my $j = WTSI::NPG::HTS::PacBio::Sequel::JobExecute::WrJob->new(@init_args);
        $j->pre_execute;
        if($self->execute){ $j->submit; $processed++; }
      }
    } catch {
      $errors++;
      $self->error('Failed to submit ',$run->{context},' cleanly', $_);
    }
  }
  return($processed,$errors);
}


__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

more =head1 NAME

WTSI::NPG::HTS::PacBio::Sequel::RunMonitor

=head1 DESCRIPTION

The run monitor contacts the PacBio SMRT Link services API to
get a list of runs and then publishes relevant runs to iRODS
or submits publish jobs if the submit_wr option has been used.

It does not query iRODS to find which runs have been published
previously, instead the checks are done at the file and metadata
level.

=head1 AUTHOR

Keith James E<lt>kdj@sanger.ac.ukE<gt>
Guoying Qi E<lt>gq1@sanger.ac.ukE<gt>

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
