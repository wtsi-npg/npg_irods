package WTSI::NPG::HTS::PacBio::Sequel::RunMonitor;

use namespace::autoclean;
use Data::Dump qw[pp];
use File::Spec::Functions qw[catfile splitdir];
use Moose;
use MooseX::StrictConstructor;
use Try::Tiny;

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::HTS::PacBio::MonitorBase
         WTSI::NPG::HTS::PacBio::Sequel::MonitorBase
         WTSI::NPG::HTS::PacBio::Sequel::RunPublisherBase
       ];

our $VERSION = '';

=head2 publish_completed_runs

  Arg [1]    : None

  Example    : my ($num_files, $num_published, $num_errors) =
                 $monitor->publish_completed_runs
  Description: Publish all completed runs to iRODS. Return the number of
               files, the number published and the number of errors.
  Returntype : Array[Int]

=cut

sub publish_completed_runs {
  my ($self) = @_;

  my $started_runs = $self->api_client->query_runs;

  my ($num_runs, $num_processed, $num_errors) = (0, 0, 0);

  if (ref $started_runs eq 'ARRAY') {
    my @runs = @{$started_runs};
    $num_runs = scalar @runs;

    foreach my $run (@runs) {
      try {
        my $runfolder_path = $self->get_runfolder_path($run);

        if ($runfolder_path) {
          my $publisher = $self->run_publisher_handle($runfolder_path);
          my ($nf, $np, $ne) = $publisher->publish_files();
          $self->debug("Processed [$np / $nf] files in ",
                       "'$runfolder_path' with $ne errors");

          if ($ne > 0) {
            $self->logcroak("Encountered $ne errors while processing ",
                            "[$np / $nf] files in '$runfolder_path'");
          }
          $num_processed++;
        }
      } catch {
        $num_errors++;
        $self->error('Failed to process ',$run->{context},' cleanly',
                     "[$num_processed / $num_runs]: ", $_);
      };
    }

    if ($num_errors > 0) {
      $self->error("Encountered errors on $num_errors / ",
                   "$num_processed runs processed");
    }
  }

  return ($num_runs, $num_processed, $num_errors);
}


__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::Sequel::RunMonitor

=head1 DESCRIPTION

The run monitor contacts the PacBio SMRT Link services API to
get a list of runs and then publishes relevant runs to iRODS.

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
