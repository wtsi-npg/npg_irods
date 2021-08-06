package WTSI::NPG::HTS::PacBio::Sequel::RunDeleteMonitor;

use namespace::autoclean;
use Moose;
use MooseX::StrictConstructor;
use Try::Tiny;

use WTSI::NPG::HTS::PacBio::Sequel::RunDelete;

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::HTS::PacBio::MonitorBase
         WTSI::NPG::HTS::PacBio::Sequel::MonitorBase
         WTSI::NPG::HTS::PacBio::Sequel::RunPublisherBase
       ];

our $VERSION = '';

has 'check_format' => (
  isa        => 'Bool',
  is         => 'ro',
  required   => 0,
  default    => 0,
  documentation => 'check format of runfolder path, false by default',
);

has 'dry_run' => (
  isa        => 'Bool',
  is         => 'ro',
  required   => 0,
  default    => 0,
  documentation => 'dry run mode flag, false by default',
);


=head2 delete_runs

  Arg [1]    : None
  Example    : 
  Description: 
  Returntype : Array[Int]

=cut

sub delete_runs {
  my ($self) = @_;

  my ($num_runs, $num_processed, $num_deleted, $num_errors) = (0, 0, 0, 0);

  my $completed_runs = $self->api_client->query_runs;

  if (ref $completed_runs eq 'ARRAY') {

    my @runs = @{$completed_runs};
    $num_runs = scalar @runs;

    foreach my $run (@runs) {
        try {
            # only pick up runs that would be picked up for publication
            my $runfolder_path = $self->get_runfolder_path($run);

            if($runfolder_path && $self->valid_runfolder_directory($runfolder_path)) {
                if(defined $self->check_format && $self->check_format == 1) {
                    $self->valid_runfolder_format($runfolder_path);
                }

                ## dry run publish - to check no changes to published files
                my $publisher = $self->run_publisher_handle($runfolder_path);
                my ($nf, $np, $ne) = $publisher->publish_files();

                if ($ne > 0) {
                    $self->logcroak("Encountered $ne errors while processing ",
                            "[$np / $nf] files in $runfolder_path");
                }

                $self->warn("Found deletable runfolder $runfolder_path");
                if(!$self->dry_run) {
                    if ($self->_delete_runfolder_path($runfolder_path)) {
                        $num_deleted++;
                    }
                }
                $num_processed++;
            }
        } catch {
            $num_errors++;
            $self->error('Failed to process ',$run->{context},' cleanly');
        };
    }
  }
  return ($num_runs, $num_processed, $num_deleted, $num_errors);
}

sub _delete_runfolder_path {
  my ($self, $runfolder_path) = @_;

  $self->warn("Deleting data in runfolder '$runfolder_path'");

  my @init_args = (runfolder_path => $runfolder_path,);

  my $deleter = WTSI::NPG::HTS::PacBio::Sequel::RunDelete->new(@init_args);
  return $deleter->delete_run($self->check_format);
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::Sequel::RunDeleteMonitor

=head1 DESCRIPTION

=head1 AUTHOR

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2020 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
