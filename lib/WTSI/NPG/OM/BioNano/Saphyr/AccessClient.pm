package WTSI::NPG::OM::BioNano::Saphyr::AccessClient;

use Moose::Role;

our $VERSION = '';

requires qw[
             find_bnx_results
             get_bnx_file
          ];

=head2 get_bnx_file

  Arg [1]    : Job ID, Int.
  Arg [2]    : Local directory path, Str.

  Example    : my $local_path = $obj->get_bnx_file(1234, '/tmp')

  Description: Download the RawMolecules.bnx.gz file for job with ID
               1234 to /tmp and return the path to the newly created
               file.
  Returntype : Str

=cut

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

no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::OM::BioNano::Saphyr::AccessClient

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
