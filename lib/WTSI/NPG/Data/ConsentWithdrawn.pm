#########
# Author:        gq1
# Maintainer:    $Author: mg8 $
# Created:       24 October 2012
# Last Modified: $Date: 2018-06-18 17:48:42 +0100 (Mon, 18 Jun 2018) $
# Id:            $Id: BamConsentWithdrawn.pm 19810 2018-06-18 16:48:42Z mg8 $
# $HeadURL: svn+ssh://svn.internal.sanger.ac.uk/repos/svn/new-pipeline-dev/data_handling/branches/prerelease-46.0/lib/npg_common/irods/BamConsentWithdrawn.pm $
#

package npg_common::irods::BamConsentWithdrawn;

use strict;
use warnings;
use Moose;
use Carp;
use English qw(-no_match_vars);
use npg_common::irods::Loader;
use IPC::Open3;
use MIME::Lite;

with qw{MooseX::Getopt
        npg_common::irods::iRODSCapable};

use Readonly; Readonly::Scalar our $VERSION => do { my ($r) = q$Revision: 19810 $ =~ /(\d+)/mxs; $r; };

Readonly::Scalar my $EXIT_CODE_SHIFT => 8;
Readonly::Scalar my $RT_TICKET_EMAIL_ADDRESS => q{new-seq-pipe@sanger.ac.uk};

=head1 NAME

npg_common::irods::BamConsentWithdrawn

=head1 VERSION

$LastChangedRevision: 19810 $

=head1 SYNOPSIS

=head1 DESCRIPTION

Identifies bam files with sample_consent_withdrawn flag set and sample_consent_withdraw_email_sent not set.
Finds .bai files (if exist) for these .bam files.

Creates an RT ticket for all these files, sets the sample_consent_withdraw_email_sent flag for them and
withdraws all permissions for these files for groups and individuals that are not owners of the files.

=head1 SUBROUTINES/METHODS

=head2 dry_run

dry_run flag

=cut

has 'dry_run'     =>  ( isa           => 'Bool',
                        is            => 'ro',
                        required      => 0,
                        documentation => 'dry_run flag',
                      );
=head2 v

verbose flag

=cut

has 'v'           =>  ( isa           => 'Bool',
                        is            => 'ro',
                        required      => 0,
                        documentation => 'verbose flag',
                      );

=head2 zone

iRODS zone name, defaults to seq, can be set to undef

=cut
has 'zone'  =>  ( isa           => 'Maybe[Str]',
                  is            => 'ro',
                  required      => 0,
                  metaclass     => 'NoGetopt',
                  default       => 'seq',
                );

=head2 collection

iRODS collection name, defaults to undef, below which files are considered

=cut
has 'collection'  =>  ( isa           => 'Maybe[Str]',
                  is            => 'ro',
                  required      => 0,
                  metaclass     => 'NoGetopt',
                  default       => undef,
                );

=head2 bam_files

list of bam files with sample_consent_withdrawn flag set

=cut
has 'bam_files'  => (
                      isa        => 'ArrayRef',
                      is         => 'ro',
                      required   => 0,
                      lazy_build => 1,
                      metaclass => 'NoGetopt',
                    );
sub _build_bam_files {
  my $self = shift;

  my $iquest_cmd = $self->_iquery;
  $self->_log_message($iquest_cmd);
  my @files;
  my $no_rows_error = 0;

  my $pid = open3( undef, my $iquest_out_fh, undef, $iquest_cmd);
  while (my $line = <$iquest_out_fh> ) {
    chomp $line;
    if($line =~ /^ERROR/mxs) {
      if ($line !~ /CAT_NO_ROWS_FOUND/mxs) {
        $self->_log_message($line);
      } else {
        $no_rows_error = 1;
      }
      last;
    }
    if ($line =~/[.]bam$|[.]cram$/mxs) {
      $self->_log_message('Found ' . $line);
      push @files, $line;
    }
  }

  waitpid $pid, 0;
  if( $CHILD_ERROR >> $EXIT_CODE_SHIFT ) {
    if (!$no_rows_error) {
      croak "Failed: $iquest_cmd";
    }
  }
  close $iquest_out_fh or croak "Cannot close iquest command output: $ERRNO";

  return \@files;
}

=head2 new_bam_files

list of bam files with sample_consent_withdrawn flag set
and sample_consent_withdrawn_email_sent not set

=cut
has 'new_bam_files'  => (
                         isa        => 'ArrayRef',
                         is         => 'ro',
                         required   => 0,
                         lazy_build => 1,
                         metaclass => 'NoGetopt',
                        );
sub _build_new_bam_files {
  my $self = shift;
  my @files = ();
  foreach my $bam_file (@{$self->bam_files}) {
    if (!_rt_ticket_exists($self->_util->_check_meta_data($bam_file))) {
      push @files, $bam_file;
    }
  }
  return \@files;
}

=head2 new_files

list of bam and bai files to inform about

=cut
has 'new_files'  => (
                         isa        => 'ArrayRef',
                         is         => 'ro',
                         required   => 0,
                         lazy_build => 1,
                         metaclass => 'NoGetopt',
                        );
sub _build_new_files {
  my $self = shift;
  my @files = ();
  foreach my $bam_file (@{$self->new_bam_files}) {
    push @files, $bam_file;
    my $bai = $bam_file;
    $bai =~ s/[.]bam$/.bai/smx;
    $bai =~ s/[.]cram$/.cram.crai/smx;
    if ($self->_util->file_exists($bai)) {
      push @files, $bai;
    }
  }
  return \@files;
}


has '_util'  =>     (
                      isa        => 'npg_common::irods::Loader',
                      is         => 'ro',
                      required   => 0,
                      lazy_build => 1,
                    );
sub _build__util {
  my $self = shift;
  return npg_common::irods::Loader->new(irods    => $self->irods,
                                        _dry_run => $self->dry_run);
}

=head2 process

process bam and bai files for samples where consent has been withdrawn

=cut
sub process {
  my $self = shift;

  if (!@{$self->new_files}) {
    $self->_log_message('No files to process found');
    return;
  }

  foreach my $file (@{$self->new_files}) {
    $self->_util->restrict_file_permissions($file);
  }

  $self->_create_rt_ticket();

  my $meta_data = { sample_consent_withdrawn_email_sent => 1 };
  foreach my $file (@{$self->new_bam_files}) {
    $self->_util->add_meta($file, $meta_data);
  }

  return;
}

sub _rt_ticket_exists {
  my ($irods_meta) = @_;
  return $irods_meta->{sample_consent_withdrawn_email_sent} &&
    keys %{$irods_meta->{sample_consent_withdrawn_email_sent}};
}

sub _create_rt_ticket {
  my $self = shift;

  my $payload  = "The files with consent withdrawn:\n\n" . join qq[\n] , @{$self->new_files};
  my $msg = MIME::Lite->new(
			  To            => $RT_TICKET_EMAIL_ADDRESS,
			  Subject       => q{iRODS files: sample consent withdrawn},
			  Type          => 'TEXT',
			  Data          => $payload,
  );

  $self->_log_message(qq{The following email will be sent to $RT_TICKET_EMAIL_ADDRESS\n$payload\n});
  if (!$self->dry_run) {
    eval {
      $msg->send();
      1;
    } or do {
      croak "Error sending email : $EVAL_ERROR";
    };
  }

  return;
}

sub _iquery {
  my $self = shift;

  my $query = q{iquest --no-page};
  if ($self->zone) {
    $query .= q[ -z ] .$self->zone;
  }
  $query .=  q{ "%s/%s" "select COLL_NAME, DATA_NAME where META_DATA_ATTR_NAME = 'sample_consent_withdrawn' and META_DATA_ATTR_VALUE = '1' and DATA_NAME not like '%header.bam%'};
  if (defined $self->collection) { $query .= q{ and COLL_NAME like '} . $self->collection() . q{%'}; }
  $query .=  q{"};
  return $query;
}

sub _log_message {
  my ($self, $message) = @_;
  if ($self->v) {
    warn '***** ' .  $message ."\n";
  }
  return;
}

no Moose;

1;

__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item MooseX::Getopt

=item Carp

=item English -no_match_vars

=item Readonly

=item npg_common::irods::Loader

=item IPC::Open3

=item MIME::Lite

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Guoying Qi E<lt>gq1@sanger.ac.ukE<gt>

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2012 GRL, by Guoying Qi

This file is part of NPG.

NPG is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

=cut
