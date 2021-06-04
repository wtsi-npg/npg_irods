package WTSI::NPG::Data::ConsentWithdrawn;

use Moose;
use English qw{-no_match_vars};
use IPC::Open3;
use MIME::Lite;
use Readonly;

use WTSI::NPG::HTS::DataObject;

with qw{
  MooseX::Getopt
  WTSI::DNAP::Utilities::Loggable
       };

our $VERSION = '0';

Readonly::Scalar my $EXIT_CODE_SHIFT => 8;
Readonly::Scalar my $RT_TICKET_EMAIL_ADDRESS => q{new-seq-pipe@sanger.ac.uk};
Readonly::Scalar my $RT_TICKET_FLAG_META_KEY => q{sample_consent_withdrawn_email_sent};

=head1 NAME

WTSI::NPG::Data::ConsentWithdrawn

=head1 SYNOPSIS

=head1 DESCRIPTION

Identifies data with sample_consent_withdrawn flag set and
sample_consent_withdraw_email_sent not set. Finds corresponding sequence files.

Creates an RT ticket for each sample, sets the sample_consent_withdraw_email_sent
flag for them and withdraws all permissions for these files for groups and
individuals that are not owners of the files.

=head1 SUBROUTINES/METHODS

=head2 dry_run

Dry run flag, false by default. No changes to iRODS data
no email sent.

=cut

has 'dry_run' => (
  isa           => 'Bool',
  is            => 'ro',
  required      => 0,
  documentation => 'dry run flag',
);

=head2 zone

iRODS zone name, no default.

=cut

has 'zone' => (
  isa           => 'Str',
  is            => 'ro',
  required      => 0,
  documentation => 'iRODS zone name, unset by default',
);

=head2 collection

iRODS collection name, no default. If set the search is
constraint to this collection.

=cut

has 'collection' => (
  isa           => 'Str',
  is            => 'ro',
  required      => 0,
  documentation => 'iRODS collection',
);

=head2 irods

WTSI::NPG::iRODS type object mediating access to iRODS,
required.

=cut

has 'irods' => (
  isa        => 'WTSI::NPG::iRODS',
  is         => 'ro',
  required   => 1,
  metaclass => 'NoGetopt',
);

=head2 process

Processes files for samples where consent has been withdrawn.

=cut

sub process {
  my $self = shift;

  $self->dry_run and $self->info('DRY RUN - no metadata and permissions change');

  if (!@{$self->_new_files}) {
    $self->info('No files to process found');
    return;
  }

  foreach my $file (@{$self->_new_files}) {
    $self->dry_run or $self->_restrict_permissions($file);
  }

  $self->_create_rt_ticket();

  foreach my $file (@{$self->_new_files}) {
    $self->dry_run or $self->irods->add_object_avu(
      $file, $RT_TICKET_FLAG_META_KEY, 1);
  }

  return;
}

# List of files with sample_consent_withdrawn flag set
has '_files' => (
  isa        => 'ArrayRef',
  is         => 'ro',
  required   => 0,
  lazy_build => 1,
);
sub _build__files {
  my $self = shift;

  my $iquest_cmd = $self->_iquery;
  $self->info("Will run: $iquest_cmd");
  my @files;
  my $no_rows_error = 0;

  my $pid = open3( undef, my $iquest_out_fh, undef, $iquest_cmd);
  while (my $line = <$iquest_out_fh> ) {
    chomp $line;
    if($line =~ /^ERROR/mxs) {
      if ($line !~ /CAT_NO_ROWS_FOUND/mxs) {
        $self->debug($line);
      } else {
        $no_rows_error = 1;
      }
      last;
    }
    if ($line =~/[.]bam$|[.]cram$/mxs) {
      $self->debug('Found ' . $line);
      push @files, $line;
    }
  }

  waitpid $pid, 0;
  if( $CHILD_ERROR >> $EXIT_CODE_SHIFT ) {
    if (!$no_rows_error) {
      $self->logcroak("Failed: $iquest_cmd");
    }
  }
  close $iquest_out_fh or
    $self->logcroak("Cannot close iquest command output: $ERRNO");

  @files = sort @files;
  return \@files;
}

# List of files with sample_consent_withdrawn flag set
# and sample_consent_withdrawn_email_sent not set
has '_new_files' => (
  isa        => 'ArrayRef',
  is         => 'ro',
  required   => 0,
  lazy_build => 1,
);
sub _build__new_files {
  my $self = shift;

  my @files = ();
  foreach my $file (@{$self->_files}) {
    my @meta =
      grep { $_->{value} }
      grep { $_->{attribute} eq $RT_TICKET_FLAG_META_KEY }
      $self->irods->get_object_meta($file);
    @meta or push @files, $file;
  }

  return \@files;
}

sub _create_rt_ticket {
  my $self = shift;

  my $payload  = "The files with consent withdrawn:\n\n" . join qq[\n] , @{$self->_new_files};
  $self->info(qq{The following email will be sent to $RT_TICKET_EMAIL_ADDRESS\n$payload\n});

  $self->dry_run and return;

  MIME::Lite->new(
    To            => $RT_TICKET_EMAIL_ADDRESS,
    Subject       => q{iRODS files: sample consent withdrawn},
    Type          => 'TEXT',
    Data          => $payload,
  )->send();

  return;
}

sub _iquery {
  my $self = shift;

  my $query = q{iquest --no-page};
  if ($self->zone) {
    $query .= q[ -z ] .$self->zone;
  }
  $query .=
    q{ "%s/%s" "select COLL_NAME, DATA_NAME where META_DATA_ATTR_NAME = 'sample_consent_withdrawn'} .
    q{ and META_DATA_ATTR_VALUE = '1' and DATA_NAME not like '%header.bam%'};
  if ($self->collection) {
    $query .= q{ and COLL_NAME like '} . $self->collection() . q{%'};
  }
  $query .= q{"};

  return $query;
}

sub _restrict_permissions {
  my ($self, $file) = @_;

  my @files = ( $file );

  my $index_file = $file;
  $index_file =~ s/[.]bam$/.bai/smx;
  $index_file =~ s/[.]cram$/.cram.crai/smx;
  my $obj = WTSI::NPG::HTS::DataObject->new($self->irods, $index_file);
  if ($obj->is_present) {
    push @files, $index_file;
  }

  my $nullp = $WTSI::NPG::iRODS::NULL_PERMISSION;
  my $ownp  = $WTSI::NPG::iRODS::OWN_PERMISSION;
  for my $f (@files) {
    my @to_remove = grep { $_->{level} ne $ownp }
                    $self->irods->get_object_permissions($f);
    for my $p (@to_remove) {
        my $owner_zone = join q[#],$p->{owner},$p->{zone};
        $self->irods->set_object_permissions($nullp, $owner_zone, $f);
    }
  }

  return;
}

1;

__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item MooseX::Getopt

=item English -no_match_vars

=item Readonly

=item IPC::Open3

=item MIME::Lite

=item WTSI::NPG::HTS::DataObject

=item WTSI::DNAP::Utilities::Loggable

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2012,2013,2015,2020 Genome Research Ltd.

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
