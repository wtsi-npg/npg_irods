package WTSI::NPG::Data::ConsentWithdrawn;

use Moose;
use English qw{-no_match_vars};
use IPC::Open3;
use MIME::Lite;
use Readonly;

use WTSI::NPG::HTS::DataObject;
use WTSI::NPG::iRODS::icommands qw[iquest];

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

  if (!@{$self->files_to_email}) {
    $self->info('No files to process found');
    return;
  }

  foreach my $file (@{$self->files_to_email}) {
    $self->dry_run or $self->_restrict_permissions($file);
  }

  $self->_create_rt_ticket();

  foreach my $file (@{$self->files_to_email}) {
    $self->dry_run or $self->irods->add_object_avu(
      $file, $RT_TICKET_FLAG_META_KEY, 1);
  }

  return;
}

# List of files with sample_consent_withdrawn flag set
has 'files_withdrawn' => (
  isa        => 'ArrayRef',
  is         => 'ro',
  required   => 0,
  lazy_build => 1,
);

sub _build_files_withdrawn {
  my $self = shift;

  my @query;
  if ($self->zone) {
    push @query, q[-z], $self->zone;
  }

  my $select = q[select COLL_NAME, DATA_NAME ] .
      q[where META_DATA_ATTR_NAME = 'sample_consent_withdrawn' ] .
      q[and META_DATA_ATTR_VALUE = '1' ] .
      q[and DATA_NAME not like '%header.bam%'];

  if ($self->collection) {
      $select .= sprintf q[ and COLL_NAME like '%s%%'], $self->collection();
  }
  push @query, q[%s/%s], qq("$select");

  $self->info('Will run: iquest ', join q[ ], @query);

  my @files;
  foreach my $rec (iquest(@query)) {
    if ($rec =~/[.]bam$|[.]cram$/mxs) {
      $self->debug('Found ', $rec);
      push @files, $rec;
    }
  }

  @files = sort @files;
  return \@files;
}

# List of files with sample_consent_withdrawn flag set
# and sample_consent_withdrawn_email_sent not set
has 'files_to_email' => (
  isa        => 'ArrayRef',
  is         => 'ro',
  required   => 0,
  lazy_build => 1,
);

sub _build_files_to_email {
  my $self = shift;

  my @files;
  foreach my $file (@{$self->files_withdrawn}) {
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

  my $payload = "The files with consent withdrawn:\n\n" . join qq[\n] ,
    @{$self->_build_files_to_email};
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

sub _restrict_permissions {
  my ($self, $file) = @_;

  my @files = ($file);

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

=item MIME::Lite

=item WTSI::NPG::HTS::DataObject

=item WTSI::DNAP::Utilities::Loggable

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Marina Gourtovaia

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2012, 2013, 2015, 2020, 2023 Genome Research Ltd.

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
