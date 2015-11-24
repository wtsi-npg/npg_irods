package WTSI::NPG::HTS::HTSFilePublisher;

use namespace::autoclean;
use File::Spec::Functions;
use Moose;

use st::api::lims;

use WTSI::NPG::HTS::Types qw(HTSFileFormat);
use WTSI::NPG::iRODS;

with 'WTSI::DNAP::Utilities::Loggable', 'WTSI::NPG::HTS::HTSFilenameParser',
  'WTSI::NPG::HTS::Annotator',
  'npg_tracking::illumina::run::short_info',
  'npg_tracking::illumina::run::folder';

our $VERSION = '';

our $DEFAULT_ROOT_COLL = '/seq';
our $DEFAULT_NUM_LANES = 8;
our $EXIT_CODE_SHIFT   = 8;
our $QC_COLL           = 'qc';

has 'irods' =>
  (is            => 'ro',
   isa           => 'WTSI::NPG::iRODS',
   required      => 1,
   documentation => 'An iRODS handle to run searches and perform updates.');

has 'lims' =>
  (is            => 'ro',
   isa           => 'st::api::lims',
   required      => 1,
   lazy          => 1,
   default       => sub {
     return st::api::lims->new(id_run => $_[0]->id_run)
   },
   documentation => 'A st::api::lims handle used to find metadata');

has 'file_format' =>
  (isa           => HTSFileFormat,
   is            => 'ro',
   required      => 1,
   lazy          => 1,
   default       => 'cram',
   documentation => 'The format of the file to be published');

has 'positions' =>
  (isa           => 'ArrayRef',
   is            => 'ro',
   required      => 1,
   lazy          => 1,
   builder       => '_build_positions',
   documentation => 'The instrument positions of data to be published');

has alignment_files =>
  (isa => 'ArrayRef',
   is            => 'ro',
   required      => 1,
   lazy          => 1,
   builder       => 'list_alignment_files',
   documentation => 'The alignment file to be published');

has 'collection' =>
  (isa           => 'Str',
   is            => 'ro',
   lazy          => 1,
   builder       => '_build_collection',
   documentation => 'The target collection within irods to store results');

has 'alt_process' =>
  (isa           => 'Maybe[Str]',
   is            => 'ro',
   documentation => 'Non-standard process used');

sub _build_positions {
  my ($self) = @_;

  return [sort map {$_->position} $self->lims->children];
}

sub _build_collection  {
  my ($self) = @_;

  my @colls = ($DEFAULT_ROOT_COLL, $self->id_run);
  if ($self->alt_process) {
    push @colls, $self->alt_process
  }

  return catdir(@colls);
}

sub list_alignment_files {
  my ($self) = @_;

  my $positions_pattern;
  my @positions = @{$self->positions};
  if (@positions) {
    $positions_pattern = sprintf '{%s}', join q[,], @positions;
  }
  else {
    $positions_pattern = q[*];
  }

  my $file_pattern  = sprintf '%d_%s*.%s', $self->id_run,
    $positions_pattern, $self->file_format;
  my $plex_file_pattern = sprintf 'lane%s/*.%s',
    $positions_pattern, $self->file_format;

  my @file_list;
  push @file_list, glob catfile($self->archive_path, $file_pattern);
  push @file_list, glob catfile($self->archive_path, $plex_file_pattern);

  return \@file_list;
}

sub publish_alignment_files {
  my ($self) = @_;

  my $num_processed = 0;
  my $num_errors    = 0;
  foreach my $file (@{$self->alignment_files}) {

  }

  return;
}

# sub is_phix_control {
#   my ($self, $alignment_file) = @_;

#   return $self->lims->is_control;
# }

__PACKAGE__->meta->make_immutable;

no Moose;

1;
