package WTSI::NPG::HTS::PacBio::Sequel::ImageArchive;

use namespace::autoclean;
use File::Basename;
use File::Copy;
use File::Spec::Functions qw[catfile catdir rel2abs];
use File::Temp qw[tempdir];
use Moose;
use MooseX::StrictConstructor;
use Try::Tiny;

use WTSI::DNAP::Utilities::Runnable;

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::HTS::PathLister
       ];

our $VERSION = '';

# ARCHIVE FILE NAME SUFFIX
our $ARCHIVE_SUFFIX = q[.tar.xz];

has 'api_client' =>
  (isa           => 'WTSI::NPG::HTS::PacBio::Sequel::APIClient',
   is            => 'ro',
   required      => 1,
   documentation => 'A PacBio Sequel API client used to fetch runs');

has 'archive_name' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'Root name for image archive');

has 'dataset_id' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The dataset id for the qc report images');

has 'dataset_type' =>
  (isa           => 'Str',
   is            => 'ro',
   default       => q[subreads],
   documentation => 'The dataset type');

has 'output_dir' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'Output directory for image archive file');

has 'report_count' =>
  (isa           => 'Int',
   is            => 'ro',
   predicate     => 'has_report_count',
   documentation => 'Minimum expected report count if known');

has 'specified_files' =>
  (isa           => 'ArrayRef[Str]',
   is            => 'ro',
   predicate     => 'has_specified_files',
   documentation => 'Optionally specify additional files');


=head2 generate_image_archive

  Arg [1]    : None
  Example    : my ($archive_file) = $report->generate_image_archive
  Description: Generate a combined archive containing QC report images 
               for the specified dataset id. Return full path to archive.
  Returntype : Str

=cut

sub generate_image_archive {
  my ($self) = @_;

  my $archive = catfile($self->output_dir, $self->archive_name . $ARCHIVE_SUFFIX);
  if (! -f $archive) {
    my $reports = $self->api_client->query_dataset_reports
      ($self->dataset_type,$self->dataset_id);

    my $dirs  = $self->_find_qc_directories($reports);
    my $files = $self->_find_qc_files($reports);
    my $repc  = $self->has_report_count ?
      (@{$files} >= $self->report_count ? 1 : 0) : 1;

    if ($repc && ($dirs->[0] || $files->[0])) {
      $self->_create_archive($dirs,$files,$archive);
    }
  }
  return $archive;
}


sub _find_qc_directories {
  my ($self, $reports) = @_;

  my %directories;
  if (ref $reports eq 'ARRAY') {
    foreach my $report (@{$reports}) {
      my $dir = dirname($report->{dataStoreFile}->{path});
      if (-d $dir) { $directories{$dir}++; }
    }
  }
  return [keys %directories];
}

sub _find_qc_files {
  my ($self, $reports) = @_;

  my %files;
  if (ref $reports eq 'ARRAY') {
    foreach my $report (@{$reports}) {
      my $qcfile = $report->{dataStoreFile}->{path};
      if (-f $qcfile) { $files{$qcfile}++; }
    }
  }

  if ( $self->has_specified_files ) {
    foreach my $sfile ( @ {$self->specified_files} ) {
      if (-f $sfile) { $files{$sfile}++; }
    }
  }

  return [keys %files];
}

sub _create_archive {
  my ($self, $dirs, $files, $archive) = @_;

  my $tmpdir = tempdir(CLEANUP => 1);
  my $tardir = rel2abs(catdir($tmpdir, $self->archive_name));

  my @find_cmds;
  foreach my $dir (@{$dirs}) {
    push @find_cmds,
      qq[find "$dir" -maxdepth 1 -type f -name "*.png" -exec cp '{}' $tardir \\;];
  }
  foreach my $file (@{$files}) {
    push @find_cmds,
      qq[find "$file" -type f | xargs -n1 cp -t $tardir];
  }

  my $find   = join q[ && ], @find_cmds;
  my $tarcmd = qq[cd $tmpdir && tar cJf $archive ]. $self->archive_name;
  my $cmd    = qq[set -o pipefail && mkdir -p $tardir && ($find) && $tarcmd];

  try {
    WTSI::DNAP::Utilities::Runnable->new(executable => '/bin/bash',
                                         arguments  => ['-c', $cmd])->run;
  } catch {
    my @stack = split /\n/msx;   # Chop up the stack trace
    $self->logcroak(pop @stack); # Use a shortened error message
  };
  return;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::Sequel::ImageArchive

=head1 DESCRIPTION

Find QC images and assocated json files for a specified dataset and 
combine them into a single archive file. 

=head1 AUTHOR

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2021 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
