package WTSI::NPG::HTS::Illumina::LogPublisher;

use namespace::autoclean;
use File::Spec::Functions qw[catdir catfile rel2abs splitdir];
use File::Temp qw[tempdir];
use List::AllUtils qw[first];
use Moose;
use MooseX::StrictConstructor;
use POSIX qw[strftime];
use Try::Tiny;

use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::Metadata qw[$ID_RUN];
use WTSI::NPG::HTS::Publisher;
use WTSI::NPG::HTS::DataObject;

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::iRODS::Annotator
         npg_tracking::illumina::run::short_info
         npg_tracking::illumina::run::folder
       ];

our $VERSION = '';

# Default
our $DEFAULT_ROOT_COLL = '/seq';
our $DEFAULT_LOG_COLL  = 'log';

has 'irods' =>
  (isa           => 'WTSI::NPG::iRODS',
   is            => 'ro',
   required      => 1,
   documentation => 'An iRODS handle to run searches and perform updates');

has 'dest_collection' =>
  (isa           => 'Str',
   is            => 'ro',
   lazy          => 1,
   builder       => '_build_dest_collection',
   documentation => 'The destination collection within iRODS to store data');

has 'tarfile' =>
  (isa           => 'Str',
   is            => 'ro',
   lazy          => 1,
   builder       => '_build_tarfile',
   documentation => 'The name of the archive file to be created');

=head2 publish_logs

  Arg [1]    : None
  Example    : my $file = $pub->publish_logs;
  Description: Find all logs under the runfolder, tar and compress them
               and publish to iRODS in the given destination collection.
               Return the absolute path to the new file in iRODS.

               The runfolder analysis path will be used if it is present,
               otherwise the outgoing path will be used.
  Returntype : Str

=cut

sub publish_logs {
  my ($self) = @_;

  my $id_run = $self->id_run;
  $self->info("Loading log files for run $id_run");

  my $tarpath = rel2abs($self->tarfile, tempdir(CLEANUP => 1));

  # Search root for running `find' so that we do not need to cwd
  my $search_root = $self->runfolder_path;
  if (! -d $search_root) {
    # After trying the analysis path, try outgoing
    $search_root =~ s/\banalysis\b/outgoing/smx;
    if (! -d $search_root) {
      $self->logcroak("Can't find path '$search_root' to search for logs");
    }
  }

  # find normal log directories - use -prune to stop -path .. matching
  # subdirectories
  my $find_dlist =
     q[find . -type d ] .
     q[-a \\( \\( -path "*/RTALogs" ] .
     q[-o -path "*/log" -o -path "*/Logs" -o -path "*/metadata_cache_*" \\) ] .
     q[-a -prune \\)];

  # find specific files in viv directories - use -prune to stop -path
  # .. matching subdirectories
  my $find_p4 =
     q[find . -type f ] .
     q[-a \\( -path "*/tmp_[0-9]*" -a -prune \\) ] .
     q[-a \\( -name "*.err" -o -name "*.log" -o -name "*.json" \\)];

  # find links
  my $find_llist = q[find . -type l];

  my $tarcmd = "tar cJf $tarpath --exclude-vcs --exclude='core*' -T -";
  my $cmd =
    qq[set -o pipefail && cd $search_root && ] .
    qq[($find_dlist && $find_p4 && $find_llist) | $tarcmd];

  try {
    WTSI::DNAP::Utilities::Runnable->new(executable => '/bin/bash',
                                         arguments  => ['-c', $cmd])->run;
  } catch {
    my @stack = split /\n/msx;   # Chop up the stack trace
    $self->logcroak(pop @stack); # Use a shortened error message
  };

  my $publisher = WTSI::NPG::HTS::Publisher->new(irods => $self->irods);
  my $dest = $publisher->publish($tarpath, catfile($self->dest_collection,
                                                   $self->tarfile));
  my $obj = WTSI::NPG::HTS::DataObject->new($self->irods, $dest);

  my @primary_avus = $self->make_avu($ID_RUN, $self->id_run);
  my ($num_attr, $num_proc, $num_err) =
    $obj->set_primary_metadata(@primary_avus);

  if ($num_err > 0) {
    $self->logcroak("Failed to set primary metadata cleanly on '$dest'");
  }

  return $dest;
}

sub _build_tarfile {
  my ($self) = @_;

  my $date = strftime '%Y%m%d', localtime;
  my $runfolder_name = _runfolder_name($self->runfolder_path);

  return "${date}_${runfolder_name}.log.tar.xz";
}

sub _build_dest_collection  {
  my ($self) = @_;

  my @colls = ($DEFAULT_ROOT_COLL, $self->id_run, $DEFAULT_LOG_COLL);

  return catdir(@colls);
}

# We are required by npg_tracking::illumina::run::short_info to
# implment this method
sub _build_run_folder {
  my ($self) = @_;

  if (! ($self->_given_path or $self->has_id_run or $self->has_name)){
    $self->logconfess('The run folder cannot be determined because ',
                      'it was not supplied to the constructor and ',
                      'no path, id_run or run name were available, ',
                      'from which it could be inferred');
  }

  return _runfolder_name($self->runfolder_path);
}

sub _runfolder_name {
  my ($path) = @_;

  # splitdir may return empty path elements
  return first { length } reverse splitdir($path);
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::Illumina::LogPublisher

=head1 DESCRIPTION

Publishes a tar.xz archive of process log files to iRODS and adds
metadata.

=head1 AUTHOR

Jennifer Liddle E<lt>js10@sanger.ac.ukE<gt>
Keith James E<lt>kdj@sanger.ac.ukE<gt>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2015, 2016 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
