#!/usr/bin/env perl

use strict;
use warnings;
use FindBin qw[$Bin];
use lib (-d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib");

use Carp;
use Data::Dump qw[pp];
use File::Slurp;
use List::AllUtils qw[any];
use Log::Log4perl qw[:levels];
use Getopt::Long;
use Pod::Usage;
use Try::Tiny;

use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::Collection;
use WTSI::NPG::HTS::TreePublisher;

our $VERSION = '';

my $log_config = << 'LOGCONF'
log4perl.logger = INFO, A1

# Errors from WTSI::NPG::iRODS are propagated in the code to callers,
# so we do not need to see them directly:
log4perl.logger.WTSI.NPG.iRODS = OFF, A1

log4perl.appender.A1 = Log::Log4perl::Appender::Screen
log4perl.appender.A1.layout = Log::Log4perl::Layout::PatternLayout
log4perl.appender.A1.layout.ConversionPattern = %d %-5p %c - %m%n
log4perl.appender.A1.utf8 = 1

# Prevent duplicate messages with a non-Log4j-compliant Log4perl option
log4perl.oneMessagePerAppender = 1
LOGCONF
;

my $dest_collection;
my $debug;
my $force;
my $max_errors = 0;
my $metadata_file;
my $restart_file;
my $source_directory;
my $verbose;

my @include;
my @exclude;
my @groups;

GetOptions('collection=s'                        => \$dest_collection,
           'debug'                               => \$debug,
           'exclude=s'                           => \@exclude,
           'force'                               => \$force,
           'group=s'                             => \@groups,
           'help'                                => sub {
             pod2usage(-verbose => 2, -exitval => 0);
           },
           'include=s'                           => \@include,
           'max-errors|max_errors=i'             => \$max_errors,
           'metadata=s'                          => \$metadata_file,
           'restart-file|restart_file=s'         => \$restart_file,
           'source-directory|source_directory=s' => \$source_directory,
           'verbose'                             => \$verbose);

if ($verbose and not $debug) {
  Log::Log4perl::init(\$log_config);
}
else {
  my $level = $debug ? $DEBUG : $WARN;
  Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                            level  => $level,
                            utf8   => 1});
}

my $log = Log::Log4perl->get_logger('main');
$log->level($ALL);

sub _make_filter_fn {

  my @include_re;
  my @exclude_re;
  my $nerr = 0;

  foreach my $re (@include) {
    try {
      push @include_re, qr{$re}msx;
    } catch {
      $log->error("in include regex '$re': $_");
      $nerr++;
    };
  }

  foreach my $re (@exclude) {
    try {
      push @exclude_re, qr{$re}msx;
    } catch {
      $log->error("in exclude regex '$re': $_");
      $nerr++;
    };
  }

  if ($nerr > 0) {
    $log->error("$nerr errors in include / exclude filters");
    exit 1;
  }

  return sub {
    my ($path) = @_;

    (defined $path and $path ne q[]) or
        croak 'Path argument is required in callback';

    my $include = -f $path;
    if (@include_re) {
      $include = any {$path =~ $_} @include_re;
    }
    if ($include and @exclude_re) {
      $include = not any {$path =~ $_} @exclude_re;
    }

    return $include;
  };
}

sub _read_metadata_file {
  my $metadata_json = read_file($metadata_file);
  my $metadata = JSON->new->utf8(1)->decode($metadata_json);

  if (not ref $metadata eq 'ARRAY') {
    $log->logcroak("Malformed metadata JSON in '$metadata_file'; expected",
                   'an array');
  }
  return $metadata;
}

if (not $source_directory) {
  my $msg = 'A --source-directory argument is required';
  pod2usage(-msg     => $msg,
            -exitval => 2);
}

if (not $dest_collection) {
  my $msg = 'A --collection argument is required';
  pod2usage(-msg     => $msg,
            -exitval => 2);
}

my $irods = WTSI::NPG::iRODS->new;
my @init_args = (dest_collection        => $dest_collection,
                force                  => $force,
                irods                  => $irods,
                require_checksum_cache => [],
                source_directory       => $source_directory);
if ($max_errors) {
  push @init_args, max_errors => $max_errors;
}

my $coll = WTSI::NPG::iRODS::Collection->new($irods, $dest_collection);
my $publisher = WTSI::NPG::HTS::TreePublisher->new(@init_args);

if ($restart_file) {
  $log->info("Using restart_file '$restart_file'");
  $publisher->publish_state->read_state($restart_file);
}

# Ensure any restart file is written on signals
use sigtrap 'handler', \&handler, 'normal-signals';

sub handler {
  my ($signal) = @_;

  if ($restart_file) {
    $log->info('Writing restart file ', $restart_file);
    $publisher->publish_state->write_state($restart_file);
  }
  $log->error("Exiting due to $signal");
  exit 1;
}

my @files = grep { -f } $publisher->list_directory($source_directory,
                                                   recurse => 1);
my @publish_args = (\@files,
                    secondary_cb => sub {
                      my ($obj) = @_;

                      # This is a something of a hack to enable access
                      # restriction on everything published
                      $obj->is_restricted_access(1);

                      return ();
                    });

# Define any file filters required
if (@include or @exclude) {
  push @publish_args, filter => _make_filter_fn();
}

my ($num_files, $num_published, $num_errors) =
    $publisher->publish_tree(@publish_args);


# Set any permissions requested
if (@groups) {
  $coll->set_content_permissions($WTSI::NPG::iRODS::READ_PERMISSION, @groups);
}

# Add any metadata provided
if ($metadata_file) {
  my $metadata = _read_metadata_file();
  $log->debug('Adding to ', $coll->str, ' metadata: ', pp($metadata));
  foreach my $avu (@{$metadata}) {
    $coll->add_avu($avu->{attribute}, $avu->{value}, $avu->{units});
  }
}

if ($restart_file) {
  $publisher->publish_state->write_state($restart_file);
}

if ($num_errors == 0) {
  $log->info("Processed $num_files, published $num_published ",
             "with $num_errors errors");
}
else {
  $log->logcroak("Processed $num_files, published $num_published ",
                 "with $num_errors errors");
}

__END__

=head1 NAME

npg_publish_tree

=head1 SYNOPSIS

npg_publish_tree --source-directory <path> --collection <path>
  [--metadata <path>] [--group <iRODS group>]*
  [--force] [--max-errors <n>] [--restart-file <path>]
  [--debug] [--verbose] [--logconf <path>]

 Options:
   --collection       The destination collection in iRODS.
   --debug            Enable debug level logging. Optional, defaults to
                      false.
   --exclude          Specify one or more regexes to ignore paths under
                      the target collection. Matching paths will be not be
                      published. If more than one regex is supplied, they
                      are all applied. Exclude regexes are applied after
                      any include regexes (see below).
   --force            Force an attempt to re-publish files that have been
                      published successfully.
   --group            iRODS group to have read access. Optional, defaults
                      to none. May be used multiple times to add read
                      permissions for multiple groups.
   --help             Display help.
   --include          Specify one or more regexes to select paths under
                      the target collection. Only matching paths will be
                      published, all others will be ignored. If more than
                      one regex is supplied, the matches for all of them
                      are aggregated.

   --max-errors       The maximum number of errors permitted before aborting.
                      Optional, defaults to unlimited.
   --metadata         A JSON file containing metadata to be added to the
                      destination collection. Optional.
                      The JSON must describe the metadata in baton syntax (an
                      array of AVUs):

                      E.g. [{"attribute": "attr1", "value": "val1"},
                            {"attribute": "attr2", "value": "val2"}]


   --restart-file
   --restart_file     A file path where a record of successfully published
                      files will be recorded in JSON format on exit. If the
                      jobs is restarted, no attempt will be made to publish
                      or even check these files in iRODS. Optional. The
                      default restart file is "<archive dir>/published.json".
   --source-directory
   --source_directory The local path to load.
   --verbose          Print messages while processing. Optional.

=head1 DESCRIPTION

Publish an arbitrary directory hierarchy to iRODS, set permissions and
add metadata to the root collection.

=head1 AUTHOR

Keith James kdj@sanger.ac.uk

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
