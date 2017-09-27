package WTSI::NPG::HTS::BatchPublisher;

use namespace::autoclean;

use Data::Dump qw[pp];
use English qw[-no_match_vars];
use File::Basename;
use File::Spec::Functions qw[catfile];
use Moose;
use MooseX::StrictConstructor;
use Try::Tiny;

use WTSI::DNAP::Utilities::Params qw[function_params];
use WTSI::NPG::HTS::DefaultDataObjectFactory;
use WTSI::NPG::iRODS::Publisher;

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::DNAP::Utilities::JSONCodec
       ];

our $VERSION = '';

has 'irods' =>
  (isa           => 'WTSI::NPG::iRODS',
   is            => 'ro',
   required      => 1,
   documentation => 'An iRODS handle to run searches and perform updates');

has 'state' =>
  (isa           => 'HashRef',
   is            => 'rw',
   required      => 1,
   default       => sub { return {} },
   init_arg      => undef,
   documentation => 'State of all files published, across all batches. The ' .
                    'keys are local file paths and values are 1 if the ' .
                    'file has been published or 0 if it has not');

has 'obj_factory' =>
  (does          => 'WTSI::NPG::HTS::DataObjectFactory',
   is            => 'ro',
   required      => 1,
   lazy          => 1,
   builder       => '_build_obj_factory',
   documentation => 'A factory building data objects from files');

has 'max_errors' =>
  (isa           => 'Int',
   is            => 'ro',
   required      => 0,
   predicate     => 'has_max_errors',
   documentation => 'The maximum number of errors permitted before ' .
                    'the remainder of a publishing process is aborted');

has 'state_file' =>
  (isa           => 'Str',
   is            => 'rw',
   required      => 1,
   documentation => 'JSON state file containing an array of all local file ' .
                    'paths that have been published');

has 'force' =>
  (isa           => 'Bool',
   is            => 'ro',
   required      => 0,
   default       => 0,
   documentation => 'Force re-publication of files that have been published');

has 'require_checksum_cache' =>
  (is            => 'ro',
   isa           => 'ArrayRef[Str]',
   required      => 1,
   default       => sub { return [qw[bam cram]] },
   documentation => 'A list of file suffixes for which MD5 cache files ' .
                    'must be provided and will not be created on the fly');

=head2 publish_file_batch

  Arg [1]    : File batch, ArrayRef[Str].
  Arg [2]    : Destination collection, Str.
  Arg [3]    : Callback returning primary AVUs for a data object. CodeRef.
  Arg [4]    : Callback returning secondary AVUs for a data object.CodeRef.
  Arg [5]    : Callback returning extra AVUs for a data object. CodeRef,
               Optional.

  Example    : $pub->publish_file_batch(['x.txt', 'y.txt'],
                                        '/destination/collection',
                                        sub { ... },
                                        sub { ... });

  Description: Publish a list of files to iRODS, using callbacks to
               caculate the metadata to be applied to each file. Return the
               number of files, the number published and the number of
               errors.
  Returntype : Array[Int]

=cut

## no critic (ProhibitManyArgs)
sub publish_file_batch {
  my ($self, $files, $dest_coll, $primary_avus_callback,
      $secondary_avus_callback, $extra_avus_callback) = @_;

  defined $files or
    $self->logconfess('A defined files argument is required');
  ref $files eq 'ARRAY' or
    $self->logconfess('The files argument must be an ArrayRef');
  defined $dest_coll or
    $self->logconfess('A defined dest_coll argument is required');

  $extra_avus_callback ||= sub { return };

  my $publisher =
    WTSI::NPG::iRODS::Publisher->new
      (irods                  => $self->irods,
       require_checksum_cache => $self->require_checksum_cache);

  $self->read_state;

  my $num_files     = scalar @{$files};
  my $num_processed = 0;
  my $num_errors    = 0;

 FILE: foreach my $file (@{$files}) {
    my $dest = q[];

    if ($self->has_max_errors and $num_errors >= $self->max_errors) {
      $self->error("The number of errors $num_errors reached the maximum ",
                   'permitted of ', $self->max_errors, '. Aborting');
      last FILE;
    }

    if (exists $self->state->{$file}) {
      if ($self->state->{$file} == 1) {
        if ($self->force) {
          $self->info("Forcing re-publication of local file '$file'");
        }
        else {
          $self->info("Skipping local file '$file' already published");
          next FILE;
        }
      }
    }
    else {
      $self->debug("Added new file '$file'");
    }

    try {
      $num_processed++;
      my ($filename, $directories, $suffix) = fileparse($file);
      my $path = catfile($dest_coll, $filename);
      my $obj = $self->obj_factory->make_data_object($path);
      if (not $obj) {
        $self->logconfess("Failed to parse and make an object from '$path'");
      }

      $dest = $publisher->publish($file, $obj->str)->str;

      my @primary_avus = $primary_avus_callback->($obj);
      my ($num_pattr, $num_pproc, $num_perr) =
        $obj->set_primary_metadata(@primary_avus);

      my @secondary_avus = $secondary_avus_callback->($obj);
      my ($num_sattr, $num_sproc, $num_serr) =
        $obj->update_secondary_metadata(@secondary_avus);

      my @extra_avus = $extra_avus_callback->($obj);
      my ($num_xattr, $num_xproc, $num_xerr) =
        $self->_add_extra_metadata($obj, @extra_avus);

      # Test metadata at the end
      if ($num_perr > 0) {
        $self->logcroak("Failed to set primary metadata cleanly on '$dest'");
      }
      if ($num_serr > 0) {
        $self->logcroak("Failed to set secondary metadata cleanly on '$dest'");
      }
      if ($num_xerr > 0) {
        $self->logcroak("Failed to set extra metadata cleanly on '$dest'");
      }

      $self->state->{$file} = 1; # Mark as published

      $self->info("Published '$dest' [$num_processed / $num_files]");
    } catch {
      $num_errors++;
      my @stack = split /\n/msx;  # Chop up the stack trace
      $self->error("Failed to publish '$file' to '$dest' cleanly ",
                   "[$num_processed / $num_files]: ", pop @stack);
    };
  }

  if ($num_errors > 0) {
    $self->error("Encountered errors on $num_errors / ",
                 "$num_processed files processed");
  }

  $self->write_state;

  return ($num_files, $num_processed, $num_errors);
}
## use critic

sub read_state {
  my ($self) = @_;

  my $file = $self->state_file;

  local $INPUT_RECORD_SEPARATOR = undef;
  if (-e $file) {
    open my $fh, '<', $file or
      $self->logcroak("Failed to open '$file' for reading: ", $ERRNO);
    my $octets = <$fh>;
    close $fh or $self->warn("Failed to close '$file'");

    try {
      my $state = $self->decode($octets);
      $self->debug("Read from state file '$file': ", pp($state));
      $self->state($state);
    } catch {
      $self->logcroak('Failed to a parse JSON value from ',
                      "state file '$file': ", $_);
    };
  }

  return $self->state;
}

sub write_state {
  my ($self) = @_;

  my $file = $self->state_file;
  $self->debug("Writing to state file '$file':", pp($self->state));

  my $octets;
  try {
    $octets = $self->encode($self->state);
  } catch {
    $self->logcroak('Failed to a encode JSON value to ',
                    "state file '$file': ", $_);
  };

  open my $fh, '>', $file or
    $self->logcroak("Failed to open '$file' for writing: ", $ERRNO);
  print $fh $octets or
    $self->logcroak("Failed to write to '$file': ", $ERRNO);
  close $fh or $self->warn("Failed to close '$file'");

  return $file;
}

sub _build_obj_factory {
  my ($self) = @_;

  return WTSI::NPG::HTS::DefaultDataObjectFactory->new(irods => $self->irods)
}

sub _add_extra_metadata {
  my ($self, $obj, @avus) = @_;

  defined $obj or
    $self->logconfess('A defined obj argument is required');

  my $num_avus      = scalar @avus;
  my $num_processed = 0;
  my $num_errors    = 0;

  try {
    foreach my $avu (@avus) {
      $num_processed++;
      $obj->add_avu($avu->{attribute}, $avu->{value}, $avu->{units});
    }
  } catch {
    $num_errors++;
    $self->error('Failed to add extra avus ', pp(\@avus), q[: ], $_);
  };

  return ($num_avus, $num_processed, $num_errors);
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::BatchPublisher

=head1 DESCRIPTION

A publisher which handles errors encountered when publishing many
files to iRODS. This class provides methods to help manage the errors
and permit retries.

An instance is capable of publishing a list of files ("a batch") per
call to 'publish_file_batch'. The instance keeps track of the success
or failure of publishing each file it processes. Files which have
published successfully in any previous batch are skipped (they are
not even checked against iRODS for checksum matches and correct
metadata) unless the force attribute is set true.

=head1 AUTHOR

Keith James E<lt>kdj@sanger.ac.ukE<gt>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2017 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
