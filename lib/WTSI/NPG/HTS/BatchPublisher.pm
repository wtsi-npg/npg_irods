package WTSI::NPG::HTS::BatchPublisher;

use namespace::autoclean;

use Data::Dump qw[pp];
use File::Basename;
use File::Spec::Functions qw[catfile];
use List::MoreUtils qw[any];
use Moose;
use MooseX::StrictConstructor;
use Try::Tiny;

use WTSI::DNAP::Utilities::Params qw[function_params];
use WTSI::NPG::HTS::DefaultDataObjectFactory;
use WTSI::NPG::HTS::PublishState;
use WTSI::NPG::iRODS::Publisher;

with qw[
         WTSI::DNAP::Utilities::Loggable
       ];

our $VERSION = '';

has 'irods' =>
  (isa           => 'WTSI::NPG::iRODS',
   is            => 'ro',
   required      => 1,
   documentation => 'An iRODS handle to run searches and perform updates');

has 'publish_state' =>
  (isa           => 'WTSI::NPG::HTS::PublishState',
   is            => 'ro',
   required      => 1,
   builder       => '_build_publish_state',
   lazy          => 1,
   documentation => 'A map of file path to a boolean value, which ' .
                    'is set true if the file was published');

has 'mlwh_locations' =>
  (isa           =>'WTSI::NPG::HTS::LocationWriter',
   is            =>'ro',
   required      => 0,
   documentation => 'An object used to build and write information to be ' .
                    'loaded into the seq_product_irods_locations table.');

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
   documentation => 'The maximum number of errors permitted per call to ' .
                    'publish_file_batch before the remainder of its ' .
                    'operation is aborted');

has 'state_file' =>
  (isa           => 'Str',
   is            => 'rw',
   required      => 0,
   predicate     => 'has_state_file',
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

  Named args : primary_cb
               Callback returning primary AVUs for a data object. CodeRef.

               secondary_cb
               Callback returning secondary AVUs for a data object. CodeRef.
               Optional.

               extra_cb
               Callback returning extra AVUs for a data object. CodeRef,
               Optional.

  Example    : $pub->publish_file_batch(['x.txt', 'y.txt'],
                                        '/destination/collection',
                                        primary_cb   => sub { ... },
                                        secondary_cb => sub { ... });

  Description: Publish a list of files to iRODS, using callbacks to
               caculate the metadata to be applied to each file. Return the
               number of files, the number published and the number of
               errors.
  Returntype : Array[Int]

=cut


## no critic (Subroutines::ProhibitExcessComplexity)
{
  my $positional = 3;
  my @named      = qw[primary_cb secondary_cb extra_cb];
  my $params     = function_params($positional, @named);

  sub publish_file_batch {
    my ($self, $files, $dest_coll) = $params->parse(@_);

    defined $files or
        $self->logconfess('A defined files argument is required');
    ref $files eq 'ARRAY' or
        $self->logconfess('The files argument must be an ArrayRef');
    defined $dest_coll or
        $self->logconfess('A defined dest_coll argument is required');

    my $primary_cb = sub {return ()};
    if (defined $params->primary_cb) {
      ref $params->primary_cb eq 'CODE' or
          $self->logconfess('The primary_cb argument must be a CodeRef');
      $primary_cb = $params->primary_cb;
    }

    my $secondary_cb = sub {return ()};
    if (defined $params->secondary_cb) {
      ref $params->secondary_cb eq 'CODE' or
          $self->logconfess('The secondary_cb argument must be a CodeRef');
      $secondary_cb = $params->secondary_cb;
    }

    my $extra_cb = sub {return ()};
    if (defined $params->extra_cb) {
      ref $params->extra_cb eq 'CODE' or
          $self->logconfess('The extra_cb argument must be a CodeRef');
      $extra_cb = $params->extra_cb;
    }

    my $publisher =
        WTSI::NPG::iRODS::Publisher->new
            (irods                  => $self->irods,
             require_checksum_cache => $self->require_checksum_cache);

    my $num_files     = scalar @{$files};
    my $num_processed = 0;
    my $num_errors    = 0;

    $self->debug("Publishing a batch of $num_files files: ", pp($files));

    FILE:
    foreach my $file (@{$files}) {
      my $remote_path = q[];

      if ($self->has_max_errors and $num_errors >= $self->max_errors) {
        $self->error("The number of errors $num_errors reached the maximum ",
                     'permitted of ', $self->max_errors, '. Aborting');
        last FILE;
      }

      $self->debug("Publishing '$file', a member of a batch of $num_files");

      if ($self->publish_state->is_published($file)) {
        if ($self->force) {
          $self->info("Forcing re-publication of local file '$file'");
        }
        else {
          $self->info("Skipping local file '$file'; already published");
          next FILE;
        }
      }

      try {
        $num_processed++;
        my ($filename, $directories, $suffix) = fileparse($file);
        $remote_path                          = catfile($dest_coll, $filename);

        my $obj = $self->obj_factory->make_data_object($remote_path);
        if (not $obj) {
          $self->logconfess("Failed to make an object from '$remote_path'");
        }

        $self->debug("Publishing '$file' to '$remote_path'");
        my $dest = $publisher->publish($file, $remote_path)->str;

        my @primary_avus                       = $primary_cb->($obj);
        my ($num_pattr, $num_pproc, $num_perr) =
            $obj->set_primary_metadata(@primary_avus);

        my @secondary_avus                     = $secondary_cb->($obj);
        my ($num_sattr, $num_sproc, $num_serr) =
            $obj->update_secondary_metadata(@secondary_avus);

        my @extra_avus                         = $extra_cb->($obj);
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

        $self->publish_state->set_published($file);
        $self->info("Published '$dest' [$num_processed / $num_files]");

        if ($self->mlwh_locations){
          my $target;
          my $pid;
          # The simplest way to obtain product ids and to discover whether an
          # object is the target of a run/analysis is to search its metadata.
          for my $avu (@primary_avus){
            if ($avu->{attribute} eq 'target'){
              $target = $avu->{value};
            }elsif ($avu->{attribute} eq 'id_product') {
              $pid = $avu->{value};
            }
            if (defined($target) && $pid){
              last;
            }
          }
          my ($suffix) = $filename =~ m{[.]([^.]+)$}msx;
          if ($target && any { $suffix eq $_ } qw/bam cram/) {
            $self->mlwh_locations->add_location(
              pid  => $pid,
              coll => $dest_coll,
              path => $filename
            );
          }
        }
      }
      catch {
        $num_errors++;
        $self->error("Failed to publish '$file' to '$remote_path' cleanly ",
                     "[$num_processed / $num_files]: ", $_);
      };
    }

    if ($num_errors > 0) {
      $self->error("Encountered errors on $num_errors / ",
                   "$num_processed files processed");
    }

    return ($num_files, $num_processed, $num_errors);
  }
}
## use critic

sub read_state {
  my ($self) = @_;

  $self->has_state_file or
    $self->logconfess('Failed to read state from file: no state file defined');

  return $self->publish_state->read_state($self->state_file);
}

sub write_state {
  my ($self) = @_;

  $self->has_state_file or
    $self->logconfess('Failed to write state to file: no state file defined');

  return $self->publish_state->write_state($self->state_file);
}

sub _build_publish_state {
  my ($self) = @_;

  return WTSI::NPG::HTS::PublishState->new;
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

Copyright (C) 2017, 2018, 2021 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
