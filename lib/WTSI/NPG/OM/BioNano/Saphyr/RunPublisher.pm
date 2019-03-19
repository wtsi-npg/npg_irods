package WTSI::NPG::OM::BioNano::Saphyr::RunPublisher;

use namespace::autoclean;

use Carp;
use Config::Auto;
use Data::Dump qw[pp];
use English qw[-no_match_vars];
use File::Basename;
use File::Path qw[make_path];
use File::Spec::Functions;
use File::Temp;
use Moose;
use Try::Tiny;

use WTSI::DNAP::Utilities::Params qw[function_params];

use WTSI::NPG::HTS::BatchPublisher;
use WTSI::NPG::OM::BioNano::Saphyr::DataObjectFactory;
use WTSI::NPG::OM::BioNano::Saphyr::SSHAccessClient;
use WTSI::NPG::iRODS;

use npg_tracking::util::config_constants qw[$NPG_CONF_DIR_NAME];

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::OM::BioNano::Saphyr::Annotator
      ];

our $VERSION = '';

# Default
our $DEFAULT_ROOT_COLL = '/seq/bionano/saphyr';

has 'access_client' =>
  (does          => 'WTSI::NPG::OM::BioNano::Saphyr::AccessClient',
   is            => 'ro',
   required      => 1,
   builder       => '_build_access_client',
   lazy          => 1,
   documentation => 'The Saphyr Access database query client');

has 'batch_publisher' =>
  (isa           => 'WTSI::NPG::HTS::BatchPublisher',
   is            => 'ro',
   required      => 1,
   lazy          => 1,
   builder       => '_build_batch_publisher',
   documentation => 'A publisher implementation capable to handling errors');

has config =>
  (isa           => 'HashRef',
   is            => 'ro',
   required      => 1,
   lazy          => 1,
   builder       => '_build_config',
   init_arg      => undef,
   documentation => 'The configuration loaded from config_file');

has config_file =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   lazy          => 1,
   builder       => '_build_config_file',
   documentation => 'The path of an INI format config file');

has 'dest_collection' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   lazy          => 1,
   builder       => '_build_dest_collection',
   documentation => 'The destination collection within iRODS to store data');

has 'force' =>
  (isa           => 'Bool',
   is            => 'ro',
   required      => 0,
   default       => 0,
   documentation => 'Force re-publication of files that have been published');

has 'irods' =>
   (isa           => 'WTSI::NPG::iRODS',
    is            => 'ro',
    required      => 1,
    documentation => 'The iRODS handle');

has 'obj_factory' =>
  (does          => 'WTSI::NPG::HTS::DataObjectFactory',
   is            => 'ro',
   required      => 1,
   lazy          => 1,
   builder       => '_build_obj_factory',
   documentation => 'A factory building data objects from files');

has 'tmpdir' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   default       => '/tmp',
   documentation => 'Temporary directory for staging files for publishing');

{
  my $positional = 1;
  my @named      = qw[begin_date end_date];
  my $params = function_params($positional, @named);

  sub publish_files {
    my ($self) = $params->parse(@_);

    my @results = $self->access_client->find_bnx_results
      (begin_date => $params->begin_date,
       end_date   => $params->end_date);

    my $tmpdir =
      File::Temp->newdir('SaphyrRunPublisher.' . $PID . '.XXXXXXXXX',
                         DIR => $self->tmpdir, CLEANUP => 1);

    my ($num_files, $num_processed, $num_errors) = (0, 0, 0);

    foreach my $result (@results) {
      my $bnx_file    = q[];
      my $json_file   = q[];
      my $remote_path = q[];

      try {
        my $local_dir = catdir($tmpdir, $result->chip_run_uid,
                               $result->job_id);
        make_path $local_dir;

        $bnx_file = $self->access_client->get_bnx_file($result->job_id,
                                                       $local_dir);
        $json_file = $self->_write_job_result($result, $local_dir);

        my ($filename, $directories, $suffix) = fileparse($bnx_file);
        my $dest_coll = catdir($self->dest_collection,
                               $result->chip_run_uid,
                               $result->chip_serialnumber,
                               $result->flowcell);

        my @primary_avus  = $self->make_primary_metadata($result);
        my @secondary_avus = $self->make_secondary_metadata($result);

        my @files = ($bnx_file, $json_file);
        $self->debug('Publishing files: ', pp(\@files));

        my ($nf, $np, $ne) =
          $self->_publish_files(\@files, $dest_coll,
                                \@primary_avus, \@secondary_avus);
        $num_files     += $nf;
        $num_processed += $np;
        $num_errors    += $ne;
      } catch {
        $num_errors++;
        $self->error("Failed to publish '$bnx_file' to '$remote_path' ",
                     "cleanly [$num_processed / $num_files]: ", $_);
      };
    }

    return ($num_files, $num_processed, $num_errors);
  }
}

sub _write_job_result {
  my ($self, $job_result, $local_directory) = @_;

  my $filename = sprintf q[%s.%s.%s.json],
    $job_result->chip_run_uid,
    $job_result->chip_serialnumber,
    $job_result->flowcell;

  my $path = catfile($local_directory, $filename);
  my $json = $job_result->json;

  $self->debug("Writing job JSON to '$path'");
  open my $out, '>', $path or
    croak "Failed to open '$path' for writing: $ERRNO";
  print $out $json or croak  "Failed to write to '$path': $ERRNO";
  close $out or croak "Failed to close to '$path': $ERRNO";

  return $path;
}

## no critic (Subroutines::ProhibitManyArgs)
sub _publish_files {
  my ($self, $files, $dest_coll, $primary_avus, $secondary_avus,
      $extra_avus) = @_;

  $primary_avus   ||= [];
  $secondary_avus ||= [];
  $extra_avus     ||= [];

  ref $primary_avus eq 'ARRAY' or
    $self->logconfess('The primary_avus argument must be an ArrayRef');
  ref $secondary_avus eq 'ARRAY' or
    $self->logconfess('The secondary_avus argument must be an ArrayRef');

  my $primary_avus_callback = sub {
    return @{$primary_avus};
  };

  my $secondary_avus_callback = sub {
    return @{$secondary_avus};
  };

  my $extra_avus_callback = sub {
    return @{$extra_avus};
  };

  return $self->batch_publisher->publish_file_batch
    ($files, $dest_coll,
     $primary_avus_callback,
     $secondary_avus_callback,
     $extra_avus_callback);
}
## use critic

sub _build_access_client {
  my ($self) = @_;

  my $config  = $self->config;
  my $user    = $config->{access}->{user};
  my $host    = $config->{access}->{host};
  my $dbname  = $config->{access}->{database_name};
  my $psql    = $config->{access}->{psql_executable};
  my $datadir = $config->{access}->{molecules_files_directory};

  return  WTSI::NPG::OM::BioNano::Saphyr::SSHAccessClient->new
    (user            => $user,
     host            => $host,
     database_name   => $dbname,
     psql_executable => $psql,
     data_directory  => $datadir);
}

sub _build_batch_publisher {
  my ($self) = @_;

  return WTSI::NPG::HTS::BatchPublisher->new
    (force                  => $self->force,
     irods                  => $self->irods,
     obj_factory            => $self->obj_factory,
     require_checksum_cache => []); # no md5s pre-created for Saphyr
}

sub _build_config {
  my ($self) = @_;

  return Config::Auto::parse($self->config_file, format => 'ini');
}

sub _build_config_file {
  my ($self) = @_;

  my $dir  = $ENV{'HOME'} || q[.];
  my $path = catdir($dir, $NPG_CONF_DIR_NAME);

  my $pkg_name = __PACKAGE__;
  $pkg_name =~ s/::/-/msxg;
  my $file = catfile($path, $pkg_name);
  $self->info("Using configuration file '$file'");

  return $file;
}

sub _build_dest_collection  {
  my ($self) = @_;

  return $DEFAULT_ROOT_COLL;
}

sub _build_obj_factory {
  my ($self) = @_;

  return WTSI::NPG::OM::BioNano::Saphyr::DataObjectFactory->new
    (irods => $self->irods);
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::OM::BioNano::Saphyr::RunPublisher

=head1 DESCRIPTION

Publishes one RawMoleculesBnx.gz per chip run, per flowcell to iRODS,
adds metadata and sets permissions.

A RunPublisher queries the remote database on the BioNano Access
platform to locate completed runs and their data files and metadata
(see WTSI::NPG::OM::BioNano::Saphyr::SSHAccessClient).

The RawMoleculesBnx.gz file for each flowcell is copied to the local
host and published to iRODS. In addition, a JSON file containing
information from the database query relevant to that run is published
alongside the RawMoleculesBnx.gz file.

The publisher requires configuration to connect to the remote
database. The configuration file is located in $NPG_CONF_DIR_NAME by
default (named "WTSI-NPG-OM-BioNano-Saphyr-RunPublisher" to be found
automatically) and is an INI file which must contain the following
records:

[access]
user=<username on remote host>
host=<resolvable hostname>
database_name=<BioNano Access database name>
psql_executable=<Full path to psql executable>
molecules_files_directory=<root directory of molecules files>

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
