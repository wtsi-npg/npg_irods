package WTSI::NPG::HTS::PacBio::RunPublisher;

use namespace::autoclean;
use Data::Dump qw[pp];
use English qw[-no_match_vars];
use File::Basename;
use File::Spec::Functions qw[catdir catfile splitdir];
use List::AllUtils qw[any first];
use Moose;
use MooseX::StrictConstructor;
use Try::Tiny;

use WTSI::DNAP::Utilities::Params qw[function_params];
use WTSI::NPG::HTS::DataObject;
use WTSI::NPG::HTS::PacBio::MetaXMLParser;
use WTSI::NPG::HTS::Publisher;
use WTSI::NPG::iRODS::Metadata;
use WTSI::NPG::iRODS;

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::HTS::PathLister
         WTSI::NPG::HTS::PacBio::Annotator
       ];

our $VERSION = '';

# Default
our $DEFAULT_ROOT_COLL    = '/seq/pacbio';

# The default SMRT analysis results directory name
our $ANALYSIS_DIR = 'Analysis_Results';

has 'irods' =>
  (isa           => 'WTSI::NPG::iRODS',
   is            => 'ro',
   required      => 1,
   documentation => 'An iRODS handle to run searches and perform updates');

has 'runfolder_path' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'PacBio runfolder path');

has 'dest_collection' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   lazy          => 1,
   builder       => '_build_dest_collection',
   documentation => 'The destination collection within iRODS to store data');

has 'mlwh_schema' =>
  (is            => 'ro',
   isa           => 'WTSI::DNAP::Warehouse::Schema',
   required      => 1,
   documentation => 'A ML warehouse handle to obtain secondary metadata');

sub BUILD {
  my ($self) = @_;

  # TODO -- Use the following to set up per-class loggers
  # $self->logger(Log::Log4perl->get_logger($self->meta->name));

  # Use our logger to log activity.
  $self->irods->logger($self->logger);

  return;
}

sub run_name {
  my ($self) = @_;

  return first { $_ ne q[] } reverse splitdir($self->runfolder_path);
}

=head2 smrt_path

  Arg [1]    : None

  Example    : my @names = $pub->smrt_names;
  Description: Return the SMRT cell names within a run, sorted lexically.
  Returntype : Array[Str]

=cut

sub smrt_names {
  my ($self) = @_;

  my $dir_pattern = '\d+_\d+$';
  my @dirs = grep { -d } $self->list_directory($self->runfolder_path,
                                               $dir_pattern);

  my @names = sort map { first { $_ ne q[] } reverse splitdir($_) } @dirs;

  return @names;
}

=head2 smrt_look_indices

  Arg [1]    : SMRT cell name, Str.

  Example    : my @indices = $pub->smrt_look_indices('A01_1');
  Description: Return look indices given a cell name.
  Returntype : Array[Int]

=cut

sub smrt_look_indices {
  my ($self, $smrt_name) = @_;

  my $name = $self->_check_smrt_name($smrt_name);

  my $file_pattern = '_p\d+[.]metadata[.]xml$';
  my @files = $self->list_directory($self->smrt_path($name), $file_pattern);

  my @look_indices;
  foreach my $file (@files) {
    if ($file =~ m{_s(\d+)_p\d+[.]metadata[.]xml$}msx) {
      push @look_indices, $1;
    }
  }

  return @look_indices;
}

=head2 smrt_path

  Arg [1]    : SMRT cell name, Str.

  Example    : my $path = $pub->smrt_path('A01_1');
  Description: Return the path to SMRT cell data within a run, given
               the cell name.
  Returntype : Str

=cut

sub smrt_path {
  my ($self, $smrt_name) = @_;

  my $name = $self->_check_smrt_name($smrt_name);
  return catdir($self->runfolder_path, $name);
}

=head2 smrt_analysis_path

  Arg [1]    : SMRT cell name, Str.

  Example    : my $path = $pub->smrt_analysis_path('A01_1');
  Description: Return the path to SMRT cell analysis results within a run,
               given the cell name.
  Returntype : Str

=cut

sub smrt_analysis_path {
  my ($self, $smrt_name) = @_;

  my $name = $self->_check_smrt_name($smrt_name);
  return catdir($self->smrt_path($name), $ANALYSIS_DIR);
}

=head2 list_basx_files

  Arg [1]    : SMRT cell name, Str.
  Arg [2]    : Look index, Int. Optional.

  Example    : $pub->list_basx_files('A01_1')
  Description: Return paths of all ba[s|x] files for the given SMRT cell.
               Calling this method will access the file system.
  Returntype : ArrayRef[Str]

=cut

sub list_basx_files {
  my ($self, $smrt_name, $look_index) = @_;

  my $name = $self->_check_smrt_name($smrt_name);
  my $look_pattern = q[];
  if (defined $look_index) {
    $look_pattern = sprintf '_s%s', $look_index;
  }

  my $file_pattern = sprintf '%s_p\d+([.]\d+)?[.]ba[sx][.]h5$', $look_pattern;

  return [$self->list_directory($self->smrt_analysis_path($name),
                                $file_pattern)];
}

=head2 list_sts_xml_files

  Arg [1]    : SMRT cell name, Str.
  Arg [2]    : Look index, Int. Optional.

  Example    : $pub->list_sts_xml_files('A01_1')
  Description: Return paths of all sts XML files for the given SMRT cell.
               Calling this method will access the file system.
  Returntype : ArrayRef[Str]

=cut

sub list_sts_xml_files {
  my ($self, $smrt_name, $look_index) = @_;

  my $name = $self->_check_smrt_name($smrt_name);
  my $look_pattern = q[];
  if (defined $look_index) {
    $look_pattern = sprintf '_s%s', $look_index;
  }

  my $file_pattern = sprintf '%s_p\d+[.]sts[.]xml$', $look_pattern;

  return [$self->list_directory($self->smrt_analysis_path($name),
                                $file_pattern)];
}

=head2 list_meta_xml_file

  Arg [1]    : SMRT cell name, Str.
  Arg [2]    : Look index, Int. Optional.

  Example    : $pub->list_smrt_xml_file('A01_1')
  Description: Return the path of the metadata XML file for the given SMRT
               cell.  Calling this method will access the file system.
  Returntype : Str

=cut

sub list_meta_xml_file {
  my ($self, $smrt_name, $look_index) = @_;

  my $name = $self->_check_smrt_name($smrt_name);
  my $look_pattern = q[];
  if (defined $look_index) {
    $look_pattern = sprintf '_s%s', $look_index;
  }

  my $file_pattern = sprintf '%s_p\d+[.]metadata[.]xml$', $look_pattern;

  my @files = $self->list_directory($self->smrt_path($name), $file_pattern);

  my $num_files = scalar @files;
  if ($num_files == 0) {
    $self->logconfess('Failed to find an XML metadata file for ',
                      "SMRT cell '$smrt_name'");
  }
  if ($num_files > 1) {
    $self->logconfess("Found $num_files XML metadata files for ",
                      "SMRT cell '$smrt_name': ", pp(\@files));
  }

  return shift @files;
}

=head2 publish_files

  Arg [1]    : None

  Named args : smrt_names           ArrayRef[Str]. Optional.
               look_index           Int. Optional

  Example    : my ($num_files, $num_published, $num_errors) =
                 $pub->publish_files(smrt_names => ['A01_1', 'B01_1'],
                                     look_index => 1)
  Description: Publish all files to iRODS. If the smart_names argument is
               supplied, only those SMRT cells will be published. The default
               is to publish all SMRT cells. Return the number of files,
               the number published and the number of errors.
  Returntype : Array[Int]

=cut

{
  my $positional = 1;
  my @named      = qw[smrt_names look_index];
  my $params = function_params($positional, @named);

  sub publish_files {
    my ($self) = $params->parse(@_);

    my $smrt_names = $params->smrt_names || [$self->smrt_names];
    $self->info('Publishing files for SMRT cells: ', pp($smrt_names));

    my ($num_files, $num_processed, $num_errors) = (0, 0, 0);

    foreach my $smrt_name (@{$smrt_names}) {

      # There are no longer >1 look indices on runs. This code is to
      # support re-loading of any old data.
      my @look_indices = $self->smrt_look_indices($smrt_name);
      my $num_looks = scalar @look_indices;
      my $look_index = $params->look_index;

      if ($num_looks > 1) {
        if (not $params->look_index) {
          $self->logcroak("There are $num_looks look indices for SMRT cell ",
                          "'$smrt_name'; please select one to publish: ",
                          pp(\@look_indices));
        }
      }
      else {
        $look_index = $look_indices[0];
        $self->info("There is one look index '$look_index' ",
                    "for SMRT cell '$smrt_name'");
      }

      my ($nfx, $npx, $nex) =
        $self->publish_meta_xml_file($smrt_name, $look_index);
      my ($nfb, $npb, $neb) =
        $self->publish_basx_files($smrt_name, $look_index);
      my ($nfs, $nps, $nes) =
        $self->publish_sts_xml_files($smrt_name, $look_index);

       $num_files     += ($nfx + $nfb + $nfs);
       $num_processed += ($npx + $npb + $nps);
       $num_errors    += ($nex + $neb + $nes);
    }

    return ($num_files, $num_processed, $num_errors);
  }
}

=head2 publish_meta_xml_file

  Arg [1]    : None

  Named args : smrt_name            Str.
               look_index           Int. Optional if there is one look index,
                                    required if there is more than one.

  Example    : my ($num_files, $num_published, $num_errors) =
                 $pub->publish_meta_xml_file
  Description: Publish metadata XML file for a SMRT cell to iRODS. Return
               the number of files, the number published and the number
               of errors.
  Returntype : Array[Int]

=cut

sub publish_meta_xml_file {
  my ($self, $smrt_name, $look_index) = @_;

  my $files = [$self->list_meta_xml_file($smrt_name, $look_index)];
  my $dest_coll = catdir($self->dest_collection, $smrt_name);

  return $self->_publish_files($files, $dest_coll);
}

=head2 publish_basx_files

  Arg [1]    : None

  Named args : smrt_name            Str.
               look_index           Int. Optional if there is one look index,
                                    required if there is more than one.

  Example    : my ($num_files, $num_published, $num_errors) =
                 $pub->publish_basx_files
  Description: Publish bas and bax files for a SMRT cell to iRODS. Return
               the number of files, the number published and the number
               of errors.
  Returntype : Array[Int]

=cut

sub publish_basx_files {
  my ($self, $smrt_name, $look_index) = @_;

  my $metadata_file = $self->list_meta_xml_file($smrt_name, $look_index);
  $self->debug("Reading metadata from '$metadata_file'");

  my $metadata =
    WTSI::NPG::HTS::PacBio::MetaXMLParser->new->parse_file($metadata_file);
  my @run_records = $self->_query_ml_warehouse($metadata->run_uuid,
                                               $metadata->library_tube_uuids);

  my @primary_avus   = $self->make_primary_metadata($metadata);
  my @secondary_avus = $self->make_secondary_metadata($metadata, @run_records);

  my $files     = $self->list_basx_files($smrt_name, $look_index);
  my $dest_coll = catdir($self->dest_collection, $smrt_name, $ANALYSIS_DIR);

  return $self->_publish_files($files, $dest_coll,
                               \@primary_avus, \@secondary_avus);
}

=head2 publish_sts_xml_files

  Arg [1]    : None

  Named args : smrt_name            Str.
               look_index           Int. Optional if there is one look index,
                                    required if there is more than one.

  Example    : my ($num_files, $num_published, $num_errors) =
                 $pub->publish_sts_xml_files
  Description: Publish sts XML files for a SMRT cell to iRODS. Return
               the number of files, the number published and the number
               of errors.
  Returntype : Array[Int]

=cut

sub publish_sts_xml_files {
  my ($self, $smrt_name, $look_index) = @_;

  my $files = $self->list_sts_xml_files($smrt_name, $look_index);
  my $dest_coll = catdir($self->dest_collection, $smrt_name, $ANALYSIS_DIR);

  return $self->_publish_files($files, $dest_coll);
}

sub _publish_files {
  my ($self, $files, $dest_coll, $primary_avus, $secondary_avus) = @_;

  defined $files or
    $self->logconfess('A defined files argument is required');
  ref $files eq 'ARRAY' or
    $self->logconfess('The files argument must be an ArrayRef');
  defined $dest_coll or
    $self->logconfess('A defined dest_coll argument is required');

  $primary_avus   ||= [];
  $secondary_avus ||= [];

  ref $primary_avus eq 'ARRAY' or
    $self->logconfess('The primary_avus argument must be an ArrayRef');
  ref $secondary_avus eq 'ARRAY' or
    $self->logconfess('The secondary_avus argument must be an ArrayRef');

  my $publisher = WTSI::NPG::HTS::Publisher->new(irods  => $self->irods,
                                                 logger => $self->logger);

  my $num_files     = scalar @{$files};
  my $num_processed = 0;
  my $num_errors    = 0;

  foreach my $file (@{$files}) {
    my $dest = q[];

    try {
      $num_processed++;

      my ($filename, $directories, $suffix) = fileparse($file);
      my $obj = WTSI::NPG::HTS::DataObject->new
        ($self->irods, catfile($dest_coll, $filename));
      $dest = $obj->str;
      $dest = $publisher->publish($file, $dest);

       my ($num_pattr, $num_pproc, $num_perr) =
         $obj->set_primary_metadata(@{$primary_avus});
       my ($num_sattr, $num_sproc, $num_serr) =
         $obj->update_secondary_metadata(@{$secondary_avus});

      if ($num_perr > 0) {
        $self->logcroak("Failed to set primary metadata cleanly on '$dest'");
      }
      if ($num_serr > 0) {
        $self->logcroak("Failed to set secondary metadata cleanly on '$dest'");
      }
    } catch {
      $num_errors++;
      my @stack = split /\n/msx;   # Chop up the stack trace
      $self->error("Failed to publish '$file' to '$dest' cleanly ",
                   "[$num_processed / $num_files]: ", pop @stack);
    };
  }

  if ($num_errors > 0) {
    $self->error("Encountered errors on $num_errors / ",
                 "$num_processed files processed");
  }

  return ($num_files, $num_processed, $num_errors);
}

sub _build_dest_collection  {
  my ($self) = @_;

  return catdir($DEFAULT_ROOT_COLL, $self->run_name);
}

# Check that a SMRT cell name argument is given and valid
sub _check_smrt_name {
  my ($self, $smrt_name) = @_;

  defined $smrt_name or
    $self->logconfess('A defined smart_name argument is required');
  any { $smrt_name eq $_ } $self->smrt_names or
    $self->logconfess("Invalid smrt_name argument '$smrt_name'");

  return $smrt_name;
}

# Look up PacBio run metadata in the ML warehouse using the library
# tube UUID obtained from the run XML metadata.
sub _query_ml_warehouse {
  my ($self, $run_uuid, $library_tube_uuids) = @_;

  my @run_records = $self->mlwh_schema->resultset('PacBioRun')->search
    ({pac_bio_run_uuid          => $run_uuid,
      pac_bio_library_tube_uuid => {'-in' => $library_tube_uuids}},
     {prefetch                  => ['sample', 'study']});

  return @run_records;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::RunPublisher

=head1 DESCRIPTION


=head1 AUTHOR

Guoying Qi E<lt>gq1@sanger.ac.ukE<gt>
Keith James E<lt>kdj@sanger.ac.ukE<gt>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2011, 2016 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
