package WTSI::NPG::HTS::PacBio::Sequel::RunPublisher;

use namespace::autoclean;
use Data::Dump qw[pp];
use English qw[-no_match_vars];
use File::Spec::Functions qw[catdir splitdir];
use Moose;
use MooseX::StrictConstructor;

use WTSI::NPG::HTS::PacBio::Sequel::MetaXMLParser;

extends qw{WTSI::NPG::HTS::PacBio::RunPublisher};

our $VERSION = '';

# Sequence and index file suffixes
our $SEQUENCE_FILE_FORMAT   = 'bam';
our $SEQUENCE_INDEX_FORMAT  = 'pbi';

# Sequence file types
our $SEQUENCE_TYPES         = '(subreads|scraps)';

# Generic file prefix
our $FILE_PREFIX_PATTERN    = 'm\d+_\d+_\d+';

# Well directory pattern
our $WELL_DIRECTORY_PATTERN = '\d+_[A-Z]\d+$';

override '_build_directory_pattern' => sub {
   my ($self) = @_;

   return $WELL_DIRECTORY_PATTERN;
};

=head2 publish_files

  Arg [1]    : smrt_names, ArrayRef[Str]. Optional.

  Example    : my ($num_files, $num_published, $num_errors) =
                 $pub->publish_files(['1_A01', '2_B01'])
  Description: Publish all files to iRODS. If the smrt_names argument is
               supplied, only those SMRT cells will be published. The default
               is to publish all SMRT cells. Return the number of files,
               the number published and the number of errors.
  Returntype : Array[Int]

=cut

override 'publish_files' => sub {
  my ($self, $smrt_names) = @_;

  if (!$smrt_names) {
    $smrt_names = [$self->smrt_names];
  }

  $self->info('Publishing files for SMRT cells: ', pp($smrt_names));

  my ($num_files, $num_processed, $num_errors) = (0, 0, 0);

  foreach my $smrt_name (@{$smrt_names}) {
    my $seq_files = $self->list_sequence_files($smrt_name);

    if (defined $seq_files->[0]) {
      my ($nfx, $npx, $nex) = $self->publish_xml_files($smrt_name);
      my ($nfb, $npb, $neb) = $self->publish_sequence_files($smrt_name);
      my ($nfp, $npp, $nep) = $self->publish_index_files($smrt_name);
      my ($nfa, $npa, $nea) = $self->publish_adapter_files($smrt_name);

      $num_files     += ($nfx + $nfb + $nfp + $nfa);
      $num_processed += ($npx + $npb + $npp + $npa);
      $num_errors    += ($nex + $neb + $nep + $nea);
    }
    else {
      $self->info("Skipping $smrt_name as no seq files found");
    }

    if ($num_errors > 0) {
      $self->error("Encountered errors on $num_errors / ",
                   "$num_processed files processed");
    }
  }

  return ($num_files, $num_processed, $num_errors);
};

=head2 publish_xml_files

  Arg [1]    : smrt_name,  Str.

  Example    : my ($num_files, $num_published, $num_errors) =
                 $pub->publish_xml_files()
  Description: Publish XML files for a SMRT cell to iRODS. Return
               the number of files, the number published and the number
               of errors.
  Returntype : Array[Int]

=cut

sub publish_xml_files {
  my ($self, $smrt_name) = @_;

  my $type = q[subreadset|sts];
  my $num  = scalar split m/[|]/msx, $type;

  my $files = $self->list_xml_files($smrt_name, $type, $num);
  my $dest_coll = catdir($self->dest_collection, $smrt_name);

  my ($num_files, $num_processed, $num_errors) =
    $self->_publish_files($files, $dest_coll);

  $self->info("Published $num_processed / $num_files metadata XML files ",
              "in SMRT cell '$smrt_name'");

  return ($num_files, $num_processed, $num_errors);
}

=head2 publish_sequence_files

  Arg [1]    : smrt_name,  Str.

  Example    : my ($num_files, $num_published, $num_errors) =
                 $pub->publish_sequence_files
  Description: Publish sequence files for a SMRT cell to iRODS. Return
               the number of files, the number published and the number
               of errors.
  Returntype : Array[Int]

=cut

sub publish_sequence_files {
  my ($self, $smrt_name) = @_;

  my $metadata_file = $self->list_xml_files($smrt_name, 'subreadset', '1')->[0];
  $self->debug("Reading metadata from '$metadata_file'");

  my $metadata =
    WTSI::NPG::HTS::PacBio::Sequel::MetaXMLParser->new->parse_file
      ($metadata_file);

  # There will be 1 record for a non-multiplexed SMRT cell and >1
  # record for a multiplexed (currently no uuids recorded in XML).
  my @run_records =
    $self->find_pacbio_runs($metadata->run_name, $metadata->well_name);

  # R & D runs have no records in the ML warehouse
  my $is_r_and_d = @run_records ? 0 : 1;

  if($is_r_and_d){
      $self->warn($metadata->run_name, " : publishing '$smrt_name' as R and D data");
  }

  my @primary_avus   = $self->make_primary_metadata($metadata, $is_r_and_d);
  my @secondary_avus = $self->make_secondary_metadata(@run_records);

  my $files     = $self->list_sequence_files($smrt_name);
  my $dest_coll = catdir($self->dest_collection, $smrt_name);

  my ($num_files, $num_processed, $num_errors) =
    $self->_publish_files($files, $dest_coll,
                          \@primary_avus, \@secondary_avus);

  $self->info("Published $num_processed / $num_files sequence files ",
              "in SMRT cell '$smrt_name'");

  return ($num_files, $num_processed, $num_errors);
}

=head2 publish_index_files

  Arg [1]    : smrt_name,  Str.

  Example    : my ($num_files, $num_published, $num_errors) =
                 $pub->publish_index_files
  Description: Publish index files for a SMRT cell to iRODS. Return
               the number of files, the number published and the number
               of errors.
  Returntype : Array[Int]

=cut

sub publish_index_files {
  my ($self, $smrt_name) = @_;

  my $files = $self->list_index_files($smrt_name);
  my $dest_coll = catdir($self->dest_collection, $smrt_name);

  my ($num_files, $num_processed, $num_errors) =
    $self->_publish_files($files, $dest_coll);

  $self->info("Published $num_processed / $num_files index files ",
              "in SMRT cell '$smrt_name'");

  return ($num_files, $num_processed, $num_errors);
}

=head2 publish_adapter_files

  Arg [1]    : smrt_name,  Str.

  Example    : my ($num_files, $num_published, $num_errors) =
                 $pub->publish_index_files
  Description: Publish adapter files for a SMRT cell to iRODS. Return
               the number of files, the number published and the number
               of errors.
  Returntype : Array[Int]

=cut

sub publish_adapter_files {
  my ($self, $smrt_name) = @_;

  my $files = $self->list_adapter_files($smrt_name);
  my $dest_coll = catdir($self->dest_collection, $smrt_name);

  my ($num_files, $num_processed, $num_errors) =
    $self->_publish_files($files, $dest_coll);

  $self->info("Published $num_processed / $num_files index files ",
              "in SMRT cell '$smrt_name'");

  return ($num_files, $num_processed, $num_errors);
}




=head2 list_sequence_files

  Arg [1]    : SMRT cell name, Str.

  Example    : $pub->list_sequence_files('1_A01')
  Description: Return paths of all sequence files for the given SMRT cell.
               Calling this method will access the file system.
  Returntype : ArrayRef[Str]

=cut

sub list_sequence_files {
  my ($self, $smrt_name) = @_;

  my $name = $self->_check_smrt_name($smrt_name);

  my $file_pattern = $FILE_PREFIX_PATTERN .q{[.]}. $SEQUENCE_TYPES .q{[.]}.
        $SEQUENCE_FILE_FORMAT .q{$};

  return [$self->list_directory($self->smrt_path($name), $file_pattern)];
}

=head2 list_index_files

  Arg [1]    : SMRT cell name, Str.

  Example    : $pub->list_index_files('1_A01')
  Description: Return paths of all index files for the given SMRT cell.
               Calling this method will access the file system.
  Returntype : ArrayRef[Str]

=cut

sub list_index_files {
  my ($self, $smrt_name) = @_;

  my $name = $self->_check_smrt_name($smrt_name);

  my $file_pattern = $FILE_PREFIX_PATTERN .q{[.]}. $SEQUENCE_TYPES. q{[.]}.
        $SEQUENCE_FILE_FORMAT .q{[.]}. $SEQUENCE_INDEX_FORMAT .q{$};

  return [$self->list_directory($self->smrt_path($name), $file_pattern)];
}



=head2 list_xml_files

  Arg [1]    : SMRT cell name, Str.
  Arg [2]    : Types.
  Arg [3]    : Number of files expected.

  Example    : $pub->list_xml_files('1_A01')
  Description: Return the path of the metadata XML files for the given SMRT
               cell and type.  Calling this method will access the file system.
  Returntype : ArrayRef[Str]

=cut

sub list_xml_files {
  my ($self, $smrt_name, $type, $expect) = @_;

  my $name = $self->_check_smrt_name($smrt_name);

  defined $type or
    $self->logconfess('A defined file type argument is required');
  defined $expect or
    $self->logconfess('A defined expected file count argument is required');

  my $file_pattern = $FILE_PREFIX_PATTERN .'[.]'. '(' . $type .')[.]xml$';

  my @files = $self->list_directory($self->smrt_path($name), $file_pattern);

  my $num_files = scalar @files;
  if ($num_files != $expect) {
    $self->logconfess("Expected $expect but found $num_files XML ",
                      "metadata files for SMRT cell '$smrt_name': ",
                      pp(\@files));
  }

  return \@files;
}

=head2 list_adapter_files

  Arg [1]    : SMRT cell name, Str.

  Example    : $pub->list_adapter_files('1_A01')
  Description: Return the path of the adapter files for the given SMRT
               cell. Calling this method will access the file system.
  Returntype : ArrayRef[Str]

=cut

sub list_adapter_files {
  my ($self, $smrt_name) = @_;

  my $name = $self->_check_smrt_name($smrt_name);

  my $file_pattern = $FILE_PREFIX_PATTERN .'[.]adapters[.]fasta$';

  return [$self->list_directory($self->smrt_path($name), $file_pattern)];
}


__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::Sequel::RunPublisher

=head1 DESCRIPTION

Publishes relevant files to iRODS, adds metadata and sets permissions.

An instance of RunPublisher is responsible for copying PacBio sequencing
data from the instrument run folder to a collection in iRODS for a
single, specific run.

Data files are divided into a number of categories:

 - sequence files; sequence files for sequence data
 - index files; index files for sequence data
 - XML files; stats and subset xml
 - adapter fasta file

A RunPublisher provides methods to list the complement of these
categories and to copy ("publish") them. Each of these list or publish
operations may be restricted to a specific SMRT cell.

As part of the copying process, metadata are added to, or updated on,
the files in iRODS. Following the metadata update, access permissions
are set. The information to do both of these operations is provided by
an instance of WTSI::DNAP::Warehouse::Schema.

If a run is published multiple times to the same destination
collection, the following take place:

 - the RunPublisher checks local (run folder) file checksums against
   remote (iRODS) checksums and will not make unnecessary updates

 - if a local file has changed, the copy in iRODS will be overwritten
   and additional metadata indicating the time of the update will be
   added

 - the RunPublisher will proceed to make metadata and permissions
   changes to synchronise with the metadata supplied by
   WTSI::DNAP::Warehouse::Schema, even if no files have been modified

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
