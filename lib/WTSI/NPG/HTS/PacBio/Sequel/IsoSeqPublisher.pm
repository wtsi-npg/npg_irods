package WTSI::NPG::HTS::PacBio::Sequel::IsoSeqPublisher;

use namespace::autoclean;
use DateTime;
use English qw[-no_match_vars];
use File::Basename;
use File::Spec::Functions qw[catdir];
use File::Temp qw[tempdir];
use IO::File;
use JSON;
use Moose;
use MooseX::StrictConstructor;
use Perl6::Slurp;
use Readonly;
use Try::Tiny;
use XML::LibXML;

use WTSI::NPG::HTS::PacBio::Sequel::MetaXMLParser;
use WTSI::NPG::HTS::PacBio::Sequel::Product;
use WTSI::DNAP::Utilities::Runnable;

extends qw{WTSI::NPG::HTS::PacBio::RunPublisher};

our $VERSION = '';

# Sequence and index file suffixes
our $SEQUENCE_FILE_FORMAT    = 'bam';
our $SEQUENCE_FASTA_FORMAT   = 'fasta';
our $SEQUENCE_FASTAGZ_FORMAT = 'fasta.gz';
our $SEQUENCE_INDEX_FORMAT   = 'pbi';

# Data processing level
our $DATA_LEVEL = 'secondary';

# Metadata relatedist
our $METADATA_FORMAT = 'xml';
our $METADATA_PREFIX = 'pbmeta:';
our $METADATA_SET    = q{consensusreadset};

# Well directory pattern
our $WELL_DIRECTORY_PATTERN = '\d+_[A-Z]\d+$';

# Not permitted name signalling processing with 
# annotation which is not handled yet
Readonly::Scalar my $FNAME_NOT_PERMITTED  => qw[classification];

Readonly::Scalar my $FNAME_SEQUENCE     =>
  qq{(flnc\.*$SEQUENCE_FILE_FORMAT|mapped\.*$SEQUENCE_FILE_FORMAT|$SEQUENCE_FASTA_FORMAT)};
Readonly::Scalar my $FNAME_EXCLUDED     => qw[segmented];

Readonly::Scalar my $SAMPLE_FIELD  => q{Bio Sample Name};
Readonly::Scalar my $SAMPLE_PREFIX => q{BioSample};
Readonly::Scalar my $PRIMER_FIELD  => q{Primer Name};
Readonly::Scalar my $PRIMERS_JSON  => q{isoseq_primers.report.json};
Readonly::Scalar my $LOADED        => q{loaded.txt};

has 'analysis_id' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'PacBio analysis id');

has 'analysis_subtype' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'PacBio analysis subtype');

has 'analysis_path' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'PacBio root analysis job path');


# Location of source metadata file
our $OUTPUT_DIR  = 'outputs';


=head2 publish_files

  Example    : my ($num_files, $num_published, $num_errors) =
                 $pub->publish_files()
  Description: Publish all files for an analysis jobs to iRODS. Return
               the number of files, the number published and the number
               of errors.
  Returntype : Array[Int]

=cut

## - publish to separate sub-directory - /seq/pacbio/runfolder/well/analysis_id/ 
## - copy files to tmp dir prior to publishing and rename as analysis_id.moviename.filename

sub publish_files {
  my ($self) = @_;

  my ($num_files, $num_processed, $num_errors) = (0, 0, 0);

  ## need to get barcode from meta data if it exists
  my $barcode = $self->_determine_barcode();
  if (! $barcode ) { $self->logcroak('Not currently supporting processing without adapter barcode'); }

  ## check if directory previously loaded and if so skip
  my $lf = catdir
    ($self->runfolder_path, $self->analysis_id .q[.]. $self->_metadata->movie_name .q[.]. $LOADED);


  if (! -f $lf ) {
    my @all_files = $self->list_directory($self->runfolder_path);

    my @exists = grep { m{ $FNAME_NOT_PERMITTED }smx } @all_files;
    if (@exists) {
      $self->logcroak('Not currently permitted file names found - aborting : '. @exists);
    }

    ## remove files we don't want to publish and split list into sequence
    ## files and non sequence files
    my @seq_files = $self->list_directory
      ($self->runfolder_path, filter => $FNAME_SEQUENCE . q[$]);

    my @not_excluded_files = grep { ! m{ $FNAME_EXCLUDED }smx } @all_files;

    my %seq;
    @seq { @seq_files } = ();
    my @nonseq_files = grep { ! exists $seq{$_} } @not_excluded_files;

    $self->warn('Publishing '. $self->analysis_id .' to '. $self->_dest_path  ."\n");

    ## copy files to tmp directory & prefix file names with movie_name and analysis id
    my $tmpdir = tempdir(CLEANUP => 1);
    my ($copied_seq_files)    = $self->_create_loadable_files($tmpdir, \@seq_files);
    my ($copied_nonseq_files) = $self->_create_loadable_files($tmpdir, \@nonseq_files);

    my ($nfb, $npb, $neb) = $self->publish_sequence_files($copied_seq_files,$barcode);
    my ($nfp, $npp, $nep) = $self->publish_non_sequence_files($copied_nonseq_files);

    $num_files     += ($nfb + $nfp);
    $num_processed += ($npb + $npp);
    $num_errors    += ($neb + $nep);

    ## if no error mark directory so don't try to load again
    if ( $num_processed > 1 && ($num_files == $num_processed) && $num_errors < 1) {
      try {
        my $done = IO::File->new($lf, q[+>]) or $self->logcroak('Could not open: ', $lf);
        $done->write('Loaded on '. DateTime->now ."\n");
        $done->close or $self->logcroak('Cannot close file: ', $lf);
      } catch {
        my @stack = split /\n/msx;   # Chop up the stack trace
        $self->logcroak(pop @stack); # Use a shortened error message
      };
    }
  } else {
    $self->warn('Skipping publishing '. $self->analysis_id ." as previously loaded\n");
  }

  return ($num_files, $num_processed, $num_errors);
}


sub _create_loadable_files {
  my ($self, $tmpdir, $files) = @_;

  my (@cmds, @newfiles);
  foreach my $file (@{$files}) {
    my $filename = fileparse($file);
    my $newfile  = catdir
      ($tmpdir, $self->analysis_id .q[.]. $self->_metadata->movie_name .q[.]. $filename);
    push @cmds, qq[rsync -av -L $file $newfile];
    if ( $newfile =~ m/ [.] $SEQUENCE_FASTA_FORMAT /smx ) {
      push @cmds, qq[gzip $newfile];
      $newfile =~ s/$SEQUENCE_FASTA_FORMAT/$SEQUENCE_FASTAGZ_FORMAT/smx;
    }
    push @newfiles, $newfile;
  }

  my $cmds_to_run = join q[ && ], @cmds;
  my $cmd = qq[set -o pipefail && mkdir -p $tmpdir && ($cmds_to_run)];

  try {
    WTSI::DNAP::Utilities::Runnable->new(executable => '/bin/bash',
                                         arguments  => ['-c', $cmd])->run;
  } catch {
    my @stack = split /\n/msx;   # Chop up the stack trace
    $self->logcroak(pop @stack); # Use a shortened error message
  };

  return (\@newfiles);
}


=head2 publish_sequence_files

  Arg [1]    : Files. Required. Array ref.
  Arg [2]    : Tag name. Required. Str.
 
  Example    : my ($num_files, $num_published, $num_errors) =
                 $pub->publish_sequence_files($files, $tag_name)
  Description: Identify sequence files which match the required file 
               format regex. and publish those files to iRODS. Return 
               the number of files, the number published and the number 
               of errors. R&D data not supported - only files with 
               databased information.
  Returntype : Array[Int]

=cut

sub publish_sequence_files {
  my ($self, $files, $tag_id) = @_;

  defined $files or
    $self->logconfess('A defined file argument is required');

  defined $tag_id or
    $self->logconfess('A defined tag_id argument is required');

  my ($sample2primer) = $self->_get_primers();

  my ($num_files, $num_processed, $num_errors) = (0, 0, 0);

  my $product = WTSI::NPG::HTS::PacBio::Sequel::Product->new();

  foreach my $file ( @{$files} ){

    my @records = $self->find_pacbio_runs
      ($self->_metadata->run_name, $self->_metadata->well_name,
      $tag_id, $self->_metadata->plate_number);

    if (@records != 1) {
      $self->logcroak("Unexpected barcode from $file for SMRT cell ",
        $self->_metadata->well_name, ' run ', $self->_metadata->run_name);
    }

    if (@records >= 1) {

      my $well_label = $self->remove_well_padding
       ($self->_metadata->run_name,
        $self->_metadata->well_name);

      my $isoseq_primers;
      if ($file =~ m/ \- (\d+) [.]/smx) {
        my $num = $1;
        if ( defined $sample2primer->{$SAMPLE_PREFIX . '_'. $num} ) {
          $isoseq_primers = $sample2primer->{$SAMPLE_PREFIX . '_'. $num};
        }
      }

      my $tags = $records[0]->get_tags;

      my $id_product = $product->generate_product_id(
          $self->_metadata->run_name,
          $well_label,
          tags => $tags,
          plate_number => $self->_metadata->plate_number);

      my @primary_avus   = $self->make_primary_metadata
         ($self->_metadata,
          data_level => $DATA_LEVEL,
          id_product => $id_product,
          isoseq_primers => $isoseq_primers,
         );

      my @secondary_avus = $self->make_secondary_metadata(@records);

      my ($a_files, $a_processed, $a_errors) =
        $self->pb_publish_files([$file], $self->_dest_path,
                                \@primary_avus, \@secondary_avus);

      $num_files     += $a_files;
      $num_processed += $a_processed;
      $num_errors    += $a_errors;
    }
    else {
      $self->warn("Skipping publishing $file as no records found");
    }
  }
  $self->info("Published $num_processed / $num_files sequence files ",
              'for SMRT cell ', $self->_metadata->well_name, ' run ',
              $self->_metadata->run_name);

  return ($num_files, $num_processed, $num_errors);
}


=head2 publish_non_sequence_files

  Arg [1]    : Files. Required. Array ref.

  Example    : my ($num_files, $num_published, $num_errors) =
                 $pub->publish_non_sequence_files($files)
  Description: Identify non sequence files which match the required file 
               format regex and publish those files to iRODS. Return
               the number of files, the number published and the number
               of errors.
  Returntype : Array[Int]

=cut

sub publish_non_sequence_files {
  my ($self, $files) = @_;

  defined $files or
    $self->logconfess('A defined file argument is required');

  my ($num_files, $num_processed, $num_errors) =
    $self->pb_publish_files($files, $self->_dest_path);

  $self->info("Published $num_processed / $num_files non sequence files ",
              'for SMRT cell ', $self->_metadata->well_name, ' run ',
              $self->_metadata->run_name);

  return ($num_files, $num_processed, $num_errors);
}


override 'run_name'  => sub {
  my ($self) = @_;
  return $self->_metadata->ts_run_name;
};


override 'smrt_names'  => sub {
  my ($self)  = @_;

  ($self->_metadata->has_results_folder &&
      $self->_metadata->ts_run_name) or
      $self->logconfess('Error ts or results folder missing');

  my $rfolder = $self->_metadata->results_folder;
  my $ts_name = $self->_metadata->ts_run_name;

  $rfolder =~ /$ts_name/smx or
     $self->logconfess('Error ts name missing from results folder ', $rfolder);

  $rfolder =~ s/$ts_name//smx;
  $rfolder =~ s/\///gsmx;

  $rfolder =~ /$WELL_DIRECTORY_PATTERN/smx or
     $self->logconfess('Error derived folder name ', $rfolder,
     'does not match expected pattern');

  return [$rfolder];
};


has '_metadata_file' =>
  (isa           => 'Str',
   is            => 'ro',
   builder       => '_build_metadata_file',
   lazy          => 1,
   init_arg      => undef,
   documentation => 'Source meta data file.',);

sub _build_metadata_file{
  my ($self) = @_;

  my $output_path = catdir($self->analysis_path, $OUTPUT_DIR);

  my @metafiles;
  if (-d $output_path) {
    # As all analysis cell based all metafiles should have the correct run name,
    # well and plate number as no merged cell analysis - so just pick one.
    my @files = $self->list_directory
      ($output_path, filter => $METADATA_SET .q[.]. $METADATA_FORMAT . q[$]);
    push @metafiles, $files[0];
  }

  if (@metafiles != 1) {
    $self->logcroak('Expect one xml file in '. $self->analysis_path);
  }
  return $metafiles[0];
}


has '_metadata' =>
  (isa           => 'WTSI::NPG::HTS::PacBio::Metadata',
   is            => 'ro',
   builder       => '_build_metadata',
   lazy          => 1,
   init_arg      => undef,
   documentation => 'Load source meta data from file.',);

sub _build_metadata{
  my ($self) = @_;

  if ( !defined $self->_metadata_file ) {
    $self->logcroak('Metadata files is not defined for '. $self->analysis_path);
  }

  return  WTSI::NPG::HTS::PacBio::Sequel::MetaXMLParser->new->parse_file
                 ($self->_metadata_file, $METADATA_PREFIX);
}


sub _dest_path {
  my ($self) = @_;

  @{$self->smrt_names} == 1 or
        $self->logcroak('Error multiple smrt names found');

  return catdir($self->dest_collection, $self->smrt_names->[0], $self->analysis_id);
}


sub _determine_barcode {
  my ($self) = @_;

  ## expect just 1 barcode or none
  ## <pbsample:DNABarcodes>
  ## <pbsample:DNABarcode Name="bcM0001--bcM0001" UniqueId="8106bc66-8755-4cee-843f-b2fcbdd766f6"/>
  ## </pbsample:DNABarcodes>

  my $dom = XML::LibXML->new->parse_file($self->_metadata_file);

  my ($bc);
  if ($dom->getElementsByTagName('pbsample:DNABarcodes') ) {
      my @barcodes = $dom->getElementsByTagName('pbsample:DNABarcode');
      if (scalar @barcodes > 1) { $self->logcroak('More than 1 adapter barcode found') };
      foreach my $b (@barcodes) {
          my $name = $b->getAttribute('Name');
          my @bcn  = split /\-\-/smx, $name;
          $bc = $bcn[0];
      }
  }

  return($bc);
}


sub _get_primers {
  my ($self) = @_;

  ## get cDNA tags if exist from isoseq_primers.report.json
  ## BioSample_1, BioSample_2...N (12 max) map to files 1 -> N
  ## except sample* files numbered 0 -> (N - 1) 

  my @files = $self->list_directory($self->runfolder_path, filter => $PRIMERS_JSON);

  my (@names, @primers);
  if (scalar @files == 1) {
    my $file_contents = slurp $files[0];
    my $decoded = decode_json($file_contents);
    if (defined $decoded->{'tables'} ) {
      foreach my $t ( @{$decoded->{'tables'}} ) {
        foreach my $c ( @{$t->{'columns'}} ) {
          if ($c->{'header'} eq $SAMPLE_FIELD) {
            @names = @{$c->{'values'}};
          } elsif ($c->{'header'} eq $PRIMER_FIELD) {
            @primers = @{$c->{'values'}};
          }
        }
      }
    }
  }
  my %primers_names = map { $names[$_] => $primers[$_] } 0..$#names;

  return(\%primers_names);
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::Sequel::IsoSeqPublisher

=head1 DESCRIPTION

Publishes relevant files to iRODS, adds metadata and sets permissions.

=head1 AUTHOR

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2024 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
