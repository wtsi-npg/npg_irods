package WTSI::NPG::HTS::PacBio::Sequel::AnalysisFastaManager;

use namespace::autoclean;
use File::Spec::Functions qw[catfile catdir];
use IO::Compress::Gzip;
use IO::File;
use Moose;
use MooseX::StrictConstructor;
use Readonly;
use Try::Tiny;
use XML::LibXML;

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::HTS::PathLister
       ];

our $VERSION = '';

Readonly::Scalar my $ISODEMUX => 'cromwell-job/call-demux_cluster_files/execution';
Readonly::Scalar my $FASUFFIX => 'fasta.gz';

has 'analysis_path' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'PacBio root analysis job path');

has 'meta_data' =>
  (isa           => 'WTSI::NPG::HTS::PacBio::Metadata',
   is            => 'ro',
   required      => 1,
   documentation => 'Meta data from file');

has 'output_dir' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 0,
   predicate     => 'has_output_dir',
   documentation => 'Optional output directory for report file');

has 'runfolder_path' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'Primary analysis output path');



=head2 make_loadable_files

  Arg [1]    : None
  Example    : my($is_success) = $fm->make_loadable_files
  Description: Rename and reformat fasta files.
  Returntype : Boolean, defaults to false.

=cut

sub make_loadable_files {
  my ($self) = shift;

  my $output_dir  = $self->has_output_dir ? $self->output_dir :
    $self->runfolder_path;

  my $is_success = 0;
  try {
    my $fasta_path = catdir($self->analysis_path,$ISODEMUX);
    if (! -e $fasta_path || ! -d $fasta_path) {
      $self->info('Currently only handling multiplexed data');
    } else {
      my @fa  = $self->list_directory
        ($fasta_path, filter => 'fasta' .q[$]);
      my @xml = $self->list_directory
        ($self->runfolder_path, filter => 'xml' .q[$]);

      if (scalar @fa < 2 || scalar @fa != (scalar @xml * 2)) {
        $self->logcroak('Expect >= 2 fasta files and 2 fasta per xml');
      }
      $self->_process_deplexed_data(\@fa,\@xml);
      $is_success = 1;
    }
  } catch {
    my @stack = split /\n/msx;   # Chop up the stack trace
    $self->logcroak(pop @stack); # Use a shortened error message
  };

  if (!$is_success) {
    my @gz = $self->list_directory
      ($self->runfolder_path, filter => $FASUFFIX .q[$]);
    foreach my $file (@gz) {
      unlink $file or $self->logcroak('Failed to remove: ', $file);
    }
  }
  return $is_success;
}

sub _process_deplexed_data {

  my ($self, $fafiles, $xmlfiles) = @_;

  my %sample2barcode;
  foreach my $xmlfile (@{$xmlfiles}) {
    my $prefix = 'pbsample:';
    my $dom = XML::LibXML->new->parse_file($xmlfile);

    my @biosample = $dom->getElementsByTagName($prefix . 'BioSample')->[1];
    my $sample = $biosample[0]->getAttribute('Name');

    my @dnabarcode = $dom->getElementsByTagName($prefix . 'DNABarcode')->[0];
    my $barcode = $dnabarcode[0]->getAttribute('Name');

    if(!$barcode || !$sample ){
      $self->logcroak('XML file without sample or barcode info: ', $xmlfile);
    } else {
      $sample =~ s/BioSample_//smx;
      $sample2barcode{$sample} = $barcode;
    }
  }

  foreach my $fafile (@{$fafiles}) {
    my($type,$sample);
    if ($fafile =~ /(\wq_transcripts) [-] (\d+) [.] fasta/smx) {
      ($type,$sample) = ($1,$2);
    } else {
      $self->logcroak('Fasta file found with unexpected name: ', $fafile);
    }

    my $bc = defined $sample2barcode{$sample} ? $sample2barcode{$sample} :
      $self->logcroak('Fasta file found with no barcode conversion: ', $fafile);

    my $faout = catfile($self->runfolder_path,
      $self->meta_data->movie_name. q[.]. $bc .q[.]. $type .q[.]. $FASUFFIX);

    if (! -f $faout) {
      my $fa = IO::File->new($fafile, q[<])
        or $self->logcroak('Could not open: ', $fafile);
      my $gz = IO::Compress::Gzip->new($faout)
        or $self->logcroak('Could not write to: ', $faout);

      $self->info('Writing compressed fasta to: ', $faout);
      if (defined $fa) {
        while (my $line = <$fa>) {
          if ( $line =~ /^>/smx ){
            my @l = split /\s+/smx, $line;
            $gz->write($l[0]."\n");
          } else {
            $gz->write($line);
          }
        }
      }
      $fa->close or $self->logcroak('Cannot close file: ', $fafile);
      $gz->flush;
      $gz->close or $self->logcroak('Cannot close file: ', $faout);
    }
  }
  return();
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::Sequel::AnalysisFastaManager

=head1 DESCRIPTION

Find, reformat, compress and rename fasta files so they are suitable
for iRODS loading.

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
