package WTSI::NPG::HTS::PacBio::Sequel::SeqchkCalculator;

use namespace::autoclean;
use File::Basename;
use File::Spec::Functions qw[catfile];
use Moose;
use MooseX::StrictConstructor;
use Perl6::Slurp;
use Readonly;
use Try::Tiny;

use WTSI::DNAP::Utilities::Runnable;

with qw[
         WTSI::DNAP::Utilities::Loggable
       ];

our $VERSION = '';

our $SEQCHKSUM_SUFFIX = 'seqchksum';

Readonly::Scalar my $SEQCHKSUM_LINE_COUNT   => 7;
Readonly::Scalar my $SEQCHKSUM_FIELD_COUNT  => 8;


has 'input_file' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'Full path to input file');

has 'file_format' =>
  (isa           => 'Str',
   is            => 'ro',
   default       => q[bam],
   documentation => 'Input file format');

has 'output_file' =>
  (isa           => 'Str',
   is            => 'ro',
   builder       => '_build_output_file',
   lazy          => 1,
   documentation => 'Generate output file from input file path if not defined');

sub _build_output_file {
  my ($self) = @_;

  if( !-f $self->input_file ) {
    $self->logcroak('Error finding input file: '. $self->input_file);
  }
  my($f,$dir,$ext) = fileparse($self->input_file, q[.]. $self->file_format);
  my $output_file  = catfile($dir,$f.q[.].$SEQCHKSUM_SUFFIX);

  return $output_file;
}

=head2 calculate_seqchksum

  Arg [1]    : None
  Example    : $sc->calculate_seqchksum
  Description: Generate a seqchksum file for an input file
  Returntype : Boolean, 0 for success and 1 for fail 

=cut

sub calculate_seqchksum {
  my ($self) = @_;

  my $num_errors = 0;
  try {
    if( !-f $self->output_file ) {
      my $cmd = q{set -o pipefail && bamseqchksum inputformat=}.
        $self->file_format .q{ < }. $self->input_file .q{ > }. $self->output_file;
      WTSI::DNAP::Utilities::Runnable->new(executable => '/bin/bash',
        arguments  => ['-c', $cmd])->run;
    }

    if( !-f $self->output_file || -z  $self->output_file ) {
      $self->logcroak('Error calculating seqchksum: '. $self->output_file);
    }

    ## check file contents valid
    my @contents = slurp $self->output_file;
    if(scalar @contents != $SEQCHKSUM_LINE_COUNT) {
      $self->logcroak('Wrong number of lines in '. $self->output_file);
    }

    foreach my $c (@contents) {
      $c =~ s/\n//smxg;
      my @l = split /\t/smx, $c;
      if(scalar @l != $SEQCHKSUM_FIELD_COUNT) {
        $self->logcroak('Wrong number of fields in lines in '. $self->output_file);
      }
    }

  } catch {
    $num_errors++;
    $self->error('Failed to generate seqchecksum file for '. $self->input_file);
  };

  if ( $num_errors > 0 ) {
    if ($self->output_file && -e $self->output_file) {
      unlink $self->output_file;
    }
  }

  return ($num_errors < 1 || -z $self->input_file) ? 0 : 1;
}


__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::Sequel::SeqchkCalculator

=head1 DESCRIPTION

Generate a seqchksum file for a given input file and make basic
checks on the format of the output file.

=head1 AUTHOR

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2022 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
