package WTSI::NPG::HTS::Samtools;

use strict;
use warnings;
use feature qw[fc];
use English qw[-no_match_vars];
use Carp;

use Exporter qw[import];
our @EXPORT_OK = qw[put_xam get_xam_header get_xam_records];

use Capture::Tiny qw[capture];
use File::Basename;
use File::Temp qw[tempdir];

our $VERSION = '';

our $DEFAULT_SAMTOOLS_EXECUTABLE = 'samtools';
our $DEFAULT_IPUT_EXECUTABLE = 'iput';
our $DEFAULT_IGET_EXECUTABLE = 'iget';

=head2 put_xam

  Arg [1]    : Str $local_path
               The path to the local SAM/BAM/CRAM file.

  Arg [2]    : Str $remote_path
               The destination path in iRODS where the file will be uploaded.

  Arg [3]    : Str $reference_path
               The path to the reference genome file required for conversion.

  Example    : put_xam('/path/to/local.bam', '/irods/path/to/remote.cram', '/path/to/reference.fa');

  Description: Uploads a SAM/BAM/CRAM file to iRODS after converting it to the appropriate format.
               The function creates a temporary directory to store the converted file. It determines
               the file type based on the extension of the remote path and sets the appropriate
               `samtools` arguments for conversion. The converted file is then uploaded to iRODS
               using the `iput` command.

  Returntype : None

  Throws     : Croaks if the file type is unsupported or if any of the commands (`samtools` or `iput`) fail.

=cut

sub put_xam {
    my ($local_path, $remote_path, $reference_path) = @_;

    my $tmpdir = tempdir('put_xam_XXXXXX', TMPDIR => 1, CLEANUP => 1);
    my ($name, $path, $suffix) = fileparse($remote_path, qr/[.][^.]*$/msxi);

    my @samtools_args = ('view', '-T', $reference_path);
    $suffix = fc $suffix;

    if ($suffix eq '.sam') {
        push @samtools_args, '-h' # Output SAM, retain the header
    }
    elsif ($suffix eq '.bam') {
        push @samtools_args, '-b' # Output BAM
    }
    elsif ($suffix eq '.cram') {
        push @samtools_args, '-C' # Output CRAM
    }
    else {
        croak "Unsupported file type: '$suffix'";
    }

    push @samtools_args, '-o', "$tmpdir/$name", $local_path;

    # Runnable handles logging and error checking
    # First run samtools to create a local temp file
    WTSI::DNAP::Utilities::Runnable->new(
        arguments  => \@samtools_args,
        executable => $DEFAULT_SAMTOOLS_EXECUTABLE)->run;

    # Then run iput to put the temp file into iRODS
    WTSI::DNAP::Utilities::Runnable->new(
        arguments  => ['-f', '-k', '-a', "$tmpdir/$name", $remote_path],
        executable => $DEFAULT_IPUT_EXECUTABLE)->run;

    return;
}

=head2 get_xam_header

  Arg [1]    : Str $remote_path
               The path to the remote SAM/BAM/CRAM file in iRODS.

  Example    : my $header = get_xam_header('/path/to/file.bam');

  Description: Retrieves the header of a SAM/BAM/CRAM file stored in iRODS.
               Uses `iget` to fetch the file and `samtools` to extract the header.

  Returntype : ArrayRef[Str]
               An array reference containing the header lines as strings.

  Throws     : Croaks if the `iget` or `samtools` command fails.

=cut

sub get_xam_header {
    my ($remote_path) = @_;

    my $cmd = "$DEFAULT_IGET_EXECUTABLE $remote_path - | $DEFAULT_SAMTOOLS_EXECUTABLE head";
    my $exit;
    my ($stdout, $stderr) = capture {
        $exit = system $cmd
    };

    if ($exit != 0) {
        croak "Failed to run '$cmd': $ERRNO : $stderr";
    }

    return [split /\n/msx, $stdout];
}


=head2 get_xam_records

  Arg [1]    : Str $remote_path
               The path to the remote SAM/BAM/CRAM file in iRODS.

  Arg [2]    : Int $num_records (optional)
               The number of records to retrieve. Defaults to 1024 if not provided.

  Example    : my $records = get_xam_records('/path/to/file.bam', 500);

  Description: Retrieves a specified number of records from a SAM/BAM/CRAM file
               stored in iRODS. Uses `iget` to fetch the file and `samtools` to
               extract the records.

  Returntype : ArrayRef[Str]
               An array reference containing the retrieved records as strings.

  Throws     : Croaks if the `iget` or `samtools` command fails.

=cut

sub get_xam_records {
    my ($remote_path, $num_records) = @_;
    $num_records //= 1024; # Default to 1024 records

    my $cmd = "$DEFAULT_IGET_EXECUTABLE $remote_path - | " .
        "$DEFAULT_SAMTOOLS_EXECUTABLE head --headers 0 --records $num_records";
    my $exit;
    my ($stdout, $stderr) = capture {
        $exit = system $cmd
    };

    if ($exit != 0) {
        croak "Failed to run '$cmd': $ERRNO : $stderr";
    }

    return [split /\n/msx, $stdout];
}

1;


__END__

=head1 NAME

WTSI::NPG::HTS::Samtools

=head1 DESCRIPTION

Utility functions for working with samtools and iRODS, specifically for
obtaining headers and records from SAM/BAM/CRAM files in iRODS.

This package is a temporary workaround for

https://github.com/samtools/htslib-plugins/issues/6
https://github.com/samtools/htslib-plugins/issues/7

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2025 Genome Research Ltd.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
