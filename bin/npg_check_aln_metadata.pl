#!/usr/bin/env perl

use strict;
use warnings;
use feature 'say';
use FindBin qw[$Bin];
use lib (-d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib");

use Carp;
use File::Basename;
use Getopt::Long;
use Pod::Usage;
use Log::Log4perl qw[:levels];

use WTSI::NPG::HTS::Illumina::AlnDataObject;
use WTSI::NPG::iRODS;

our $VERSION = '';

my $log_config = << 'LOGCONF'
log4perl.logger = INFO, A1

log4perl.logger.WTSI.NPG.HTS.Illumina.AlnDataObject = INFO, A1
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

my $dataobj_filename;
my $commands_filename = 'imetaCommands.txt';
my $dataobj_attribute;
my $dataobj_value_pattern;
my $stdio;

sub _read_from_input {
    my ($in) = @_;
    my @dataobjects;
    if (not $in) {
        while (my $line = <>) {
            chomp $line;
            push @dataobjects, $line;
        }
    } else {
        while (my $line = <$in>) {
            chomp $line;
            push @dataobjects, $line;
        }
    }
    return @dataobjects;
}

sub _get_metadata_value {
    my ($obj,$attr) = @_;
    foreach my $avu (@{$obj->metadata}) {
        my %myavu = %{$avu};
        if ($myavu{'attribute'} eq $attr) {
            return $myavu{'value'}
        }
    }
    return;
}

sub _get_matches {
    my ($obj,$pattern) = @_;
    my @matched_headers;
    foreach my $header (@{$obj->header}) {
        my @match = $header =~ qr/$pattern/sxm;
        if (scalar(@match) == 1) {
            push @matched_headers, $match[0];
        }
    }
    return @matched_headers;
}

GetOptions( 'dataobjfile=s'         => \$dataobj_filename,
            'commandsfile|out=s'    => \$commands_filename,
            'attribute=s'           => \$dataobj_attribute,
            'pattern=s'             => \$dataobj_value_pattern,
            'help'                  => sub { pod2usage(-verbose => 2,
                                                    -exitval => 0) },
            q[]                     => \$stdio);

Log::Log4perl::init(\$log_config);

my $log = Log::Log4perl->get_logger('main');
$log->level($ALL);

if (not ($dataobj_filename xor $stdio)) {
    my $msg = 'A list of irods data objects is required in input. ' .
                'Please specify a list from file (--dataobjfile) or from STDIN.';
    pod2usage(  -msg     => $msg,
                -exitval => 2);
}

if (not $dataobj_attribute) {
    my $msg = 'A --attribute argument is required';
    pod2usage(  -msg     => $msg,
                -exitval => 2);
}

if (not $dataobj_value_pattern) {
    my $msg = 'A --pattern argument is required';
    pod2usage(  -msg     => $msg,
                -exitval => 2);
}

my @dataobjects_list;
if ($stdio) {
    @dataobjects_list = _read_from_input();
} else {
    my $dataobj_file_pid = open my $dataobj_file, '<', $dataobj_filename;
    if (not $dataobj_file_pid) {
        $log->logcroak(qq[Could not open the file $dataobj_filename]);
    }
    @dataobjects_list = _read_from_input($dataobj_file);
}


my $irods = WTSI::NPG::iRODS->new(  environment          => \%ENV,
                                    strict_baton_version => 0);

my $commands_file_pid = open my $commands_file, '>', $commands_filename;
if (not $commands_file_pid) {
    $log->logcroak(qq[Could not open the file $commands_filename]);
}
foreach my $dataobj_path (@dataobjects_list) {
    my ($dataobj,$path) = fileparse($dataobj_path);
    my ($id_run, $position, $tag_index, $subset, $format) = $dataobj =~ /(\d+)[_](\d+)[#](\d+)[_]*(\w+)*[.](\w+)/smx;
    my @initargs  = (id_run    => $id_run,
                   position  => $position,
                   tag_index => $tag_index);
    if (defined $subset) {
        push @initargs, subset => $subset;
    }

    my $obj = WTSI::NPG::HTS::Illumina::AlnDataObject->new
        (collection  => $path,
        data_object => $dataobj,
        file_format => $format,
        irods       => $irods,
        @initargs);

    my $actual_value = _get_metadata_value($obj, $dataobj_attribute);
    if (not $actual_value) {
        $log->info("$dataobj_path - No $dataobj_attribute specified in iRODS metadata!");
        continue;
    }
    my @matched_headers = _get_matches($obj, $dataobj_value_pattern);
    my $n_matches = scalar @matched_headers;
    my $expected_value;
    if ($n_matches == 1) {
        $expected_value = $matched_headers[0];
        if ($actual_value ne $expected_value) {
            $log->info("$dataobj_path - Inconsistent metadata on $dataobj_path:\n\t" .
                "Actual: $actual_value\n\t" .
                "Expected: $expected_value");
            my $imeta_rm_pid = print $commands_file "imeta rm -d $dataobj_path \"$dataobj_attribute\" \"$actual_value\"\n";
            if (not $imeta_rm_pid) {
                $log->logcroak(qq[Cannot write on $commands_filename]);
            }
            my $imeta_add_pid = print $commands_file "imeta add -d $dataobj_path \"$dataobj_attribute\" \"$expected_value\"\n";
            if (not $imeta_add_pid) {
                $log->logcroak(qq[Cannot write on $commands_filename]);
            }
        }
    } elsif ($n_matches > 1) {
        $log->info("$dataobj_path - The chosen pattern is matching too many headers. Only one must match.");
    } else {
        $log->info("$dataobj_path - The chosen pattern is not matching with any headers.");
    }
}

__END__

=head1 NAME

npg_check_aln_metadata.pl

=head1 SYNOPSIS

npg_check_aln_metadata.pl 
    --dataobjfile <path> | default from STDIN
    --attribute <str> 
    --pattern <str> 
    [--commandsfile <path> | default to 'imetaCommands.txt']
    [--help]

    Options:
        --dataobjfile       A file containing a list of data objects in iRODS.
                                Each of them separated by newline.
        --attribute         Attribute name of iRODS metadata that should be checked.
        --pattern           Regex pattern with group notation to check the metadata 
                                value in the file headers. The first matched group is
                                compared to the actual iRODS metadata.
                                For example, if you need to match a path consider 
                                the pattern "lineid\S+(path\/to\/file)".
                                'path/to/file' will be compared to the actual iRODS 
                                metadata under "attribute".
        --help              Display help.
        -                   Read data object list from standard input.

=head1 DESCRIPTION

Identify those alignment files (bam or cram) with inconsistent iRODS
metadata.
Some iRODS metadata (e.g. reference) may be found in the file 
content as well (e.g. header of reads in cram files).
Inconsistent metadata is defined as that iRODS metadata value that
is different from what the file content specifies.
The script will output a set of imeta commands for those files
that have inconsistent metadata and need to be updated.

=head1 AUTHOR

Marco M Mosca mm51@sanger.ac.uk

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2023 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
