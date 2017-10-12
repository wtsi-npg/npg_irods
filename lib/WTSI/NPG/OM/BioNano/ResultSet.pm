package WTSI::NPG::OM::BioNano::ResultSet;

use Moose;
use namespace::autoclean;

use Cwd qw[abs_path];
use DateTime;
use File::Basename qw[fileparse];
use File::Spec;

use WTSI::DNAP::Utilities::Collector;
use WTSI::NPG::OM::BioNano::BnxFile;

with 'WTSI::DNAP::Utilities::Loggable';

our $VERSION = '';

our $DATA_DIRECTORY_NAME = 'Detect Molecules';

our $BNX_NAME_FILTERED = 'Molecules.bnx';
our $BNX_NAME_RAW = 'RawMolecules.bnx';

our @ANCILLARY_FILE_NAMES = qw[analysisLog.txt
                               analysisResult.json
                               iovars.json
                               RunReport.txt
                               Stitch.fov
                               workset.json];

has 'directory' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1);

has 'bnx_file' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::OM::BioNano::BnxFile',
   lazy     => 1,
   default  => sub {
       my ($self,) = @_;
       return WTSI::NPG::OM::BioNano::BnxFile->new($self->bnx_path);
   },
   init_arg => undef,
   documentation => 'Object representing the filtered (not raw) BNX file',
);

has 'data_directory' =>
  (is       => 'ro',
   isa      => 'Str',
   lazy     => 1,
   builder  => '_build_data_directory',
   init_arg => undef,
);

has 'raw_bnx_path' =>
  (is       => 'ro',
   isa      => 'Str',
   lazy     => 1,
   builder  => '_build_raw_bnx_path',
   init_arg => undef,
);

has 'run_date' =>
  (is       => 'ro',
   isa      => 'DateTime',
   lazy     => 1,
   builder  => '_build_run_date',
   init_arg => undef,
   documentation => 'Date and time of run, parsed from the runfolder name',
);

has 'bnx_path' =>
  (is       => 'ro',
   isa      => 'Str',
   lazy     => 1,
   builder  => '_build_bnx_path',
   init_arg => undef,
);

has 'ancillary_files' =>
  (is       => 'ro',
   isa      => 'ArrayRef[Str]',
   lazy     => 1,
   builder  => '_build_ancillary_files',
   init_arg => undef,
   documentation => 'Paths of ancillary files with information on the run',
);

has 'stock' =>
  (is       => 'ro',
   isa      => 'Str',
   lazy     => 1,
   builder  => '_build_stock',
   init_arg => undef,
   documentation => 'Stock barcode parsed from the main directory name',
);


sub BUILD {
    my ($self) = @_;
    # validate main directory
    if (! -e $self->directory) {
        $self->logconfess(q[BioNano directory path '], $self->directory,
                          q[' does not exist]);
    }
    if (! -d $self->directory) {
        $self->logconfess(q[BioNano directory path '], $self->directory,
                          q[' is not a directory]);
    }
    # directory name must be of the form barcode_yyyy-mm-dd_HH_MM
    # barcode may include _ (underscore) characters, but not whitespace
    my $dirname = fileparse($self->directory);
    if (!($dirname =~ qr{^\S+_\d{4}-\d{2}-\d{2}_\d{2}_\d{2}$}msx)) {
        $self->logcroak(q[Incorrectly formatted name '], $dirname,
                        q[' for BioNano unit runfolder: should be ],
                        q[of the form barcode_yyyy-mm-dd_HH_MM]);
    }
    return 1;
}

sub _build_ancillary_files {
    my ($self) = @_;
    my @ancillary_files;
    # exclude .bnx files from collection
    my @files = WTSI::DNAP::Utilities::Collector->new(
        root => $self->directory,
    )->collect_files(sub {!($_[0] =~ m/[.]bnx$/msx)});
    my %ancillary_file_names;
    foreach my $name (@ANCILLARY_FILE_NAMES) {
        $ancillary_file_names{$name} = 1;
    }
    foreach my $file (@files) {
        if (!$ancillary_file_names{fileparse($file)}) {
            $self->logwarn(q[Unexpected ancillary file name for '],
                           $file, q[']);
        }
        push @ancillary_files, $file;
        $self->debug('Added ', $file, ' to list of ancillary files');
    }
    return \@ancillary_files;
}

sub _build_data_directory {
    my ($self) = @_;
    my $data_directory = File::Spec->catfile($self->directory,
                                             $DATA_DIRECTORY_NAME);
    if (! -e $data_directory) {
        $self->logconfess(q[BioNano data directory path '], $data_directory,
                          q[' does not exist]);
    }
    if (! -d $data_directory) {
        $self->logconfess(q[BioNano data directory path '], $data_directory,
                          q[' is not a directory"]);
    }
    return $data_directory;
}

sub _build_bnx_path {
    my ($self) = @_;
    my $bnx_path = File::Spec->catfile($self->data_directory,
                                             $BNX_NAME_FILTERED);
    if (! -e $bnx_path) {
        $self->logconfess(q[BioNano filtered bnx path '],
                          $bnx_path, q[' does not exist]);
    }
    return $bnx_path;
}

sub _build_raw_bnx_path {
    my ($self) = @_;
    my $bnx_path = File::Spec->catfile($self->data_directory,
                                             $BNX_NAME_RAW);
    if (! -e $bnx_path) {
        $self->logconfess(q[BioNano raw bnx path '],
                          $bnx_path, q[' does not exist]);
    }
    return $bnx_path;
}

sub _build_run_date {
    # parse the datestamp from main directory name
    my ($self) = @_;
    my ($barcode, $datetime) = $self->_parse_runfolder_name();
    return $datetime;
}

sub _build_stock {
    # parse stock barcode from main directory name
    my ($self) = @_;
    my ($barcode, $datetime) = $self->_parse_runfolder_name();
    return $barcode;
}

sub _parse_runfolder_name {
    # parse sample barcode from main directory name
    # name is of the form barcode_time: barcode_yyyy-mm-dd_HH_MM
    # name format is checked by the BUILD method
    my ($self) = @_;
    my $dirname = fileparse($self->directory);
    my @terms = split /_/msx, $dirname;
    my $minute = pop @terms;
    my $hour = pop @terms;
    my $date = pop @terms;
    my $barcode = join '_', @terms;
    my ($year, $month, $day) = split /-/msx, $date;
    my $dt = DateTime->new(
        year   => $year,
        month  => $month,
        day    => $day,
        hour   => $hour,
        minute => $minute,
    );
    return ($barcode, $dt);
}



__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 AUTHOR

Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2016 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=head1 DESCRIPTION

Class to represent a BioNano result set. The result set is a directory
containing a data subdirectory, which in turn contains two BNX files,
respectively filtered and unfiltered. The directory and subdirectory may
also contain ancillary files with information on the run.


=cut
