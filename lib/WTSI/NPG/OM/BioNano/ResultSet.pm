package WTSI::NPG::OM::BioNano::ResultSet;

use Moose;

use Cwd qw(abs_path);
use File::Basename qw(fileparse);
use File::Spec;

use WTSI::DNAP::Utilities::Collector;

with 'WTSI::DNAP::Utilities::Loggable';

our $VERSION = '';

our $DATA_DIRECTORY_NAME = 'Detect Molecules';

our $BNX_NAME_FILTERED = 'Molecules.bnx';
our $BNX_NAME_RAW = 'RawMolecules.bnx';

our @ANCILLARY_FILE_NAMES = qw(analysisLog.txt
                               analysisResult.json
                               iovars.json
                               RunReport.txt
                               Stitch.fov
                               workset.json);

has 'directory' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1);

has 'data_directory' =>
  (is       => 'ro',
   isa      => 'Str',
   lazy     => 1,
   builder  => '_build_data_directory',
   init_arg => undef,
);

has 'raw_molecules_file' =>
  (is       => 'ro',
   isa      => 'Str',
   lazy     => 1,
   builder  => '_build_raw_molecules_file',
   init_arg => undef,
);

has 'molecules_file' =>
  (is       => 'ro',
   isa      => 'Str',
   lazy     => 1,
   builder  => '_build_molecules_file',
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

has 'sample' =>
  (is       => 'ro',
   isa      => 'Str',
   lazy     => 1,
   builder  => '_build_sample',
   init_arg => undef,
   documentation => 'Sample barcode parsed from the main directory name',
);


sub BUILD {
    my ($self) = @_;
    # validate main directory
    if (! -e $self->directory) {
        $self->logconfess("BioNano directory path '", $self->directory,
                          "' does not exist");
    }
    if (! -d $self->directory) {
        $self->logconfess("BioNano directory path '", $self->directory,
                          "' is not a directory");
    }
    # directory name must be of the form barcode_yyyy-mm-dd_MM_SS
    # barcode may include _ (underscore) characters, but not whitespace
    my $dirname = fileparse($self->directory);
    if (!($dirname =~ qr{^\S+_\d{4}-\d{2}-\d{2}_\d{2}_\d{2}$}msx)) {
        $self->logcroak("Incorrectly formatted name '", $dirname,
                        "' for BioNano unit runfolder: should be ",
                        "of the form barcode_yyyy-mm-dd_MM_SS");
    }
}

sub _build_ancillary_files {
    my ($self) = @_;
    my @ancillary_files;
    my @files = WTSI::DNAP::Utilities::Collector->new(
        root => $self->directory,
    )->collect_files(sub {!($_[0] =~ qr{\.bnx$});}); # exclude .bnx files
    my %ancillary_file_names;
    foreach my $name (@ANCILLARY_FILE_NAMES) {
        $ancillary_file_names{$name} = 1;
    }
    foreach my $file (@files) {
        if (!$ancillary_file_names{fileparse($file)}) {
            $self->logwarn("Unexpected ancillary file name for '",
                           $file, "'");
        }
        push(@ancillary_files, $file);
        $self->debug("Added $file to list of ancillary files");
    }
    return \@ancillary_files;
}

sub _build_data_directory {
    my ($self) = @_;
    my $data_directory = File::Spec->catfile($self->directory,
                                             $DATA_DIRECTORY_NAME);
    if (! -e $data_directory) {
        $self->logconfess("BioNano data directory path '", $data_directory,
                          "' does not exist");
    }
    if (! -d $data_directory) {
        $self->logconfess("BioNano data directory path '", $data_directory,
                          "' is not a directory");
    }
    return $data_directory;
}

sub _build_molecules_file {
    my ($self) = @_;
    my $molecules_path = File::Spec->catfile($self->data_directory,
                                             $BNX_NAME_FILTERED);
    if (! -e $molecules_path) {
        $self->logconfess("BioNano filtered molecules path '",
                          $molecules_path, "' does not exist");
    }
    return $molecules_path;
}

sub _build_raw_molecules_file {
    my ($self) = @_;
    my $molecules_path = File::Spec->catfile($self->data_directory,
                                             $BNX_NAME_RAW);
    if (! -e $molecules_path) {
        $self->logconfess("BioNano raw molecules path '",
                          $molecules_path, "' does not exist");
    }
    return $molecules_path;
}

sub _build_sample {
    # parse sample barcode from main directory name
    # name is of the form barcode_time: barcode_yyy-mm-dd_HH_MM
    # TODO save the timestamp also?
    my ($self) = @_;
    my $dirname = fileparse($self->directory);
    my @terms = split '_', $dirname;
    my $minutes = pop @terms;
    my $hours = pop @terms;
    my $date = pop @terms;
    my $barcode = join '_', @terms;
    return $barcode;
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
