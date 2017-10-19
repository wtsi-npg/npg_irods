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

our $BNX_NAME_FILTERED = 'Molecules.bnx';

has 'directory' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1);

has 'ancillary_file_paths' =>
  (is       => 'ro',
   isa      => 'ArrayRef[Str]',
   lazy     => 1,
   builder  => '_build_ancillary_file_paths',
   init_arg => undef,
   documentation => 'Paths of ancillary files with information on the '.
       'run. Excludes BNX files and TIFF image files.',
);

has 'bnx_file' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::OM::BioNano::BnxFile',
   lazy     => 1,
   default  => sub {
       my ($self,) = @_;
       return WTSI::NPG::OM::BioNano::BnxFile->new($self->filtered_bnx_path);
   },
   init_arg => undef,
   documentation => 'Object representing the filtered (not raw) BNX file',
);

has 'bnx_paths' =>
  (is       => 'ro',
   isa      => 'ArrayRef[Str]',
   lazy     => 1,
   builder  => '_build_bnx_paths',
   init_arg => undef,
   documentation => 'Paths of all BNX files in the directory',
);

has 'filtered_bnx_path' =>
  (is       => 'ro',
   isa      => 'Str',
   lazy     => 1,
   builder  => '_build_filtered_bnx_path',
   init_arg => undef,
   documentation => 'Path of the filtered BNX file. Exactly one '.
       'filtered BNX file must be present, otherwise an error is raised',
);

has 'run_date' =>
  (is       => 'ro',
   isa      => 'DateTime',
   lazy     => 1,
   builder  => '_build_run_date',
   init_arg => undef,
   documentation => 'Date and time of run, parsed from the runfolder name',
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


sub _build_ancillary_file_paths {
    my ($self) = @_;
    # exclude .bnx files from collection
    my @ancillary_files = WTSI::DNAP::Utilities::Collector->new(
        root => $self->directory,
    )->collect_files(sub {!($_[0] =~ m/[.](bnx|tiff)$/msx)});
    foreach my $file (@ancillary_files) {
        $self->debug('Added ', $file, ' to list of ancillary files');
    }
    return \@ancillary_files;
}

sub _build_bnx_paths {
    my ($self) = @_;
    my @files = WTSI::DNAP::Utilities::Collector->new(
        root => $self->directory,
    )->collect_files(sub {$_[0] =~ m/[.](bnx)$/msx});
    foreach my $file (@files) {
        $self->debug('Added ', $file, ' to list of BNX paths');
    }
    return \@files;
}

sub _build_filtered_bnx_path {
    my ($self) = @_;
    # consider BNX files, looking for exactly one with the name Molecules.bnx
    my @bnx = grep {
        fileparse($_) eq $BNX_NAME_FILTERED
    } @{$self->bnx_paths};
    if (scalar @bnx == 0) {
        $self->logcroak('No filtered BNX file found in directory ',
                        $self->directory);
    } elsif (scalar @bnx > 1) {
        my $files = join ', ', @bnx;
        $self->logcroak('Found more than one filtered BNX file in directory ',
                        $self->directory, ': ', $files);
    }
    my $bnx = shift @bnx;
    $self->debug("Found filtered BNX file $bnx");
    return $bnx;
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

Copyright (C) 2016, 2017 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=head1 DESCRIPTION

Class to represent a BioNano result set, given a directory path.

There must be exactly one Molecules.bnx file in the tree rooted at the
given directory. The directory may also contain additional BNX files,
and non-BNX ancillary files. TIFF image files are omitted from the
ancillary file listing. Run timestamp and stock barcode are parsed from
the directory name.

=cut
