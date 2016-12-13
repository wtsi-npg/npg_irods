package WTSI::NPG::OM::BioNano::BnxFile;

use Moose;
use namespace::autoclean;

use Digest::MD5;

our $VERSION = '';

our $INSTRUMENT_KEY = 'InstrumentSerial';
our $CHIP_ID_KEY = 'ChipId';
our $FLOWCELL_KEY = 'Flowcell';

our @REQUIRED_FIELDS = ($INSTRUMENT_KEY, $CHIP_ID_KEY, $FLOWCELL_KEY);

with 'WTSI::DNAP::Utilities::Loggable';

has 'path' =>
  (is       => 'ro',
   isa      => 'Str',
   required => 1,
   documentation => 'Path of the BNX file',
);

has 'chip_id' =>
  (is       => 'ro',
   isa      => 'Str',
   init_arg => undef,
   lazy     => 1,
   default  => sub {
       my ($self,) = @_;
       return $self->header->{$CHIP_ID_KEY};
   },
   documentation => 'Chip identifier',
);

has 'header' =>
  (is       => 'ro',
   isa      => 'HashRef',
   init_arg => undef,
   lazy     => 1,
   builder  => '_build_header',
   documentation => 'Data structure parsed from the BNX file header',
);

has 'instrument' =>
  (is       => 'ro',
   isa      => 'Str',
   init_arg => undef,
   lazy     => 1,
   default  => sub {
       my ($self,) = @_;
       return $self->header->{$INSTRUMENT_KEY};
   },
   documentation => 'Instrument name',
);

has 'flowcell' =>
  (is       => 'ro',
   isa      => 'Int',
   init_arg => undef,
   lazy     => 1,
   default  => sub {
       my ($self,) = @_;
       return $self->header->{$FLOWCELL_KEY};
   },
   documentation => 'Identifier for a flowcell, one of two channels '.
       'on a BioNano chip.',
);

has 'md5sum' =>
  (is       => 'ro',
   isa      => 'Str',
   init_arg => undef,
   lazy     => 1,
   builder  => '_build_md5sum',
   documentation => 'MD5 checksum of the BNX file',
);


around BUILDARGS => sub {
  my ($orig, $class, @args) = @_;

  # Permit a Str as an anonymous argument mapping to path
  if (@args == 1 && !ref $args[0]) {
    return $class->$orig(path => $args[0]);
  }
  else {
    return $class->$orig(@_);
  }
};

sub BUILD {
    my ($self,) = @_;
    if (! -r $self->path) {
        $self->logcroak(q[Cannot read BNX path '], $self->path, q[']);
    }
    return 1;
}

sub _build_header {
    my ($self,) = @_;
    my @header_keys;
    my %header;
    open my $fh, '<', $self->path ||
        $self->logcroak(q[Failed to open BNX path '], $self->path, q[']);
    while (<$fh>) {
        chomp;
        if (m/^[#][ ]BNX[ ]File[ ]Version:\t/msx) {
            my @fields = split /\t/msx;
            my $version = pop @fields;
            if ($version !~ /1[.][012]/msx) {
                $self->logwarn(q[Unsupported BNX version number: '],
                               $version, q[']);
            }
        } elsif (m/^[#]rh/msx) {
            @header_keys = split /\t/msx;
        } elsif (@header_keys && m/^[#][ ]Run[ ]Data\t/msx) {
            my @header_values = split /\t/msx;
            if (scalar @header_keys != scalar @header_values) {
                $self->logcroak('Numbers of keys and values in BNX ',
                                'file header do not match');
            }
            for (0 .. scalar @header_keys - 1) {
                $header{$header_keys[$_]} = $header_values[$_];
            }
            last;
        }
    }
    close $fh ||
        $self->logcroak(q[Failed to close BNX path '], $self->path, q[']);
    foreach my $key (@REQUIRED_FIELDS) {
        if (! $header{$key}) {
            $self->logcroak(q[Required BNX header field '], $key,
                            q[' not found]);
        }
    }
    return \%header;
}

sub _build_md5sum {
    my ($self) = @_;
    my $md5 = Digest::MD5->new;
    my $fh;
    open $fh, '<', $self->path ||
        $self->logcroak(q[Failed to open BNX path '], $self->path, q[']);
    $md5->addfile($fh);
    close $fh ||
        $self->logcroak(q[Failed to close BNX path '], $self->path, q[']);
    return $md5->hexdigest;
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

Class to represent a BioNano BNX molecules file. Parses the file header to
find variables used for iRODS metadata.

=cut
