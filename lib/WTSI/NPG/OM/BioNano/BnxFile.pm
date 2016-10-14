package WTSI::NPG::OM::BioNano::BnxFile;

use Moose;

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


around BUILDARGS => sub {
  my ($orig, $class, @args) = @_;

  # Permit a Str as an anonymous argument mapping to path
  if (@args == 1 and !ref $args[0]) {
    return $class->$orig(path => $args[0]);
  }
  else {
    return $class->$orig(@_);
  }
};

sub BUILD {
    my ($self,) = @_;
    if (! -r $self->path) {
        $self->logcroak("Cannot read BNX path '", $self->path, "'");
    }
}

sub _build_header {
    my ($self,) = @_;
    my @header_keys;
    my %header;
    open my $fh, '<', $self->path ||
        $self->logcroak("Failed to open BNX path '", $self->path, "'");
    while (<$fh>) {
        chomp;
        if ($_ =~ /^\#\ BNX\ File\ Version:\t/msx) {
            my @fields = split "\t";
            my $version = pop @fields;
            if ($version !~ /1\.[012]/msx) {
                $self->logwarn("Unsupported BNX version number: '",
                               $version, "'");
            }
        } elsif (m/^\#rh/msx) {
             @header_keys = split "\t";
         } elsif (@header_keys && m/^\#\ Run\ Data\t/msx) {
             my @header_values = split "\t";
             if (scalar @header_keys != scalar @header_values) {
                 $self->logcroak("Numbers of keys and values in BNX ",
                                 "file header do not match");
             }
             for (my $i=0;$i<@header_keys;$i++) {
                 $header{$header_keys[$i]} = $header_values[$i];
             }
         } elsif (%header) {
             last;
         }
    }
    close $fh ||
        $self->logcroak("Failed to close BNX path '", $self->path, "'");
     foreach my $key (@REQUIRED_FIELDS) {
         if (! $header{$key}) {
             $self->logcroak("Required BNX header field '", $key,
                             "' not found");
         }
     }
    return \%header;
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
