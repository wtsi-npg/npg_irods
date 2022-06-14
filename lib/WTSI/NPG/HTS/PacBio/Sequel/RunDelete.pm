package WTSI::NPG::HTS::PacBio::Sequel::RunDelete;

use namespace::autoclean;
use Moose;
use MooseX::StrictConstructor;
use English qw[-no_match_vars];

with qw[
         WTSI::DNAP::Utilities::Loggable
       ];

our $VERSION = '';

has 'runfolder_path' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'PacBio runfolder path');


=head2 delete_run

  Arg [1]    : Check format option for production runs
  Example    : $deleter->delete_run({runfolder_path => $path});
  Description: Return true if successfully deleted
  Returntype : Bool

=cut

sub delete_run {
    my ($self, $checkformat) = @_;

    defined $self->runfolder_path or
        $self->logconfess('A defined runfolder is required');

    ## touch first to check runfolder permissions
    my $touch = q{touch }. $self->runfolder_path;
    if (system($touch) != 0) {
        my $e = $CHILD_ERROR || q[];
        $self->logcroak(qq[Aborting deletion - error $e running "$touch"]);
    }

    ## then remove
    my $remove = q{rm -rf }. $self->runfolder_path;

    if (system($remove) != 0) {
        my $f = $CHILD_ERROR || q[];
        $self->logcroak(qq[Deletion failed - error $f running "$remove"]);
    }

    return -d $self->runfolder_path ? 0 : 1;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::PacBio::Sequel::RunDelete

=head1 DESCRIPTION

=head1 AUTHOR

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2020 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
