package WTSI::NPG::HTS::BatchPublisherWithReporting;

use namespace::autoclean;
use Moose;

our $VERSION = '';

extends 'WTSI::NPG::HTS::BatchPublisher';

with 'WTSI::NPG::iRODS::Reportable::PublisherMQ';

sub BUILD {
    my ($self, ) = @_;
    return $self->rmq_init();
}

sub DEMOLISH {
    my ($self, ) = @_;
    return $self->rmq_disconnect();
}


__PACKAGE__->meta->make_immutable;

no Moose;

1;



__END__

=head1 NAME

WTSI::NPG::HTS::BatchPublisherWithReporting

=head1 DESCRIPTION

A BatchPublisher class which reports file publication to a RabbitMQ server.

An instance is capable of publishing a list of files ("a batch") per
call to 'publish_file_batch'. The instance keeps track of the success
or failure of publishing each file it processes. Files which have
published successfully in any previous batch are skipped (they are
not even checked against iRODS for checksum matches and correct
metadata) unless the force attribute is set true.

=head1 AUTHOR

Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2018 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
