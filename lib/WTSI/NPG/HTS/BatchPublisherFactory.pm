package WTSI::NPG::HTS::BatchPublisherFactory;

use strict;
use warnings;
use Moose;

use WTSI::NPG::HTS::BatchPublisher;

with qw [WTSI::NPG::iRODS::Reportable::ConfigurableForRabbitMQ
         WTSI::DNAP::Utilities::Loggable
    ];

our $VERSION = '';


has 'force' =>
  (isa           => 'Bool',
   is            => 'ro',
   required      => 0,
   default       => 0,
   documentation => 'Force re-publication of files that have been published');

has 'irods' =>
  (isa           => 'WTSI::NPG::iRODS',
   is            => 'ro',
   required      => 1,
   documentation => 'An iRODS handle to run searches and perform updates');

has 'max_errors' =>
  (isa       => 'Int',
   is        => 'ro',
   required  => 0,
   predicate => 'has_max_errors',
   documentation => 'The maximum number of errors permitted before ' .
                    'the remainder of a publishing process is aborted');

has 'obj_factory' =>
  (does          => 'WTSI::NPG::HTS::DataObjectFactory',
   is            => 'ro',
   required      => 1,
   lazy          => 1,
   builder       => '_build_obj_factory',
   documentation => 'A factory building data objects from files');

has 'require_checksum_cache' =>
  (is            => 'ro',
   isa           => 'ArrayRef[Str]',
   required      => 1,
   default       => sub { return [qw[bam cram]] },
   documentation => 'A list of file suffixes for which MD5 cache files ' .
                    'must be provided and will not be created on the fly');

has 'restart_file' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'A file containing a record of files successfully ' .
                    'published');



=head2 make_batch_publisher

  Args [n]   : Arguments for creation of the BatchPublisher object.

  Example    : my $publisher = $factory->make_batch_publisher();

  Description: Factory for creating BatchPublisher objects of an appropriate
               class, depending if RabbitMQ messaging is enabled. Arguments
               for Publisher construction are derived from class attributes.

  Returntype : WTSI::NPG::HTS::BatchPublisher or
               WTSI::NPG::HTS::BatchPublisherWithReporting

=cut

sub make_batch_publisher {

    my ($self, ) = @_;
    my @args;
    if ($self->enable_rmq) {
        push @args, 'enable_rmq'         => 1;
        push @args, 'channel'            => $self->channel;
        push @args, 'exchange'           => $self->exchange;
        push @args, 'routing_key_prefix' => $self->routing_key_prefix;
    }
    if ($self->has_max_errors) {
        push @args, 'max_errors'         => $self->max_errors;
    }
    push @args, 'force'                  => $self->force;
    push @args, 'irods'                  => $self->irods;
    push @args, 'obj_factory'            => $self->obj_factory;
    push @args, 'require_checksum_cache' => $self->require_checksum_cache;
    push @args, 'state_file'             => $self->restart_file;

    my $batch_pub;
    if ($self->enable_rmq) {
        # 'require' ensures BatchPublisherWithReporting not used unless wanted
        # eg. prerequisite module Net::AMQP::RabbitMQ may not be installed
        require WTSI::NPG::HTS::BatchPublisherWithReporting;
        $batch_pub = WTSI::NPG::HTS::BatchPublisherWithReporting->new(@args);
    } else {
        $batch_pub = WTSI::NPG::HTS::BatchPublisher->new(@args);
    }
    return $batch_pub;
}

sub _build_obj_factory {
  my ($self) = @_;

  return WTSI::NPG::HTS::DefaultDataObjectFactory->new(irods => $self->irods);
}


no Moose;

1;



__END__

=head1 NAME

WTSI::NPG::HTS::BatchPublisherFactory

=head1 DESCRIPTION

A Role for creating BatchPublisher objects of an appropriate class:

=over

=item

WTSI::NPG::HTS::BatchPublisherWithReporting if RabbitMQ is enabled;

=item

WTSI::NPG::HTS::BatchPublisher otherwise.

=back


RabbitMQ is enabled if the attribute enable_rmq is true; disabled otherwise.


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
