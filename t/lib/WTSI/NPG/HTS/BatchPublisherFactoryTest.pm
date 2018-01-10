package WTSI::NPG::HTS::BatchPublisherFactoryTest;

use strict;
use warnings;
use File::Temp qw[tempdir];
use Log::Log4perl;

use Test::More;
use Test::Exception;
use WTSI::NPG::iRODS;
use WTSI::NPG::HTS::BatchPublisherFactory;

use base qw[WTSI::NPG::HTS::TestRabbitMQ];

# Tests below do not require a RabbitMQ server, but *do* require the
# Net::AMQP::RabbitMQ module. The TEST_RABBITMQ variable defined in the
# base class WTSI::NPG::HTS::TestRabbitMQ can be used to skip this class,
# if Net::AMQP::RabbitMQ is not installed.

Log::Log4perl::init('./etc/log4perl_tests.conf');

sub require : Test(1) {
    require_ok('WTSI::NPG::HTS::BatchPublisherFactory');
}

sub make_publishers : Test(6) {

    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      strict_baton_version => 0);

    my $tmp = tempdir('BatchPublisherFactoryTest_temp_XXXXXX',
                      CLEANUP => 1);

    my $factory0 = WTSI::NPG::HTS::BatchPublisherFactory->new(
        enable_rmq         => 0,
        irods              => $irods,
        restart_file       => "$tmp/restart.json",
    );
    my $publisher0 = $factory0->make_batch_publisher();
    isa_ok($publisher0, 'WTSI::NPG::HTS::BatchPublisher');
    # ensure we have an instance of the parent class, not the subclass
    ok(!($publisher0->isa('WTSI::NPG::HTS::BatchPublisherWithReporting')),
       'Factory does not return a BatchPublisherWithReporting');

    my $factory1 = WTSI::NPG::HTS::BatchPublisherFactory->new(
        channel            => 42,
        enable_rmq         => 1,
        exchange           => 'foo',
        irods              => $irods,
        routing_key_prefix => 'bar',
        restart_file       => "$tmp/restart.json",
    );
    my $publisher1 = $factory1->make_batch_publisher();
    isa_ok($publisher1, 'WTSI::NPG::HTS::BatchPublisherWithReporting');
    is($publisher1->channel, 42, 'channel attribute is correct');
    is($publisher1->exchange, 'foo', 'exchange attribute is correct');
    is($publisher1->routing_key_prefix, 'bar',
       'routing_key_prefix attribute is correct');

}
