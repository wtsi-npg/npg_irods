package WTSI::DNAP::Utilities::ParamsTest;

use strict;
use warnings;

use Log::Log4perl;
use Test::Exception;
use Test::More;

use base qw[WTSI::NPG::HTS::Test];

Log::Log4perl::init('./etc/log4perl_tests.conf');

use WTSI::DNAP::Utilities::Params qw[function_params];

sub has_named_params : Test(3) {
  my $params0 = function_params(0);
  ok(!$params0->has_named_params, 'No named params');

  my $params1 = function_params(0, 'a');
  ok($params1->has_named_params, '1 named params');

  my $params2 = function_params(0, 'a', 'b');
  ok($params2->has_named_params, '2 named params');
}

sub has_positional_params : Test(3) {
  my $params0 = function_params(0);
  ok(!$params0->has_positional_params, 'No positional params');

  my $params1 = function_params(1);
  ok($params1->has_positional_params, '1 positional params');

  my $params2 = function_params(2);
  ok($params2->has_positional_params, '2 positional params');
}

sub positional_arg_parsing : Test(12) {
  my $params0 = function_params(0);
  my @args0 = $params0->parse;

  is_deeply($params0->arguments, [],
            'arguments: 0 positional defined, 0 provided');
  is_deeply(\@args0, [],
            'parsed args: 0 positional defined, 0 provided');
  dies_ok { $params0->arg_at(0) }
    'arg_at: 0 positional defined, 0 provided';

  my $params1 = function_params(1);
  my @args1 = $params1->parse;
  is_deeply($params1->arguments, [],
            'arguments: 1 positional defined, 0 provided');
  is_deeply(\@args1, [undef],
            'parsed args: 1 positional defined, 0 provided');
  is($params1->arg_at(0), undef,
     'arg_at: 1 positional defined, 0 provided');

  my @argsa = $params1->parse('a');
  is_deeply($params1->arguments, ['a'],
            'arguments: 1 positional defined, 1 provided');
  is_deeply(\@argsa, ['a'],
            'parsed args: 1 positional defined, 1 provided');
  is($params1->arg_at(0), 'a',
     'arg_at: 1 positional defined, 1 provided');

  my @argsab;
  {
    local $SIG{__WARN__} = sub {
      # Discard warning
    };

    @argsab = $params1->parse('a', 'b');
  }

  is_deeply($params1->arguments, ['a', 'b'],
            'arguments: 1 positional defined, 2 provided');
  is_deeply(\@argsab, ['a'],
            'parsed args: 1 positional defined, 2 provided');
  dies_ok { $params1->arg_at(1) }
    'arg_at: 1 positional defined, 2 provided, 2nd is out of bounds';
}

sub named_arg_parsing : Test(7) {
  my $params0 = function_params(0, 'a', 'b');
  $params0->parse;

  is_deeply($params0->arguments, [],
            'arguments: 2 named defined, 0 provided');
  is($params0->a, undef, 'named arg a not provided');
  is($params0->b, undef, 'named arg b not provided');

  dies_ok { $params0->c } 'invalid named argument';

  $params0->parse(a => 99, b => 100);
  is_deeply($params0->arguments, [a => 99, b => 100],
            'arguments: 2 named defined, 2 provided');
  is($params0->a, 99, 'named arg a provided and correct');
  is($params0->b, 100, 'named arg b provided and correct');
}

1;
