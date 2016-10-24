package WTSI::NPG::DataSub::SubtrackClientTest;

use strict;
use warnings;

use DBI;
use Test::MockObject::Extends;
use Log::Log4perl;
use Test::More;

use base qw[WTSI::NPG::HTS::Test];

Log::Log4perl::init('./etc/log4perl_tests.conf');

use WTSI::NPG::DataSub::SubtrackClient;

sub find_submitted_files : Test(2) {

  my @expected = ({
                   ebi_run_acc => "ERR1609600",
                   ebi_sub_acc => "ERA697039",
                   file_name   => "20222_6#184.cram",
                   md5         => "1577edf4b059c5fea738e51d59639264",
                   timestamp   => "2016-09-09"
                  },
                  {
                   ebi_run_acc => "ERR1609601",
                   ebi_sub_acc => "ERA697039",
                   file_name   => "20222_6#185.cram",
                   md5         => "8f26f32df3313b206354910f1529cb93",
                   timestamp   => "2016-09-09"
                  },
                  {
                   ebi_run_acc => "ERR1609602",
                   ebi_sub_acc => "ERA697039",
                   file_name   => "20222_6#186.cram",
                   md5         => "6c4de3a3d281839b4ecfde458e506675",
                   timestamp   => "2016-09-09"
                  });

  my $dbh = _make_mock_dbh(@expected);
  my $client = WTSI::NPG::DataSub::SubtrackClient->new(dbh => $dbh);

  # 2016-09-09T15:02:01
  my $begin_date = DateTime->from_epoch(epoch => 1473433321);
  my $end_date   = $begin_date;
  my @observed =
    map { {ebi_run_acc => $_->run_accession,
           ebi_sub_acc => $_->submission_accession,
           file_name   => $_->file_name,
           md5         => $_->submission_md5,
           timestamp   => $_->submission_date->ymd} }
    $client->query_submitted_files(begin_date => $begin_date,
                                   end_date   => $end_date);

  is_deeply(\@observed, \@expected, 'Query returned results')
    or diag explain \@observed;

  # Extract the SQL query argument passed
  my ($method_name, $method_call) = $dbh->prepare->next_call;
  my ($callee, @args) = @{$method_call};

  my @observed_args = @args;
  my @expected_args = map { $_->iso8601 } $begin_date, $end_date;

  is_deeply(\@observed_args, \@expected_args,
            'Query arguments bound to expected values')
    or diag explain \@observed_args;
}

sub _make_mock_dbh {
  my (@expected) = @_;

  my $sth = Test::MockObject->new;
  $sth->set_true('execute');
  $sth->set_series('fetchrow_hashref', @expected);

  my $dbh = DBI->connect('dbi:SQLite:dbname=:memory:', q[], q[]);
  $dbh = Test::MockObject::Extends->new($dbh);
  $dbh->set_always('prepare', $sth);

  return $dbh;
}

1;
