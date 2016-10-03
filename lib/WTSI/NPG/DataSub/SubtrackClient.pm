package WTSI::NPG::DataSub::SubtrackClient;

use namespace::autoclean;
use DBI;
use DateTime::Format::ISO8601;
use DateTime;
use Moose;

our $VERSION = '';

use WTSI::DNAP::Utilities::Params qw[function_params];
use WTSI::NPG::DataSub::File;

with qw[
         WTSI::DNAP::Utilities::Loggable
         npg_tracking::util::db_config
       ];

has 'dbh' =>
  (isa           => 'DBI::db',
   is            => 'ro',
   required      => 1,
   builder       => '_build_dbh',
   lazy          => 1,
   documentation => 'The database handle');

has 'default_interval' =>
  (isa           => 'Int',
   is            => 'ro',
   required      => 1,
   default       => 7,
   documentation => 'The default number of days activity to report');

=head2 query_submitted_files

  Arg [1]    : None

  Named args : begin_date           Date beginning the query interval,
                                    DateTime. Optional, defaults to the
                                    default_interval number of days (7)
                                    before the end date.

               end_date             Date ending the query interval,
                                    DateTime. Optional, the current date.

  Example    : my @submitted_files = $client->query_submitted_files;
               my @submitted_files =
                 $client->query_submitted_files(begin_date => $begin,
                                                end_date   => $end_date);

  Description: Return the paths of files submitted to the EBI during the
               specified interval.
  Returntype : Array[WTSI::NPG::HTS::DataSub::File]

=cut

{
  my $positional = 1;
  my @named      = qw[begin_date end_date];
  my $params = function_params($positional, @named);

  sub query_submitted_files {
    my ($self) = $params->parse(@_);

    my $end   = $params->end_date    ? $params->end_date   : DateTime->now;
    my $begin = $params->begin_date  ? $params->begin_date :
      DateTime->from_epoch(epoch => $end->epoch)->subtract
      (days => $self->default_interval);

    return $self->_do_query($begin, $end);
  }
}

sub _build_dbh {
  my ($self) = @_;

  $self->debug(q[Using database config in ], $self->config_file, q[']);
  $self->debug(q[Connecting to '], $self->dsn, q[' as user '],
               $self->dbuser, q[']);

  return DBI->connect($self->dsn, $self->dbuser, $self->dbpass, $self->dbattr);
}

sub _do_query {
  my ($self, $begin_date, $end_date) = @_;

  my $sql = <<'SQL';
   SELECT
       sub.ebi_run_acc,
       sub.ebi_sub_acc,
       file.file_name,
       file.md5,
       DATE(stat.timestamp) timestamp
   FROM submission sub
   JOIN sub_status stat ON (stat.id = sub.id AND stat.is_current = 'Y')
   JOIN files file ON (sub.id = file.sub_id)
   WHERE (stat.status = 'SD' OR stat.status = 'P')
   AND DATE(stat.timestamp) >= ?
   AND DATE(stat.timestamp) <= ?
SQL

  my $sth = $self->dbh->prepare($sql);

  $self->debug('Executing query with arguments ',
               "begin_date: $begin_date, end_date: $end_date");

  $sth->execute($begin_date->iso8601, $end_date->iso8601)
    or $self->logcroak($sth->errstr);

  my @rows;
  while (my $row = $sth->fetchrow_hashref) {
    my $timestamp = DateTime::Format::ISO8601->parse_datetime
      ($row->{timestamp});
    push @rows, WTSI::NPG::DataSub::File->new
      (submission_accession => $row->{ebi_sub_acc},
       run_accession        => $row->{ebi_run_acc},
       file_name            => $row->{file_name},
       submission_md5       => $row->{md5},
       submission_date      => $timestamp);
  }

  $self->info('Found ', scalar @rows, ' files updated between ',
              $begin_date->iso8601, ' and ', $end_date->iso8601);

  return @rows;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__


=head1 NAME

WTSI::NPG::DataSub::SubtrackClient

=head1 DESCRIPTION

A client to query the subtrack database for information on files
submitted to the EBI.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

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

=cut
