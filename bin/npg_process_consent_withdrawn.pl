#!/usr/bin/env perl

use strict;
use warnings;
use FindBin qw($Bin);
use lib ( -d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib" );

use Log::Log4perl;
use WTSI::NPG::iRODS;
use WTSI::NPG::Data::ConsentWithdrawn;

our $VERSION = '0';

my $log4perl_config =<< 'CONFIG';
log4perl.logger.dnap.npg.irods = ERROR, A1

log4perl.appender.A1           = Log::Log4perl::Appender::Screen
log4perl.appender.A1.utf8      = 1
log4perl.appender.A1.layout    = Log::Log4perl::Layout::PatternLayout
log4perl.appender.A1.layout.ConversionPattern = %d %-5p %c %M - %m%n
CONFIG

Log::Log4perl::init_once(\$log4perl_config);

my $logger = Log::Log4perl->get_logger('dnap.npg.irods');
my $irods = WTSI::NPG::iRODS->new(logger => $logger);

npg_common::irods::BamConsentWithdrawn
  ->new_with_options(irods => $irods)->process();

exit 0;

__END__

=head1 NAME

npg_process_consent_withdrawn.pl

=head1 USAGE

npg_process_consent_withdrawn.pl

or, just to report actions,

npg_process_consent_withdrawn.pl --dry_run

=head1 CONFIGURATION

=head1 SYNOPSIS

=head1 DESCRIPTION

Identifies bam or cram files with sample_consent_withdrawn flag set
and sample_consent_withdraw_email_sent flag not set.

Withdraws all permissions for these files and their indexes for groups
and individuals that are not owners of the files.

Creates an RT ticket listing the files,
sets the sample_consent_withdraw_email_sent flag for the files.

=head1 SUBROUTINES/METHODS

=head1 REQUIRED ARGUMENTS

=head1 OPTIONS

  --zone - an optional iRODS zone, defaults to /seq
  
  --collection - an optional iRODS collection

=head1 EXIT STATUS

=head1 REQUIRED ARGUMENTS

=head1 OPTIONS

=head1 EXIT STATUS

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item strict

=item warnings

=item FindBin

=item Log::Log4perl

=item WTSI::NPG::iRODS

=item npg_common::irods::run::BamConsentWithdrawn

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

MArina Gourtovaia E<lt>mg8@sanger.ac.ukE<gt>

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2012,2013,2015,2020 Genome Research Ltd.

This file is part of NPG.

NPG is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

=cut

