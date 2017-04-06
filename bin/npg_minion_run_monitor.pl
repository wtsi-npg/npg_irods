#!/usr/bin/env perl

use strict;
use warnings;

use FindBin qw[$Bin];
use lib (-d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib");

use Carp;
use Data::Dump qw[pp];
use File::Spec::Functions qw[abs2rel splitpath];
use Getopt::Long;
use Log::Log4perl qw[:levels];
use Pod::Usage;

use WTSI::NPG::HTS::ONT::MinIONRunMonitor;

our $VERSION = '';

##no critic (ValuesAndExpressions::ProhibitMagicNumbers)
my $debug;
my $log4perl_config;
my $staging_path;
my $tar_capacity = 10_000;
my $tar_timeout  = 60 * 5;
my $verbose;
#use critic

GetOptions('debug'                       => \$debug,
           'help'                        => sub {
             pod2usage(-verbose => 2,
                       -exitval => 0)
           },
           'logconf=s'                   => \$log4perl_config,
           'staging_path|staging-path=s' => \$staging_path,
           'tar_capacity|tar-capacity=i' => \$tar_capacity,
           'tar_timeout|tar-timeout=i'   => \$tar_timeout,
           'verbose'                     => \$verbose);

if ($log4perl_config) {
  Log::Log4perl::init($log4perl_config);
}
else {
  my $level = $debug ? $DEBUG : $verbose ? $INFO : $WARN;
  Log::Log4perl->easy_init({layout => '%d %-5p %c - %m%n',
                            level  => $level,
                            utf8   => 1});
}

my $log = Log::Log4perl->get_logger('main');
if ($log4perl_config) {
  $log->info("Using log config file '$log4perl_config'");
}

my $monitor = WTSI::NPG::HTS::ONT::MinIONRunMonitor->new
  (staging_path => $staging_path,
   tar_capacity => $tar_capacity,
   tar_timeout  => $tar_timeout);

$monitor->start;
