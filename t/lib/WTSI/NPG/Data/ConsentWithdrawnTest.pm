use strict;
use warnings;
use Test::More tests => 16;
use Test::Exception;
use Test::Deep;
use File::Temp qw/ tempdir  /;
use Log::Log4perl;
use Test::MockObject;
use JSON;
use Perl6::Slurp;

use WTSI::NPG::iRODS;

my $env_file = $ENV{'WTSI_NPG_iRODS_Test_IRODS_ENVIRONMENT_FILE'} || q[];
local $ENV{'IRODS_ENVIRONMENT_FILE'} = $env_file;

my $EXIST_EXECUTABLES = exist_irods_executables();
my $IRODS_ENV= 0;
if ($EXIST_EXECUTABLES && -e $env_file) {
  $IRODS_ENV = from_json(slurp $env_file);
}

Log::Log4perl::init_once('./t/log4perl_test.conf');
my $logger = Log::Log4perl->get_logger(q[]);
my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0, logger => $logger);

use_ok('npg_common::irods::BamConsentWithdrawn');

isa_ok(npg_common::irods::BamConsentWithdrawn->new(irods => $irods),
       'npg_common::irods::BamConsentWithdrawn');

is(npg_common::irods::BamConsentWithdrawn->new(irods => $irods)->_iquery,
 q{iquest --no-page -z seq "%s/%s" "select COLL_NAME, DATA_NAME where META_DATA_ATTR_NAME = 'sample_consent_withdrawn' and META_DATA_ATTR_VALUE = '1' and DATA_NAME not like '%header.bam%'"}, 'default iquery');

is(npg_common::irods::BamConsentWithdrawn->new(irods => $irods,
                                               zone  => undef)->_iquery,
 q{iquest --no-page "%s/%s" "select COLL_NAME, DATA_NAME where META_DATA_ATTR_NAME = 'sample_consent_withdrawn' and META_DATA_ATTR_VALUE = '1' and DATA_NAME not like '%header.bam%'"}, 'iquery without zone');

my $tdir = tempdir( CLEANUP => 1 );
my @path = split '/', $tdir;

my $IZONE= $IRODS_ENV ? $IRODS_ENV->{'irods_zone_name'} : undef;
my $username = $IRODS_ENV ? $IRODS_ENV->{'irods_user_name'} : q[];
my $imeta = "imeta";
my $ichmod = "ichmod";
my $OTHER_USER = 'public';

my $IRODS_TEST_AREA = '';
if ($IRODS_ENV and $IRODS_ENV->{'irods_home'}) {
  my $coll = join q{/}, $IRODS_ENV->{'irods_home'}, pop @path;
  $IRODS_TEST_AREA = create_irods_test_area($coll);
}

SKIP: {

    if ( !$EXIST_EXECUTABLES ) {
        skip 'unable to access iRODS executables', 12;
    } elsif (!$IRODS_ENV or !$IRODS_TEST_AREA) { 
        skip 'unable to create iRODs test area (try kinit to log in to iRODS)', 12;
    }

    my $util = npg_common::irods::BamConsentWithdrawn->new
      (irods => $irods)->_util;
    my @bam_files = qw /1.bam 2.bam 3.bam 5.cram 6.cram 7.cram/;
    my @files = ();
    push @files, @bam_files, qw /1.bai 3.bai 4.bai 5.cram.crai 7.cram.crai 8.cram.crai/;
    foreach my $file (@files) {
      my $source = "$tdir/$file";
      `touch $source`;
      my $target = "$IRODS_TEST_AREA/$file";
      my $iput = "iput";
      `$iput $source $target`;
      $util->file_exists($target) or die "Cannot create $target";
    }

    my $b = npg_common::irods::BamConsentWithdrawn->new
      (dry_run       => 1,
       new_bam_files =>
       ["$IRODS_TEST_AREA/1.bam", "$IRODS_TEST_AREA/2.bam",
        "$IRODS_TEST_AREA/3.bam",
        "$IRODS_TEST_AREA/5.cram", "$IRODS_TEST_AREA/6.cram",
        "$IRODS_TEST_AREA/7.cram"],
       irods   => $irods);

    is(join(q[ ],@{$b->new_files}),
       "$IRODS_TEST_AREA/1.bam $IRODS_TEST_AREA/1.bai $IRODS_TEST_AREA/2.bam $IRODS_TEST_AREA/3.bam $IRODS_TEST_AREA/3.bai ".
       "$IRODS_TEST_AREA/5.cram $IRODS_TEST_AREA/5.cram.crai $IRODS_TEST_AREA/6.cram $IRODS_TEST_AREA/7.cram $IRODS_TEST_AREA/7.cram.crai",
       'bai file found');
    lives_ok {$b->process} q[dry run for 'process' when no file has sample_consent_withdrawn_email_sent flag set];

    `$imeta add -d  $IRODS_TEST_AREA/1.bam sample_consent_withdrawn "1"`;
    `$imeta add -d  $IRODS_TEST_AREA/2.bam sample_consent_withdrawn "1"`;
    `$imeta add -d  $IRODS_TEST_AREA/3.bam sample_consent_withdrawn "1"`;
    `$imeta add -d  $IRODS_TEST_AREA/3.bam sample_consent_withdrawn_email_sent "1"`;
    `$imeta add -d  $IRODS_TEST_AREA/5.cram sample_consent_withdrawn "1"`;
    `$imeta add -d  $IRODS_TEST_AREA/6.cram sample_consent_withdrawn "1"`;
    `$imeta add -d  $IRODS_TEST_AREA/7.cram sample_consent_withdrawn "1"`;
    `$imeta add -d  $IRODS_TEST_AREA/7.cram sample_consent_withdrawn_email_sent "1"`;
     my $other = join q[#], $OTHER_USER, $IZONE;
    `$ichmod write $other $IRODS_TEST_AREA/1.bam`;

    my $found = ["$IRODS_TEST_AREA/1.bam", "$IRODS_TEST_AREA/2.bam", "$IRODS_TEST_AREA/3.bam",
                 "$IRODS_TEST_AREA/5.cram", "$IRODS_TEST_AREA/6.cram", "$IRODS_TEST_AREA/7.cram"];

    $b = npg_common::irods::BamConsentWithdrawn->new
      (dry_run    => 1,
       bam_files => $found,
       irods     => $irods);
    is(join(q[ ],@{$b->new_bam_files}), "$IRODS_TEST_AREA/1.bam $IRODS_TEST_AREA/2.bam $IRODS_TEST_AREA/5.cram $IRODS_TEST_AREA/6.cram",
        'bam files with sample_consent_withdrawn_email_sent flag not set found');
    is(join(q[ ],@{$b->new_files}),
        "$IRODS_TEST_AREA/1.bam $IRODS_TEST_AREA/1.bai $IRODS_TEST_AREA/2.bam $IRODS_TEST_AREA/5.cram $IRODS_TEST_AREA/5.cram.crai $IRODS_TEST_AREA/6.cram",
        'full file list');
    lives_ok {$b->_create_rt_ticket} 'dry run for creating an rt ticket';
    lives_ok {$b->process} q[dry run for 'process'];

    my $mock = Test::MockObject->new();
    $mock->fake_new( 'MIME::Lite' );
    $mock->set_true('send');
    my $user = join q[#], $username, $IZONE;

    $b = npg_common::irods::BamConsentWithdrawn->new(irods      => $irods,
                                                     zone       => undef,
                                                     collection => $IRODS_TEST_AREA);
    lives_ok {$b->process} q[live run for 'process'];

    cmp_deeply($b->_util->get_permissions("$IRODS_TEST_AREA/1.bam"),
          {'own' => [$user]}, 'permissions restricted correctly');
    my $get_meta_cmd = "$imeta ls -d $IRODS_TEST_AREA/1.bam";
    my $meta = `$get_meta_cmd`;
    ok($meta =~ /sample_consent_withdrawn_email_sent/, 'sample_consent_withdrawn_email_sent flag set') or diag explain $meta;

    ok(npg_common::irods::BamConsentWithdrawn::_rt_ticket_exists(
                    $util->_check_meta_data("$IRODS_TEST_AREA/1.bam")), 'set flag is recognised');

    $b = npg_common::irods::BamConsentWithdrawn->new(bam_files => $found,
                                                     irods     => $irods);
    ok(!@{$b->new_files}, 'no files to process');
    lives_ok {$b->process} q[live run for 'process' where no files found];
};

exit;

sub exist_irods_executables {
   return 0 unless `which ienv`;
   return 0 unless `which imkdir`;
   return 1;
}

sub create_irods_test_area {
  my ($dir) = @_;
  system("imkdir $dir") == 0 or return 0;

  return $dir;
}

END {
  local $ENV{'IRODS_ENVIRONMENT_FILE'} = $env_file;
  if ($IRODS_ENV) {
    my @commands = (
      "$imeta rmw -d  $IRODS_TEST_AREA/1.bam sample_consent_withdrawn %",
      "$imeta rmw -d  $IRODS_TEST_AREA/1.bam sample_consent_withdrawn_email_sent %",
      "$imeta rmw -d  $IRODS_TEST_AREA/2.bam sample_consent_withdrawn %",
      "$imeta rmw -d  $IRODS_TEST_AREA/2.bam sample_consent_withdrawn_email_sent %",
      "$imeta rmw -d  $IRODS_TEST_AREA/3.bam sample_consent_withdrawn %",
      "$imeta rmw -d  $IRODS_TEST_AREA/3.bam sample_consent_withdrawn_email_sent %",
      "$imeta rmw -d  $IRODS_TEST_AREA/5.cram sample_consent_withdrawn %",
      "$imeta rmw -d  $IRODS_TEST_AREA/5.cram sample_consent_withdrawn_email_sent %",
      "$imeta rmw -d  $IRODS_TEST_AREA/6.cram sample_consent_withdrawn %",
      "$imeta rmw -d  $IRODS_TEST_AREA/6.cram sample_consent_withdrawn_email_sent %",
      "$imeta rmw -d  $IRODS_TEST_AREA/7.cram sample_consent_withdrawn %",
      "$imeta rmw -d  $IRODS_TEST_AREA/7.cram sample_consent_withdrawn_email_sent %",
      "irm -r $IRODS_TEST_AREA"
                   );
    foreach my $command (@commands) {
      eval {system($command)};
    }
  }
}

1;
