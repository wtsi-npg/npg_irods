package WTSI::NPG::HTS::SeqchksumTest;

use strict;
use warnings;
use Carp;
use English qw[-no_match_vars];
use Log::Log4perl;
use Test::More;
use Test::Exception;

use base qw[WTSI::NPG::HTS::Test];

Log::Log4perl::init('./etc/log4perl_tests.conf');

use WTSI::NPG::HTS::Seqchksum;

my $data_path = './t/data/seqchksum';

sub require : Test(1) {
  require_ok('WTSI::NPG::HTS::Seqchksum');
}

sub constructor : Test(3) {
  my $seqchksum_path = "$data_path/17550_1#1.seqchksum";

  open my $fh1, '<:encoding(utf8)', $seqchksum_path or
    die "Failed to open '$seqchksum_path' for reading: $ERRNO";
  new_ok('WTSI::NPG::HTS::Seqchksum', [fh => $fh1],
         'Construct from file handle');
  close $fh1;

  new_ok('WTSI::NPG::HTS::Seqchksum', [file_name => $seqchksum_path],
         'Construct from file name');

  open my $fh2, '<:encoding(utf8)', $seqchksum_path or
    die "Failed to open '$seqchksum_path' for reading: $ERRNO";
  dies_ok {
    WTSI::NPG::HTS::Seqchksum->new(fh        => $fh2,
                                   file_name => $seqchksum_path);
  } 'Fail when both file handle and file name passed';
  close $fh2;
}

sub records : Test(2) {
  my $expected = [{
                   'b_seq'                      => '323e8897',
                   'b_seq_qual'                 => '73464b1c',
                   'b_seq_tags(BC,FI,QT,RT,TC)' => '68baabcc',
                   'count'                      => 71488156,
                   'group'                      => 'all',
                   'name_b_seq'                 => '84f885e',
                   'set'                        => 'all'
                  },
                  {
                   'b_seq'                      => '6bf47dc5',
                   'b_seq_qual'                 => '4058c127',
                   'b_seq_tags(BC,FI,QT,RT,TC)' => '118c5f4e',
                   'count'                      => 71483150,
                   'group'                      => 'all',
                   'name_b_seq'                 => '6b010bfb',
                   'set'                        => 'pass'
                  },
                  {
                   'b_seq'                      => 1,
                   'b_seq_qual'                 => 1,
                   'b_seq_tags(BC,FI,QT,RT,TC)' => 1,
                   'count'                      => 0,
                   'group'                      => q[],
                   'name_b_seq'                 => 1,
                   'set'                        => 'all'
                  },
                  {
                   'b_seq'                      => 1,
                   'b_seq_qual'                 => 1,
                   'b_seq_tags(BC,FI,QT,RT,TC)' => 1,
                   'count'                      => 0,
                   'group'                      => q[],
                   'name_b_seq'                 => 1,
                   'set'                        => 'pass'
                  },
                  {
                   'b_seq'                      => '323e8897',
                   'b_seq_qual'                 => '73464b1c',
                   'b_seq_tags(BC,FI,QT,RT,TC)' => '68baabcc',
                   'count'                      => 71488156,
                   'group'                      => '1#1',
                   'name_b_seq'                 => '84f885e',
                   'set'                        => 'all'
                  },
                  {
                   'b_seq'                      => '6bf47dc5',
                   'b_seq_qual'                 => '4058c127',
                   'b_seq_tags(BC,FI,QT,RT,TC)' => '118c5f4e',
                   'count'                      => 71483150,
                   'group'                      => '1#1',
                   'name_b_seq'                 => '6b010bfb',
                   'set'                        => 'pass'
                  }];

  my $seqchksum_path = "$data_path/17550_1#1.seqchksum";

  open my $fh, '<:encoding(utf8)', $seqchksum_path or
    die "Failed to open '$seqchksum_path' for reading: $ERRNO";
  my $seqchksum1 = WTSI::NPG::HTS::Seqchksum->new(fh => $fh);
  close $fh;

  my $observed1 = $seqchksum1->records;
  is_deeply($observed1, $expected) or diag explain $observed1;

  my $seqchksum2 = WTSI::NPG::HTS::Seqchksum->new(file_name => $seqchksum_path);
  my $observed2 = $seqchksum2->records;
  is_deeply($observed2, $expected) or diag explain $observed2;
}

sub all_records : Test(1) {
  my $seqchksum_path = "$data_path/17550_1#1.seqchksum";
  my $seqchksum = WTSI::NPG::HTS::Seqchksum->new(file_name => $seqchksum_path);

  my $expected = [{
                   'b_seq'                      => '323e8897',
                   'b_seq_qual'                 => '73464b1c',
                   'b_seq_tags(BC,FI,QT,RT,TC)' => '68baabcc',
                   'count'                      => 71488156,
                   'group'                      => 'all',
                   'name_b_seq'                 => '84f885e',
                   'set'                        => 'all'
                  },
                  {
                   'b_seq'                      => '6bf47dc5',
                   'b_seq_qual'                 => '4058c127',
                   'b_seq_tags(BC,FI,QT,RT,TC)' => '118c5f4e',
                   'count'                      => '71483150',
                   'group'                      => 'all',
                   'name_b_seq'                 => '6b010bfb',
                   'set'                        => 'pass'
                  }];
  my @observed = $seqchksum->all_records;
  is_deeply(\@observed, $expected, 'expected all records')
    or diag explain \@observed;
}

sub read_groups : Test(1) {
  my $seqchksum_path = "$data_path/17550_1#1.seqchksum";
  my $seqchksum = WTSI::NPG::HTS::Seqchksum->new(file_name => $seqchksum_path);

  my @expected = ('1#1');
  my @observed = $seqchksum->read_groups;
  is_deeply(\@observed, \@expected, 'expected read groups')
    or diag explain \@observed;
}

sub read_group_records : Test(2) {
  my $expected = [{
                   'b_seq'                      => '323e8897',
                   'b_seq_qual'                 => '73464b1c',
                   'b_seq_tags(BC,FI,QT,RT,TC)' => '68baabcc',
                   'count'                      => 71488156,
                   'group'                      => '1#1',
                   'name_b_seq'                 => '84f885e',
                   'set'                        => 'all'
                  },
                  {
                   'b_seq'                      => '6bf47dc5',
                   'b_seq_qual'                 => '4058c127',
                   'b_seq_tags(BC,FI,QT,RT,TC)' => '118c5f4e',
                   'count'                      => 71483150,
                   'group'                      => '1#1',
                   'name_b_seq'                 => '6b010bfb',
                   'set'                        => 'pass'
                  }];

  my $seqchksum_path = "$data_path/17550_1#1.seqchksum";
  my $seqchksum = WTSI::NPG::HTS::Seqchksum->new(file_name => $seqchksum_path);

  my @observed = $seqchksum->read_group_records('1#1');
  is_deeply(\@observed, $expected, 'expected read group records')
    or diag explain \@observed;

  dies_ok {
    $seqchksum->read_group_records('no_such_group');
  } 'read_group_records fails on invalid read group';
}

sub digest : Test(3) {
  my $seqchksum_path = "$data_path/17550_1#1.seqchksum";
  my $seqchksum = WTSI::NPG::HTS::Seqchksum->new(file_name => $seqchksum_path);

  my $digest = $seqchksum->digest('1#1');
  is($digest, '323e8897=084f885e=73464b1c=68baabcc=' .
              '6bf47dc5=6b010bfb=4058c127=118c5f4e', 'expected digest')
    or diag explain $digest;

  my $all_digest = $seqchksum->digest($seqchksum->all_group);
  is($all_digest, '323e8897=084f885e=73464b1c=68baabcc=' .
                  '6bf47dc5=6b010bfb=4058c127=118c5f4e', 'expected all digest')
    or diag explain $all_digest;

  dies_ok {
    $seqchksum->digest('no_such_group');
  } 'digest fails on invalid read group';
}
