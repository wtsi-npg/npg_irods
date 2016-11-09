package WTSI::NPG::OM::BioNano::BnxFileTest;

use strict;
use warnings;

use base qw[WTSI::NPG::HTS::Test]; # FIXME better path for shared base

use Test::More tests => 9;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::OM::BioNano::BnxFile'); }

my $data_path = './t/data/bionano/';
my $bnx_path = $data_path.'/MoleculesDummy.bnx';
my $bad_header_path = $data_path.'/MoleculesDummyBadHeader.bnx';

use WTSI::NPG::OM::BioNano::BnxFile;

sub construction : Test(4) {

    my $bnx1 = WTSI::NPG::OM::BioNano::BnxFile->new(path => $bnx_path);
    ok($bnx1, "BnxFile created with hash argument");

    is($bnx1->md5sum, 'd50fb6797f561e74ae2a5ae6e0258d16', 'BNX MD5 sum OK');

    my $bnx2 = WTSI::NPG::OM::BioNano::BnxFile->new($bnx_path);
    ok($bnx2, "BnxFile created with anonymous file path argument");

    sub read_bad_header {
        my $bnx =  WTSI::NPG::OM::BioNano::BnxFile->new($bad_header_path);
        my $header = $bnx->header;
    };
    dies_ok {read_bad_header()} "Dies with incorrect BNX header";

}

sub header : Test(4) {

    my $bnx = WTSI::NPG::OM::BioNano::BnxFile->new(path => $bnx_path);

    my $expected_header = {
          'LabelSNRFilterType' => 'Static',
          'BasesPerPixel' => '567.89',
          'Time' => '10/1/2016 12:00:37 PM',
          'StretchFactor' => '0.85',
          'Flowcell' => '1',
          'MinLabelSNR' => '0',
          '#rh' => '# Run Data',
          'NumberofScans' => '30',
          'NanoChannelPixelsPerScan' => '70000000',
          'ChipId' => '20000,10000,1/1/2015,987654321',
          'InstrumentSerial' => 'B001',
          'RunId' => '1',
          'SourceFolder' => 'C:\\Irys_Data\\2016-10\\Foo_2016-10-01_12_00\\Detect Molecules',
          'MinMoleculeLength' => '0'
        };

    is_deeply($bnx->header, $expected_header,
              "BNX header matches expected values");

    my $expected_chip_id = $expected_header->{'ChipId'};
    is($bnx->chip_id, $expected_chip_id, "Chip ID is '$expected_chip_id'");

    my $expected_instrument = $expected_header->{'InstrumentSerial'};
    is($bnx->instrument, $expected_instrument,
       "Instrument ID is '$expected_instrument'");

    my $expected_flowcell = $expected_header->{'Flowcell'};
    is($bnx->flowcell, $expected_flowcell,
       "Flowcell is '$expected_flowcell'");


}
