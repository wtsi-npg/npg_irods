package WTSI::NPG::HTS::SamtoolsTest;

use strict;
use warnings;
use Carp;
use English qw[-no_match_vars];
use Log::Log4perl;
use Test::More;
use Test::Exception;

use WTSI::NPG::HTS::Samtools qw[put_xam get_xam_header get_xam_records];
use WTSI::NPG::iRODS;

use base qw[WTSI::NPG::HTS::Test];

Log::Log4perl::init('./etc/log4perl_tests.conf');

my $pid = $PID;
my $test_counter = 0;

my $irods_tmp_coll;

my $data_path = './t/data/aln_data_object';
my $reference_file = 'test_ref.fa';
my $samtools_available = `which samtools`;

my $run7915_lane5_tag0 = '7915_5#0';

sub setup_test :Test(setup) {
    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      strict_baton_version => 0);

    $irods_tmp_coll =
        $irods->add_collection("HTS::SamtoolsTest.$pid.$test_counter");
    $test_counter++;
}

sub teardown_test :Test(teardown) {
    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      strict_baton_version => 0);
    $irods->remove_collection($irods_tmp_coll);
}

sub require :Test(1) {
    require_ok('WTSI::NPG::HTS::Samtools');
}

SKIP: {
    if (not $samtools_available) {
        skip 'samtools executable not on the PATH', 3;
    }

    sub test_put_xam :Test(3) {
        my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                          strict_baton_version => 0);

        for my $suffix (qw[sam bam cram]) {
            my $local_path = "$data_path/$run7915_lane5_tag0.sam";
            my $remote_path = "$irods_tmp_coll/$run7915_lane5_tag0.$suffix";
            my $reference_file = "$data_path/$reference_file";

            put_xam($local_path, $remote_path, $reference_file);

            my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $remote_path);
            ok($obj->is_present, "$suffix data object is present in iRODS");
        }
    }

    sub test_get_xam_header :Test(1) {
        my $local_path = "$data_path/$run7915_lane5_tag0.sam";
        my $remote_path = "$irods_tmp_coll/$run7915_lane5_tag0.sam";
        WTSI::DNAP::Utilities::Runnable->new
            (arguments  => ['-f', '-k', '-a', $local_path, $remote_path],
             executable => 'iput')->run;

        my $header = get_xam_header($remote_path);
        is_deeply($header, [
            '@SQ	SN:test_ref0	LN:1000',
            '@SQ	SN:test_ref1	LN:1000',
            '@SQ	SN:test_ref2	LN:1000',
            '@SQ	SN:test_ref3	LN:1000',
            '@SQ	SN:test_ref4	LN:1000',
            '@SQ	SN:test_ref5	LN:1000',
            '@SQ	SN:test_ref6	LN:1000',
            '@SQ	SN:test_ref7	LN:1000',
            '@SQ	SN:test_ref8	LN:1000',
            '@SQ	SN:test_ref9	LN:1000',
            '@PG	ID:bwa	PN:emacs	CL:emacs 7915_5#0.sam ./t/data/aln_data_object/test_ref.fa'
        ]) or diag explain $header;
    }

    sub test_get_xam_records :Test(2) {
        my $local_path = "$data_path/$run7915_lane5_tag0.sam";
        my $remote_path = "$irods_tmp_coll/$run7915_lane5_tag0.sam";
        WTSI::DNAP::Utilities::Runnable->new
            (arguments  => ['-f', '-k', '-a', $local_path, $remote_path],
             executable => 'iput')->run;

        my $header = get_xam_records($remote_path, 3);
        is_deeply($header, [
            'read.0	16	test_ref0	1	40	100M	*	0	0	AGCACCAGTAGCCAGACAACATTGCCAGGTATCGGTGAATTTAGTGCAATGCCAATTATTTCGCAGAAGGAGGCTTAATCGCTGAGTTTGTGGGGACAGT	IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII',
            'read.1	16	test_ref0	2	40	100M	*	0	0	GCACCAGTAGCCAGACAACATTGCCAGGTATCGGTGAATTTAGTGCAATGCCAATTATTTCGCAGAAGGAGGCTTAATCGCTGAGTTTGTGGGGACAGTC	IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII',
            'read.2	16	test_ref0	3	40	100M	*	0	0	CACCAGTAGCCAGACAACATTGCCAGGTATCGGTGAATTTAGTGCAATGCCAATTATTTCGCAGAAGGAGGCTTAATCGCTGAGTTTGTGGGGACAGTCG	IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII'
        ]) or diag explain $header;

        dies_ok { get_xam_records($remote_path, "Not a number") }
                'get_xam_records dies with non-numeric argument'
    }
}

1;

