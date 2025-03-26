package WTSI::NPG::HTS::HeaderParserTest;

use strict;
use warnings;
use Carp;
use English qw[-no_match_vars];
use Log::Log4perl;
use Test::More;
use Test::Exception;

use base qw[WTSI::NPG::HTS::Test];

Log::Log4perl::init('./etc/log4perl_tests.conf');

use WTSI::NPG::HTS::HeaderParser;

my $data_path    = './t/data/header_parser';

sub require : Test(1) {
  require_ok('WTSI::NPG::HTS::HeaderParser');
}

sub get_records : Test(4) {
  my $header_path = "$data_path/17550_1#1.txt";
  my $parser = WTSI::NPG::HTS::HeaderParser->new;

  my @all_records = $parser->get_records(read_file($header_path));
  cmp_ok(scalar @all_records, '==', 1155, 'get_records from string') or
    diag explain \@all_records;
  cmp_ok(scalar $parser->get_records(split_lines(read_file($header_path))),
         '==', 1155, 'get_records from array');

  my @sq_records = $parser->get_records(read_file($header_path), 'SQ');
  cmp_ok(scalar @sq_records, '==', 1133, 'get_records from string with tag') or
    diag explain \@sq_records;
  cmp_ok(scalar $parser->get_records(split_lines(read_file($header_path)), 'SQ'),
         '==', 1133, 'get_records from array with tag');
}

sub get_tag_values : Test(1) {
  my $parser = WTSI::NPG::HTS::HeaderParser->new;

  my @observed = $parser->get_tag_values("\@RG\tID:1\tLB:aaa\tLB:bbb", 'LB');
  my $expected = ['LB:aaa', 'LB:bbb'];

  is_deeply(\@observed, $expected) or diag explain \@observed;
}

sub get_values :Test(1) {
  my $parser = WTSI::NPG::HTS::HeaderParser->new;

  my @observed = $parser->get_values("\@RG\tID:1\tLB:aaa\tLB:bbb", 'LB');
  my $expected = ['aaa', 'bbb'];

  is_deeply(\@observed, $expected) or diag explain \@observed;
}

sub get_unique_value : Test(2) {
  my $parser = WTSI::NPG::HTS::HeaderParser->new;

  my @observed = $parser->get_unique_value("\@RG\tID:1\tLB:aaa\tXY:bbb", 'LB');
  my $expected = ['aaa'];

  is_deeply(\@observed, $expected) or diag explain \@observed;

  dies_ok {
    $parser->get_unique_value("\@RG\tID:1\tLB:aaa\tLB:bbb", 'LB');
  } 'get_unique_value dies with multiple values';
}

sub pg_walk : Test(1) {
  my $header_path = "$data_path/17550_1#1.txt";
  my $parser = WTSI::NPG::HTS::HeaderParser->new;

  my @observed = $parser->pg_walk(read_file($header_path));
  my $expected =
    [[
      '@PG	ID:SCS	PN:HiSeq Control Software	DS:Controlling software on instrument	VN:2.2.68',
      '@PG	ID:basecalling	PN:RTA	PP:SCS	DS:Basecalling Package	VN:1.18.66.3',
      '@PG	ID:Illumina2bam	PN:Illumina2bam	PP:basecalling	DS:Convert Illumina BCL to BAM or SAM file	VN:V1.17	CL:uk.ac.sanger.npg.illumina.Illumina2bam INTENSITY_DIR=/nfs/sf55/ILorHSany_sf55/analysis/150910_HS40_17550_A_C75BCANXX/Data/Intensities BASECALLS_DIR=/nfs/sf55/ILorHSany_sf55/analysis/150910_HS40_17550_A_C75BCANXX/Data/Intensities/BaseCalls LANE=1 OUTPUT=/dev/stdout SAMPLE_ALIAS=ERS804057,ERS804058,ERS804059,ERS804060,ERS804061,ERS804062,ERS804063,ERS804064,ERS804077,ERS804078,ERS804079,ERS804080,ERS804081,ERS804082,ERS804083,ERS804084 STUDY_NAME=ERP011114: Total RNA was extracted from zebrafish embryos at 80% epiboly and genotyped for tcf3a and tcf3b to identify wild type, heterozygous and homozygous knockout embryos. The RNA was DNase treated.  Stranded RNAseq libraries were constructed using the Illumina TruSeq Stranded RNA protocol with oligo dT pulldown.  This data is part of a pre-publication release. For information on the proper use of pre-publication data shared by the Wellcome Trust Sanger Institute (including details of any publication moratoria), please see http://www.sanger.ac.uk/datasharing/ PLATFORM_UNIT=150910_HS40_17550_A_C75BCANXX_1 COMPRESSION_LEVEL=0    GENERATE_SECONDARY_BASE_CALLS=false PF_FILTER=true READ_GROUP_ID=1 LIBRARY_NAME=unknown SEQUENCING_CENTER=SC PLATFORM=ILLUMINA BARCODE_SEQUENCE_TAG_NAME=BC BARCODE_QUALITY_TAG_NAME=QT ADD_CLUSTER_INDEX_TAG=false VERBOSITY=INFO QUIET=false VALIDATION_STRINGENCY=STRICT MAX_RECORDS_IN_RAM=500000 CREATE_INDEX=false CREATE_MD5_FILE=false',
      '@PG	ID:bamadapterfind	PN:bamadapterfind	PP:Illumina2bam	VN:2.0.16	CL:bamadapterfind level=0',
      '@PG	ID:BamIndexDecoder	PN:BamIndexDecoder	PP:bamadapterfind	DS:A command-line tool to decode multiplexed bam file	VN:V1.17	CL:uk.ac.sanger.npg.picard.BamIndexDecoder INPUT=/dev/stdin OUTPUT=/dev/stdout BARCODE_FILE=/nfs/sf55/ILorHSany_sf55/analysis/150910_HS40_17550_A_C75BCANXX/Data/Intensities/BAM_basecalls_20150914-100512/metadata_cache_17550/lane_1.taglist METRICS_FILE=/nfs/sf55/ILorHSany_sf55/analysis/150910_HS40_17550_A_C75BCANXX/Data/Intensities/BAM_basecalls_20150914-100512/17550_1.bam.tag_decode.metrics VALIDATION_STRINGENCY=SILENT CREATE_MD5_FILE=false    BARCODE_TAG_NAME=BC BARCODE_QUALITY_TAG_NAME=QT MAX_MISMATCHES=1 MIN_MISMATCH_DELTA=1 MAX_NO_CALLS=2 CONVERT_LOW_QUALITY_TO_NO_CALL=false MAX_LOW_QUALITY_TO_CONVERT=15 VERBOSITY=INFO QUIET=false COMPRESSION_LEVEL=5 MAX_RECORDS_IN_RAM=500000 CREATE_INDEX=false',
      '@PG	ID:spf	PN:spatial_filter	PP:BamIndexDecoder	DS:A program to apply a spatial filter	VN:v10.23	CL:/software/solexa/pkg/pb_calibration/v10.23/bin/spatial_filter -c  -F pb_align_17550_1.bam.filter -t /nfs/sf55/ILorHSany_sf55/analysis/150910_HS40_17550_A_C75BCANXX/Data/Intensities/BAM_basecalls_20150914-100512/no_cal/archive/qc/tileviz/17550_1 --region_size 200 --region_mismatch_threshold 0.0160 --region_insertion_threshold 0.0160 --region_deletion_threshold 0.0160 pb_align_17550_1.bam ; /software/solexa/pkg/pb_calibration/v10.23/bin/spatial_filter -a  -f  -u  -F pb_align_17550_1.bam.filter -',
      '@PG	ID:bwa	PN:bwa	PP:spf	VN:0.5.10-tpx',
      '@PG	ID:BamMerger	PN:BamMerger	PP:bwa	DS:A command-line tool to merge BAM/SAM alignment info in the first input file with the data in an unmapped BAM file, producing a third BAM file that has alignment data and all the additional data from the unmapped BAM	VN:V1.17	CL:uk.ac.sanger.npg.picard.BamMerger ALIGNED_BAM=pb_align_17550_1.bam INPUT=/dev/stdin OUTPUT=17550_1.bam KEEP_EXTRA_UNMAPPED_READS=true REPLACE_ALIGNED_BASE_QUALITY=true VALIDATION_STRINGENCY=SILENT CREATE_MD5_FILE=true    ALIGNMENT_PROGRAM_ID=bwa KEEP_ALL_PG=false VERBOSITY=INFO QUIET=false COMPRESSION_LEVEL=5 MAX_RECORDS_IN_RAM=500000 CREATE_INDEX=false',
      '@PG	ID:SplitBamByReadGroup	PN:SplitBamByReadGroup	PP:BamMerger	DS:Split a BAM file into multiple BAM files based on ReadGroup. Headers are a copy of the original file, removing @RGs where IDs match with the other ReadGroup IDs	VN:V1.17	CL:uk.ac.sanger.npg.picard.SplitBamByReadGroup INPUT=/nfs/sf55/ILorHSany_sf55/analysis/150910_HS40_17550_A_C75BCANXX/Data/Intensities/BAM_basecalls_20150914-100512/no_cal/17550_1.bam OUTPUT_PREFIX=/nfs/sf55/ILorHSany_sf55/analysis/150910_HS40_17550_A_C75BCANXX/Data/Intensities/BAM_basecalls_20150914-100512/no_cal/lane1/17550_1 OUTPUT_COMMON_RG_HEAD_TO_TRIM=1 VALIDATION_STRINGENCY=SILENT CREATE_MD5_FILE=true    VERBOSITY=INFO QUIET=false COMPRESSION_LEVEL=5 MAX_RECORDS_IN_RAM=500000 CREATE_INDEX=false',
      '@PG	ID:bamcollate2	PN:bamcollate2	PP:SplitBamByReadGroup	VN:2.0.16	CL:/software/solexa/pkg/biobambam/2.0.16/bin/bamcollate2 collate=1 level=0',
      '@PG	ID:bamreset	PN:bamreset	PP:bamcollate2	VN:2.0.16	CL:/software/solexa/pkg/biobambam/2.0.16/bin/bamreset resetaux=0 level=0 verbose=0',
      '@PG	ID:bamadapterclip	PN:bamadapterclip	PP:bamreset	VN:2.0.16	CL:/software/solexa/pkg/biobambam/2.0.16/bin/bamadapterclip verbose=0 level=0',
      '@PG	ID:bwa\'	PN:bwa	PP:bamadapterclip	VN:0.7.12-r1039	CL:/software/solexa/pkg/bwa/bwa-0.7.12/bwa mem -t 16 -p -T 0 /lustre/scratch110/srpipe/references/Danio_rerio/zv9/all/bwa0_6/zv9_toplevel.fa /tmp/NGSVDwG58C/alntgt_bamtofastq_out',
      '@PG	ID:scramble	PN:scramble	PP:bwa\'	VN:1.14.0	CL:/software/solexa/pkg/scramble/1.14.0/bin/scramble -I sam -O bam ',
      '@PG	ID:bam12split	PN:bam12split	PP:scramble	VN:2.0.16	CL:/software/solexa/pkg/biobambam/2.0.16/bin/bam12split verbose=0 level=0',
      '@PG	ID:bamsort	PN:bamsort	PP:bam12split	VN:2.0.16	CL:/software/solexa/pkg/biobambam/2.0.16/bin/bamsort SO=queryname level=0',
      '@PG	ID:AlignmentFilter	PN:AlignmentFilter	PP:bamsort	DS:Give a list of SAM/BAM files with the same set of records and in the same order but aligned with different references, split reads into different files according to alignments. You have option to put unaligned reads into one of output files or a separate file	VN:V1.17	CL:uk.ac.sanger.npg.picard.AlignmentFilter INPUT_ALIGNMENT=[./initial_phix_aln_17550_1#1.bam, /tmp/DAxj5I1FJj/postalntgt_bam12auxmerge_out] OUTPUT_ALIGNMENT=[/tmp/VXWWMrWqut/alignment_filter:__PHIX_OUTBAM___out, /dev/stdout] METRICS_FILE=17550_1#1.bam_alignment_filter_metrics.json VERBOSITY=INFO QUIET=false VALIDATION_STRINGENCY=SILENT COMPRESSION_LEVEL=5 MAX_RECORDS_IN_RAM=500000 CREATE_INDEX=false CREATE_MD5_FILE=false   ',
      '@PG	ID:bamsort\'	PN:bamsort	CL:/software/solexa/pkg/biobambam/2.0.16/bin/bamsort SO=coordinate level=0 verbose=0 fixmate=1 adddupmarksupport=1 tmpfile=/nfs/sf55/ILorHSany_sf55/analysis/150910_HS40_17550_A_C75BCANXX/Data/Intensities/BAM_basecalls_20150914-100512/no_cal/archive/lane1/bsfopt_17550_1#1.tmp	PP:AlignmentFilter	VN:2.0.16',
      '@PG	ID:bamstreamingmarkduplicates	PN:bamstreamingmarkduplicates	CL:/software/solexa/pkg/biobambam/2.0.16/bin/bamstreamingmarkduplicates level=0 verbose=0 tmpfile=/nfs/sf55/ILorHSany_sf55/analysis/150910_HS40_17550_A_C75BCANXX/Data/Intensities/BAM_basecalls_20150914-100512/no_cal/archive/lane1/bmdfopt_17550_1#1.tmp M=/nfs/sf55/ILorHSany_sf55/analysis/150910_HS40_17550_A_C75BCANXX/Data/Intensities/BAM_basecalls_20150914-100512/no_cal/archive/lane1/17550_1#1.markdups_metrics.txt	PP:bamsort\'	VN:2.0.16',
      '@PG	ID:scramble.1	PN:scramble	PP:bamstreamingmarkduplicates	VN:1.14.0	CL:/software/solexa/pkg/scramble/1.14.0/bin/scramble -I bam -O cram -r /lustre/scratch110/srpipe/references/Danio_rerio/zv9/all/fasta/zv9_toplevel.fa '
     ]];

  is_deeply(\@observed, $expected) or diag explain \@observed;
}

sub alignment_reference : Test(4) {
  my $header_path = "$data_path/17550_1#1.txt";
  my $parser = WTSI::NPG::HTS::HeaderParser->new;

  is($parser->alignment_reference(read_file($header_path)),
     '/lustre/scratch110/srpipe/references/Danio_rerio/zv9/all/bwa0_6/zv9_toplevel.fa');

  $header_path = "$data_path/17550_3#1.txt";
  is($parser->alignment_reference(read_file($header_path)),
     '/lustre/scratch110/srpipe/references/Mus_musculus/GRCm38/all/bowtie2/Mus_musculus.GRCm38.68.dna.toplevel.fa');

  $header_path = "$data_path/20625_8#99.txt";
  is($parser->alignment_reference(read_file($header_path)),
     '/lustre/scratch117/core/sciops_repository/references/Plasmodium_falciparum/PF3K_Dd2v1/all/star');

  $header_path = "$data_path/37220_1#1.txt";
  is($parser->alignment_reference(read_file($header_path)),
     '/lustre/scratch120/npg_repository/references/Homo_sapiens/GRCh38_15_plus_hs38d1/all/minimap2/Homo_sapiens.GRCh38_15_plus_hs38d1.fa.mmi')
}

sub dehumanising_method : Test(64) {

  my $get_header = sub {
    my $file = shift;
    my $header =  `samtools view -H $file`;
    return $header;
  };

  my $parser = WTSI::NPG::HTS::HeaderParser->new();
  my $ref_root = q[/lustre/scratch120/npg_repository/references/];
  my $bowtie2_human_ref = q[Homo_sapiens/T2T-CHM13v2.0/all/bowtie2/T2T-CHM13v2.0.fa];
  my $bwa_human_ref = q[Homo_sapiens/1000Genomes/all/bwa0_6/human_g1k_v37.fasta];
  my $phix_ref = q[PhiX/Sanger-SNPs/all/minimap2/phix_unsnipped_short_no_N.fa.mmi];
  my $viral_ref = q[Mixed_virus/SarsCoV2_RSV_FLU/all/bwa0_6/SARS_RSV_FLU.fa];

  my $path = 't/data/dehumanised/run_50138/lane2';
  for my $plex ((0,1,3,5)) {

    my $file = join q[/], $path, "plex${plex}/50138_2#${plex}_human.cram";
    my $header = $get_header->($file);
    is ($parser->dehumanising_method($header), 'npg2025',
      "$file human split-put data, 2025 method, adapters clipped");
    is ($parser->alignment_reference($header),
      $ref_root . $bowtie2_human_ref, 'correct human reference');

    $file = join q[/], $path, "plex${plex}/50138_2#${plex}.cram";
    $header = $get_header->($file);
    is ($parser->dehumanising_method($header), 'see_human',
      "$file target file, pointer to the human data sibling file");
    if ($plex != 0) { # Tag0 is not aligned
      is ($parser->alignment_reference($header), $ref_root . $viral_ref,
        'correct viral reference');
    }

    $file = join q[/], $path, "plex${plex}/50138_2#${plex}_phix.cram";
    $header = $get_header->($file);
    is ($parser->dehumanising_method($header), undef,
      "$file PhiX split-out data, value undefined");
    is ($parser->alignment_reference($header), $ref_root . $phix_ref, 
      'correct PhiX reference');
  }
  my $f = join q[/], $path, 'plex888/50138_2#888.cram';
  my $h = $get_header->($f);
  is ($parser->dehumanising_method($h), undef,
    "$f PhiX control tag, value undefined");
  is ($parser->alignment_reference($h), $ref_root . $phix_ref,
    'correct PhiX reference');
  
  $path = 't/data/dehumanised/run_50138/lane1';
  for my $plex ((0,1,3,5)) {

    my $file = join q[/], $path, "plex${plex}/50138_1#${plex}_human.cram";
    my $header = $get_header->($file);
    is ($parser->dehumanising_method($header), 'npg2018nc',
      "$file human split-put data, 2018 method, adapters not clipped");
    is($parser->alignment_reference($header),
      $ref_root . $bwa_human_ref, 'correct human reference');

    $file = join q[/], $path, "plex${plex}/50138_1#${plex}.cram";
    $header = $get_header->($file);
    is ($parser->dehumanising_method($header), 'see_human',
      "$file target file, pointer to the human data sibling file");
    if ($plex != 0) { # Tag0 is not aligned
      is ($parser->alignment_reference($header), $ref_root . $viral_ref,
        'correct viral reference');
    }

    $file = join q[/], $path, "plex${plex}/50138_1#${plex}_phix.cram";
    is ($parser->dehumanising_method($get_header->($file)), undef,
      "$file PhiX split-out data, value undefined");
  }
  $f = join q[/], $path, 'plex888/50138_1#888.cram';
  is ($parser->dehumanising_method($get_header->($f)), undef,
    "$f PhiX control tag, value undefined");

  $path = 't/data/dehumanised/run_49822/lane1';
  $f = join q[/], $path, "49822_1#9_human.cram";
  is ($parser->dehumanising_method($get_header->($f)), 'npg2018',
    "$f human split-put data, 2018 method, adapters clipped");
  $f = join q[/], $path, "49822_1#9.cram";
  is ($parser->dehumanising_method($get_header->($f)), 'see_human',
    "$f target file, pointer to the human data sibling file");
  $f = join q[/], $path, "49822_1#9_phix.cram";
  is ($parser->dehumanising_method($get_header->($f)), undef,
    "$f PhiX split-out data, value undefined");

  #######
  # Historic cases, important for back-populating iRODS metadata.
  #

  $path = 't/data/dehumanised/run_22767';
  $f = join q[/], $path, '22767_1_human.cram';
  is ($parser->dehumanising_method($get_header->($f)), 'npg2010',
    "$f human split-put data, 2010 method, adapters clipped");
  $f = join q[/], $path, '22767_1.cram';
  is ($parser->dehumanising_method($get_header->($f)), 'see_human',
    "$f target file, pointer to the human data sibling file");
  $f = join q[/], $path, '22767_1_phix.cram';
  is ($parser->dehumanising_method($get_header->($f)), undef,
    "$f PhiX split-out data, value undefined");

  $path = 't/data/dehumanised/run_7110';
  $f = join q[/], $path, '7110_1#7_human.bam';
  is ($parser->dehumanising_method($get_header->($f)), 'npg2010nc',
    "$f human split-put data, 2010 method, adapters not clipped");
  $f = join q[/], $path, '7110_1#7.bam';
  is ($parser->dehumanising_method($get_header->($f)), 'see_human',
    "$f target file, pointer to the human data sibling file");

  ######
  # Non-human sample,no human split.
  #
  $path = 't/data/dehumanised/run_25409';
  $f = join q[/], $path, '25409_1#8.cram';
  is ($parser->dehumanising_method($get_header->($f)), undef,
    "$f, target file, no human split");
  $f = join q[/], $path, '25409_1#8_phix.cram';
  is ($parser->dehumanising_method($get_header->($f)), undef,
    "$f, PhiX split-out data, value undefined"); 

  ######
  # Enforce that human split-out took place.
  #

  $path = 't/data/dehumanised/run_22767';
  $f = join q[/], $path, '22767_1_human.cram';
  is ($parser->dehumanising_method($get_header->($f), 1), 'npg2010',
    'no change for true human split-out data');
  $f = join q[/], $path, '22767_1.cram';
  is ($parser->dehumanising_method($get_header->($f), 1), 'see_human',
    'no change for a target file');
  $f = join q[/], $path, '22767_1_phix.cram';
  is ($parser->dehumanising_method($get_header->($f), 1), undef,
    'no change for identifiable Phix data');

  $path = 't/data/dehumanised/run_25409';
  $f = join q[/], $path, '25409_1#8.cram';
  $h = $get_header->($f); 
  is ($parser->dehumanising_method($h, 1), 'see_human',
    "$f forced 'see_human' where no human split is detected");
  $h =~ s{/Mycobacterium_abscessus/}{/Homo_sapiens/}gxms;
  is ($parser->dehumanising_method($h, 1), 'unknown',
    "$f (changed header) forced 'unknown' where no human split is detected"); 

  ######
  # Check for false positives - yhuman split.
  #
  $path = 't/data/dehumanised/run_45165';
  $f = join q[/], $path, '45165_1#3.cram';
  is ($parser->dehumanising_method($get_header->($f), 1), undef,
    "$f target file after yhuman split, value undefined");
  $f = join q[/], $path, '45165_1#3_yhuman.cram';
  is ($parser->dehumanising_method($get_header->($f), 1), undef,
    "$f split out Y chr data, value undefined");
  
  ######
  # Check for false positives - xahuman split.
  #
  $path = 't/data/dehumanised/run_14474';
  $f = join q[/], $path, '14474_1#44.bam';
  is ($parser->dehumanising_method($get_header->($f), 1), undef,
    "$f target file after xahuman split, value undefined");
  $f = join q[/], $path, '14474_1#44_xahuman.bam';
  is ($parser->dehumanising_method($get_header->($f), 1), undef,
    "$f split out X and auto chr data, value undefined");
}

sub split_lines {
  my ($content) = @_;

  return [split m{\n}, $content];
}

sub read_file {
  my ($filename) = @_;

  my $content;

  {
    local $INPUT_RECORD_SEPARATOR = undef;

    open my $in, '<', $filename or croak "Failed to open $filename for reading";
    $content = <$in>;
    close $in or carp "Failed to close $filename cleanly";
  }

  return $content;
}


1;
