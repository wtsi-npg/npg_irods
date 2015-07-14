
package WTSI::NPG::HTS::HTSFileDataObjectTest;

use strict;
use warnings;

use List::AllUtils qw(each_array);
use Log::Log4perl;
use Test::More tests => 52;

use base qw(Test::Class);

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::HTS::HTSFileDataObject') }

use WTSI::NPG::HTS::HTSFileDataObject;
use WTSI::NPG::HTS::Samtools;
use WTSI::NPG::iRODS;

my $fixture_counter = 0;
my $data_path = './t/data';
my $data_file = '1234_5#6';
my $reference_file = 'test_ref.fa';
my $irods_tmp_coll;
my $samtools = `which samtools`;

my $pid = $$;

sub setup_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);

  $irods_tmp_coll =
    $irods->add_collection("HTSFileDataObjectTest.$pid.$fixture_counter");
  $fixture_counter++;

  if ($samtools) {
    WTSI::NPG::HTS::Samtools->new
        (arguments => ['view', '-C',
                       '-T', qq{$data_path/$reference_file},
                       '-o', qq{irods:$irods_tmp_coll/$data_file.cram}],
         path      => "$data_path/$data_file.sam")->run;

    WTSI::NPG::HTS::Samtools->new
        (arguments => ['view', '-b',
                       '-T', qq{$data_path/$reference_file},
                       '-o', qq{irods:$irods_tmp_coll/$data_file.bam}],
         path      => "$data_path/$data_file.sam")->run;
  }
}

sub teardown_fixture : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);

  $irods->remove_collection($irods_tmp_coll);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::HTS::HTSFileDataObject');
}

my @example_paths =
  ('/seq/6345/6345_5',
   '/seq/6345/6345_5_phix',
   '/seq/6345/6345_5_phix#6',
   '/seq/6345/6345_5_nonhuman#6',
   '/seq/6345/6345_5#6',
   '/seq/6345/6345_5#6_phix');

sub id_run : Test(6) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);

  foreach my $path (@example_paths) {
    my $full_path = $path . q{.cram};
    cmp_ok(WTSI::NPG::HTS::HTSFileDataObject->new($irods, $full_path)->id_run,
           '==', 6345, "$full_path id_run is correct");
  }
}

sub position : Test(12) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);

  foreach my $format (qw(bam cram)) {
    foreach my $path (@example_paths) {
      my $full_path = $path . ".$format";
      cmp_ok(WTSI::NPG::HTS::HTSFileDataObject->new
             ($irods, $full_path)->position,
             '==', 5, "$full_path position is correct");
    }
  }
}

sub tag_index : Test(12) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);

  foreach my $format (qw(bam cram)) {
    my @objs;
    foreach my $path (@example_paths) {
      push @objs, WTSI::NPG::HTS::HTSFileDataObject->new
        ($irods, $path . ".$format");
    }
    my @tag_indices = (undef, undef, 6, 6, 6, 6);

    my $iter = each_array(@objs, @tag_indices);
    while (my ($obj, $tag_index) = $iter->()) {
      my $full_path = $obj->str;
      if (defined $tag_index) {
        cmp_ok($obj->tag_index, '==', $tag_index,
               "$full_path tag_index is correct");
      }
      else {
        isnt(defined $obj->tag_index,
             "$full_path tag_index is correct");
      }
    }
  }
}

sub align_filter : Test(12) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);

  foreach my $format (qw(bam cram)) {
    my @objs;
    foreach my $path (@example_paths) {
      push @objs, WTSI::NPG::HTS::HTSFileDataObject->new
        ($irods, $path . ".$format");
    }
    my @align_filters = (undef, 'phix', 'phix', 'nonhuman', undef, 'phix');

    my $iter = each_array(@objs, @align_filters);
    while (my ($obj, $filter) = $iter->()) {
      my $full_path = $obj->str;
      is($obj->align_filter, $filter, "$full_path align_filter is correct");
    }
  }
}

sub header : Test(4) {
 SKIP: {
    if (not $samtools) {
      skip 'samtools executable not on the PATH', 4;
    }

    my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);

    foreach my $format (qw(bam cram)) {
      my $obj = WTSI::NPG::HTS::HTSFileDataObject->new
        (collection  => $irods_tmp_coll,
         data_object => "$data_file.$format",
         file_format => $format,
         id_run      => 1,
         irods       => $irods,
         position    => 1);

      ok($obj->header, "$format eader can be read");

      cmp_ok(scalar @{$obj->header}, '==', 11,
             "Correct number of $format header lines") or
               diag explain $obj->header;
    }
  };
}

sub is_aligned : Test(2) {
 SKIP: {
    if (not $samtools) {
      skip 'samtools executable not on the PATH', 2;
    }

    my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);

    foreach my $format (qw(bam cram)) {
      my $obj = WTSI::NPG::HTS::HTSFileDataObject->new
        (collection  => $irods_tmp_coll,
         data_object => "$data_file.$format",
         file_format => $format,
         id_run      => 1,
         irods       => $irods,
         position    => 1);

      ok($obj->is_aligned, "$format data are aligned");
    }
  };
}

sub reference : Test(2) {
 SKIP: {
    if (not $samtools) {
      skip 'samtools executable not on the PATH', 2;
    }

    my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);

    foreach my $format (qw(bam cram)) {
      my $obj = WTSI::NPG::HTS::HTSFileDataObject->new
        (collection  => $irods_tmp_coll,
         data_object => "$data_file.$format",
         file_format => $format,
         id_run      => 1,
         irods       => $irods,
         position    => 1);

      my $regex = qr{\./t\/data\/test_ref.fa}msx;
      my $filter = sub {
        my ($line) = @_;
        return $line =~ m{$regex}msx;
      };
      is($obj->reference($filter), './t/data/test_ref.fa',
         "$format reference is correct");
    }
  };
}

1;
