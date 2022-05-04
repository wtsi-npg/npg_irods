package WTSI::NPG::HTS::TreePublisherTest;

use strict;
use warnings;

use English qw[-no_match_vars];
use File::Basename;
use File::Spec::Functions qw[abs2rel catfile];
use Log::Log4perl;
use Test::More;
use Test::Exception;
use File::Temp;

use base qw[WTSI::NPG::HTS::Test];

use WTSI::NPG::HTS::TreePublisher;
use WTSI::NPG::iRODS;

use JSON;

Log::Log4perl::init('./etc/log4perl_tests.conf');

my $pid          = $PID;
my $test_counter = 0;
my $data_path    = 't/data';
my $bin_path     = 'bin';

my $irods_tmp_coll;

sub setup_test : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  $irods_tmp_coll =
    $irods->add_collection("TreePublisherTest.$pid.$test_counter");
  $test_counter++;
}

sub teardown_test : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  $irods->remove_collection($irods_tmp_coll);
}

sub publish_tree : Test(58) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $source_path = "$data_path/treepublisher";

  my $pub = WTSI::NPG::HTS::TreePublisher->new
    (irods            => $irods,
     source_directory => $source_path,
     dest_collection  => $irods_tmp_coll);

  my $obj_factory = WTSI::NPG::HTS::DefaultDataObjectFactory->new
    (irods => $pub->irods);

  my @files = grep { -f } $pub->list_directory($source_path, recurse => 1);

  my $primary_avus = sub {
    return ({attribute => 'primary', value => 'pvalue'})
  };
  my $secondary_avus = sub {
    return ({attribute => 'secondary', value => 'svalue'})
  };
  my $extra_avus = sub {
    return ({attribute => 'extra', value => 'evalue'})
  };

  my ($num_files, $num_processed, $num_errors) =
      $pub->publish_tree(\@files,
                         primary_cb   => $primary_avus,
                         secondary_cb => $secondary_avus,
                         extra_cb     => $extra_avus);

  my $num_expected = scalar @files;
  cmp_ok($num_errors,    '==', 0, 'No errors on publishing');
  cmp_ok($num_files, '==', $num_expected,
         'Found the expected number of files');
  cmp_ok($num_processed, '==', $num_expected,
         'Published the expected number of files');

  my @observed_paths = observed_data_objects($irods, $irods_tmp_coll,
                                             $irods_tmp_coll);
  my @expected_paths =('a/x/1.txt',
                       'a/x/2.txt',
                       'a/y/3.txt',
                       'a/y/4.txt',
                       'a/z/5.txt',
                       'a/z/6.txt',
                       'b/x/1.txt',
                       'b/x/2.txt',
                       'b/y/3.txt',
                       'b/y/4.txt',
                       'b/z/5.txt',
                       'b/z/6.txt',
                       'c/x/1.txt',
                       'c/x/2.txt',
                       'c/y/3.txt',
                       'c/y/4.txt',
                       'c/z/5.txt',
                       'c/z/6.txt');

  is_deeply(\@observed_paths, \@expected_paths,
            'Published correctly named files') or
              diag explain \@observed_paths;

  check_metadata($irods, map { catfile($irods_tmp_coll, $_) } @observed_paths);
}

sub npg_publish_tree_pl_metadata_from_stdin : Test(3) {
  my $source_path = "${data_path}/treepublisher";
  my @attributes = (  
    {attribute => 'attr1', value => 'val1', units => q[]},
    {attribute => 'attr2', value => 'val2', units => q[]}
  );

  my $metadata_text = JSON->new->utf8->encode(\@attributes);
  my $md_stdio = File::Temp->new(SUFFIX => ".json");
  my $metadata_file_in = $md_stdio->filename;
  print $md_stdio "${metadata_text}\n";

  my $script_args = "--collection ${irods_tmp_coll} " . 
                      "--source_directory ${source_path} -";
  ok(system("cat ${metadata_file_in} | " .
            "${bin_path}/npg_publish_tree.pl ${script_args}") == 0,
      'Script npg_publish_tree.pl with metadata in STDIN');

  my $imeta_output = `imeta ls -C ${irods_tmp_coll}`;
  foreach my $avu (@attributes) {
    ok($imeta_output =~ m/attribute:\s$avu->{attribute}\n
                              value:\s$avu->{value}\n
                              units:\s$avu->{units}/x,
        'Expected attributes from STDIN found by imeta');
  }
}

sub npg_publish_tree_pl_metadata_from_stdin_plus_cmd : Test(1) {
  my $source_path = "${data_path}/treepublisher";
  my @attributes = (  
    {attribute => 'attr1', value => 'val1', units => q[]},
    {attribute => 'attr2', value => 'val2', units => q[]}
  );

  my $metadata_text = JSON->new->utf8->encode(\@attributes);
  my $md_stdio = File::Temp->new(SUFFIX => ".json");
  my $metadata_file_in = $md_stdio->filename;
  print $md_stdio "${metadata_text}\n";

  my $script_args = "--collection ${irods_tmp_coll} " .
                      "--source_directory ${source_path} " . 
                        "--metadata ${metadata_file_in} -";
  ok(system("cat ${metadata_file_in} | " .
            "${bin_path}/npg_publish_tree.pl ${script_args}") != 0, 
      'npg_publish_tree.pl with metadata in STDIN (STDIN and CMD clash)');
}

sub npg_publish_tree_pl_writes_json : Test(2) {
  my $source_path = "${data_path}/treepublisher";
  my $mlwh_json_filename = "metadata.json";

  my @script_args = (q[--mlwh_json], ${mlwh_json_filename}, q[--collection], ${irods_tmp_coll}, q[--source_directory], ${source_path});
  ok(system($^X, "${bin_path}/npg_publish_tree.pl", @script_args) == 0, 'Script npg_publish_tree.pl correctly exited');

  ok(-e $mlwh_json_filename, 'File json in npg_publish_tree_script correctly created');
  unlink $mlwh_json_filename;
}

sub write_json_correct_keyvalue : Test(2) {
  my $source_path = "${data_path}/treepublisher";
  my $mlwh_json_filename = "metadata.json";

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $pub = WTSI::NPG::HTS::TreePublisher->new
    (irods            => $irods,
     source_directory => $source_path,
     dest_collection  => $irods_tmp_coll,
     mlwh_json        => $mlwh_json_filename);
  my @files = grep { -f } $pub->list_directory($source_path, recurse => 1);
  
  $pub->publish_tree(\@files);
  ok(-e $mlwh_json_filename, 'File json in write_json correctly created');
  my ($json_fh, $json_hash);
  open $json_fh, '<:encoding(UTF-8)', $mlwh_json_filename or
    self->logcroak(q[could not open ml warehouse json file] .
    qq[$mlwh_json_filename]);
  $json_hash = decode_json <$json_fh>;
  ok($json_hash->{irods_collection} eq ${irods_tmp_coll}, 'Correct irods collection folder in json file');
  unlink $mlwh_json_filename;
}

sub publish_tree_mlwh_json : Test(1) {
  my $source_path = "${data_path}/treepublisher";
  my $mlwh_json_filename = "metadata.json";

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $pub = WTSI::NPG::HTS::TreePublisher->new
    (irods            => $irods,
     source_directory => $source_path,
     dest_collection  => $irods_tmp_coll,
     mlwh_json        => $mlwh_json_filename);
  my @files = grep { -f } $pub->list_directory($source_path, recurse => 1);
  
  $pub->publish_tree(\@files);
  ok(-e $mlwh_json_filename, 'File json correctly created with no callback');
  unlink $mlwh_json_filename;
}

sub publish_tree_mlwh_json_plus_cb : Test(2) {
  my $source_path = "${data_path}/treepublisher";
  my $mlwh_json_filename = "metadata.json";

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $pub = WTSI::NPG::HTS::TreePublisher->new
    (irods            => $irods,
     source_directory => $source_path,
     dest_collection  => $irods_tmp_coll,
     mlwh_json        => $mlwh_json_filename);
  my @files = grep { -f } $pub->list_directory($source_path, recurse => 1);
  
  dies_ok{
    $pub->publish_tree(\@files,
                        mlwh_json_cb => sub {
                          return 1;
                        });
  }, 'publish_tree correctly exited with error (json callback clash)'; 
  ok(! -e $mlwh_json_filename, 'No json file as expected (json callback clash)');
}

sub publish_tree_filter : Test(4) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $source_path = "$data_path/treepublisher";

  my $pub = WTSI::NPG::HTS::TreePublisher->new
      (irods            => $irods,
       source_directory => $source_path,
       dest_collection  => $irods_tmp_coll);

  my $obj_factory = WTSI::NPG::HTS::DefaultDataObjectFactory->new
      (irods => $pub->irods);

  my @files = grep { -f } $pub->list_directory($source_path, recurse => 1);

  my ($num_files, $num_processed, $num_errors) =
      $pub->publish_tree(\@files,
                        filter => sub {
                          my ($f) = @_;
                          my ($n) = $f =~ m{(\d)[.]txt$}; # parse digit
                          # Return true (i.e. pass/include) for even numbers
                          return $n % 2 == 0;
                        });

  my $num_expected = 9;
  cmp_ok($num_errors,    '==', 0, 'No errors on publishing');
  cmp_ok($num_files, '==', $num_expected,
         'Found the expected number of files');
  cmp_ok($num_processed, '==', $num_expected,
         'Published the expected number of files');

  my @observed_paths = observed_data_objects($irods, $irods_tmp_coll,
                                             $irods_tmp_coll);
  my @expected_paths =('a/x/2.txt',
                       'a/y/4.txt',
                       'a/z/6.txt',
                       'b/x/2.txt',
                       'b/y/4.txt',
                       'b/z/6.txt',
                       'c/x/2.txt',
                       'c/y/4.txt',
                       'c/z/6.txt');

  is_deeply(\@observed_paths, \@expected_paths,
            'Published correctly filtered files') or
      diag explain \@observed_paths;
}

sub observed_data_objects {
  my ($irods, $dest_collection, $root_collection) = @_;

  my ($observed_paths) = $irods->list_collection($root_collection, 'RECURSE');
  my @observed_paths = @{$observed_paths};
  @observed_paths = sort @observed_paths;
  @observed_paths = map { abs2rel($_, $root_collection) } @observed_paths;

  return @observed_paths;
}

sub check_metadata {
  my ($irods, @paths) = @_;

  foreach my $path (@paths) {
    my $obj = WTSI::NPG::HTS::DataObject->new($irods, $path);
    my $file_name = fileparse($obj->str);

    my %attrs = ('primary'   => 'pvalue',
                 'secondary' => 'svalue',
                 'extra'     => 'evalue');

    while (my ($attr, $value) = each %attrs) {
      ok($obj->get_avu($attr, $value), "$path has AVU '$attr' => '$value'");
    }
  }
}

1;
