package WTSI::NPG::HTS::LIMSFactoryTest;

use strict;
use warnings;

use English qw[-no_match_vars];
use Log::Log4perl;
use Test::More;
use Test::Exception;

use base qw[WTSI::NPG::HTS::Test];

use npg_tracking::glossary::composition;
use npg_tracking::glossary::composition::component::illumina;
use WTSI::NPG::HTS::LIMSFactory;

Log::Log4perl::init('./etc/log4perl_tests.conf');

sub make_composition {
  my ($id_run, $position) = @_;
  my $component = npg_tracking::glossary::composition::component::illumina->new(
    {
      id_run   => $id_run,
      position => $position,
    });
  return npg_tracking::glossary::composition->new({components => [$component]});
}

sub make_lims_mlwh_driver : Test(10){
  my $lims_factory = WTSI::NPG::HTS::LIMSFactory->new(
    {driver_type => 'ml_warehouse_fc_cache'});
  my $composition = make_composition(1, 1);
  my $lims;
  lives_ok{$lims = $lims_factory->make_lims($composition)}
    'lims made successfully';
  my $cached_schema = $lims_factory->{mlwh_schema};
  is($lims->{_driver_arguments}->{mlwh_schema}, $cached_schema, 'lims uses cached mlwh_schema');
  is($lims, $lims_factory->{lims_cache}->{$composition->freeze2rpt},
    'lims in lims cache');
  my $diff_composition = make_composition(2, 1);
  my $diff_lims = $lims_factory->make_lims($diff_composition);
  isnt($diff_lims, $lims, 'lims are not the same');
  is($diff_lims->{_driver_arguments}->{mlwh_schema}, $cached_schema,
    'cached schema used for new lims');
  lives_ok{$lims_factory->clear_mlwh_schema} 'can clear mlwh_schema';
  is($lims_factory->has_mlwh_schema, '', 'mlwh_schema cleared');
  my $post_clear_composition = make_composition(1, 2);
  my $post_clear_lims;
  lives_ok{$post_clear_lims = $lims_factory->make_lims($post_clear_composition)}
    'can make a new lims after clearing mlwh_schema';
  is $lims_factory->has_mlwh_schema, 1, 'new mlwh_schema cached';
  isnt($post_clear_lims->{_driver_arguments}->{mlwh_schema}, $lims->{mlwh_schema},
    'new mlwh_schema is different from previous mlwh_schema');
}

sub make_lims_non_mlwh_driver : Test(2){
  my $lims_factory = WTSI::NPG::HTS::LIMSFactory->new(
    {driver_type => 'samplesheet'});
  my $composition = make_composition(1, 1);
  my $lims;
  lives_ok{$lims = $lims_factory->make_lims($composition)}
    'lims made successfully';
  is($lims->{_driver_arguments}->{mlwh_schema}, undef, 'no mlwh_schema with non-mlwh driver');
}

1;