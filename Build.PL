
use strict;
use warnings;

# These paths are propagated to @INC in the build script. The 't/lib'
# directory is added because the Test::Class tests are there.
use lib qw[lib t/lib];

use Build;

my $build = Build->new
  (module_name        => 'npg_irods',
   dist_author        => 'NPG <npg@sanger.ac.uk>',
   dist_abstract      => 'NPG iRODS data/metadata loading and update tools',
   dist_version       => WTSI::DNAP::Utilities::Build->report_version,
   license            => 'gpl',
   configure_requires =>
   {
    'Module::Build'                 => ">= 0.4218"
   },
   build_requires =>
   {
    'File::Copy::Recursive'         => 0,
    'TAP::Harness'                  => '>= 3.30',
    'Test::Class'                   => '>= 0.41',
    'Test::Compile'                 => 0,
    'Test::HTTP::Server'            => 0,
    'Test::MockObject::Extends'     => 0,
    'Test::More'                    => '>= 0.98',
    'Test::Exception'               => '>= 0.32',
    'Test::Perl::Critic'            => '>= 1.02',
    'npg_tracking::glossary::composition' => 0,
   },
   requires =>
   {
    'Cache::LRU'                    => 0,
    'Carp'                          => '>= 1.20',
    'Data::Dump'                    => 0,
    'DateTime'                      => '>= 1.18',
    'DateTime::Format::ISO8601'     => 0,
    'File::Touch'                   => 0,
    'Log::Log4perl'                 => '>= 1.42',
    'List::AllUtils'                => '>= 0.09',
    'Linux::Inotify2'               => 0,
    'Moose'                         => '>= 2.1',
    'MooseX::Types'                 => '>= 0.45',
    'MooseX::StrictConstructor'     => 0,
    'Text::CSV'                     => '>= 1.33',
    'Try::Tiny'                     => '>= 0.12',
    'URI'                           => 0,
    'UUID'                          => 0,
    'WTSI::DNAP::Utilities'         => '>= 0.5.2',   # perl-dnap-utilities
    'WTSI::NPG::iRODS'              => '>= 2.5.0',   # perl-irods-wrap
    'WTSI::DNAP::Warehouse::Schema' => '>= 2.1',     # ml_warehouse
    'npg_testing::db'               => '>= 85.4',    # npg_tracking
    'st::api::lims'                 => '>= 85.4',    # npg_tracking
    'st::api::lims::ml_warehouse'   => '>= 39.1',    # npg_ml_warehouse
    'npg_tracking::illumina::runfolder' => 0,
   },
   recommends =>
   {
    'npg_common::roles::software_location' => 0,     # npg_seq_common
   }
  );

$build->create_build_script;

1;
