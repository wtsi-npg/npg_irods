
use strict;
use warnings;

# These paths are propagated to @INC in the build script. The 't/lib'
# directory is added because the Test::Class tests are there.
use lib qw[lib t/lib];

use WTSI::DNAP::Utilities::Build;
use BuildONT;

my $build = BuildONT->new
  (module_name     => 'npg_irods',
   dist_author     => ['NPG <npg@sanger.ac.uk>'],
   dist_abstract   => 'NPG iRODS data/metadata loading and update tools',
   dist_version    => BuildONT->report_version,
   license         => 'gpl',
   build_script    => 'BuildONT',
   configure_requires => {
                          'Module::Build'   => ">= 0.4218"
                         },
   test_requires      => {
                          'Test::Class'                   => '>= 0.41',
                          'Test::More'                    => '>= 0.98',
                          'Test::Exception'               => '>= 0.32',
                          'Test::Perl::Critic'            => '>= 1.02'
                         },
   requires          => {
                         'Data::Dump'                    => '>= 1.22',
                         'DateTime'                      => '>= 1.18',
                         'Digest::MD5'                   => 0,
                         'IO::Compress::Bzip2'           => 0,
                         'Linux::Inotify2'               => 0,
                         'List::AllUtils'                => '>= 0.09',
                         'Log::Log4perl'                 => '>= 1.46',
                         'Moose'                         => '>= 2.1',
                         'MooseX::StrictConstructor'     => 0,
                         'Parallel::ForkManager'         => 0,
                         'PDL::IO::HDF5'                 => 0,
                         'Try::Tiny'                     => '>= 0.22',
                         'URI'                           => '>= 1.67',
                         'WTSI::DNAP::Utilities'         => 0,
                         'WTSI::NPG::iRODS'              => '>= 2.8.0'
                        },
   recommends        => {
                         'UUID' => '>= 0.24',
                        },
   test_files        => [
                         './t/compile_scripts.t',
                         './t/critic.t',
                         './t/gridion_run_monitor.t',
                         './t/gridion_run_publisher.t',
                         './t/minion_run_publisher.t',
                        ]
  );

$build->create_build_script;
