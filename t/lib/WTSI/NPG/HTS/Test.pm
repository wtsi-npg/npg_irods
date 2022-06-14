package WTSI::NPG::HTS::Test;

use strict;
use warnings;

use base qw[Test::Class];
use Test::More;

# Run full tests (requiring a test iRODS server) only if TEST_AUTHOR
# is true. If full tests are run, require that both irodsEnvFile
# IRODS_ENVIRONMENT_FILE and be set. This is for safety because we do
# not know which of 3.x or 4.x clients will be first on the PATH. The
# unused variable may be set to a dummy value.

sub runtests {
  my ($self) = @_;

  my $default_irods_env = 'IRODS_ENVIRONMENT_FILE';
  my $test_irods_env = "WTSI_NPG_iRODS_Test_$default_irods_env";
  defined $ENV{$test_irods_env} or
    die "iRODS test environment variable $test_irods_env was not set";

  my %env_copy = %ENV;

  # Ensure that the iRODS connection details are set to the test environment
  $env_copy{$default_irods_env} = $ENV{$test_irods_env};

  # For tests involving samtools, disable the CRAM reference cache
  # throughout all tests
  $env_copy{'REF_PATH'} = 'DUMMY_VALUE';

  {
    local %ENV = %env_copy;
    return $self->SUPER::runtests;
  }
}

# If any test methods fail to complete, count all their remaining
# tests as failures.
sub fail_if_returned_early {
  return 1;
}

1;
