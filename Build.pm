use utf8;

package Build;

use strict;
use warnings;

use base 'WTSI::DNAP::Utilities::Build';

#
# Prepare environment for tests
#
sub ACTION_test {
  my ($self) = @_;
  # Ensure that the tests can see the Perl scripts
  {
      local $ENV{PATH} = "./bin:$ENV{PATH}";
      $self->SUPER::ACTION_test;
  }
}

1;
