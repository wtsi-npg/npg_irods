package WTSI::NPG::HTS::PacBio::RunPublisherTest;

use strict;
use warnings;

use Test::More;

use base qw[WTSI::NPG::HTS::Test];

sub require : Test(1) {
  require_ok('WTSI::NPG::HTS::PacBio::RunPublisher');
}

1;
