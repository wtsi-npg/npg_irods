package npg_tracking::glossary::composition::component::generic;

use namespace::autoclean;

use Moose;
use MooseX::StrictConstructor;

with qw[
         npg_tracking::glossary::composition::component
         npg_tracking::glossary::run
         npg_tracking::glossary::lane
         npg_tracking::glossary::subset
         npg_tracking::glossary::tag
      ];

our $VERSION = '';

__PACKAGE__->meta->make_immutable;

1;
