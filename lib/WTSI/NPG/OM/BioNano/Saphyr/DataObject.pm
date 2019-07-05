package WTSI::NPG::OM::BioNano::Saphyr::DataObject;

use namespace::autoclean;
use Moose;
use MooseX::StrictConstructor;

our $VERSION = '';

extends 'WTSI::NPG::HTS::DataObject';

has '+is_restricted_access' =>
  (is            => 'ro');

has '+primary_metadata' =>
  (is            => 'ro');

sub BUILD {
  my ($self) = @_;

  # Modifying read-only attribute
  push @{$self->primary_metadata},
    $WTSI::NPG::OM::BioNano::Saphyr::Annotator::SAPHYR_RUN_UID,
    $WTSI::NPG::OM::BioNano::Saphyr::Annotator::SAPHYR_CHIP_SERIALNUMBER,
    $WTSI::NPG::OM::BioNano::Saphyr::Annotator::SAPHYR_CHIP_FLOWCELL,
    $WTSI::NPG::OM::BioNano::Saphyr::Annotator::SAPHYR_SAMPLE_NAME,
    $WTSI::NPG::OM::BioNano::Saphyr::Annotator::SAPHYR_PROJECT_NAME,
    $WTSI::NPG::OM::BioNano::Saphyr::Annotator::SAPHYR_EXPERIMENT_NAME;

  return;
}

sub _build_is_restricted_access {
  my ($self) = @_;

  return 1;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;
