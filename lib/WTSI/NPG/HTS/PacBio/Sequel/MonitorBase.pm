package WTSI::NPG::HTS::PacBio::Sequel::MonitorBase;

use Moose::Role;
use DateTime;
use English qw[-no_match_vars];
use File::Spec::Functions qw[catfile];
use WTSI::NPG::HTS::PacBio::Sequel::APIClient;

with qw[ WTSI::DNAP::Utilities::Loggable ];

our $VERSION = '';

# filename for mark file
our $MARK_FILENAME  = 'processing_in_progess';

has 'api_client' =>
  (isa           => 'WTSI::NPG::HTS::PacBio::Sequel::APIClient',
   is            => 'ro',
   lazy_build    => 1,
   builder       => q[_build_api_client],
   documentation => 'A PacBio Sequel API client used to fetch runs');

sub _build_api_client {
  my $self = shift;
  my @init_args = $self->api_uri ? ('api_uri' => $self->api_uri) : ();
  if($self->interval) { push @init_args, ('default_interval' => $self->interval) };
  if($self->older_than) { push @init_args, ('default_end' => $self->older_than) };
  return WTSI::NPG::HTS::PacBio::Sequel::APIClient->new(@init_args);
}

has 'api_uri' =>
  (isa           => 'Str',
   is            => 'ro',
   documentation => 'PacBio root API URL');

has 'interval' =>
  (isa           => 'Str',
   is            => 'ro',
   documentation => 'Interval of time in days');

has 'older_than' =>
  (isa           => 'Str',
   is            => 'ro',
   documentation => 'Time in days to remove from end date');


=head2 mark_folder

  Arg [1]    : Folder path name. Required.
  Example    : my ($marked) = $self->mark_folder($folder)
  Description: 
  Returntype : Boolean

=cut

sub mark_folder {
  my ($self,$folder,$run_name) = @_;

  defined $folder or
    $self->logconfess('A defined folder argument is required');
  defined $run_name or
    $self->logconfess('A defined run_name argument is required');

  my $marked = 0;
  my $file = catfile($folder,$run_name .q[_]. $MARK_FILENAME);
  if(-d $folder && !-f $file){
    open my $fh, '>', $file or
      $self->logcroak("Failed to open '$file' for writing: ", $ERRNO);

    print $fh 'Processing started at: '. DateTime ->now()
      or $self->logcroak("Failed to write to '$file': $ERRNO");

    close $fh or $self->warn("Failed to close '$file'");

    if(-f $file) { $marked = 1; }
  }
  return $marked;
}

=head2 unmark_folder

  Arg [1]    : Folder path name. Required.
  Example    : my ($unmarked) = $self->unmark_folder($folder)
  Description: 
  Returntype : Boolean

=cut

sub unmark_folder {
  my ($self,$folder,$run_name) = @_;

  defined $folder or
    $self->logconfess('A defined folder argument is required');
  defined $folder or
    $self->logconfess('A defined run_name argument is required');

  my $unmarked = 0;
  my $file = catfile($folder,$run_name .q[_]. $MARK_FILENAME);
  if(-f $file){
     unlink $file;
     if(!-f $file) {
       $unmarked = 1;
     } else {
       $self->error("Failed to unmark '$folder'");
     }
  }
  return $unmarked;
}

no Moose::Role;

1;

__END__


=head1 NAME

WTSI::NPG::HTS::PacBio::Sequel::MonitorBase

=head1 DESCRIPTION

Base for Sequel Monitors.

=head1 AUTHOR

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2019 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
