package WTSI::NPG::Data::BamDeletion;

use Moose;
use POSIX qw[strftime];
use Cwd;
use File::Slurp;
use File::Temp qw[tempdir];
use File::Basename;
use Readonly;
use Carp;


use WTSI::NPG::HTS::Illumina::AlnDataObject;
use WTSI::NPG::iRODS::Publisher;

with qw{
  WTSI::DNAP::Utilities::Loggable
  WTSI::NPG::HTS::ChecksumCalculator
       };

our $VERSION = '0';

our $DATE = strftime '%Y%m%d', localtime;
our $DELETION_MSG = qq[\@CO\tThe data from this file was removed on $DATE as consent was withdrawn];

=head1 NAME

WTSI::NPG::Data::BamDeletion

=head1 SYNOPSIS

=head1 DESCRIPTION

Update specified iRODS files leaving behind a header file for alignment files and a stub mentioning consent withdrawn for other files.  File name remains the same as the original.

The original meta data is attached and the RT ticket added.

For use with e.g. iRODS files identified with npg_process_consent_withdrawn.pl

=head1 SUBROUTINES/METHODS

=head2 dry_run

Dry run flag, false by default. No changes to iRODS data

=cut

has 'dry_run' => (
  isa           => 'Bool',
  is            => 'ro',
  required      => 0,
  default       => 1,
  documentation => 'dry run flag',
);


=head2 irods

WTSI::NPG::iRODS type object mediating access to iRODS,
required.

=cut

has 'irods' => (
  isa        => 'WTSI::NPG::iRODS',
  is         => 'ro',
  required   => 1,
);

=head2 rt_ticket

=cut

has 'rt_ticket' => (
  isa        => 'Int',
  is         => 'ro',
  required   => 0,
  documentation=>'RT ticket number as integer, an optional argument'
);

=head2 input_fofn

File of File Names  specified

=cut

has 'input_fofn' => (
  isa        => 'Str',
  is         => 'ro',
  required   => 0,
  documentation=>'File of iRODS file name paths'
);

=head2 file

File to be removed 

=cut

has 'file' => (
  isa        => 'Str',
  is         => 'ro',
  required   => 1,
  documentation=>'File to be removed from iRODS and replaced with stub file with meta data retained'
);

=head2 md5_file

=cut

has 'md5_file' => (
  isa        => 'Str',
  is         => 'ro',
  required   => 0,
  lazy_build =>1
);
sub _build_md5_file {
    my $self = shift;
    my $filename = fileparse($self->file);
    my $md5file  = $self->outdir.qq[/$filename].q[.md5];
   return $md5file;
}

=head2

Generate an md5sum for the stub/header file to load 

=cut

has 'md5sum' =>
  (is       => 'ro',
   isa      => 'Str',
   init_arg => undef,
   lazy     => 1,
   builder  => '_build_md5sum',
   documentation => 'MD5 checksum of the file to load',
);
sub _build_md5sum {
    my ($self) = @_;
    my $checksum = $self->calculate_checksum($self->outfile);

    my $md5_fh;
    open $md5_fh, '>', $self->md5_file ||
         $self->logcroak(q[Failed to open md5 file '], $self->md5_file, q[']);

    $self->info(q[MD5 file ],$self->md5_file);

    print $md5_fh $checksum || $self->logcroak(q[Failed to print to md5 file '], $self->md5_file, q[']);;
    close $md5_fh ||
          $self->logcroak(q[Failed to close md5 file '], $self->md5_file, q[']);
    return $checksum;
}

=head2 outdir

Where to write data stripped file prior to loading to iRODS

=cut

has 'outdir' => (
    isa        => 'Str',
    is         => 'ro',
    required   => 0,
    default    => tempdir( CLEANUP => 1 ),
    documentation => 'Output directory for header/comment file. Default is a directory in /tmp'
);

=head2 outfile

Locally written file

=cut

has 'outfile' => (
         isa        => 'Str',
         is         => 'ro',
         required   => 0,
         lazy_build => 1,
);
sub _build_outfile {
    my $self = shift;
    my $filename = fileparse($self->file);
    my $outfile     = $self->outdir.qq[/$filename];
    return $outfile;
}

=head2 process

Processes files for samples where consent has been withdrawn.

=cut



sub process {
  my $self = shift;

  $self->dry_run and $self->info('DRY RUN - no data removal');

   $self->irods->ensure_object_path($self->file);

    ###check file suffix
   if ($self->file =~ /[b|cr]am$/sxm){
      $self->_write_header($self->_generate_header());
    } else {
      $self->_write_stub_file();
    }
      $self->_write_md5_file();

     if ($self->dry_run){ carp q[Would be re-loading file ], $self->file,qq[\n] }
     else { $self->_reload_file(); }

  return $self->file;
}

sub _generate_header{
    my $self = shift;

    my $obj = WTSI::NPG::HTS::Illumina::AlnDataObject->new($self->irods,$self->file);# sub header

       $obj->is_present or $self->info($self->file ,q[ not found]);

    my $header = $obj->header;
    return $header;
}


sub _write_header{
    my $self = shift;
    my $header = shift;
    push @{$header}, $DELETION_MSG;
    if ($self->dry_run){ carp ("Would be writing header and \"$DELETION_MSG\" to ",$self->outfile) ; return 1 }
    return write_file($self->outfile,map { "$_\n" } @{$header}) || $self->logcroak(q[Cannot write ] ,$self->outfile);
}

sub _write_stub_file{
    my $self = shift;
    if ($self->dry_run){ carp ("Would be writing \"$DELETION_MSG\" to ",$self->outfile) ; return 1 }
    return write_file($self->outfile, $DELETION_MSG) || $self->logcroak(q[Cannot write ] ,$self->outfile);
}

sub _write_md5_file{
    my $self = shift;
    if ($self->dry_run){ carp (q[Would be generating md5 file ],$self->md5_file); return 1 }
    return $self->md5sum();
}

sub _reload_file {
    my $self = shift;

    ## Publisher does forced overwrites which should affect all replicates

    my $publisher = WTSI::NPG::iRODS::Publisher->new(irods => $self->irods);

    ##returns WTSI::NPG::iRODS::DataObject
    my $obj =$publisher->publish_file($self->outfile,$self->file);

     ## this also adds a target_history avu
      $obj->supersede_avus('target','0');

     if ($self->rt_ticket){ $obj->add_avu('rt_ticket',$self->rt_ticket) }

     return $obj;
}
1;

__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

=head1 DEPENDENCIES

=over

=item Moose

=item Readonly

=item WTSI::NPG::HTS::Illumina::AlnDataObject

=item WTSI::NPG::iRODS::Publisher

=item WTSI::DNAP::Utilities::Loggable

=item WTSI::NPG::HTS::ChecksumCalculator

=item POSIX

=item File::Temp

=item File::Slurp

=item File::Basename

=item Carp

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

=head1 AUTHOR

Jillian Durham

=head1 LICENSE AND COPYRIGHT

Copyright (C) 2020 Genome Research Ltd.

This file is part of NPG.

NPG is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.

=cut
