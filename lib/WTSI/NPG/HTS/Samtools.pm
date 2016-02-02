package WTSI::NPG::HTS::Samtools;

use namespace::autoclean;
use English qw(-no_match_vars);
use Moose;
use MooseX::StrictConstructor;

use WTSI::DNAP::Utilities::Runnable;

with 'WTSI::DNAP::Utilities::Loggable';

## no critic (ErrorHandling::RequireCheckingReturnValueOfEval)
eval { with 'npg_common::roles::software_location' };
## critic

our $VERSION = '';
our $DEFAULT_SAMTOOLS_EXECUTABLE = 'samtools';

our $TOTAL_PASSED      = __PACKAGE__ . 'total_passed';
our $TOTAL_FAILED      = __PACKAGE__ . 'total_failed';
our $SEQ_PAIRED_PASSED = __PACKAGE__ . 'seq_paired_passed';
our $SEQ_PAIRED_FAILED = __PACKAGE__ . 'seq_paired_failed';

has 'executable' =>
  (is            => 'ro',
   isa           => 'Str',
   required      => 1,
   builder       => '_build_samtools',
   lazy          => 1,
   documentation => 'Path to the samtools executable');

has 'arguments' =>
  (is            => 'ro',
   isa           => 'ArrayRef',
   lazy          => 1,
   default       => sub { ['-H'] },
   documentation => 'The CLI arguments passed to the samtools executable');

has 'path' =>
  (isa           => 'Str',
   is            => 'ro',
   required      => 1,
   documentation => 'The path of a BAM or CRAM file');


sub num_reads {
  my ($self) = @_;

  my $flagstat = $self->_flagstat;

  return ($flagstat->{$TOTAL_PASSED}, $flagstat->{$TOTAL_FAILED});
}

sub num_seq_paired_reads {
  my ($self) = @_;

  my $flagstat = $self->_flagstat;

  return ($flagstat->{$SEQ_PAIRED_PASSED}, $flagstat->{$SEQ_PAIRED_FAILED});
}

=head2 run

  Arg [1]    : None

  Example    : $samtools->run
  Description: Run a samtools command and ignore any output.
  Returntype : WTSI::NPG::Runnable

=cut

sub run {
  my ($self) = @_;

  my $p = $self->path;
  my @args = (@{$self->arguments}, qq[$p]);

  return WTSI::DNAP::Utilities::Runnable->new
    (arguments  => \@args,
     executable => $self->executable,
     logger     => $self->logger)->run;
}

=head2 iterate

  Arg [1]    : A callback to be executed on each line of SAM data, CodeRef

  Example    : $samtools->iterate(sub { print $_[0] });
  Description: Iterate over lines of SAM format data returned by a samtools
               view command, executing a callback on each line.
  Returntype : WTSI::NPG::Runnable

=cut

sub iterate {
  my ($self, $callback) = @_;

  my $p = $self->path;
  my @args = ('view', @{$self->arguments}, qq[$p]);

  my $out = q[];
  my $stdout_sink = sub {
    $out .= shift;

    # Wait for at least one EOL
    if ($out =~ m{.+(\r\n?|\n)$}msx) {
      if ($callback) {
        # Call back for each of any internal EOLs
        foreach my $line (split m{\r\n?|\n}msx, $out) {
          $callback->($line);
        }
      }

      # Reset out. See IPC::Run
      $out = q[];
    }
  };

  return WTSI::DNAP::Utilities::Runnable->new
    (arguments  => \@args,
     executable => $self->executable,
     logger     => $self->logger,
     stdout     => $stdout_sink)->run;
}

=head2 collect

  Arg [1]    : A filter to be applied to each line of SAM data, CodeRef.
               Any lines for which the filter returns true will be
               collected and returned.

  Example    : my @pg_lines = $samtools->collect(sub { $_[0] =~ /^@PG/ })
  Description: Iterate over lines of SAM format data returned by a samtools
               view command, filtering and collecting lines.
  Returntype : Array

=cut

sub collect {
  my ($self, $filter) = @_;

  defined $filter and ref $filter ne 'CODE' and
    $self->logconfess('The filter argument must be a CodeRef');

  my @collected;
  my $collector = sub {
    my $elt = shift;

    if ($filter) {
      if ($filter->($elt)) {
        push @collected, $elt;
      }
    }
    else {
      push @collected, $elt;
    }
  };

  $self->iterate($collector);

  return @collected;
}

sub _build_samtools {
  my ($self) = @_;

  if ($self->can('samtools_cmd')) {
    $self->debug('Using npg_common::roles::software_location to find ',
                 'samtools: ', $self->samtools_cmd);
    return $self->samtools_cmd;
  }
  else {
    $self->debug('Using the default samtools executable on PATH: ',
                 $DEFAULT_SAMTOOLS_EXECUTABLE);
    return $DEFAULT_SAMTOOLS_EXECUTABLE;
  }
}

# my $flagstat_cache = {};
sub _flagstat {
  my ($self) = @_;

  my $flagstat_cache = {};
  # if (not keys %{$flagstat_cache}) {
    my $p = $self->path;

    # FIXME -- this filter may need to be moved else where (to caller?)
    my $view = WTSI::DNAP::Utilities::Runnable->new
      (arguments  => ['view', q[-u], q[-F], q[0x0900], qq[$p]],
       executable => $self->executable,
       logger     => $self->logger);
    my $flagstat = WTSI::DNAP::Utilities::Runnable->new
      (arguments  => ['flagstat', q[-]],
       executable => $self->executable,
       logger     => $self->logger);

    my @stats = $view->pipe($flagstat)->split_stdout;
    foreach my $record (@stats) {
      if ($record =~ m{^(\d+)\s+[+]\s+(\d+)\s+in\s+total}mxs) {
        $flagstat_cache->{$TOTAL_PASSED} = $1;
        $flagstat_cache->{$TOTAL_FAILED} = $2;
        next;
      }
      if ($record =~ m{^(\d+)\s+[+]\s+(\d+)\s+paired\sin\s+sequencing}mxs) {
        $flagstat_cache->{$SEQ_PAIRED_PASSED} = $1;
        $flagstat_cache->{$SEQ_PAIRED_FAILED} = $2;
        next;
      }
    # }
  }

  return $flagstat_cache;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::HTS::Samtools

=head1 DESCRIPTION

Wrapper for samtools with convenience methods for samtools view.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2015, 2016 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
