package BuildONT;

use strict;
use warnings;
use List::AllUtils qw[any];

use base 'WTSI::DNAP::Utilities::Build';

# Build only this subset of files
our @ont_subset = (
                   'WTSI/DNAP/Utilities/Params.pm',
                   'WTSI/DNAP/Utilities/ParamsParser.pm',
                   'WTSI/NPG/HTS/AVUCollator.pm',
                   'WTSI/NPG/HTS/ArchiveSession.pm',
                   'WTSI/NPG/HTS/DataObject.pm',
                   'WTSI/NPG/HTS/DataObjectFactory.pm',
                   'WTSI/NPG/HTS/Metadata.pm',
                   'WTSI/NPG/HTS/ONT/Annotator.pm',
                   'WTSI/NPG/HTS/ONT/GridIONMetaUpdater.pm',
                   'WTSI/NPG/HTS/ONT/GridIONRun.pm',
                   'WTSI/NPG/HTS/ONT/GridIONRunAuditor.pm',
                   'WTSI/NPG/HTS/ONT/GridIONRunMonitor.pm',
                   'WTSI/NPG/HTS/ONT/GridIONRunPublisher.pm',
                   'WTSI/NPG/HTS/ONT/GridIONTarAuditor.pm',
                   'WTSI/NPG/HTS/ONT/MetaQuery.pm',
                   'WTSI/NPG/HTS/ONT/MinIONRunMonitor.pm',
                   'WTSI/NPG/HTS/ONT/MinIONRunPublisher.pm',
                   'WTSI/NPG/HTS/ONT/TarDataObject.pm',
                   'WTSI/NPG/HTS/ONT/Watcher.pm',
                   'WTSI/NPG/HTS/ChecksumCalculator.pm',
                   'WTSI/NPG/HTS/PathLister.pm',
                   'WTSI/NPG/HTS/TarItem.pm',
                   'WTSI/NPG/HTS/TarManifest.pm',
                   'WTSI/NPG/HTS/TarPublisher.pm',
                   'WTSI/NPG/HTS/TarStream.pm',
                   'WTSI/NPG/HTS/Types.pm',
                   'npg_audit_gridion_run.pl',
                   'npg_audit_gridion_tar.pl',
                   'npg_gridion_meta_updater.pl',
                   'npg_gridion_run_monitor.pl',
                   'npg_minion_run_monitor.pl',
                   'npg_publish_minion_run.pl',
                  );

sub ACTION_test {
  my ($self) = @_;

  {
    # Ensure that the tests can see the Perl scripts
    local $ENV{PATH} = "./bin:" . $ENV{PATH};

    $self->SUPER::ACTION_test;
  }
}

sub ACTION_code {
  my ($self) = @_;

  $self->SUPER::ACTION_code;

  # Prune everything apart from the ONT components
  my @built_modules = _find_files(qr{\.pm$}msx, 'blib/lib');
  my @built_scripts = _find_files(qr{\.pl$}msx, 'blib/script');
  foreach my $file (@built_modules, @built_scripts) {
    if (any { $file =~ m{$_$}msx } @ont_subset) {
      $self->log_debug("Matched $file with ONT subset\n");
    }
    else {
      $self->log_debug("Pruning $file from ./blib\n");
      unlink $file or warn "Failed to unlink $file: $!";
    }
  }
}

sub _find_files {
  my ($regex, $root) = @_;

  my @results;
  if (-d $root) {
    File::Find::find(sub {
                       if (m{$regex} and -f) {
                         push @results, $File::Find::name;
                       }
                     }, $root);
  }

  return @results;
}

1;
