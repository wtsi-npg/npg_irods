# npg_irods

[![Unit tests](https://github.com/wtsi-npg/npg_irods/actions/workflows/run-tests.yml/badge.svg)](https://github.com/wtsi-npg/npg-irods-python/actions/workflows/run-tests.yml)

## Overview

This repository is the home of application code used by NPG to manage data and
metadata in WSI [iRODS](https://irods.org).

It includes:

- Tools for managing data in iRODS from various analysis platforms, including:
   - Illumina sequencing instruments
   - PacBio sequencing instruments
   - BioNano optical mapping instruments

- General purpose tools for bulk data management in iRODS

For each platform there are scripts to upload data to iRODS, annotate it
with metadata describing its origin and set data access permissions. There
are further scripts to update metadata and access permissions in response to
upstream changes.

The core metadata used by NPG to annotate analysis results in iRODS are
described in a separate [repository](https://github.com/wtsi-npg/irods-metadata)


## Building and testing

This repository has dependencies declared in `Build.PL`, which must be installed
prior to building. There is a working example of this in the CI workflow
`run-tests.yml`.

An iRODS server is required for the tests. The iRODS configuration environment
variable used by the tests is intentionally different from iRODS' default to
reduce the risk of accidentally running tests against a different iRODS server
than intended. The environment variable
`WTSI_NPG_iRODS_Test_IRODS_ENVIRONMENT_FILE` must be set to the path of a
client iRODS environment JSON configuration file corresponding to the test
server. See the [iRODS documentation](https://irods.org/documentation/) for more
information about this file.

Once this is done, tests may be run with the command:

```commandline
perl Build.PL
./Build test
```
