#!/bin/bash

set -euo pipefail
set -x

QUERY_NAME=find_single_replica_targets

query=$(
tr '\n' ' ' <<- EOF
select distinct c.coll_name, d.data_name
from r_data_main d, r_coll_main c
where d.coll_id = c.coll_id
and d.data_id in
((select object_id from r_meta_main mm, r_objt_metamap om
  where mm.meta_id = om.meta_id
  and mm.meta_attr_name = 'ebi_sub_md5'
  intersect
  select object_id from r_meta_main mm, r_objt_metamap om
  where mm.meta_id = om.meta_id
  and mm.meta_attr_name = 'dcterms:created'
  and mm.meta_attr_value > ?
  and mm.meta_attr_value < ?)
 except
 select object_id from r_meta_main mm, r_objt_metamap om
 where mm.meta_id = om.meta_id
 and mm.meta_attr_name = 'tier:single-copy')
EOF
)

iquest --sql ls | grep "$QUERY_NAME" >/dev/null && iadmin rsq "$QUERY_NAME"

iadmin asq "$query" "$QUERY_NAME"
