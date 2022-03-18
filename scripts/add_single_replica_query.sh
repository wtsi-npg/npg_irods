#!/bin/bash

set -euo pipefail
set -x

QUERY_NAME=find_single_replica_targets

query=$(
tr '\n' ' ' <<- EOF
select distinct
c.coll_name as coll_name,
d.data_name as data_name
from r_data_main d join r_coll_main as c on c.coll_id = d.coll_id
join r_objt_metamap mm1 on d.data_id = mm1.object_id
join r_meta_main m1 on mm1.meta_id = m1.meta_id
join r_objt_metamap mm2 on d.data_id = mm2.object_id
join r_meta_main m2 on mm2.meta_id = m2.meta_id
where d.data_id not in
  (select d.data_id from r_data_main d
   join r_objt_metamap mm on d.data_id = mm.object_id
   join r_meta_main m on mm.meta_id = m.meta_id
   where m.meta_attr_name = 'tier:single-copy')
and m1.meta_attr_name = 'ebi_sub_md5'
and m2.meta_attr_name = 'dcterms:created'
and m2.meta_attr_value > ?
and m2.meta_attr_value < ?
order by coll_name, data_name
EOF
)

iquest --sql ls | grep "$QUERY_NAME" >/dev/null && iadmin rsq "$QUERY_NAME"

iadmin asq "$query" "$QUERY_NAME"
