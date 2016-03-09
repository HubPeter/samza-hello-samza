select b.fl 5, b.xz 6, count(*) as cnt from url_rz a join zb_test b on a.pz_id 1 = b.pz_id 0 group by b.fl 5, b.xz 6

o based index


1. join on a.pz_id 1 = b.pz_id 0
  inputs:

  url_rz:
  pz_id 1

  zb_test:
  pz_id 0
  fl  5
  xz  6

  outputs:
  pz_id 0
  fl  1
  xz  2

2. group by => count(*)

  inputs:

  pz_id 0
  fl  1
  xz  2

  outputs:
  fl 0
  xz 1
  cnt 2
