








select count(ip), count(distinct ip) from a join b on a.1 = b.1 group by
  a.1, a.2 ;



select pz_id 1,ydz_ip 2,count(*)from url_rz where pz_id 1 in (963677,146870,429074) group by pz_id 1,ydz_ip 2 order by count(*) desc limit 100
select pz_id ,ydz_ip ,count(*)from url_rz where pz_id  in (963677,146870,429074) group by pz_id ,ydz_ip  order by count(*) desc limit 100

0 based index

1. group by
  input:
  0
  1 pz_id
  2 ydz_ip
  ==>
  groupby putputstream:
  0 pz_id
  1 ydz_ip

2. count(*) order by limit 100
  ==>
  \
  0 pz_id
  1 ydz_ip
  2 count(*)

