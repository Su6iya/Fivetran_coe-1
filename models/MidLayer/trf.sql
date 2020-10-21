
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='view') }}

with source_data as (

  /*select * from  {{ source('stage_tables','EKKO') }} */

  select 
a.ebeln as PO_number,
a.ebelp as PO_item,
a.pstyp as Item_caterory,
c.bukrs as Company_code,
e.butxt as Company,
c.BSART as PO_type, 
a.BSTYP as PO_category,
a.LOEKZ as Deletion_Ind,
C.aedat as created_on,
c.ekorg as Pur_Org,
c.ekgrp as Pur_grp,
c.lifnr as Vendor_No,
I.name_1 as Vendor_Name,
c.bedat as PO_Date,
c.kdatb as star_date,
c.kdate as End_date,
c.reswk as supply_plant,
A.STATU as PO_status,
a.aedat as PO_item_date,
a.matnr as Material,
D.MAKTX as Mat_Descr,
a.werks as plant,
F.name_1 as plant_desc,
a.lgort as Storage_Location,
g.matkl as Mat_group,
A.KTMNG as Target_Qty,
A.MENGE as Purchase_Qty,
a.meins as Order_unit,
A.NETPR as Net_price,
a.netwr as net_ordervalue,
a.webaz as proctime_in_days,
A.ELIKZ as delivery_ind,
A.WEPOS as GR_indicator,
A.REPOS as IR_indicator,
A.kunnr as Customer,
H.name_1 AS Customer_name,
a.attyp as Mat_cat,
g.mtart as mat_type,
B.BANFN as PR_No,
B.BNFPO as PR_itemno, 
b.bsart as PR_doctype,
B.bstyp as PR_doccat,
b.statu as PR_status,
B.erdat as PR_changedon,
b.afnam as PR_requestor,
b.bsmng as PR_qty_against_ord,
b.bmein as PR_unit,
B.MENGE as pur_req_qty,
b.meins as PR_qty_unit

from {{ source('stage_tables','EKPO') }} as A 
inner join {{ source('stage_tables','EKKO') }} as c 
on a.ebeln = c.ebeln
left outer JOIN {{ source('stage_tables','EBAN') }} as B
on A.banfn = B.banfn
and A.bnfpo = B.bnfpo
left outer join {{ source('stage_tables','MARA') }}  as G
on a.matnr = g.matnr
left outer JOIN {{ source('stage_tables','MAKT') }} AS D 
ON g.MATNR = D.MATNR
AND D.SPRAS = 'E'
left outer JOIN {{ source('stage_tables','T_001') }} as E
on a.bukrs = e.bukrs
left outer join {{ source('stage_tables','T_001_W') }} as F 
on a.werks = f.werks
and f.SPRAS = 'E'
left outer join {{ source('stage_tables','KNA_1') }} as H
on a.kunnr = h.kunnr
and h.SPRAS = 'E'
left outer join {{ source('stage_tables','LFA_1') }} as I
on C.lifnr = I.lifnr
and h.SPRAS = 'E'
/*WHERE A.ebeln in ( 4500001063,4500001203 )*/

)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
