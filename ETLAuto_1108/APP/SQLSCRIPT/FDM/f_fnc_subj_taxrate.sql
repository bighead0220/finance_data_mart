/*
Author             :徐铭浩
Function           :insert
Load method        :
Source table       :dw_sdata.ACC_000_T_ACC_ITMTAX_PARA
Destination Table  :f_fdm.f_fnc_subj_taxrate
Frequency          :D
Modify history list:            

                    
*/

-------------------------------------------逻辑说明---------------------------------------------
-------------------------------------------逻辑说明END------------------------------------------

/*临时表创建区*/
/*临时表创建区end*/
/*数据回退区*/
DELETE FROM f_fdm.f_fnc_subj_taxrate WHERE ETL_DATE = '$TXDATE'::DATE 
;
/*数据回退区END*/
/*数据处理区*/
insert into f_fdm.f_fnc_subj_taxrate
 (
 grp_typ,
 etl_date,
 Subj,
 Org_Typ,
 Incom_Expns_Typ,
 Tax_Attr,
 Tax_Rate,
 Sys_Src
 )
select 1
       ,'$TXDATE'::DATE
       ,T.ITM_NO
       ,T.INST_TYPE
       ,T.IN_OUT_TYPE
       ,T.TAX_ATTR
       ,T.TAX_RATE
       ,'ACC'
  from dw_sdata.ACC_000_T_ACC_ITMTAX_PARA T
where t.start_dt<='$TXDATE'::date
and t.end_dt>'$TXDATE'::date
;

/*数据处理区END*/

COMMIT;

