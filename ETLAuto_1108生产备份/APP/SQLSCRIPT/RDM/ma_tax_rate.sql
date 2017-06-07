/*
Author             :liudongyan
Function           :税率
Load method        :INSERT
Source table       :f_fnc_subj_taxrate,f_fnc_subj_info
Destination Table  :ma_tax_rate 税率
Frequency          :M
Modify history list:Created by liudongyan 2016/5/18 10:19:34
                   :Modify  by
*/

-------------------------------------------逻辑说明---------------------------------------------
/* 

*/
-------------------------------------------逻辑说明END------------------------------------------
/*月末跑批控制语句*/
SELECT COUNT(1) 
FROM dual
where '$MONTHENDDAY'='$TXDATE';
.IF ActivityCount <= 0 THEN .GOTO SCRIPT_END;
/*月末跑批控制语句END*/
/*临时表创建区*/
/*临时表创建区END*/
/*数据回退区*/
DELETE FROM f_rdm.ma_tax_rate
WHERE  etl_date='$TXDATE'::date ;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_rdm.ma_tax_rate
(
      etl_date                                                   --数据日期
     ,Subj                                                       --科目
     ,Subj_Nm                                                    --科目名称
     ,org_typ                                                    --机构类型
     ,Incom_Expns_Typ                                            --收入支出类型
     ,Tax_Attr                                                   --计税属性
     ,Tax_Rate                                                   --税率


)
SELECT
     '$TXDATE'::date                                          as etl_date
     ,T.Subj                                                              as Subj
     ,T1.Subj_Nm                                                           as Subj_Nm
     ,T.Org_Typ                                                           as org_typ
     ,T.Incom_Expns_Typ                                                   as Incom_Expns_Typ
     ,T.Tax_Attr                                                          as Tax_Attr
     ,T.Tax_Rate                                                          as Tax_Rate
 FROM f_fdm.f_fnc_subj_taxrate t --科目税率信息表 
left join  f_fdm.f_fnc_subj_info t1 --科目信息表
  on t.Subj=t1.Subj_Cd
 and t1.Sys_Src = 'ACC' -- 会计处理平台
 AND T1.etl_date = '$TXDATE'::date 
WHERE T.etl_date = '$TXDATE'::date
;
/*数据处理区END*/

COMMIT;
