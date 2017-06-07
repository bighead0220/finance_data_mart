/*
Author             :liudongyan
Function           :月授信客户表
Load method        :INSERT
Source table       :f_loan_indv_crdt,f_loan_corp_crdt
Destination Table  :ma_mth_crdt_cust   月授信客户表
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
DELETE FROM f_rdm.ma_mth_crdt_cust
WHERE  to_char(etl_date,'yyyymm')=substr( '$TXDATE',1,6);
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_rdm.ma_mth_crdt_cust 
(
       etl_date                                    --数据日期
       ,Org_Num                                    --机构号
       ,cust_num                                   --客户号
       ,crdt_amt                                   --授信金额
       ,sys_src                                    --系统来源
)
SELECT
     '$TXDATE'::date                      as etl_date
     ,belg_org_num                                 as Org_Num
     ,cust_num                                     as cust_num
     ,sum(crdt_amt)                                as crdt_amt
     ,sys_src                                      as sys_src

FROM  f_fdm.f_loan_indv_crdt --个人授信额度信息表
WHERE to_char(efft_dt,'yyyymm')=substr( '$TXDATE',1,6)
and Grp_Typ='1' --组别1包含（除小组联保类型里组员的数据外）所有的授信客户
GROUP BY cust_num,
      belg_org_num,
      sys_src
UNION ALL 
SELECT
     '$TXDATE'::date                      as etl_date
     ,Belg_Org_Num                                 as Org_Num
     ,Cust_Num                                     as cust_num
     ,sum(Crdt_Amt)                                as crdt_amt
     ,Sys_Src                                      as sys_src
FROM  f_fdm.f_loan_corp_crdt --公司授信额度信息表
WHERE to_char(efft_dt,'yyyymm')=substr('$TXDATE',1,6)
GROUP BY Cust_Num,
      Belg_Org_Num,
      Sys_Src
;
/*数据处理区END*/

COMMIT;
