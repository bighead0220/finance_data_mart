/*
Author             :Liuxz
Function           :
Load method        :
Source table       :dw_sdata.ics_001_borgt_pol,dw_sdata.ics_001_smctl_insu_kind_code,dw_sdata.ics_001_borgt_cust,dw_sdata.ecf_001_t01_cust_info,dw_sdata.ecf_004_t01_cust_info
Destination Table  :f_fdm.f_acct_insu_agn
Frequency          :D
Modify history list:Created by Liuxz at 2016-04-26 13:19:56.794000
                   :Modify  by Liuxz at 2016-05-25 更改贴源层表名
                   modified by liuxz 20160704 修改“客户编号”的取数逻辑 (变更记录94)
                   modified by zmx 20160819   修改 Insu_Bred_Efft_Dt 字段
				   modified by wyh at 20161014   增加T1表的关联条件 修改t3为个人客户表
*/


-------------------------------------------逻辑说明---------------------------------------------
/*业务逻辑说明
保险代销账户信息表
*/
-------------------------------------------逻辑说明END------------------------------------------
/*创建临时表区*/ 
/*创建临时表区END*/
/*数据回退区*/
delete /* +direct */ from f_fdm.f_acct_insu_agn
where etl_date='$TXDATE'::date
;
/*数据回退区END*/
/*数据处理区*/
insert /* +direct */ into f_fdm.f_acct_insu_agn
        (grp_typ                                                     --组别
        ,ETL_Date                                                    --数据日期
        ,Agmt_Id                                                     --协议编号
        ,Insu_Corp_Cd                                                --保险公司代码
        ,Prod_Cd                                                     --产品代码
        ,Dpst_Acct_Num                                               --存款账号
        ,Cust_Id                                                     --客户编号
        ,Org_Num                                                     --机构号
        ,Cur_Cd                                                      --货币代码
        ,Insu_Lot                                                    --保险份额
        ,Accm_Prem                                                   --累计保费
        ,Insu_Amt                                                    --保额
        ,Paym_Mode_Cd                                                --交费方式代码
        ,Sign_Dt                                                     --签单日期
        ,Insu_Bred_Cd                                                --险种代码
        ,Insu_Bred_Efft_Dt                                           --险种生效日期
        ,Agmt_Stat_Cd                                                --协议状态代码
        ,Cust_Mgr_Id                                                 --客户经理编号
        ,Sys_Src                                                     --系统来源
        )
select  '1'                                                             as   grp_typ               --组别
        ,'$TXDATE'::date                                       as   ETL_Date              --数据日期
        ,T.POL_ID                                                       as   Agmt_Id               --协议编号
        ,T.INSU_CODE                                                    as   Insu_Corp_Cd          --保险公司代码
        ,T.INSU_PRO_KIND                                                as   Prod_Cd               --产品代码
        ,T.SAV_ACCT                                                     as   Dpst_Acct_Num         --存款账号
        ,COALESCE(T3.ECIF_CUST_NO,T2.CUST_ID)                           as   Cust_Id               --客户编号
        ,T.UNIT_CODE                                                    as   Org_Num               --机构号
        ,'156'                                                          as   Cur_Cd                --货币代码
        ,0.00                                                           as   Insu_Lot              --保险份额
        ,0.00                                                           as   Accm_Prem             --累计保费
        ,T.POL_AMT                                                      as   Insu_Amt              --保额
        ,T.PAY_MODE                                                     as   Paym_Mode_Cd          --交费方式代码
        ,to_date(T.CLR_DATE,'YYYYMMDD')                                 as   Sign_Dt               --签单日期
        ,T.INSU_KIND                                                    as   Insu_Bred_Cd          --险种代码
        ,coalesce(to_date(T1.INSU_START_DATE,'YYYYMMDD'),'$MINDATE'::date)                         as   Insu_Bred_Efft_Dt     --险种生效日期
        ,T.POL_STAT_FLAG                                                as   Agmt_Stat_Cd          --协议状态代码
        ,T.SALE_ID                                                      as   Cust_Mgr_Id           --客户经理编号
        ,'ICS'                                                          as   Sys_Src               --系统来源
from    dw_sdata.ics_001_borgt_pol T
left join   dw_sdata.ics_001_smctl_insu_kind_code T1
on      T.INSU_PRO_KIND=T1.INSU_PROD_CODE 
AND     T.INSU_KIND=T1.INSU_KIND_CODE
and     T.INSU_CODE = t1.INSU_CODE            --modified by wyh at 20161014 
AND     T1.START_DT<='$TXDATE'::date
AND     T1.END_DT>'$TXDATE'::date
left join dw_sdata.ics_001_borgt_cust T2
on      T.POH_CUST_ID=T2.CUST_ID 
AND     T2.START_DT<='$TXDATE'::date
AND     T2.END_DT>'$TXDATE'::date
left join (select * from 
(select ecif_cust_no,CERT_NO,PARTY_NAME, is_vip_flag,START_DT,end_dt,
row_number() over(partition by CERT_NO,PARTY_NAME  order by is_vip_flag desc)  as num2 from
(select * from 
(select ecif_cust_no,CERT_NO,PARTY_NAME, is_vip_flag,START_DT,end_dt,
row_number() over(partition by CERT_NO,PARTY_NAME  order by cert_due_date desc)  as num
from dw_sdata.ecf_001_t01_cust_info where 
UPDATED_TS = '99991231 00:00:00:000000' 
and START_DT<='$TXDATE'::date
and '$TXDATE'::date<end_dt) a 
where a.num=1
union all
select * from 
(select ecif_cust_no,CERT_NO,PARTY_NAME, is_vip_flag,START_DT,end_dt,
row_number() over(partition by CERT_NO,PARTY_NAME  order by cert_due_date desc)  as num
from dw_sdata.ecf_004_t01_cust_info where 
UPDATED_TS = '99991231 00:00:00:000000' 
and START_DT<='$TXDATE'::date
and '$TXDATE'::date<end_dt) b 
where b.num=1) c) d
where d.num2=1      ) t3                   --modified by wyh at 20161014 
on         T2.CERT_ID=T3.CERT_NO
AND        T2.NAME=T3.PARTY_NAME
WHERE   T.START_DT<='$TXDATE'::date
AND     T.END_DT>'$TXDATE'::date
;
/*数据处理区END*/
commit;
