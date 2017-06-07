/*
Author             :刘东燕
Function           :承兑汇票信息表
Load method        :INSERT
Source table       :dw_sdata.bbs_001_accept_contract,dw_sdata.bbs_001_accept_details,dw_sdata.bbs_001_draft_info,dw_sdata.bbs_001_customer_info,dw_sdata.bbs_001_branch_info,dw_sdata.bbs_001_accept_due_pay,dw_sdata.bbs_001_bail,dw_sdata.bbs_001_cust_bail_main_account,dw_sdata.bbs_001_cust_bail_sub_account,dw_sdata.bbs_000_draft_classification
Destination Table  :f_agt_acptn_info  承兑汇票信息表
Frequency          :D
Modify history list:Created by刘东燕2016年4月19日10:05:55
                   :Modify  by liuxz 20160714 保证金账号Margn_Acct_Num映射改为coalesce(T6.BAIL_ACCOUNT,'')
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/
/*临时表创建区*/
--月积数等临时表--
create local temporary table tt_f_agt_acptn_info_temp_yjs  --月积数，年积数，月日均余额，年日均余额临时表
on commit preserve rows as 
select * 
from f_fdm.f_agt_acptn_info
where 1=2;
/*临时表创建区END*/
/*数据回退区*/
DELETE/* +direct */ from f_fdm.f_agt_acptn_info
where etl_date='$TXDATE'::date
;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_fdm.f_agt_acptn_info
(
         Grp_Typ                                                              --组别
        ,ETL_Date                                                             --数据日期
        ,Biz_Id                                                               --业务编号
        ,Bill_Num                                                             --票号
        ,Drawr_Cust_Id                                                        --出票人客户编号
        ,Org_Num                                                              --机构号
        ,Cur_Cd                                                               --货币代码
        ,Prod_Cd                                                              --产品代码
        ,Acpt_Dt                                                              --承兑日期
        ,Pay_Dt                                                               --付款日期
        ,Subj_Cd                                                              --科目代码
        ,Acpt_Amt                                                             --承兑金额
        ,Acpt_Bal                                                             --承兑余额
        ,Adv_Money_Amt                                                        --垫付金额
        ,Margn_Acct_Num                                                       --保证金账号
        ,Margn_Cur                                                            --保证金币种
        ,Margn_CFL_Amt                                                        --保证金圈存金额
        ,Margn_Ratio                                                          --保证金比例
        ,Acct_Stat_Cd                                                         --账户状态代码
        ,Fiv_Cls_Cd                                                           --五级分类代码
        ,Deval_Prep_Bal                                                       --减值准备余额
        ,Deval_Prep_Amt                                                       --减值准备发生额
        ,Mth_Accm                                                             --月积数
        ,Yr_Accm                                                              --年积数
        ,Mth_Day_Avg_Bal                                                      --月日均余额
        ,Yr_Day_Avg_Bal                                                      --年日均余额
        ,Sys_Src                                                             --系统来源
)
SELECT
       1                                                                   as Grp_Typ
       ,'$TXDATE'::date                                           as ETL_Date
       ,coalesce(to_char(T.ID),'')                                         as Biz_Id
       ,coalesce(T2.DRAFT_NUMBER,'')                                       as Bill_Num
       ,coalesce(T3.CUST_NO,'')                                            as Drawr_Cust_Id
       ,coalesce(T4.BRH_NO,'')                                             as Org_Num
       ,'156'                                                              as Cur_Cd
       ,'1000'                                                             as Prod_Cd
       ,'$MINDATE'::date                                               as Acpt_Dt
       ,coalesce(to_date(T5.ACCOUNT_DATE,'yyyymmdd'),'$MINDATE'::date) as Pay_Dt
       ,'9135'                                                             as Subj_Cd
       ,coalesce(T2.FACE_AMOUNT,0)                                         as Acpt_Amt
       ,0.00                                                               as Acpt_Bal
       ,0.00                                                               as Adv_Money_Amt
       ,coalesce(T6.BAIL_ACCOUNT,'')                                       as Margn_Acct_Num
       ,'156'                                                              as Margn_Cur
       ,coalesce(T8.TXN_AMT,0)                                             as Margn_CFL_Amt
       ,coalesce(T.BAIL_SCALE,0)                                           as Margn_Ratio
       ,T.CONTRACT_STATUS                                                  as Acct_Stat_Cd
       ,coalesce(T9.DRAFT_GRADE,'')                                        as Fiv_Cls_Cd
       ,0                                                               as Deval_Prep_Bal
       ,0                                                               as Deval_Prep_Amt
       ,0.00                                                               as Mth_Accm
       ,0.00                                                               as Yr_Accm
       ,0.00                                                               as Mth_Day_Avg_Bal
       ,0.00                                                               as Yr_Day_Avg_Bal
       ,'BBS'                                                              as Sys_Src
FROM        
      dw_sdata.bbs_001_accept_contract                                    AS T   --承兑协议表                         
INNER JOIN  dw_sdata.bbs_001_accept_details                               AS T1 --承兑明细表
ON    T.ID=T1.CONTRACT_ID
AND   T1.start_dt<='$TXDATE'::date
AND   T1.end_dt>'$TXDATE'::date                                
INNER JOIN  dw_sdata.bbs_001_draft_info                                   AS T2 --票据信息表
ON    T1.DRAFT_ID=T2.ID
AND   T2.DRAFT_NUMBER is not null 
AND   T2.DRAFT_NUMBER!='' -- ADD BY DHY 20160802
AND   T2.start_dt<='$TXDATE'::date
AND   T2.end_dt>'$TXDATE'::date     
AND   T2.STATUS!='99'
LEFT JOIN  dw_sdata.bbs_001_customer_info                                AS T3 --客户信息表
ON    T.REMITTER_CUST_ID=T3.ID
AND   T3.start_dt<='$TXDATE'::date
AND   T3.end_dt>'$TXDATE'::date     
LEFT JOIN  dw_sdata.bbs_001_branch_info                                  AS T4 --机构信息表
ON    T.BRANCH_ID=T4.ID
AND   T4.start_dt<='$TXDATE'::date
AND   T4.end_dt>'$TXDATE'::date     
LEFT JOIN  dw_sdata.bbs_001_accept_due_pay                               AS T5 --承兑到期兑付表
ON          T2.ID = T5.draft_id
AND   T5.start_dt<='$TXDATE'::date
AND   T5.end_dt>'$TXDATE'::date     
AND         T5.ACCOUNT_FLAG = '2'
LEFT JOIN dw_sdata.bbs_001_bail                                          AS T6 --保证金表
ON          T.ID=T6.CONTRACT_ID
AND   T6.start_dt<='$TXDATE'::date
AND   T6.end_dt>'$TXDATE'::date     
INNER JOIN dw_sdata.bbs_001_cust_bail_main_account                       AS T7 --保证金主账户
ON          T6.BAIL_ACCOUNT=T7.MAIN_ACC_NO
AND   T7.start_dt<='$TXDATE'::date
AND   T7.end_dt>'$TXDATE'::date     
INNER JOIN dw_sdata.bbs_001_cust_bail_sub_account                        AS T8 --保证金子账户表
ON          T7.ID =T8.MAIN_ACC_ID 
AND   T8.start_dt<='$TXDATE'::date
AND   T8.end_dt>'$TXDATE'::date     
AND   TRIM(T6.BAIL_CHILD_NO)=TRIM(T8.SUB_ACC_NO)
--LEFT JOIN dw_sdata.bbs_000_draft_classification                           AS T9 --票据分类等级表
left join (SELECT DRAFT_ID, 
                  DRAFT_GRADE,
                  START_DT,
                  END_DT,
                  ROW_NUMBER() OVER(PARTITION BY DRAFT_ID ORDER BY CLASSIFY_DATE DESC) AS NUM  
            FROM dw_sdata.bbs_000_draft_classification 
           WHERE start_dt<='$TXDATE'::date
             AND end_dt>'$TXDATE'::date
          ) t9
ON          T1.DRAFT_ID=T9.DRAFT_ID
and   t9.num = 1
AND   T9.start_dt<='$TXDATE'::date
AND   T9.end_dt>'$TXDATE'::date     
WHERE T.start_dt<='$TXDATE'::date
AND   T.end_dt>'$TXDATE'::date     
; 
 /*月积数、年积数、月日均余额、年日均余额临时表创建区*/
insert into tt_f_agt_acptn_info_temp_yjs
(
      Biz_Id
     ,Bill_Num
     ,Mth_Accm 
     ,Yr_Accm 
     ,Mth_Day_Avg_Bal 
     ,Yr_Day_Avg_Bal 
)
select 
      t.Biz_Id
     ,t.Bill_Num 
      ,(case 
            when '$TXDATE'= '$MONTHBGNDAY' 
            then t.Acpt_Bal
            else t.Acpt_Bal+coalesce(t1.mth_accm,0)
            end
       )                                                                      as mth_accm  --月积数
      ,(case 
            when  '$TXDATE' = '$YEARBGNDAY' 
            then t.Acpt_Bal
            else t.Acpt_Bal+coalesce(t1.yr_accm,0)
            end
       )                                                                      as yr_accm   --年积数
      ,(case 
            when '$TXDATE' = '$MONTHBGNDAY' 
            then t.Acpt_Bal
            else t.Acpt_Bal+coalesce(t1.mth_accm,0)
            end
       )/('$TXDATE'::date-'$MONTHBGNDAY'::date+1)               as mth_day_avg_bal  --月日均余额
      ,(case 
           when '$TXDATE' = '$YEARBGNDAY' 
           then t.Acpt_Bal
           else t.Acpt_Bal+coalesce(t1.yr_accm,0)
           end
       )/('$TXDATE'::date-'$YEARBGNDAY'::date+1)                    as Yr_Day_Avg_Bal   --年日均余额
from  f_fdm.f_agt_acptn_info     t
left join f_fdm.f_agt_acptn_info t1
on        t.Biz_Id=t1.Biz_Id
and       t.Bill_Num=t1.Bill_Num
and       t1.etl_date='$TXDATE'::date-1
where     t.etl_date='$TXDATE'::date
;
/*月积数、年积数、月日均余额、年日均余额临时表创建区END*/
/*更新月积数、年积数、月日均余额、年日均余额*/
update f_fdm.f_agt_acptn_info   t
set mth_accm=t1.mth_accm 
,yr_accm=t1.yr_accm
,mth_day_avg_bal=t1.mth_day_avg_bal
,Yr_Day_Avg_Bal=t1.Yr_Day_Avg_Bal
from  tt_f_agt_acptn_info_temp_yjs    t1
where t.Biz_Id=t1.Biz_Id
and   t.Bill_Num=t1.Bill_Num
and   t.etl_date='$TXDATE'::date
;
/*数据处理区END*/
COMMIT;

