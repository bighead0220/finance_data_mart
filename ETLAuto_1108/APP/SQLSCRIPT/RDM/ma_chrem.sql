/*
Author             :ch
Function           :理财
Load method        :INSERT
Source table       :F_ACCT_CHREM,f_prd_chrem
Destination Table  :MA_CHREM
Frequency          :M
Modify history list:Created by ch 2016/5/18 10:19:34
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
DELETE FROM f_rdm.MA_CHREM WHERE  etl_date =  '$TXDATE'::date ;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_rdm.MA_CHREM
(
       etl_date                   --数据日期
      ,cust_chrem_acct            --客户理财账户
      ,chrem_acct_contr_org       --理财账户的签约机构
      ,cur_natl_std               --币别（国标）
      ,prod_id                    --产品编号
      ,prod_series                --产品系列
      ,FOLDERS                    --FOLDERS
      ,prin_curr_bal              --本金的当前余额
      ,accm_bal_m                 --月累计余额
      ,accm_bal_y                 --年累计余额
      ,prin_subj                  --本金科目
      ,upto_to_today_accm_int     --截至到当日累计利息
      ,stop_to_last_day_accm_int  --截止到上日累计利息
      ,today_int                  --当日利息
      ,curmth_int_accm            --当月利息积数
      ,int_subj                   --利息科目
      ,src_sys                    --数据源系统
      ,mth_day_avg_bal            --月日均余额
      ,yr_day_avg_bal             --年日均余额
      ,FTP_Price                  --FTP价格
      ,ftp_tranfm_incom           --FTP转移收入
)
SELECT
       '$TXDATE'::date               as etl_date
      ,t.mdl_biz_tx_acct_num||'_'||t.contr_id||'_'||t.prod_cd  as cust_chrem_acct
      ,t.Org_Num                                as chrem_acct_contr_org
      ,t.cur_cd                                 as cur_natl_std
      ,t.prod_typ_cd                            as prod_id
      ,t1.Open_Mode_Ind                       as prod_series
      ,t.Cust_Id                                as FOLDERS
      ,t.prin_bal                               as prin_curr_bal
      ,t.mth_accm                               as accm_bal_m
      ,t.yr_accm                                as accm_bal_y
      ,t.prin_subj_cd                           as prin_subj
      ,t.stop_to_today_accm_int                 as upto_to_today_accm_int
      ,t.stop_to_last_day_accm_int              as stop_to_last_day_accm_int
      ,t.today_prft                             as today_int
      ,t.curmth_prft                            as curmth_int_accm
      ,t.prft_subj_cd                           as int_subj
      ,t.sys_src                                as src_sys
      ,t.mth_day_avg_bal                        as mth_day_avg_bal
      ,t.yr_day_avg_bal                         as yr_day_avg_bal
      ,0.00                                   as FTP_Price
      ,0.00                                   as ftp_tranfm_incom
 FROM f_fdm.f_acct_chrem  t
 left join  f_fdm.f_prd_chrem t1
   on t.prod_cd  = t1.prod_cd 
  and t1.etl_date = '$TXDATE'::date
WHERE t.etl_date = '$TXDATE'::date
;
/*数据处理区END*/

COMMIT;
