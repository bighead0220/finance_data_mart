/*fss_001_ta_cust_asset_acct
Author             :刘东燕
Function           :理财账户信息表
Load method        :INSERT
Source table       :dw_sdata.fss_001_ta_cust_asset_acct,dw_sdata.fss_001_ta_cust_acct_info,dw_sdata.fss_001_ta_prod_asset_list,dw_sdata.fss_001_gf_prod_base_info
                     dw_sdata.fss_001_gf_prod_netvalue_info
                     dw_sdata .fss_001_gf_sell_trade_list_note          
Destination Table  :f_acct_chrem  理财账户信息表
Frequency          :D
Modify history list:Created by刘东燕2016年4月19日10:05:55
                   :Modify  by liudongyan 20160707修改货币代码取数规则（变更记录104）
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/
/*数据回退区*/
DELETE FROM f_fdm.f_acct_chrem
WHERE etl_date='$TXDATE'::date
;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_fdm.f_acct_chrem
(
        Grp_Typ                                            --组别
       ,etl_date                                           --数据日期
       ,mdl_biz_tx_acct_num                                --中间业务交易账号
       ,contr_id                                           --合同编号
       ,prod_cd                                            --产品代码
       ,cust_id                                            --客户编号
       ,org_num                                            --机构号
       ,cur_cd                                             --货币代码
       ,dpst_acct_num                                      --存款账号
       ,open_acct_dt                                       --开户日期
       ,prod_typ_cd                                        --产品类型代码
       ,prin_subj_cd                                       --本金科目代码
       ,prin_bal                                           --本金余额
       ,chrem_lot                                          --理财份额
       ,net_worth                                          --净值
       ,amt                                                --发生额
       ,actl_prft_amt                                      --实际收益金额
       ,stop_to_today_accm_int                             --截止到当日累计利息
       ,stop_to_last_day_accm_int                          --截止到上日累计利息
       ,prft_subj_cd                                       --收益科目代码
       ,today_prft                                         --当日收益
       ,curmth_prft                                        --当月收益
       ,mth_accm                                           --月积数
       ,yr_accm                                            --年积数
       ,mth_day_avg_bal                                    --月日均余额
       ,yr_day_avg_bal                                     --年日均余额
       ,cust_mgr_id                                        --客户经理编号
       ,sys_src                                            --系统来源
)
SELECT
       '1'                                                as Grp_Typ
        ,'$TXDATE'::date                         as etl_date
       ,coalesce(T1.retailer_cust_code,'')                as mdl_biz_tx_acct_num
       ,coalesce(T2.contract_no,'')                       as contr_id
       ,coalesce(T.prod_code,'')                          as prod_cd
       ,''                                                as cust_id
       ,coalesce(T1.open_organ,'')                        as org_num
       ,coalesce(T.currency,'')                           as cur_cd
       ,''                                                as dpst_acct_num
       ,coalesce(to_date(T.open_date,'yyyymmdd'),'$MINDATE'::date)                    as open_acct_dt
       ,coalesce(T3.profit_type,'')                       as prod_typ_cd
       ,'274006'                                          as prin_subj_cd
       ,coalesce(T.sum_affirm_quot*T4.net_value,0)        as prin_bal
       ,coalesce(T.sum_affirm_quot,0)                     as chrem_lot
       ,coalesce(T4.net_value,0)                          as net_worth
       ,amt                                               as amt
       ,0.00                                              as actl_prft_amt
       ,0.00                                              as stop_to_today_accm_int
       ,0.00                                              as stop_to_last_day_accm_int
       ,''                                                as prft_subj_cd
       ,0.00                                              as today_prft
       ,0.00                                              as curmth_prft
       ,0.00                                              as mth_accm
       ,0.00                                              as yr_accm
       ,0.00                                              as mth_day_avg_bal
       ,0.00                                              as yr_day_avg_bal
       ,coalesce(T5.cust_mgr_code,'')                     as cust_mgr_id
       ,'FSS'                                             as sys_src


         
FROM  dw_sdata.fss_001_ta_cust_asset_acct            AS T                   
LEFT  JOIN  dw_sdata.fss_001_ta_cust_acct_info          AS T1                          
ON        T.ta_code=T1.ta_code
AND       T.retailer_code=T1.retailer_code
AND       T1.start_dt<='$TXDATE'::date
AND       T1.end_dt>'$TXDATE'::date
LEFT JOIN dw_sdata.fss_001_ta_prod_asset_list           AS T2             
ON         T.ta_code=T2.ta_code
AND        T.retailer_code=T2.retailer_code
AND        T.prod_code=T2.prod_code
AND        T2.start_dt<='$TXDATE'::date
AND        T2.end_dt>'$TXDATE'::date
LEFT JOIN  dw_sdata.fss_001_gf_prod_base_info    AS T3
ON         T.prod_code=T3.prod_code
AND        T3.start_dt<='$TXDATE'::date
AND        T3.end_dt>'$TXDATE'::date
LEFT JOIN  dw_sdata.fss_001_gf_prod_netvalue_info  AS T4                                             
ON         T.prod_code=T4.prod_code
AND        T4.start_dt<='$TXDATE'::date
AND        T4.end_dt>'$TXDATE'::date
LEFT JOIN 
          (select trade_amt,cust_mgr_code,prod_code,sum(trade_amt) as amt 
           from dw_sdata.fss_001_gf_sell_trade_list_note   a
           where  a.etl_dt='$TXDATE'::date
           group by trade_amt,cust_mgr_code,prod_code ) T5
ON         T.prod_code=T5.prod_code
WHERE      T.start_dt<='$TXDATE'::date
AND        T.end_dt>'$TXDATE'::date
;
 /*月积数、年积数、月日均余额、年日均余额临时表创建区*/
 
create local temporary table IF NOT EXISTS f_acct_chrem_temp_1
on commit preserve rows as
select 
      t.mdl_biz_tx_acct_num
     ,t.contr_id 
     ,t.prod_cd
      ,(case 
            when '$TXDATE'= '$MONTHBGNDAY' 
            then t.prin_bal
            else t.prin_bal+coalesce(t1.mth_accm,0)
            end
       )                                                                      as mth_accm  --月积数
      ,(case 
            when  '$TXDATE' = '$YEARBGNDAY' 
            then t.prin_bal
            else t.prin_bal+coalesce(t1.yr_accm,0)
            end
       )                                                                      as yr_accm   --年积数
      ,(case 
            when '$TXDATE' = '$MONTHBGNDAY' 
            then t.prin_bal
            else t.prin_bal+coalesce(t1.mth_accm,0)
            end
       )/('$TXDATE'::date-'$MONTHBGNDAY'::date+1)               as mth_day_avg_bal  --月日均余额
      ,(case 
           when '$TXDATE' = '$YEARBGNDAY' 
           then t.prin_bal
           else t.prin_bal+coalesce(t1.yr_accm,0)
           end
       )/('$TXDATE'::date-'$YEARBGNDAY'::date+1)                    as Yr_Day_Avg_Bal   --年日均余额
from  f_fdm.f_acct_chrem     t
left join f_fdm.f_acct_chrem t1
on        t.mdl_biz_tx_acct_num=t1.mdl_biz_tx_acct_num
and       t.contr_id=t1.contr_id 
and       t.prod_cd=t1.prod_cd
and       t1.etl_date='$TXDATE'::date-1
where     t.etl_date='$TXDATE'::date
;
/*月积数、年积数、月日均余额、年日均余额临时表创建区END*/
/*更新月积数、年积数、月日均余额、年日均余额*/
update f_fdm.f_acct_chrem   t
set mth_accm=t1.mth_accm 
,yr_accm=t1.yr_accm
,mth_day_avg_bal=t1.mth_day_avg_bal
,Yr_Day_Avg_Bal=t1.Yr_Day_Avg_Bal
from  f_acct_chrem_temp_1    t1
where t.mdl_biz_tx_acct_num=t1.mdl_biz_tx_acct_num
and   t.contr_id=t1.contr_id 
and   t.prod_cd=t1.prod_cd
and   t.etl_date='$TXDATE'::date
;
/*数据处理区END*/
COMMIT;
