/*
Author             :zhangwj
Function           :贵金属账户信息表
Load method        :
Source table       :dw_sdata.gts_000_cust_info,dw_sdata.gts_000_storage,dw_sdata.gts_000_t_pim_customer_info,dw_sdata.gts_000_fund,dw_sdata.gts_000_variety,dw_sdata.gts_000_prod_settle_price ,dw_sdata.gts_000_acct_broker_info,dw_sdata.acc_003_t_acc_cdm_ledger    
Destination Table  :f_fdm.f_acct_prec_metal
Frequency          :D
Modify history list:Created by zhangwj at 2016-05-05 9:49 v1.0
                    Changed by zhangwj at 2016-5-26 13:12 v1.1   大数据贴源层表名修改，表为拉链表或流水表，与之保持一致
                    Changed by zhangwj at 2016-6-14 14:47 v1.2   新增月积数、年积数、月日均余额、年日均余额
                   :Modify  by xmh at 20160822 根据模型变更，更改T3表为 gts_ooo_his_m_fund及关联条件
                    modify by zhangliang 20160927  修改关�,t2��表t6
                    modified by liudongyan at 20160930 将T5的 拉链日期改为流水日期
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/

/*临时表创建区*/
--月积数等临时表--
create local temporary table tt_f_acct_prec_metal_temp_yjs  --月积数，年积数，月日均余额，年日均余额临时表
on commit preserve rows as 
select * 
from f_fdm.f_acct_prec_metal
where 1=2;
/*临时表创建区END*/

/*数据回退区*/
delete /* +direct */ from f_fdm.f_acct_prec_metal
where  etl_date = '$TXDATE'
;
/*数据回退区END*/

/*数据处理区*/
insert /* +direct */  into f_fdm.f_acct_prec_metal
      (grp_typ                                                         --组别
      ,etl_date                                                        --数据日期
      ,agmt_id                                                         --协议编号
      ,prod_cd                                                         --产品代码
      ,dpst_acct_num                                                   --存款账号
      ,cust_id                                                         --客户编号
      ,org_num                                                         --机构号
      ,cur_cd                                                          --货币代码
      ,open_acct_dt                                                    --开户日期
      ,prin_subj                                                       --本金科目
      ,prec_metal_lot                                                  --贵金属份额
      ,curr_mkt_val                                                    --当前市值
      ,prec_metal_amt                                                  --贵金属金额
      ,acct_bal                                                        --账户余额
      ,cust_mgr_id                                                     --客户经理编号
      ,mth_accm                                                        --月积数
      ,yr_accm                                                         --年积数
      ,mth_day_avg_bal                                                 --月日均余额
      ,yr_day_avg_bal                                                  --年日均余额
      ,sys_src                                                         --系统来源
      )
 select
       1                                                               as  grp_typ             --组别
       ,'$TXDATE'::date                                       as  etl_date            --数据日期
       ,coalesce(t.acct_no,'')                                         as  agmt_id             --协议编号
       ,coalesce(t1.variety_id,'')                                     as  prod_cd             --产品代码
       ,coalesce(t.account_no,'')                                      as  dpst_acct_num       --存款账号
       ,coalesce(t2.customer_id,'')                                    as  cust_id             --客户编号
       ,coalesce(t.branch_id,'')                                       as  org_num             --机构号
       ,coalesce(t3.currency_id,'')                                    as  cur_cd              --货币代码
       ,coalesce(t.o_date,'$MAXDATE'::date)                        as  open_acct_dt        --开户日期
       ,coalesce(t7.itm_no,'')                                         as  prin_subj           --本金科目
       ,coalesce(t1.curr_amt,0)                                        as  prec_metal_lot      --贵金属份额
       ,coalesce(t5.settle_price,0)                                    as  cur_mkt_val         --当前市值
       ,coalesce(t1.curr_amt,0)*coalesce(t5.settle_price,0)            as  prec_metal_amt      --贵金属金额
       ,coalesce(t3.curr_bal,0)                                        as  acct_bal            --账户余额
       ,t6.broker_id                                                   as cust_mgr_id--客户经理编号
       ,0.00                                                           as  mth_accm            --月积数
       ,0.00                                                           as  yr_accm             --年积数
       ,0.00                                                           as  mth_day_avg_bal     --月日均余额
       ,0.00                                                           as  yr_day_avg_bal      --年日均余额
       ,'GTS'                                                          as  sys_src             --系统来源   
 from  dw_sdata.gts_000_cust_info                  t                   --客户信息表
 left join dw_sdata.gts_000_storage                t1                  --库存余额表
 on        t.acct_no=t1.acct_no
 and       t1.start_dt<='$TXDATE'::date 
 and       t1.end_dt>'$TXDATE'::date 
 left join (select customer_id,gold_exch_no,M_DATE_PWD,start_dt,end_dt,row_number() over(partition by gold_exch_no order by M_DATE_PWD desc) as num 
              from dw_sdata.gts_000_t_pim_customer_info 
             where start_dt<='$TXDATE'
               and end_dt>'$TXDATE')   t2                  --客户信息表
 on        t.acct_no=t2.gold_exch_no
and        t2.num=1
 and       t2.start_dt<='$TXDATE'::date 
 and       t2.end_dt>'$TXDATE'::date 
 left join dw_sdata.gts_000_his_m_fund                t3                  --历史会员清算资金余额表
 on        t.acct_no=t3.acct_no
 and       t3.exch_date='$TXDATE'
 and       t3.start_dt<='$TXDATE'::date 
 and       t3.end_dt>'$TXDATE'::date 
 left join dw_sdata.gts_000_variety                t4                  --交割品种表
 on        t1.variety_id=t4.variety_id
 and       t4.start_dt<='$TXDATE'::date 
 and       t4.end_dt>'$TXDATE'::date 
 left join dw_sdata.gts_000_prod_settle_price      t5                 --合约结算价信息表
 on        t4.name =t5.prod_code
-- and       t5.start_dt<='$TXDATE'::date 
-- and       t5.end_dt>'$TXDATE'::date 
and         t5.etl_dt='$TXDATE'::date
LEFT JOIN  (select acct_no,broker_id,start_dt,end_dt,row_number() over(partition by acct_no order by rate desc) as num
 from dw_sdata.gts_000_acct_broker_info
 where start_dt<='$TXDATE'
and end_dt>'$TXDATE') t6
ON         t.acct_no=t6.acct_no
and t6.num=1
and        t6.start_dt<='$TXDATE'::date
 and       t6.end_dt>'$TXDATE'::date
 left join  dw_sdata.acc_003_t_acc_cdm_ledger        t7                 --负债类储蓄本币账户活期分户账
 on         t.ACCT_NO=t7.ACC 
 and        t7.SYS_CODE='99550000000'
 and        t7.start_dt<='$TXDATE'::date 
 and        t7.end_dt>'$TXDATE'::date 
 where      t.start_dt<='$TXDATE'::date 
 and        t.end_dt>'$TXDATE'::date 

 ;
 /*月积数、年积数、月日均余额、年日均余额临时表创建区*/
 
insert into tt_f_acct_prec_metal_temp_yjs
(
      dpst_acct_num
      ,agmt_id
      ,Mth_Accm 
      ,Yr_Accm 
      ,Mth_Day_Avg_Bal 
      ,Yr_Day_Avg_Bal 
)
select 
      t.dpst_acct_num
      ,t.agmt_id
      ,(case 
            when '$TXDATE'= '$MONTHBGNDAY' 
            then t.acct_bal
            else t.acct_bal+coalesce(t1.mth_accm,0)
            end
       )                                                                      as mth_accm  --月积数
      ,(case 
            when  '$TXDATE' = '$YEARBGNDAY' 
            then t.acct_bal
            else t.acct_bal+coalesce(t1.yr_accm,0)
            end
       )                                                                      as yr_accm   --年积数
      ,(case 
            when '$TXDATE' = '$MONTHBGNDAY' 
            then t.acct_bal
            else t.acct_bal+coalesce(t1.mth_accm,0)
            end
       )/('$TXDATE'::date-'$MONTHBGNDAY'::date+1)               as mth_day_avg_bal  --月日均余额
      ,(case 
           when '$TXDATE' = '$YEARBGNDAY' 
           then t.acct_bal
           else t.acct_bal+coalesce(t1.yr_accm,0)
           end
       )/('$TXDATE'::date-'$YEARBGNDAY'::date+1)                    as Yr_Day_Avg_Bal   --年日均余额
from  f_fdm.f_acct_prec_metal     t
left join f_fdm.f_acct_prec_metal t1
on        t.dpst_acct_num=t1.dpst_acct_num
and       t.agmt_id=t1.agmt_id
and       t1.etl_date='$TXDATE'::date-1
where     t.etl_date='$TXDATE'::date
;
/*月积数、年积数、月日均余额、年日均余额临时表创建区END*/
/*更新月积数、年积数、月日均余额、年日均余额*/
update f_fdm.f_acct_prec_metal t
set mth_accm=t1.mth_accm 
,yr_accm=t1.yr_accm
,mth_day_avg_bal=t1.mth_day_avg_bal
,Yr_Day_Avg_Bal=t1.Yr_Day_Avg_Bal
from tt_f_acct_prec_metal_temp_yjs t1
where  t.dpst_acct_num=t1.dpst_acct_num
and    t.agmt_id=t1.agmt_id

and    t.etl_date='$TXDATE'::date
;
/*数据处理区END*/

 COMMIT;
