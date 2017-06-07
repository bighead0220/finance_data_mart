/*
Author             :liudongyan
Function           :存款
Load method        :INSERT
Source table       :f_dpst_indv_acct,f_dpst_corp_acct     
Destination Table  :ma_dpst
Frequency          :M
Modify history list:Created by liudongyan 2016/5/18 10:19:34
                   :Modify  by dhy 20160802 add ftp
                   :Modify by zhangliang 2016-8-9 10:11:24 (1)将select中to_number(a.Int_Base_Cd)修改为 a.Int_Base_Cd
                                                           (2)将UNION ALL中to_number(a.Int_Base_Cd)修改为 a.Int_Base_Cd
                    modified by wyh at 20161009优化了关联条件，将UNION换为INSERT
*/

-------------------------------------------逻辑说明---------------------------------------------
/* 

*/
-------------------------------------------逻辑说明END------------------------------------------
SELECT COUNT(1) 
FROM dual
where '$MONTHENDDAY'='$TXDATE';
.IF ActivityCount <= 0 THEN .GOTO SCRIPT_END;
/*月末跑批控制语句END*/
/*临时表创建区*/
/*临时表创建区END*/
/*数据回退区*/
DELETE FROM f_rdm.ma_dpst
WHERE  etl_date='$TXDATE'::date  ;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_rdm.ma_dpst  
(
      etl_date                              --渠道编码
     ,Dpst_Acct_Num                         --存款账号
     ,Org_Num                               --机构号
     ,cust_num                              --客户号
     ,Cur_Cd                                --币种
     ,prod_cd                               --产品代码
     ,St_Int_Dt                             --起息日
     ,Due_Dt                                --到期日
     ,Ori_Int_Rate                          --原始利率
     ,Curr_Int_Rate                         --当前利率
     ,Bmk_Int_Rate                          --基准利率
     ,Basic_Diff                            --基差
     ,Int_Days                              --计息天数
     ,Cmpd_Int_Calc_Mode_Cd                 --复利计算方式代码
     ,Int_Mode_Cd                           --计息方式(先收后收)
     ,Int_Rate_Attr_Cd                      --固定浮动属性
     ,Orgnl_Term                            --原始期限
     ,Orgnl_Term_Corp_Cd                    --原始期限单位
     ,Rprc_Prd                              --重定价周期
     ,Rprc_Prd_Corp_Cd                      --重定价周期单位
     ,Last_Rprc_Day                         --上次重定价日
     ,Next_Rprc_Day                         --下次重定价日
     ,sys_src                               --系统来源
     ,Is_AutoRnw                            --是否自动转存
     ,Last_AutoRnw_Dt                       --上次自动转存日期
     ,Acct_Stat_Cd                          --账户状态
     ,prin_subj                             --本金科目
     ,Curr_Bal                              --当前余额
     ,int_subj                              --利息科目
     ,Today_Provs_Int                       --当日计提利息
     ,CurMth_Provs_Int                      --当月计提利息
     ,Accm_Provs_Int                        --累计计提利息
     ,Today_Int_Pay                         --当日付息
     ,CurMth_Paid_Int                       --当月已付息
     ,Accm_Paid_Int                         --累计已付息
     ,int_adj_amt                           --利息调整金额
     ,mth_day_avg_bal                       --月日均余额
     ,yr_day_avg_bal                        --年日均余额
     ,FTP_Price                             --FTP价格
     ,ftp_tranfm_incom                      --FTP转移收入
 
)
SELECT
      '$TXDATE'::date                     as etl_date
     ,a.Cust_Acct_Num||'_'||a.Sub_Acct                as Dpst_Acct_Num
     ,a.Org_Num                                      as Org_Num
     ,a.Cust_Num                                     as cust_num
     ,a.Cur_Cd                                       as Cur_Cd
     ,a.Prod_Cd                                      as prod_cd
     ,a.St_Int_Dt                                    as St_Int_Dt
     ,a.Due_Dt                                       as Due_Dt
     ,0                                              as Ori_Int_Rate
     ,a.Exec_Int_Rate                                as Curr_Int_Rate
     ,a.Bmk_Int_Rate                                 as Bmk_Int_Rate
     ,a.Basis                                        as Basic_Diff
     ,a.Int_Base_Cd                                  as Int_Days
     ,a.Cmpd_Int_Calc_Mode_Cd                        as Cmpd_Int_Calc_Mode_Cd
     ,a.Pre_Chrg_Int                                 as Int_Mode_Cd
     ,a.Int_Rate_Attr_Cd                             as Int_Rate_Attr_Cd
     ,a.Orgnl_Term                                   as Orgnl_Term
     ,a.Orgnl_Term_Corp_Cd                           as Orgnl_Term_Corp_Cd
     ,a.Rprc_Prd                                     as Rprc_Prd
     ,a.Rprc_Prd_Corp_Cd                             as Rprc_Prd_Corp_Cd
     ,a.Last_Rprc_Day                                as Last_Rprc_Day
     ,a.Next_Rprc_Day                                as Next_Rprc_Day
     ,a.Sys_Src                                      as sys_src
     ,a.Is_AutoRnw                                   as Is_AutoRnw
     ,a.Last_AutoRnw_Dt                              as Last_AutoRnw_Dt
     ,a.Agmt_Stat_Cd                                 as Acct_Stat_Cd
     ,a.Prin_Subj                                    as prin_subj
     ,a.Curr_Bal                                     as Curr_Bal
     ,a.Int_Subj                                     as int_subj
     ,a.Today_Provs_Int                              as Today_Provs_Int
     ,a.CurMth_Provs_Int                             as CurMth_Provs_Int
     ,a.Accm_Provs_Int                               as Accm_Provs_Int
     ,a.Today_Int_Pay                                as Today_Int_Pay
     ,a.CurMth_Paid_Int                              as CurMth_Paid_Int
     ,a.Accm_Paid_Int                                as Accm_Paid_Int
     ,a.Int_Adj_Amt                                  as int_adj_amt
     ,a.Mth_Day_Avg_Bal                              as mth_day_avg_bal
     ,a.Yr_Day_Avg_Bal                               as yr_day_avg_bal
     ,b.Adj_Post_FTP_Prc                             as FTP_Price
     ,b.Adj_Post_FTP_Tran_Incom_Expns                as ftp_tranfm_incom

 FROM  f_fdm.f_dpst_corp_acct a	  --公司存款账户信息表     
 --left join f_fdm.f_agt_ftp_info b --ftp 信息表           
 /*  on
    (  a.Cust_Acct_Num||a.Sub_Acct=replace(b.acct_num,'TO','')
    OR a.Cust_Acct_Num||a.Sub_Acct=replace(b.acct_num,'YH','')       
    OR a.Cust_Acct_Num||a.Sub_Acct=replace(b.acct_num,'YB','')
    OR a.Cust_Acct_Num||a.Sub_Acct=replace(b.acct_num,'YD','')
    OR a.Cust_Acct_Num||a.Sub_Acct=replace(b.acct_num,'TT','')
    OR a.Cust_Acct_Num||a.Sub_Acct=replace(b.acct_num,'TD','')
    OR a.Cust_Acct_Num||a.Sub_Acct=replace(b.acct_num,'TB','')
    or a.Cust_Acct_Num||a.Sub_Acct=replace(b.acct_num,'DD','')
   )
 */
-- on a.Cust_Acct_Num||a.Sub_Acct = translate(b.acct_num,'0123456789'||b.acct_num,'0123456789')
-- AND B.DATA_SOURCE = '99200000000'
-- AND B.ETL_DATE = '$TXDATE'::date
 left join (select * 
              from (select translate(acct_num,'0123456789'||acct_num,'0123456789') as acct_num ,
                           Adj_Post_FTP_Prc,
                           Adj_Post_FTP_Tran_Incom_Expns
                           ,row_number()over(partition by translate(acct_num,'0123456789'||acct_num,'0123456789') order by FTP_DATE desc ) Rn
                      from f_fdm.f_agt_ftp_info --ftp 信息表   
                     where DATA_SOURCE = '99200000000'
                       AND ETL_DATE = '$TXDATE'::date
                      ) t1
                where Rn = 1
               ) b
 on a.Cust_Acct_Num||a.Sub_Acct=b.acct_num
WHERE  a.Mth_Day_Avg_Bal<> 0
  and  a.etl_date='$TXDATE'::date
;
INSERT INTO f_rdm.ma_dpst  
(
      etl_date                              --渠道编码
     ,Dpst_Acct_Num                         --存款账号
     ,Org_Num                               --机构号
     ,cust_num                              --客户号
     ,Cur_Cd                                --币种
     ,prod_cd                               --产品代码
     ,St_Int_Dt                             --起息日
     ,Due_Dt                                --到期日
     ,Ori_Int_Rate                          --原始利率
     ,Curr_Int_Rate                         --当前利率
     ,Bmk_Int_Rate                          --基准利率
     ,Basic_Diff                            --基差
     ,Int_Days                              --计息天数
     ,Cmpd_Int_Calc_Mode_Cd                 --复利计算方式代码
     ,Int_Mode_Cd                           --计息方式(先收后收)
     ,Int_Rate_Attr_Cd                      --固定浮动属性
     ,Orgnl_Term                            --原始期限
     ,Orgnl_Term_Corp_Cd                    --原始期限单位
     ,Rprc_Prd                              --重定价周期
     ,Rprc_Prd_Corp_Cd                      --重定价周期单位
     ,Last_Rprc_Day                         --上次重定价日
     ,Next_Rprc_Day                         --下次重定价日
     ,sys_src                               --系统来源
     ,Is_AutoRnw                            --是否自动转存
     ,Last_AutoRnw_Dt                       --上次自动转存日期
     ,Acct_Stat_Cd                          --账户状态
     ,prin_subj                             --本金科目
     ,Curr_Bal                              --当前余额
     ,int_subj                              --利息科目
     ,Today_Provs_Int                       --当日计提利息
     ,CurMth_Provs_Int                      --当月计提利息
     ,Accm_Provs_Int                        --累计计提利息
     ,Today_Int_Pay                         --当日付息
     ,CurMth_Paid_Int                       --当月已付息
     ,Accm_Paid_Int                         --累计已付息
     ,int_adj_amt                           --利息调整金额
     ,mth_day_avg_bal                       --月日均余额
     ,yr_day_avg_bal                        --年日均余额
     ,FTP_Price                             --FTP价格
     ,ftp_tranfm_incom                      --FTP转移收入
)
 SELECT
      '$TXDATE'::date                      as etl_date
      ,Agmt_Id                                      as Dpst_Acct_Num
     ,Org_Num                                       as Org_Num
     ,Cust_Num                                      as cust_num
     ,Cur_Cd                                        as Cur_Cd
     ,Prod_Cd                                       as prod_cd
     ,St_Int_Dt                                     as St_Int_Dt
     ,Due_Dt                                        as Due_Dt
     ,0.00                                          as Ori_Int_Rate
     ,Exec_Int_Rate                                 as Curr_Int_Rate
     ,Bmk_Int_Rate                                  as Bmk_Int_Rate
     ,Basis                                         as Basic_Diff
     ,Int_Base_Cd                        as Int_Days
     ,Cmpd_Int_Calc_Mode_Cd                         as Cmpd_Int_Calc_Mode_Cd
     ,Pre_Chrg_Int                                   as Int_Mode_Cd
     ,Int_Rate_Attr_Cd                              as Int_Rate_Attr_Cd
     ,Orgnl_Term                                    as Orgnl_Term
     ,Orgnl_Term_Corp_Cd                            as Orgnl_Term_Corp_Cd
     ,Rprc_Prd                                      as Rprc_Prd
     ,Rprc_Prd_Corp_Cd                              as Rprc_Prd_Corp_Cd
     ,Last_Rprc_Day                                 as Last_Rprc_Day
     ,Next_Rprc_Day                                 as Next_Rprc_Day
     ,Sys_Src                                       as sys_src
     ,Is_AutoRnw                                    as Is_AutoRnw
     ,Last_AutoRnw_Dt                               as Last_AutoRnw_Dt
     ,Agmt_Stat_Cd                                  as Acct_Stat_Cd
     ,Prin_Subj                                     as prin_subj
     ,Curr_Bal                                      as Curr_Bal
     ,Int_Subj                                      as int_subj
     ,Today_Provs_Int                               as Today_Provs_Int
     ,CurMth_Provs_Int                              as CurMth_Provs_Int
     ,Accm_Provs_Int                                as Accm_Provs_Int
     ,Today_Int_Pay                                 as Today_Int_Pay
     ,CurMth_Paid_Int                               as CurMth_Paid_Int
     ,Accm_Paid_Int                                 as Accm_Paid_Int
     ,Int_Adj_Amt                                   as int_adj_amt
     --,nvl(curr_Bal,0)/30       as mth_day_avg_bal
     ,Mth_Day_Avg_Bal                               as mth_day_avg_bal
     ,Yr_Day_Avg_Bal                                as yr_day_avg_bal
     --,nvl(curr_Bal,0)/182                                as yr_day_avg_bal
     ,0.00                                          as FTP_Price
     ,0.00                                          as ftp_tranfm_incom
 FROM f_fdm.f_dpst_indv_acct	--个人存款账户信息表
WHERE etl_date='$TXDATE'::date
and nvl(Mth_Day_Avg_Bal,0)<>0
;
/*数据处理区END*/

COMMIT;
