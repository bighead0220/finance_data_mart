/*
Author             :liudongyan
Function           :法人透支
Load method        :INSERT
Source table       :f_acct_lpr_od               
Destination Table  :ma_lpr_od  法人透支
Frequency          :M
Modify history list:Created by liudongyan 2016/5/18 10:19:34
                   :Modify  by xsh 20160630 将Pre_Chrg_In修改为Pre_Chrg_Int,将to_number(Int_Base_Cd)修改为Int_Base_Cd::numeric(此部分模型映射异常,待模型验证),将to_number(Orgnl_Term)改为to_char(Orgnl_Term)
                   :Modify  by zhangliang 2016-8-9 10:59:34 (1)将select中Cust_Num与Org_Num顺序调换；(2)将select中(Int_Base_Cd)::numeric 修改为 Int_Base_Cd
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
DELETE FROM f_rdm.ma_lpr_od 
WHERE  etl_date='$TXDATE'::date  ;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_rdm.ma_lpr_od 
(
      etl_date                                  --数据日期
     ,lpr_od_acct_num                           --法人透支账号
     ,Org_Num                                   --机构号
     ,cust_num                                  --客户号
     ,Cur_Cd                                    --币种
     ,prod_cd                                   --产品代码
     ,St_Int_Dt                                 --起息日
     ,Due_Dt                                    --到期日
     ,Curr_Int_Rate                             --当前利率
     ,Bmk_Int_Rate                              --基准利率
     ,Basic_Diff                                --基差
     ,Int_Days                                  --计息天数
     ,Cmpd_Int_Calc_Mode_Cd                     --复利计算方式代码
     ,Int_Mode_Cd                               --计息方式(先收后收)
     ,Int_Rate_Attr_Cd                          --固定浮动属性
     ,Orgnl_Term                                --原始期限
     ,Orgnl_Term_Corp_Cd                        --原始期限单位
     ,Rprc_Prd                                  --重定价周期
     ,Rprc_Prd_Corp_Cd                          --重定价周期单位
     ,Last_Rprc_Day                             --上次重定价日
     ,Next_Rprc_Day                             --下次重定价日
     ,sys_src                                   --系统来源
     ,Acct_Stat_Cd                              --账户状态
     ,prin_subj                                 --本金科目
     ,Curr_Bal                                  --当前余额
     ,int_subj                                  --利息科目
     ,Today_Provs_Int                           --当日计提利息
     ,CurMth_Provs_Int                          --当月计提利息
     ,Accm_Provs_Int                            --累计计提利息
     ,Today_Chrg_Int                            --当日收息
     ,CurMth_Recvd_Int                          --当月已收息
     ,Accm_Recvd_Int                            --产品代码表
     ,int_adj_amt                               --利息调整金额
     ,mth_day_avg_bal                           --月日均余额
     ,yr_day_avg_bal                            --年日均余额
     ,FTP_Price                                 --FTP价格
     ,ftp_tranfm_expns                          --FTP转移支出
     ,prosp_loss                                --预期损失
     ,deval_prep_bal                            --减值准备余额
     ,deval_prep_amt                            --减值准备发生额
     ,Crdt_Risk_Econ_Cap                        --信用风险经济资本
     ,Mkt_Risk_Econ_Cap                         --市场风险经济资本
     ,Opr_Risk_Econ_Cap                         --操作风险经济资本
     ,Crdt_Risk_Econ_Cap_Cost                   --信用风险经济资本成本
     ,Mkt_Risk_Econ_Cap_Cost                    --市场风险经济资本成本
     ,Opr_Risk_Econ_Cap_Cost                    --操作风险经济资本成本
)
SELECT
     '$TXDATE'::date                      as etl_date
     ,Agmt_Id                                      as lpr_od_acct_num
     ,Org_Num                                      as Org_Num
     ,Cust_Num                                     as cust_num
     ,Cur_Cd                                       as Cur_Cd
     ,Prod_Cd                                      as prod_cd
     ,St_Int_Dt                                    as St_Int_Dt
     ,Due_Dt                                       as Due_Dt
     ,Exec_Int_Rate                                as Curr_Int_Rate
     ,Bmk_Int_Rate                                 as Bmk_Int_Rate
     ,Basic_Diff                                   as Basic_Diff
     ,Int_Base_Cd                                  as Int_Days
     ,Cmpd_Int_Calc_Mode_Cd                        as Cmpd_Int_Calc_Mode_Cd
     ,Pre_Chrg_Int                                 as Int_Mode_Cd
     ,Int_Rate_Attr_Cd                             as Int_Rate_Attr_Cd
     ,to_char(Orgnl_Term)                          as Orgnl_Term
     ,Orgnl_Term_Corp_Cd                           as Orgnl_Term_Corp_Cd
     ,Rprc_Prd                                     as Rprc_Prd
     ,Rprc_Prd_Corp_Cd                             as Rprc_Prd_Corp_Cd
     ,Last_Rprc_Day                                as Last_Rprc_Day
     ,Next_Rprc_Day                                as Next_Rprc_Day
     ,Sys_Src                                      as sys_src
     ,Agmt_Stat_Cd                                 as Acct_Stat_Cd
     ,Prin_Subj                                    as prin_subj
     ,Curr_Bal                                     as Curr_Bal
     ,Int_Subj                                     as int_subj
     ,Today_Provs_Int                              as Today_Provs_Int
     ,CurMth_Provs_Int                             as CurMth_Provs_Int
     ,Accm_Provs_Int                               as Accm_Provs_Int
     ,Today_Chrg_Int                               as Today_Chrg_Int
     ,CurMth_Recvd_Int                             as CurMth_Recvd_Int
     ,Accm_Recvd_Int                               as Accm_Recvd_Int
     ,Int_Adj_Amt                                  as int_adj_amt
     ,Mth_Day_Avg_Bal                              as mth_day_avg_bal
     ,Yr_Day_Avg_Bal                               as yr_day_avg_bal
     ,0.00                                         as FTP_Price    
     ,0.00                                         as ftp_tranfm_expns  
     ,0.00                                         as prosp_loss
     ,Deval_Prep_Bal                               as deval_prep_bal
     ,Deval_Prep_Amt                               as deval_prep_amt
     ,0.00                                         as Crdt_Risk_Econ_Cap
     ,0.00                                         as Mkt_Risk_Econ_Cap
     ,0.00                                         as Opr_Risk_Econ_Cap
     ,0.00                                         as Crdt_Risk_Econ_Cap_Cost
     ,0.00                                         as Mkt_Risk_Econ_Cap_Cost
     ,0.00                                         as Opr_Risk_Econ_Cap_Cost

FROM f_fdm.f_acct_lpr_od      --法人透支账户信息表                               
WHERE etl_date='$TXDATE'::date

;
/*数据处理区END*/

COMMIT;
