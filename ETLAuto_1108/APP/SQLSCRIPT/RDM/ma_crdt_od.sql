/*
Author             :liudongyan
Function           :信用卡透支
Load method        :INSERT
Source table       :f_acct_crdt_info 信用卡账户信息表
Destination Table  :ma_crdt_od   信用卡透支
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
DELETE FROM f_rdm.ma_crdt_od
WHERE  etl_date='$TXDATE'::date;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_rdm.ma_crdt_od 
(
       etl_date                              --数据日期
       ,Acct_Num                             --账号
       ,card_num                             --卡号
       ,Org_Num                              --机构号
       ,cust_num                             --客户号
       ,Cur_Cd                               --币种
       ,prod_cd                              --产品代码
       ,St_Int_Dt                            --起息日
       ,Due_Dt                               --到期日
       ,Curr_Int_Rate                        --当前利率
       ,Bmk_Int_Rate                         --基准利率
       ,Basic_Diff                           --基差
       ,Int_Days                             --计息天数
       ,Cmpd_Int_Calc_Mode_Cd                --复利计算方式代码
       ,sys_src                              --系统来源
       ,Acct_Stat_Cd                         --账户状态
       ,Prin_Subj_Od                         --本金科目-透支
       ,Spl_Pay_Bal                          --溢缴款余额
       ,Prin_Subj_Dpst                       --本金科目-存款
       ,Curr_Bal                             --当前余额
       ,int_subj                             --利息科目
       ,Today_Provs_Int                      --当日计提利息
       ,CurMth_Provs_Int                     --当月计提利息
       ,Accm_Provs_Int                       --累计计提利息
       ,Today_Chrg_Int                       --当日收息
       ,CurMth_Recvd_Int                     --当月已收息
       ,Accm_Recvd_Int                       --累计已收息
       ,int_adj_amt                          --利息调整金额
       ,mth_day_avg_bal                      --月日均余额
       ,yr_day_avg_bal                       --年日均余额
       ,prosp_loss                           --预期损失
       ,deval_prep_bal                       --减值准备余额
      ,deval_prep_amt                        --减值准备发生额
      ,Crdt_Risk_Econ_Cap                    --信用风险经济资本
      ,Mkt_Risk_Econ_Cap                     --市场风险经济资本
      ,Opr_Risk_Econ_Cap                     --操作风险经济资本
      ,Crdt_Risk_Econ_Cap_Cost               --信用风险经济资本成本
      ,Mkt_Risk_Econ_Cap_Cost                --市场风险经济资本成本
      ,Opr_Risk_Econ_Cap_Cost                --操作风险经济资本成本
      ,prmt_org_num                          --推广机构号
 
)
SELECT
       '$TXDATE'::date                 as etl_date
       ,T.Agmt_Id                               as Acct_Num
       ,T.Card_Num                              as card_num
       ,T.Open_Acct_Org_Num                     as Org_Num
       ,T.Cust_Id                               as cust_num
       ,T.Cur_Cd                                as Cur_Cd
       ,T.Prod_Cd                               as prod_cd
       ,T.Open_Acct_Dt                          as St_Int_Dt
       ,'$MINDATE'::date                    as Due_Dt
       ,T.Curr_Int_Rate                         as Curr_Int_Rate
       ,0                                       as Bmk_Int_Rate
       ,0                                       as Basic_Diff
       ,T.Int_Base_Cd                           as Int_Days
       ,T.Cmpd_Int_Calc_Mode_Cd                 as Cmpd_Int_Calc_Mode_Cd
       ,T.Sys_Src                               as sys_src
       ,T.Acct_Stat_Cd                          as Acct_Stat_Cd
       ,T.Prin_Subj_Od                          as Prin_Subj_Od
       ,T.Spl_Pay_Bal                           as Spl_Pay_Bal
       ,T.Prin_Subj_Dpst                        as Prin_Subj_Dpst
       ,T.curr_bal                              as Curr_Bal
       ,T.Int_Subj                              as int_subj
       ,T.Today_Provs_Int                       as Today_Provs_Int
       ,T.CurMth_Provs_Int                      as CurMth_Provs_Int
       ,T.Accm_Provs_Int                        as Accm_Provs_Int
       ,T.Today_Chrg_Int                        as Today_Chrg_Int
       ,T.CurMth_Recvd_Int                      as CurMth_Recvd_Int
       ,T.Accm_Recvd_Int                        as Accm_Recvd_Int
       ,T.Int_Adj_Amt                           as int_adj_amt
       ,T.Mth_Day_Avg_Bal                       as mth_day_avg_bal
       ,T.Yr_Day_Avg_Bal                        as yr_day_avg_bal
       ,0.00                                    as prosp_loss
       ,T.Loan_Deval_Prep_Bal                   as deval_prep_bal
       ,T.Loan_Deval_Prep_Amt                   as deval_prep_amt
       ,0.00                                    as Crdt_Risk_Econ_Cap
       ,0.00                                    as Mkt_Risk_Econ_Cap
       ,0.00                                    as Opr_Risk_Econ_Cap
       ,0.00                                    as Crdt_Risk_Econ_Cap_Cost
       ,0.00                                    as Mkt_Risk_Econ_Cap_Cost
       ,0.00                                    as Opr_Risk_Econ_Cap_Cost
       ,T1.Prmt_Org_Cd                          as prmt_org_num

FROM   f_fdm.f_acct_crdt_info T
LEFT JOIN  f_fdm.f_acct_crdt_card_info  T1  --信用卡信息表
ON T.Card_Num =T1.Card_Num
and T1.etl_date='$TXDATE'::date
WHERE T.etl_date ='$TXDATE'::date
;


/*数据处理区END*/

COMMIT;




