/*
Author             :liudongyan
Function           :保函
Load method        :INSERT
Source table       :f_agt_Guarantee
Destination Table  :ma_guar   保函
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
DELETE FROM f_rdm.ma_guar
WHERE  etl_date= '$TXDATE'::date ;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_rdm.ma_guar 
(
      etl_date                                                   --数据日期
     ,biz_id                                                     --业务编号
     ,Org_Num                                                    --机构号
     ,cust_num                                                   --客户号
     ,Cur_Cd                                                     --币种
     ,prod_cd                                                    --产品代码
     ,St_Int_Dt                                                  --起息日
     ,Due_Dt                                                     --到期日
     ,Fiv_Cls                                                    --五级分类
     ,sys_src                                                    --系统来源
     ,loan_deval_prep_bal                                        --贷款减值准备余额
     ,loan_deval_prep_amt                                        --贷款减值准备发生额
     ,Acct_Stat_Cd                                               --账户状态
     ,prin_subj                                                  --本金科目
     ,Guar_Amt                                                   --保函金额
     ,Guar_Bal                                                   --保函余额
     ,mth_day_avg_bal                                            --月日均余额
     ,yr_day_avg_bal                                             --年日均余额
     ,prosp_loss                                                 --预期损失
     ,Crdt_Risk_Econ_Cap                                         --信用风险经济资本
     ,Mkt_Risk_Econ_Cap                                          --市场风险经济资本
     ,Opr_Risk_Econ_Cap                                          --操作风险经济资本
     ,Crdt_Risk_Econ_Cap_Cost                                    --信用风险经济资本成本
     ,Mkt_Risk_Econ_Cap_Cost                                     --市场风险经济资本成本
     ,Opr_Risk_Econ_Cap_Cost                                     --操作风险经济资本成本
     ,margn_acct_num                                             --保证金账号

)
SELECT
      '$TXDATE'::date                                          as etl_date
     ,Agmt_Id                                                           as biz_id
     ,Org_Num                                                           as Org_Num
     ,Cust_Num                                                          as cust_num
     ,Cur_Cd                                                            as Cur_Cd
     ,Prod_Cd                                                           as prod_cd
     ,Open_Dt                                                           as St_Int_Dt
     ,Due_Dt                                                            as Due_Dt
     ,NULL                                                              as Fiv_Cls
     ,Sys_Src                                                           as sys_src
     ,0.00                                                              as loan_deval_prep_bal
     ,0.00                                                              as loan_deval_prep_amt
     ,Agmt_Stat_Cd                                                      as Acct_Stat_Cd
     ,Subj_Cd                                                           as prin_subj
     ,Guar_Amt                                                          as Guar_Amt
     ,Guar_Bal                                                          as Guar_Bal
     ,Mth_Day_Avg_Bal                                                   as mth_day_avg_bal
     ,Yr_Day_Avg_Bal                                                    as yr_day_avg_bal
     ,0.00                                                              as prosp_loss
     ,0.00                                                              as Crdt_Risk_Econ_Cap
     ,0.00                                                              as Mkt_Risk_Econ_Cap
     ,0.00                                                              as Opr_Risk_Econ_Cap
     ,0.00                                                              as Crdt_Risk_Econ_Cap_Cost
     ,0.00                                                              as Mkt_Risk_Econ_Cap_Cost
     ,0.00                                                              as Opr_Risk_Econ_Cap_Cost
     ,Margn_Acct_Num                                                    as margn_acct_num
FROM  f_fdm.f_agt_Guarantee --保函信息表
WHERE etl_date =  '$TXDATE'::date
;
/*数据处理区END*/

COMMIT;
