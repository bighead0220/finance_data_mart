/*
Author             :liudongyan
Function           :贷款
Load method        :INSERT
Source table       :f_loan_indv_dubil,f_loan_corp_dubil_info,f_loan_corp_contr         
Destination Table  :ma_loan  贷款
Frequency          :M
Modify history list:Created by liudongyan 2016/5/18 10:19:34
                   :Modify  by
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
DELETE FROM f_rdm.ma_loan
WHERE  etl_date='$TXDATE'::date  ;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_rdm.ma_loan
(
      etl_date                --数据日期
     ,loan_acct               --贷款账号
     ,Org_Num                 --机构号
     ,cust_num                --客户号
     ,Cur_Cd                  --币种
     ,prod_cd                --产品代码
     ,St_Int_Dt              --起息日
     ,Due_Dt                 --到期日
     ,Ori_Int_Rate           --原始利率
     ,Curr_Int_Rate          --当前利率
     ,Bmk_Int_Rate           --基准利率
     ,Basic_Diff             --基差
     ,Ovrd_Int_Rate          --逾期利率
     ,Loan_Orgnl_Amt         --贷款原始金额
     ,Int_Days               --计息天数
     ,Cmpd_Int_Calc_Mode_Cd  --复利计算方式代码
     ,Int_Mode_Cd            --计息方式(先收后收)
     ,Int_Rate_Attr_Cd       --固定浮动属性
     ,repay_mode             --还款方式
     ,repay_prd              --还款周期
     ,repay_prd_corp         --还款周期单位
     ,Next_Pay_Amt           --下次付款金额
     ,Last_Pay_Day           --上次付款日
     ,Next_Pay_Day           --下次付款日
     ,Orgnl_Term             --原始期限
     ,Orgnl_Term_Corp_Cd     --原始期限单位
     ,Rprc_Prd               --重定价周期
     ,Rprc_Prd_Corp_Cd       --重定价周期单位
     ,Last_Rprc_Day          --上次重定价日
     ,Next_Rprc_Day          --下次重定价日
     ,loan_drct              --贷款投向
     ,Fiv_Cls                --产品代码表
     ,loan_stat              --贷款状态
     ,sys_src                --系统来源
     ,lvrg_flg               --融资标识
     ,is_repay_plan          --是否有还款计划
     ,loan_deval_prep_bal    --贷款减值准备余额
     ,loan_deval_prep_amt    --贷款减值准备发生额
     ,Acct_Stat_Cd           --账户状态
     ,prin_subj              --本金科目
     ,Curr_Bal               --当前余额
     ,int_subj               --利息科目
     ,Today_Provs_Int        --当日计提利息
     ,CurMth_Provs_Int       --当月计提利息
     ,Accm_Provs_Int         --累计计提利息
     ,Today_Chrg_Int         --当日收息
     ,CurMth_Recvd_Int         --当月已收息
     ,Accm_Recvd_Int          --累计已收息
     ,int_adj_amt           --利息调整金额
     ,mth_day_avg_bal        --月日均余额
     ,yr_day_avg_bal         --年日均余额
     ,FTP_Price              --FTP价格
     ,ftp_tranfm_expns       --FTP转移支出
     ,prosp_loss             --预期损失
     ,Crdt_Risk_Econ_Cap     --信用风险经济资本
     ,Mkt_Risk_Econ_Cap             --市场风险经济资本
     ,Opr_Risk_Econ_Cap             --操作风险经济资本
     ,Crdt_Risk_Econ_Cap_Cost             --信用风险经济资本成本
     ,Mkt_Risk_Econ_Cap_Cost             --市场风险经济资本成本
     ,Opr_Risk_Econ_Cap_Cost             --操作风险经济资本成本

 
)
SELECT
      '$TXDATE'::date                 as etl_date
     ,T2.Agmt_Id                            as loan_acct
     ,T2.Org_Num                            as Org_Num
     ,T2.Cust_Num                           as cust_num
     ,T2.Cur_Cd                             as Cur_Cd
     ,T2.Prod_Cd                            as prod_cd
     ,T2.St_Int_Dt                          as St_Int_Dt
     ,T2.Due_Dt                             as Due_Dt
     ,0.00                               as Ori_Int_Rate
     ,T2.Exec_Int_Rate                      as Curr_Int_Rate
     ,T2.Bmk_Int_Rate                       as Bmk_Int_Rate
     ,T2.Basis                              as Basic_Diff
     ,Ovrd_Int_Rate                      as Ovrd_Int_Rate
     ,Loan_Orgnl_Amt                     as Loan_Orgnl_Amt
     ,Int_Base_Cd                        as Int_Days
     ,Cmpd_Int_Calc_Mode_Cd              as Cmpd_Int_Calc_Mode_Cd
     ,Pre_Chrg_Int                        as Int_Mode_Cd
     ,Int_Rate_Attr_Cd                   as Int_Rate_Attr_Cd
     ,Repay_Mode_Cd                      as repay_mode
     ,(case 
        when Repay_Prd_Cd in ('1','3')  then 1 
        when Repay_Prd_Cd ='2'  then 3 
        when Repay_Prd_Cd ='6'  then 6 
        when Repay_Prd_Cd='7'  then 15 
        when Repay_Prd_Cd='8'  then 7 
      end 
      )                                 as repay_prd
     ,(case 
     when Repay_Prd_Cd in ('1','2','6')  then 'M' 
     when Repay_Prd_Cd='3'  then 'Y'  
     when Repay_Prd_Cd  in ('7','8','9')  then 'D'
      end 
      )                                  as repay_prd_corp
     ,Next_Pay_Amt                       as Next_Pay_Amt
     ,Last_Pay_Day                       as Last_Pay_Day
     ,Next_Pay_Day                       as Next_Pay_Day
     ,to_char(Orgnl_Term)                         as Orgnl_Term
     ,Orgnl_Term_Corp_Cd                 as Orgnl_Term_Corp_Cd
     ,Rprc_Prd                           as Rprc_Prd
     ,Rprc_Prd_Corp_Cd                   as Rprc_Prd_Corp_Cd
     ,Last_Rprc_Day                      as Last_Rprc_Day
     ,Next_Rprc_Day                      as Next_Rprc_Day
     ,Loan_Drct_Inds_Cd                  as loan_drct
     ,Fiv_Cls                            as Fiv_Cls
     ,t2.Agmt_Stat_Cd                       as loan_stat
     ,t2.Sys_Src                            as sys_src
     ,NULL                              as lvrg_flg
     ,NULL                              as is_repay_plan
     ,Loan_Deval_Prep_Bal                as loan_deval_prep_bal
     ,Loan_Deval_Prep_Amt                as loan_deval_prep_amt
     ,NULL                              as Acct_Stat_Cd
     ,t2.Prin_Subj                          as prin_subj
     ,Curr_Bal                           as Curr_Bal
     ,Int_Subj                           as int_subj
     ,Today_Provs_Int                    as Today_Provs_Int
     ,CurMth_Provs_Int                   as CurMth_Provs_Int
     ,Accm_Provs_Int                     as Accm_Provs_Int
     ,Today_Chrg_Int                     as Today_Chrg_Int
     ,CurMth_Recvd_Int                   as CurMth_Recvd_Int
     ,Accm_Recvd_Int                     as Accm_Recvd_Int
     ,Int_Adj_Amt                        as int_adj_amt
     ,Mth_Day_Avg_Bal                    as mth_day_avg_bal
     ,Yr_Day_Avg_Bal                     as yr_day_avg_bal
     ,t4.Adj_Post_FTP_Prc                as FTP_Price   
     ,t4.Adj_Post_FTP_Tran_Incom_Expns   as ftp_tranfm_expns  
     ,0.00                               as prosp_loss
     ,0.00                               as Crdt_Risk_Econ_Cap
     ,0.00                               as Mkt_Risk_Econ_Cap
     ,0.00                               as Opr_Risk_Econ_Cap
     ,0.00                               as Crdt_Risk_Econ_Cap_Cost
     ,0.00                               as Mkt_Risk_Econ_Cap_Cost
     ,0.00                               as Opr_Risk_Econ_Cap_Cost

FROM f_fdm.f_loan_indv_dubil	 AS T2 --个人贷款借据信息表           
left join ( select distinct agmt_id,Loan_Drct_Inds_Cd from f_fdm.f_loan_indv_contr 
            where etl_date= '$TXDATE'::date) T3---个人贷款合同信息表        
  on t2.contr_agmt_id=t3.agmt_id   
left join f_fdm.f_agt_ftp_info as t4
  on t2.Agmt_Id = substr(t4.acct_num,1,length(t4.acct_num)-2)
 and t4.data_source = '99340000000'
 and t4.etl_date = '$TXDATE'::date                 
WHERE t2.Agmt_Stat_Cd <>'7'--已结清   
  AND t2.etl_date= '$TXDATE'::date  
UNION ALL  
SELECT 
     '$TXDATE'::date                       as etl_date
     ,T.Agmt_Id                            as loan_acct
     ,T.Org_Num                            as Org_Num
     ,T.Cust_Num                           as cust_num
     ,T.Cur_Cd                             as Cur_Cd
     ,T.Prod_Cd                            as prod_cd
     ,T.St_Int_Dt                          as St_Int_Dt
     ,T.Due_Dt                             as Due_Dt
     ,0.00                               as Ori_Int_Rate
     ,T.Exec_Int_Rate                      as Curr_Int_Rate
     ,T.Bmk_Int_Rate                       as Bmk_Int_Rate
     ,T.Basis                              as Basic_Diff
     ,T.Ovrd_Int_Rate                      as Ovrd_Int_Rate
     ,T.Loan_Orgnl_Amt                     as Loan_Orgnl_Amt
     ,T.Int_Base_Cd                        as Int_Days
     ,T.Cmpd_Int_Calc_Mode_Cd              as Cmpd_Int_Calc_Mode_Cd
     ,T.Pre_Chrg_Int                        as Int_Mode_Cd
     ,T.Int_Rate_Attr_Cd                   as Int_Rate_Attr_Cd
     ,T.Repay_Mode_Cd                      as repay_mode
     ,(case 
     when Repay_Prd_Cd in ('1','3') then 1 
     when Repay_Prd_Cd='2' then 3 
     when Repay_Prd_Cd='6' then 6 
      end
      )                                 as repay_prd
     ,(case
      when Repay_Prd_Cd  in ('1' ,'2','6') then 'M'  
      when Repay_Prd_Cd='3' then 'Y'  
      end
      )                               as repay_prd_corp
     ,T.Next_Pay_Amt                    as Next_Pay_Amt
     ,T.Last_Pay_Day                    as Last_Pay_Day
     ,T.Next_Pay_Day                    as Next_Pay_Day
     ,to_char(Orgnl_Term)                      as Orgnl_Term
     ,T.Orgnl_Term_Corp_Cd              as Orgnl_Term_Corp_Cd
     ,T.Rprc_Prd                        as Rprc_Prd
     ,T.Rprc_Prd_Corp_Cd                as Rprc_Prd_Corp_Cd
     ,T.Last_Rprc_Day                   as Last_Rprc_Day
     ,T.Next_Rprc_Day                   as Next_Rprc_Day
     ,Loan_Drct_Inds_Cd               as loan_drct
     ,T.Fiv_Cls                         as Fiv_Cls
     ,T.Agmt_Stat_Cd                    as loan_stat
     ,T.Sys_Src                         as sys_src
     ,T1.Lvrg_Mode                    as lvrg_flg
     ,NULL                           as is_repay_plan
     ,T.Loan_Deval_Prep_Bal             as loan_deval_prep_bal
     ,T.Loan_Deval_Prep_Amt             as loan_deval_prep_amt
     ,NULL                           as Acct_Stat_Cd
     ,T.Prin_Subj                       as prin_subj
     ,T.Curr_Bal                        as Curr_Bal
     ,T.Int_Subj                        as int_subj
     ,T.Today_Provs_Int                 as Today_Provs_Int
     ,T.CurMth_Provs_Int                as CurMth_Provs_Int
     ,T.Accm_Provs_Int                  as Accm_Provs_Int
     ,T.Today_Chrg_Int                  as Today_Chrg_Int
     ,T.CurMth_Recvd_Int                as CurMth_Recvd_Int
     ,T.Accm_Recvd_Int                  as Accm_Recvd_Int
     ,T.Int_Adj_Amt                     as int_adj_amt
     ,T.Mth_Day_Avg_Bal                 as mth_day_avg_bal
     ,T.Yr_Day_Avg_Bal                  as yr_day_avg_bal 
     ,t2.Adj_Post_FTP_Prc               as FTP_Price   
     ,t2.Adj_Post_FTP_Tran_Incom_Expns  as ftp_tranfm_expns  
     ,0.00                            as prosp_loss
     ,0.00                            as Crdt_Risk_Econ_Cap
     ,0.00                            as Mkt_Risk_Econ_Cap
     ,0.00                            as Opr_Risk_Econ_Cap
     ,0.00                            as Crdt_Risk_Econ_Cap_Cost
     ,0.00                            as Mkt_Risk_Econ_Cap_Cost
     ,0.00                            as Opr_Risk_Econ_Cap_Cost
FROM f_fdm.f_loan_corp_dubil_info   AS T       --公司贷款借据信息表 
LEFT JOIN  f_fdm.f_loan_corp_contr   AS T1     --公司贷款合同信息表 
  ON T.Contr_Agmt_Id=T1.agmt_id 
 and T1.etl_date='$TXDATE'::date
left join f_fdm.f_agt_ftp_info t2
  on t.Agmt_Id = substr(t2.acct_num,1,length(t2.acct_num)-2)
 and t2.data_source = '99460000000'
 and t2.etl_date = '$TXDATE'::date 
WHERE T.etl_date='$TXDATE'::date
and  t.Agmt_Stat_Cd <>'7'--已结清      
;
/*数据处理区END*/

COMMIT;
