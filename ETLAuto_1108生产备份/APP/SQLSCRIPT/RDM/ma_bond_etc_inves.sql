/*
Author             :liudongyan
Function           :资金业务-债券投资
Load method        :INSERT
Source table       :f_Cap_Bond_Inves 资金债券投资信息表
                    f_agt_ftp_info FTP信息表
Destination Table  :ma_bond_etc_inves 资金业务-债券投资
Frequency          :M
Modify history list:Created by liudongyan 2016/8/10 10:19:34
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
DELETE FROM f_rdm.ma_bond_etc_inves
WHERE  etl_date=  '$TXDATE'::date ;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_rdm.ma_bond_etc_inves
(
       etl_date                     --数据日期
      ,Acct_Num                     --账号
      ,Org_Num                      --机构号
      ,dept_ent                     --部门实体
      ,Bond_Cd                      --债券代码
      ,TX_Cnt_Pty_Cust_Num          --交易对手客户号
      ,acct_typ                     --账户类型
      ,tx_comb                      --交易组合
      ,bond_typ                     --债券类型
      ,Bond_Issur                   --债券发行人
      ,Cur_Cd                       --币种
      ,prod_cd                      --产品代码
      ,St_Int_Dt                    --起息日
      ,Due_Dt                       --到期日
      ,Curr_Int_Rate                --当前利率
      ,Bmk_Int_Rate                 --基准利率
      ,Basic_Diff                   --基差
      ,Int_Days                     --计息天数
      ,Cmpd_Int_Calc_Mode_Cd        --复利计算方式代码
      ,Int_Rate_Attr_Cd             --固定浮动属性
      ,Orgnl_Term                   --原始期限
      ,Orgnl_Term_Corp_Cd           --原始期限单位
      ,Rprc_Prd                     --重定价周期
      ,Rprc_Prd_Corp_Cd             --重定价周期单位
      ,Last_Rprc_Day                --上次重定价日
      ,Next_Rprc_Day                --下次重定价日
      ,sys_src                      --系统来源
      ,prin_subj                    --本金科目
      ,Book_Bal                     --账面余额
      ,Buy_Cost                     --购入成本
      ,Mkt_Val                      --市场价值
      ,int_subj                     --利息科目
      ,Today_Provs_Int              --当日计提利息
      ,CurMth_Provs_Int             --当月计提利息
      ,Today_Chrg_Int               --当日收息
      ,CurMth_Recvd_Int             --当月已收息
      ,Accm_Recvd_Int               --累计已收息
      ,Valtn_Prft_Loss_Subj         --估值损益科目
      ,Today_Valtn_Prft_Loss_Amt    --当日估值损益金额
      ,CurMth_Valtn_Prft_Loss_Amt   --当月估值损益金额
      ,Biz_Prc_Diff_Prft_Subj       --买卖价差收益科目
      ,Today_Biz_Prc_Diff_Amt       --当日买卖价差金额
      ,CurMth_Biz_Prc_Diff_Amt      --当月买卖价差金额
      ,comm_fee_subj                --手续费科目
      ,today_comm_fee_amt           --当日手续费发生额
      ,curmth_comm_fee_happ         --当月手续费发生
      ,mth_day_avg_bal              --月日均余额
      ,yr_day_avg_bal               --年日均余额
      ,FTP_Price                    --FTP价格
      ,ftp_tranfm_expns             --FTP转移支出
      ,prosp_loss                   --预期损失
      ,Crdt_Risk_Econ_Cap           --信用风险经济资本
      ,Mkt_Risk_Econ_Cap            --市场风险经济资本
      ,Opr_Risk_Econ_Cap            --操作风险经济资本
      ,Crdt_Risk_Econ_Cap_Cost      --信用风险经济资本成本
      ,Mkt_Risk_Econ_Cap_Cost       --市场风险经济资本成本
      ,Opr_Risk_Econ_Cap_Cost       --操作风险经济资本成本

)
SELECT
      '$TXDATE'::date         as etl_date     --数据日期
      ,T.Agmt_Id                       as Acct_Num     --账号
      ,T.Org_Num                       as Org_Num      --机构号
      ,''                              as dept_ent     --部门实体
      ,T.Bond_Cd                       as Bond_Cd      --债券代码
      ,T.TX_Cnt_Pty_Cust_Num           as TX_Cnt_Pty_Cust_Num--交易对手客户号
      ,T.TX_Tool_Cls                   as acct_typ     --账户类型
      ,T.TX_Comb_Cd                    as tx_comb      --交易组合
      ,T.Bond_Typ_Cd                   as bond_typ     --债券类型
      ,T.Bond_Issur                    as Bond_Issur   --债券发行人
      ,T.Cur_Cd                        as Cur_Cd       --币种
      ,T.Prod_Cd                       as prod_cd      --产品代码
      ,T.St_Int_Dt                     as St_Int_Dt    --起息日
      ,T.Due_Dt                        as Due_Dt       --到期日
      ,T.Curr_Int_Rate                 as Curr_Int_Rate--当前利率
      ,T.Bmk_Int_Rate                  as Bmk_Int_Rate --基准利率
      ,T.Basis                         as Basic_Diff   --基差
      ,T.Int_Base_Cd                   as Int_Days     --计息天数
      ,T.Cmpd_Int_Calc_Mode_Cd         as Cmpd_Int_Calc_Mode_Cd--复利计算方式代码
      ,T.Int_Rate_Attr_Cd              as Int_Rate_Attr_Cd--固定浮动属性
      ,T.Orgnl_Term                    as Orgnl_Term --原始期限
      ,T.Orgnl_Term_Corp_Cd            as Orgnl_Term_Corp_Cd--原始期限单位
      ,T.Rprc_Prd                      as Rprc_Prd   --重定价周期
      ,T.Rprc_Prd_Corp_Cd              as Rprc_Prd_Corp_Cd--重定价周期单位
      ,T.Last_Rprc_Day                 as Last_Rprc_Day--上次重定价日
      ,T.Next_Rprc_Day                 as Next_Rprc_Day--下次重定价日
      ,T.Sys_Src                       as sys_src      --系统来源
      ,T.Prin_Subj                     as prin_subj    --本金科目
      ,T.Book_Bal                      as Book_Bal     --账面余额
      ,T.Buy_Cost                      as Buy_Cost     --购入成本
      ,T.Mkt_Val                       as Mkt_Val      --市场价值
      ,T.Int_Subj                      as int_subj     --利息科目
      ,T.Today_Provs_Int               as Today_Provs_Int--当日计提利息
      ,T.CurMth_Provs_Int              as CurMth_Provs_Int--当月计提利息
      ,T.Today_Chrg_Int                as Today_Chrg_Int --当日收息
      ,T.CurMth_Recvd_Int              as CurMth_Recvd_Int--当月已收息
      ,T.Accm_Recvd_Int                as Accm_Recvd_Int--累计已收息
      ,T.Valtn_Prft_Loss_Subj          as Valtn_Prft_Loss_Subj--估值损益科目
      ,T.Today_Valtn_Prft_Loss_Amt     as Today_Valtn_Prft_Loss_Amt--当日估值损益金额
      ,T.CurMth_Valtn_Prft_Loss_Amt    as CurMth_Valtn_Prft_Loss_Amt--当月估值损益金额
      ,T.Biz_Prc_Diff_Prft_Subj        as Biz_Prc_Diff_Prft_Subj--买卖价差收益科目
      ,T.Today_Biz_Prc_Diff_Amt        as Today_Biz_Prc_Diff_Amt--当日买卖价差金额
      ,T.CurMth_Biz_Prc_Diff_Amt       as CurMth_Biz_Prc_Diff_Amt--当月买卖价差金额
      ,T.Comm_Fee_Subj                 as comm_fee_subj--手续费科目
      ,T.Today_Comm_Fee_Amt            as today_comm_fee_amt--当日手续费发生额
      ,T.CurMth_Comm_Fee_Amt           as curmth_comm_fee_happ--当月手续费发生
      ,T.Mth_Day_Avg_Bal               as mth_day_avg_bal--月日均余额
      ,T.Yr_Day_Avg_Bal                as yr_day_avg_bal--年日均余额
      ,T1.Adj_Post_FTP_Prc             as FTP_PriceFTP  --价格
      ,T1.Adj_Post_FTP_Tran_Incom_Expns as ftp_tranfm_expnsFTP--转移支出
      ,0.00                             as prosp_loss --预期损失
      ,0.00                             as Crdt_Risk_Econ_Cap--信用风险经济资本
      ,0.00                             as Mkt_Risk_Econ_Cap --市场风险经济资本
      ,0.00                             as Opr_Risk_Econ_Cap --操作风险经济资本
      ,0.00                             as Crdt_Risk_Econ_Cap_Cost--信用风险经济资本成本
      ,0.00                             as Mkt_Risk_Econ_Cap_Cost --市场风险经济资本成本
      ,0.00                             as Opr_Risk_Econ_Cap_Cost --操作风险经济资本成本

from  f_fdm.f_Cap_Bond_Inves   T  --资金债券投资信息表
left join f_fdm.f_agt_ftp_info T1 --FTP信息表 
on   T.Agmt_Id= substr(T1.Acct_Num,1,length(T1.Acct_Num)-2) 
and  T1.DATA_SOURCE='99700100000' 
and  T1.etl_date= '$TXDATE'::date 
where T.etl_date= '$TXDATE'::date 
;
/*数据处理区END*/

COMMIT;
