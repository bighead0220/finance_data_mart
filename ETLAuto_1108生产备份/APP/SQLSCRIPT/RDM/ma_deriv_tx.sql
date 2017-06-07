/*
Author             :liudongyan
Function           :资金业务-衍生交易
Load method        :INSERT
Source table       :f_Cap_Raw_TX  资金衍生交易信息表
                    f_agt_ftp_info FTP信息表
Destination Table  :ma_deriv_tx 资金业务-衍生交易
Frequency          :M
Modify history list:Created by liudongyan 2016/8/10 14:19:34
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
DELETE FROM f_rdm.ma_deriv_tx
WHERE  etl_date=  '$TXDATE'::date ;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_rdm.ma_deriv_tx
(
      etl_date            --数据日期
      ,tx_id              --交易编号
      ,Acct_Num           --账号
      ,Org_Num            --机构号
      ,cust_num           --客户号
      ,prod_cd            --产品代码
      ,tx_comb            --交易组合
      ,Cur_Cd             --币种
      ,cnt_pty_cur        --对方币种
      ,St_Int_Dt          --起息日
      ,Due_Dt             --到期日
      ,Exchg_Rate         --汇率
      ,Curr_Int_Rate      --当前利率
      ,cnt_pty_curr_int_rate  --对方当前利率
      ,Bmk_Int_Rate           --基准利率
      ,Basic_Diff             --基差
      ,Int_Days               --计息天数
      ,cnt_pty_int_days       --对方计息天数
      ,Cmpd_Int_Calc_Mode_Cd  --复利计算方式代码
      ,Pre_Chrg_Int           --是否先收息
      ,Int_Pay_Freq           --付息频率
      ,Int_Rate_Attr_Cd       --固定浮动属性
      ,cnt_pty_int_rate_attr  --对方利率属性
      ,Orgnl_Term             --原始期限
      ,Orgnl_Term_Corp_Cd     --原始期限单位
      ,sys_src                --系统来源
      ,prin_subj              --本金科目
      ,Notnl_Prin             --名义本金
      ,cnt_pty_prin_subj      --对方本金科目
      ,Cnt_Pty_Notnl_Prin     --对方名义本金
      ,Net_Val                --净现值
      ,Prec_Metal_Amt         --当前市值
      ,paybl_int_subj         --应付利息科目
      ,today_paybl_int        --当日应付利息
      ,curmth_paybl_int       --当月应付利息
      ,recvbl_int_subj        --应收利息科目
      ,today_recvbl_int       --当日应收利息
      ,curmth_recvbl_int      --当月应收利息
      ,Valtn_Prft_Loss_Subj   --估值损益科目
      ,Today_Valtn_Prft_Loss_Amt    --当日估值损益金额
      ,CurMth_Valtn_Prft_Loss_Amt   --当月估值损益金额
      ,Biz_Prc_Diff_Prft_Subj       --买卖价差收益科目
      ,Today_Biz_Prc_Diff_Amt       --当日买卖价差金额
      ,CurMth_Biz_Prc_Diff_Amt      --当月买卖价差金额
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
      ,Opr_Risk_Econ_Cap_Cost        --操作风险经济资本成本
           

)
SELECT
       '$TXDATE'::date           as etl_date   --数据日期
      ,T.Agmt_Id                          as tx_id      --交易编号
      ,T.Cust_Acct_Num                    as Acct_Num   --账号
      ,T.Org_Num                          as Org_Num    --机构号
      ,T.Cust_Num                         as cust_num   --客户号
      ,T.Prod_Cd                          as prod_cd    --产品代码
      ,T.Cap_TX_Inves_Comb_Cd             as tx_comb    --交易组合
      ,T.Cur_Cd                           as Cur_Cd     --币种
      ,T.cnt_pty_cur_cd                   as cnt_pty_cur--对方币种
      ,T.St_Int_Dt                        as St_Int_Dt  --起息日
      ,T.Due_Dt                           as Due_Dt     --到期日
      ,T.exchg_rate                       as Exchg_Rate --汇率
      ,T.Curr_Int_Rate                    as Curr_Int_Rate--当前利率
      ,T.cnt_pty_curr_int_rate            as cnt_pty_curr_int_rate--对方当前利率
      ,T.Bmk_Int_Rate                     as Bmk_Int_Rate         --基准利率
      ,T.Basis                            as Basic_Diff           --基差
      ,T.Int_Base_Cd                      as Int_Days             --计息天数
      ,T.cnt_pty_int_base_cd              as cnt_pty_int_days     --对方计息天数
      ,T.Cmpd_Int_Calc_Mode_Cd            as Cmpd_Int_Calc_Mode_Cd--复利计算方式代码
      ,T.Pre_Chrg_Int                     as Pre_Chrg_Int         --是否先收息
      ,T.Int_Pay_Freq                     as Int_Pay_Freq         --付息频率
      ,T.Int_Rate_Attr_Cd                 as Int_Rate_Attr_Cd     --固定浮动属性
      ,T.cnt_pty_int_rate_attr_cd         as cnt_pty_int_rate_attr--对方利率属性
      ,T.Orgnl_Term                       as Orgnl_Term           --原始期限
      ,T.Orgnl_Term_Corp_Cd               as Orgnl_Term_Corp_Cd   --原始期限单位
      ,T.Sys_Src                          as sys_src              --系统来源
      ,T.prin_subj                        as prin_subj            --本金科目
      ,T.Notnl_Prin                       as Notnl_Prin           --名义本金
      ,T.cnt_pty_prin_subj                as cnt_pty_prin_subj    --对方本金科目
      ,T.cnt_pty_notnl_prin               as Cnt_Pty_Notnl_Prin   --对方名义本金
      ,T.Net_Val                          as Net_Val              --净现值
      ,T.Curr_Val                         as Prec_Metal_Amt       --当前市值
      ,T.paybl_int_subj                   as paybl_int_subj       --应付利息科目
      ,T.today_paybl_int                  as today_paybl_int      --当日应付利息
      ,T.curmth_paybl_int                 as curmth_paybl_int     --当月应付利息
      ,T.recvbl_int_subj                  as recvbl_int_subj      --应收利息科目
      ,T.today_recvbl_int                 as today_recvbl_int     --当日应收利息
      ,0                                  as curmth_recvbl_int    --当月应收利息
      ,''                                 as Valtn_Prft_Loss_Subj --估值损益科目
      ,T.Today_Valtn_Prft_Loss_Amt        as Today_Valtn_Prft_Loss_Amt--当日估值损益金额
      ,T.CurMth_Valtn_Prft_Loss_Amt       as CurMth_Valtn_Prft_Loss_Amt--当月估值损益金额
      ,T.Biz_Prc_Diff_Prft_Subj           as Biz_Prc_Diff_Prft_Subj    --买卖价差收益科目
      ,T.Today_Biz_Prc_Diff_Amt           as Today_Biz_Prc_Diff_Amt    --当日买卖价差金额
      ,T.CurMth_Biz_Prc_Diff_Amt          as CurMth_Biz_Prc_Diff_Amt   --当月买卖价差金额
      ,T.Mth_Day_Avg_Bal                  as mth_day_avg_bal           --月日均余额
      ,T.Yr_Day_Avg_Bal                   as yr_day_avg_bal            --年日均余额
      ,T1.Adj_Post_FTP_Prc                as FTP_Price                 --FTP价格
      ,T1.Adj_Post_FTP_Tran_Incom_Expns   as ftp_tranfm_expns          --FTP转移支出
      ,0.00                               as prosp_loss                --预期损失
      ,0.00                               as Crdt_Risk_Econ_Cap        --信用风险经济资本
      ,0.00                               as Mkt_Risk_Econ_Cap         --市场风险经济资本
      ,0.00                               as Opr_Risk_Econ_Cap         --操作风险经济资本
      ,0.00                               as Crdt_Risk_Econ_Cap_Cost   --信用风险经济资本成本
      ,0.00                               as Mkt_Risk_Econ_Cap_Cost    --市场风险经济资本成本
      ,0.00                               as Opr_Risk_Econ_Cap_Cost    --操作风险经济资本成本

from  f_fdm.f_Cap_Raw_TX  T	--资金衍生交易信息表
left join f_fdm.f_agt_ftp_info T1 --FTP信息表 
on T.Agmt_Id= substr(T1.Acct_Num,1,length(T1.Acct_Num)-2) 
and T1.DATA_SOURCE='99700100000'  
and T1.etl_date='$TXDATE'::date
where  T.etl_date='$TXDATE'::date
;
/*数据处理区END*/

COMMIT;
