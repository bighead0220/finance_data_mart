/*
Author             :liudongyan
Function           :资金业务-正回购
Load method        :INSERT
Source table       :F_agt_Cap_Bond_Buy_Back	              
Destination Table  :ma_pstv_buy_back   资金业务-正回购		
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
DELETE FROM f_rdm.ma_pstv_buy_back
WHERE  etl_date= '$TXDATE'::date  ;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_rdm.ma_pstv_buy_back
(
 
     etl_date                        --数据日期
     ,buy_back_id                    --回购编号
     ,Org_Num                        --机构号
     ,tx_comb                        --交易组合
     ,cust_num                       --客户号
     ,acct_typ                       --账户类型
     ,Cur_Cd                         --币种
     ,prod_cd                        --产品代码
     ,St_Int_Dt                      --起息日
     ,Due_Dt                         --到期日
     ,Curr_Int_Rate                  --当前利率
     ,Bmk_Int_Rate                   --基准利率
     ,Basic_Diff                     --基差
     ,Int_Days                       --计息天数
     ,Cmpd_Int_Calc_Mode_Cd          --复利计算方式代码
     ,Int_Rate_Attr_Cd               --固定浮动属性
     ,Orgnl_Term                     --原始期限
     ,Orgnl_Term_Corp_Cd             --原始期限单位
     ,Rprc_Prd                       --重定价周期
     ,Rprc_Prd_Corp_Cd               --重定价周期单位
     ,Last_Rprc_Day                  --上次重定价日
     ,Next_Rprc_Day                  --下次重定价日
     ,sys_src                        --系统来源
     ,prin_subj                      --本金科目
     ,Curr_Bal                       --当前余额
     ,int_subj                       --利息科目
     ,Today_Provs_Int                --当日计提利息
     ,CurMth_Provs_Int               --当月计提利息
     ,Accm_Provs_Int                 --累计计提利息
     ,Today_Int_Pay                  --当日付息
     ,CurMth_Paid_Int                --当月已付息
     ,Accm_Paid_Int                  --产品代码表
     ,int_adj_amt                    --利息调整金额
     ,mth_day_avg_bal                --月日均余额
     ,yr_day_avg_bal                 --年日均余额
     ,FTP_Price                      --FTP价格
     ,ftp_tranfm_incom               --FTP转移收入
     ,Exchg_Prft_Loss_Subj           --汇兑损益科目
     ,Fair_Val_Chg_Subj                   --公允价值变动科目
 
)
SELECT
      '$TXDATE'::date         as etl_date
     ,Agmt_Id                          as buy_back_id
     ,t.Org_Num                        as Org_Num
     ,t.TX_Comb_Cd                     as tx_comb
     ,t.TX_Cnt_Pty_Cust_Num            as cust_num
     ,''                               as acct_typ
     ,t.Cur_Cd                         as Cur_Cd
     ,t.Prod_Cd                        as prod_cd
     ,t.St_Int_Dt                      as St_Int_Dt
     ,t.Due_Dt                         as Due_Dt
     ,t.Exec_Int_Rate                  as Curr_Int_Rate
     ,0                                as Bmk_Int_Rate
     ,0                                as Basic_Diff
     ,''                               as Int_Days
     ,''                               as Cmpd_Int_Calc_Mode_Cd
     ,''                               as Int_Rate_Attr_Cd
     ,0                                as Orgnl_Term
     ,''                               as Orgnl_Term_Corp_Cd
     ,0                                as Rprc_Prd
     ,''                               as Rprc_Prd_Corp_Cd
     ,'$MINDATE'                   as Last_Rprc_Day
     ,'$MINDATE'                   as Next_Rprc_Day
     ,t.Sys_Src                        as sys_src
     ,t.Prin_Subj                      as prin_subj
     ,t.Buy_Back_Amt                   as Curr_Bal
     ,t.Int_Subj                       as int_subj
     ,0                                as Today_Provs_Int
     ,t.Buy_Back_Int                   as CurMth_Provs_Int
     ,0                                as Accm_Provs_Int
     ,0                                as Today_Int_Pay
     ,0                                as CurMth_Paid_Int
     ,0                                as Accm_Paid_Int
     ,0                                as int_adj_amt
     ,Mth_Day_Avg_Bal                  as mth_day_avg_bal
     ,Yr_Day_Avg_Bal                   as yr_day_avg_bal
     ,T1.Adj_Post_FTP_Prc              as FTP_Price
     ,T1.Adj_Post_FTP_Tran_Incom_Expns as ftp_tranfm_incom 
     ,''                               as exchg_prft_loss_subj
     ,''                               as val_chg_subj
FROM f_fdm.F_agt_Cap_Bond_Buy_Back	 t      --资金债券回购业务信息表    
left join f_fdm.f_agt_ftp_info T1 --FTP信息表
on T.Agmt_Id= substr(T1.Acct_Num,1,length(T1.Acct_Num)-2) 
and T1.DATA_SOURCE='99700100000'  
and T1.etl_date = '$TXDATE'::date 
where  t.etl_date= '$TXDATE'::date 
and T.Biz_Drct_Ind='SELL'                         
;
/*数据处理区END*/

COMMIT;
