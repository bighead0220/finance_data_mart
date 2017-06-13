/*
Author             :liudongyan
Function           :票据贴现
Load method        :INSERT
Source table       :f_agt_bill_discount	票据贴现信息表          
Destination Table  :ma_bill_discount  票据贴现
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
DELETE FROM f_rdm.ma_bill_discount
WHERE  etl_date='$TXDATE'::date  ;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_rdm.ma_bill_discount
(
      etl_date                   --数据日期
     ,discnt_acct_num            --贴现账号
     ,bill_num                   --票据号
     ,Org_Num                    --机构号
     ,cust_num                   --客户号
     ,Cur_Cd                     --币种
     ,prod_cd                    --产品代码
     ,St_Int_Dt                  --起息日
     ,Due_Dt                     --到期日
     ,Curr_Int_Rate              --当前利率
     ,Int_Days                   --计息天数
     ,Cmpd_Int_Calc_Mode_Cd      --复利计算方式代码
     ,Int_Rate_Attr_Cd           --固定浮动属性
     ,repay_mode                 --还款方式
     ,repay_prd                  --还款周期
     ,repay_prd_corp             --还款周期单位
     ,Next_Pay_Amt               --下次付款金额
     ,Last_Pay_Day               --上次付款日
     ,Next_Pay_Day              --下次付款日
     ,Orgnl_Term                --原始期限
     ,Orgnl_Term_Corp_Cd        --原始期限单位
     ,Rprc_Prd                  --重定价周期
     ,Rprc_Prd_Corp_Cd          --重定价周期单位
     ,Last_Rprc_Day             --上次重定价日
     ,Next_Rprc_Day             --下次重定价日
     ,Fiv_Cls                   --五级分类
     ,loan_stat                 --贷款状态
     ,sys_src                   --系统来源
     ,loan_deval_prep_bal       --贷款减值准备余额
     ,loan_deval_prep_amt       --贷款减值准备发生额
     ,Acct_Stat_Cd              --账户状态
     ,par_subj                  --产品代码表
     ,par_amt                   --面值金额
     ,int_adj_subj              --利息调整科目
     ,int_adj_amt               --利息调整金额
     ,int_subj                  --利息科目
     ,curmth_amtbl_int          --当月摊销利息
     ,mth_day_avg_bal           --月日均余额
     ,FTP_Price                 --FTP价格
     ,ftp_tranfm_expns          --FTP转移支出
     ,prosp_loss                --预期损失
     ,Crdt_Risk_Econ_Cap        --信用风险经济资本
     ,Mkt_Risk_Econ_Cap         --市场风险经济资本
     ,Opr_Risk_Econ_Cap         --操作风险经济资本
     ,Crdt_Risk_Econ_Cap_Cost   --信用风险经济资本成本
     ,Mkt_Risk_Econ_Cap_Cost    --市场风险经济资本成本
     ,Opr_Risk_Econ_Cap_Cost    --操作风险经济资本成本

 
)
SELECT
     '$TXDATE'::date  as etl_date
     ,t.agmt_id                  as discnt_acct_num
     ,t.bill_id                  as bill_num
     ,t.org_num                  as Org_Num
     ,t.cust_num                 as cust_num
     ,t.cur_cd                   as Cur_Cd
     ,t.prod_cd                  as prod_cd
     ,t.discnt_dt                as St_Int_Dt
     ,t.due_dt                   as Due_Dt
     ,t.int_rate                 as Curr_Int_Rate
     ,''                         as Int_Days
     ,''                         as Cmpd_Int_Calc_Mode_Cd
     ,''                         as Int_Rate_Attr_Cd
     ,''                         as repay_mode
     ,''                         as repay_prd
     ,''                         as repay_prd_corp
     ,0                          as Next_Pay_Amt
     ,'$MINDATE' ::date      as Last_Pay_Day
     ,'$MINDATE' ::date      as Next_Pay_Day
     ,0                          as Orgnl_Term
     ,''                         as Orgnl_Term_Corp_Cd
     ,0                          as Rprc_Prd
     ,''                         as Rprc_Prd_Corp_Cd
     ,'$MINDATE' ::date      as Last_Rprc_Day
     ,'$MINDATE' ::date      as Next_Rprc_Day
     ,t.fiv_cls_cd               as Fiv_Cls
     ,''                         as loan_stat
     ,t.sys_src                  as sys_src
     ,t.loan_deval_prep_bal      as loan_deval_prep_bal
     ,t.loan_deval_prep_amt      as loan_deval_prep_amt
     ,t.acct_stat_cd             as Acct_Stat_Cd
     ,t.prin_subj                as par_subj
     ,t.discnt_amt               as par_amt
     ,t.int_adj_subj             as int_adj_subj
     ,t.int_adj_amt              as int_adj_amt
     ,t.int_subj                 as int_subj
     ,t.curmth_amtbl_int         as curmth_amtbl_int
     ,t.mth_day_avg_bal          as mth_day_avg_bal
     ,t1.Adj_Post_FTP_Prc        as FTP_Price
     ,t1.Adj_Post_FTP_Tran_Incom_Expns   as ftp_tranfm_expns
     ,0.00                       as prosp_loss
     ,0.00                       as Crdt_Risk_Econ_Cap
     ,0.00                       as Mkt_Risk_Econ_Cap
     ,0.00                       as Opr_Risk_Econ_Cap
     ,0.00                       as Crdt_Risk_Econ_Cap_Cost
     ,0.00                       as Mkt_Risk_Econ_Cap_Cost
     ,0.00                       as Opr_Risk_Econ_Cap_Cost
 FROM f_fdm.f_agt_bill_discount	 t--票据贴现信息表     
 left join f_fdm.f_agt_ftp_info t1  --FTP信息表
    on t.agmt_id = substr(t1.acct_num ,1,length(t1.acct_num)-2)     
   and t1.data_source='99470000000'    
   and t1.etl_date='$TXDATE'::date              
WHERE  t.etl_date='$TXDATE'::date
;
/*数据处理区END*/

COMMIT;
