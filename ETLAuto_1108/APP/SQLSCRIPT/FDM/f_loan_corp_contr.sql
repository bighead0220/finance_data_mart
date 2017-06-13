/*:
Author             :XMH
Function           :公司贷款合同信息表
Load method        :INSERT
Source table       :dw_sdata.ccs_004_tb_con_contract,dw_sdata.ccs_004_tb_con_disb_plan,dw_sdata.ccs_004_tb_con_acct_summary
Destination Table  :f_fdm.f_loan_corp_contr 
Frequency          :D
Modify history list:Created by徐铭浩2016年5月4日10:05:55
                   :Modify  by
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
*-------------------------------------------逻辑说明END------------------------------------------
*/
/*临时表区*/
/*临时表区END*/
/*数据回退区*/
Delete /* +direct */ from  f_fdm.f_loan_corp_contr 
where etl_date='$TXDATE'::date
/*数据回退区end*/
;
/*数据处理区*/
insert into f_fdm.f_loan_corp_contr
(   
     ETL_Date                      --数据日期    
    ,grp_typ                       --组别
    ,Agmt_Id                       --协议编号
    ,Org_Num                       --机构号
    ,Cust_Num                      --客户号
    ,Cur_Cd                        --货币代码
    ,Contr_Amt                     --合同金额
    ,Start_Dt                      --起始日期
    ,Due_Dt                        --到期日期
    ,Agmt_Stat_Cd                  --协议状态代码
    ,Fst_Distr_Dt                  --首次放款日期
    ,Crdt_Typ_Cd                   --授信类型代码
    ,Crdt_Lmt_Id                   --授信额度编号
    ,Guar_Mode_Cd                  --担保方式代码
    ,LVRG_Mode                     --融资模式
    ,Draw_Clos_Dt_Prd              --提款截止日期
    ,Syndic_Loan_Typ_Cd            --银团贷款类型代码
    ,Loan_Drct_Inds_Cd             --贷款投向行业代码
    ,Contr_Bal                     --合同余额
    ,Contr_Next_Accm_Distr_Amt     --合同下累计放款金额
    ,Contr_Next_Accm_Repay_Amt     --合同下累计还款金额
    ,Contr_Aval_Amt                --合同可用金额
    ,Contr_Paid_Money_Bal          --合同已还款余额
    ,Sys_Src                       --数据来源
)
select                                                              
       '$TXDATE' :: date                        as etl_date                                           
       ,'1'                                              as GRP_TYP                    
       ,coalesce(t.contract_num,'')                      as Agmt_Id                                         
       ,coalesce(t.handling_org_cd,'')                   as Org_Num                                       
       ,coalesce(t.customer_num,'')                      as Cust_Num                                        
       ,coalesce(t.currency_cd,'')                       as Cur_Cd                                           
       ,coalesce(t.contract_total_amt,0)                 as Contr_Amt                                 
       ,coalesce(to_date(to_char(t.start_date),'yyyy-mm-dd'),'$MINDATE'::date)      as Start_Dt               
       ,coalesce(to_date(to_char(t.expiration_date),'yyyy-mm-dd'),'$MINDATE'::date) as Due_Dt            
       ,coalesce(t.contract_status_cd,'')                as Agmt_Stat_Cd                              
       ,coalesce(to_date(to_char(T1.payout_date),'yyyy-mm-dd'),'$MINDATE'::date) as Fst_Distr_Dt                                
       ,coalesce(t.credit_product_cd,'')                as Crdt_Typ_Cd                                
       ,coalesce(t.credit_limit_num,'')                 as Crdt_Lmt_Id                                 
       ,coalesce(t.guaranty_type,'')                    as Guar_Mode_Cd 
       ,coalesce(t.financing_model,'')                  as  LVRG_Mode 
       ,coalesce(to_date(to_char(t.loans_end_date),'yyyy-mm-dd'),'$MINDATE'::date)  as Draw_Clos_Dt_Prd           
       ,coalesce(t.bank_group_style,'')                 as Syndic_Loan_Typ_Cd 
       ,coalesce(t.loan_type_instruction_cd,'')         as Loan_Drct_Inds_Cd                                         
       ,coalesce(T2.contract_balance,0)                 as Contr_Bal                                   
       ,coalesce(T2.cumulative_payout_amt,0)            as Contr_Next_Accm_Distr_Amt              
       ,coalesce(T2.cumulative_repay_amt,0)             as Contr_Next_Accm_Repay_Amt               
       ,coalesce(T2.contract_available_amt,0)           as Contr_Aval_Amt                        
       ,'0.00'                                          as Contr_Paid_Money_Bal                                   
       ,'CCS'                                           as sys_src                                                 
from  dw_sdata.ccs_004_tb_con_contract t
left join 
(select contract_num,min(payout_date)  as payout_date 
   from dw_sdata.ccs_004_tb_con_disb_plan
  Where start_dt<='$TXDATE'::date and '$TXDATE'::date<end_dt
group by contract_num)T1
     on t.contract_num=T1.contract_num 
left join dw_sdata.ccs_004_tb_con_acct_summary  T2 
     on t.contract_num=T2.contract_num
    and T2. start_dt<='$TXDATE'::date and '$TXDATE'::date<T2.end_dt
  where T. start_dt<='$TXDATE'::date and '$TXDATE'::date<T.end_dt
    and T.credit_product_cd not like '0307%' --非公司贷款业
    and t.contract_num<>''
;
/*数据处理区END*/
commit;
                 
