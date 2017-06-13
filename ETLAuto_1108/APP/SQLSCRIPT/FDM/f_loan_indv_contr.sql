/*
Author             :Liuxz
Function           :个人贷款合同信息表
Load method        :
Source table       :dw_sdata.pcs_006_tb_lon_line_use_info,dw_sdata.pcs_006_tb_lin_line_agreement,dw_sdata.pcs_006_tb_lin_line,dw_sdata.pcs_006_tb_lon_loan,dw_sdata.pcs_005_tb_sup_loan_info,dw_sdata.pcs_006_tb_csm_customer,dw_sdata.acc_003_t_acc_assets_ledger
Destination Table  :f_fdm.f_loan_indv_contr
Frequency          :D
Modify history list:Created by zhangwj at 2016-4-15 16:10 v1.0
                    changed by zhangwj at 2016-4-22 10:14 v1.1
                   :Modify  by liuxz 20160819 增加coalesce
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/

/*临时表创建区*/
  create local temporary table IF NOT EXISTS f_loan_indv_contr_temp_1
  on commit preserve rows as
  select
        t21.loan_id                                                            --合同层ID
        ,t22.line_contract_num                                                 --额度协议编号关系
        ,t22.cycle_flag
  from
      (select
             loan_id
             ,line_id
      from   dw_sdata.pcs_006_tb_lon_line_use_info                            --贷款业务额度支用明细表
      where  start_dt<='$TXDATE'::date
      and    end_dt>'$TXDATE'::date
      group by  loan_id,line_id
      ) t21
  inner join
            (select
                   t221.line_id
                   ,(t221.line_id||'-'||coalesce(t221.contract_num,'')) as line_contract_num,t222.cycle_flag
            from   dw_sdata.pcs_006_tb_lin_line_agreement  t221               --额度协议
            inner join dw_sdata.pcs_006_tb_lin_line t222                      --额度信息
            on         t221.line_id=t222.line_id
            and        t222.line_status in  ('1','4')
            and        t222.start_dt<='$TXDATE'::date
            and        t222.end_dt>'$TXDATE'::date
            where      t221.start_dt<='$TXDATE'::date
            and        t221.end_dt>'$TXDATE'::date
            )  t22                                                          --额度LINE_ID ，限定有效额度范围
  on t21.line_id=t22.line_id
  ;
/*临时表创建区END*/

/*数据回退区*/
delete /* +direct */ from f_fdm.f_loan_indv_contr
where etl_date = '$TXDATE'::date
;
/*数据回退区END*/

/*数据处理区*/
insert /* +direct */  into f_fdm.f_loan_indv_contr
       (grp_typ                                                                          --组别
       ,etl_date                                                                         --数据日期
       ,agmt_id                                                                          --协议编号
       ,crdt_lmt_id                                                                      --授信额度编号
       ,org_num                                                                          --机构号
       ,cust_num                                                                         --客户号
       ,cur_cd                                                                           --货币代码
       ,start_dt                                                                         --起始日期
       ,due_dt                                                                           --到期日期
       ,contr_amt                                                                        --合同金额
       ,contr_bal                                                                        --合同余额
       ,contr_next_accm_distr_amt                                                        --合同下累计放款金额
       ,contr_next_accm_repay_amt                                                        --合同下累计还款金额
       ,contr_aval_amt                                                                   --合同可用金额
       ,contr_paid_money_bal                                                             --合同已还款余额
       ,agmt_stat_cd                                                                     --协议状态代码
       ,fst_distr_dt                                                                     --首次放款日期
       ,guar_mode_cd                                                                     --担保方式代码
       ,Loan_Drct_Inds_Cd                                                                --贷款投向行业代码
       ,sys_src                                                                          --系统来源
       )
 select
       1                                                                                 as  grp_typ
       ,'$TXDATE'::date                                                         as  etl_date
       ,coalesce(t.loan_contract_no ,'')                                                 as  agmt_id
       ,coalesce(t1.line_contract_num ,'')                                               as  crdt_lmt_id
       ,coalesce(t2.opn_dep ,'')                                                         as  org_num
       ,coalesce(t3.cus_no ,'')                                                          as  cust_num
       ,coalesce(t.currency ,'')                                                         as  cur_cd
       ,t.loan_begin_date                                                                as  start_dt
       ,t.loan_end_date                                                                  as  due_dt
       ,coalesce(t.loan_contract_amount,0)                                               as  contr_amt
       ,coalesce(t4.bal,0)                                                               as  contr_bal
       ,coalesce(t5.amt,0)                                                               as  contr_next_accm_distr_amt
       ,coalesce(t.total_repay_capital ,0)                                               as  contr_next_accm_repay_amt
       ,coalesce(t4.bal ,0)                                                              as  contr_aval_amt
       ,0                                                                                as  contr_paid_money_bal
       --,coalesce(t.close_flag ,'')                                                      as  agmt_stat_cd
      ,case when T.CLOSE_FLAG='0' then '11'---未结清  
           when T.CLOSE_FLAG='1' then '12'---已结清  
           when T.CLOSE_FLAG='' then '' 
           else '@'||T.CLOSE_FLAG  
       end                                                                               as  agmt_stat_cd
       ,coalesce(to_date(t6.beg_date,'yyyymmdd'),'$MINDATE'::date)                   as  fst_distr_dt
       ,coalesce(t7.TGT_CD,'@'||t.security_kind)                                         as  guar_mode_cd
       ,substr(t.LOAN_PURPOSE,instr(t.LOAN_PURPOSE,':',-1)+1)                                as Loan_Drct_Inds_Cd 
       ,'PCS'                                                                            as  sys_src
 from      dw_sdata.pcs_006_tb_lon_loan  t                                              --贷款台帐主信息表
 left join f_loan_indv_contr_temp_1 t1
 on        t.loan_id=t1.loan_id
 left join dw_sdata.pcs_005_tb_sup_loan_info  t2                                         --主档表
 on        t.loan_contract_no=t2.con_no
 and       t2.start_dt<='$TXDATE'::date
 and       t2.end_dt>'$TXDATE'::date
 left join dw_sdata.pcs_006_tb_csm_customer   t3                                         --客户总表
 on        t.cus_id=t3.cus_id
 and       t3.start_dt<='$TXDATE'::date
 and       t3.end_dt>'$TXDATE'::date
 left join  (select
                   a.con_no
                   ,sum(b.bal)  as bal
             from  dw_sdata.pcs_005_tb_sup_loan_info a                                   --主档表
             left  join  dw_sdata.acc_003_t_acc_assets_ledger b                          --资产类客户账户分户账
             on          a.due_num=b.acc
             and         b.sys_code='99340000000'
             and         b.start_dt<='$TXDATE'::date
             and         b.end_dt>'$TXDATE'::date
             where       a.con_no is not null
             and         a.start_dt<='$TXDATE'::date
             and         a.end_dt>'$TXDATE'::date
             group by    a.con_no
             ) t4
 on          t.loan_contract_no=t4.con_no
 left join (select
                  con_no
                  ,sum(amt) as amt
           from dw_sdata.pcs_005_tb_sup_loan_info                                        --主档表
           where con_no is not  null
           and   start_dt<='$TXDATE'::date
           and   end_dt>'$TXDATE'::date
           group by con_no
           ) t5
 on        t.loan_contract_no = t5.con_no
 left join (select
                  con_no
                  ,min(beg_date) as beg_date
            from  dw_sdata.pcs_005_tb_sup_loan_info                                        --主档表
            where con_no is not null
            and   start_dt<='$TXDATE'::date
            and   end_dt>'$TXDATE'::date
            group by  con_no
           ) t6
on        t.loan_contract_no=t6.con_no
left join f_fdm.CD_RF_STD_CD_TRAN_REF T7
on       T.SECURITY_KIND=T7.SRC_CD
and      T7.DATA_PLTF_SRC_TAB_NM = 'PCS_006_TB_LON_LOAN' 
and      T7.Data_Pltf_Src_Fld_Nm ='SECURITY_KIND' 
where     t.start_dt<='$TXDATE'::date
and       t.end_dt>'$TXDATE'::date
 ;
/*数据处理区END*/

 COMMIT;
