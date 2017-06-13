/*
Author             :Liuxz
Function           :贷款抵质押物信息表
Load method        :
Source table       :dw_sdata.pcs_006_tb_pub_security,dw_sdata.pcs_006_tb_ast_asset,dw_sdata.pcs_006_tb_csm_customer,dw_sdata.pcs_006_tb_lon_loan,dw_sdata.pcs_006_tb_ast_depositcer,dw_sdata.ccs_004_tb_grt_collateral,dw_sdata.ccs_004_tb_grt_business_relation,dw_sdata.ccs_004_tb_grt_collateral_registration,dw_sdata.ccs_004_tb_grt_deposit
Destination Table  :f_fdm.f_loan_mrtg_prop
Frequency          :D
Modify history list:Created by zhangwj at 2016-4-18 14:37 v1.0
                    Changed by zhangwj at 2016-4-29 14:18 v1.1
                    Changed by zhangwj at 2016-5-6  15:25 v1.2   增加公司贷款部分存单账号的取数规则
                    Changed by zhangwj at 2016-5-19 10:12 v1.3   变更主表
                    Changed by zhangwj at 2016-5-23 10:12 v1.4   大数据贴源层表名修改，表为拉链表或流水表，与之保持一致
                    Changed by zhangwj at 2016-5-30 11:14 v1.5   新增第二组存单表，汇总字段“登记价值”
                   :Modify  by liuxz 20160621 修改组1、组2担保物类型代码的取数逻辑 (变更记录82)
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/

/*临时表创建区*/
/*临时表创建区END*/

/*数据回退区*/
delete /* +direct */ from f_fdm.f_loan_mrtg_prop
where etl_date ='$TXDATE'
;
/*数据回退区END*/

/*数据处理区*/
insert /* +direct */  into f_fdm.f_loan_mrtg_prop
       (grp_typ                                                                   --组别
       ,etl_date                                                                  --数据日期
       ,pldg_id                                                                   --抵押物编号
       ,guar_contr_agmt_id                                                        --担保合同协议编号
       ,pldg_typ_cd                                                               --担保物类型代码
       ,pldg_nm                                                                   --担保物名称
       ,cur_cd                                                                    --货币代码
       ,pldg_rgst_val                                                             --抵押物登记价值
       ,pldg_estim_val                                                            --抵押物评估价值
       ,pldg_estim_dt                                                             --抵押物评估日期
       ,had_mtg_val                                                               --已抵押价值
       ,atrer_id                                                                  --权属人编号
       ,atrer_nm                                                                  --权属人名称
       ,mtg_usg_cd                                                                --抵押用途代码
       ,PSBC_cfm_val                                                             --我行确认价值
       ,val_cfm_dt                                                                --价值确定日期
       ,cret_dpst_acct_num                                                        --存单账号
       ,sys_src                                                                   --系统来源
       )
 select
       1                                                                          as  grp_typ
       ,'$TXDATE'::date                                                  as  etl_date
       ,coalesce(t1.asset_no ,'')                                                 as  pldg_id
       ,coalesce(t.security_id||'-'||coalesce(t.security_contract_no,''),'')      as  guar_contr_agmt_id
       ,'PCS_'||coalesce(t.asset_type ,'')                                        as  pldg_typ_cd
       ,coalesce(t.asset_name ,'')                                                as  pldg_nm
       ,coalesce(t.currency ,'')                                                  as  cur_cd
       ,0                                                                         as  pldg_rgst_val
       ,coalesce(t1.assessment_value_formal ,0)                                   as  pldg_estim_val
       ,coalesce(t1.new_evaluation_date,'$MAXDATE'::date)                     as  pldg_estim_dt
       ,coalesce(t.security_amount ,0)                                            as  had_mtg_val
       ,coalesce(t2.cus_no,'')                                                    as  atrer_id
       ,coalesce(t2.cus_name ,'')                                                 as  atrer_nm
       ,coalesce(substr(t3.loan_purpose,instr(t3.loan_purpose,':',-1)+1) ,'')     as  mtg_usg_cd
       ,coalesce(t1.assessment_value_formal ,0)                                   as  PSBC_cfm_val
       ,coalesce(t1.new_evaluation_date,'$MAXDATE'::date)                     as  val_cfm_dt
       ,coalesce(t4.account,'')                                                   as  cret_dpst_acct_num
       ,'PCS'                                                                     as  sys_src
 from  dw_sdata.pcs_006_tb_pub_security       t                                  --抵质押信息表
 left join dw_sdata.pcs_006_tb_ast_asset      t1                                 --客户资产
 on        t.asset_id =t1.asset_id
 and       t1.start_dt<='$TXDATE'::date
 and       t1.end_dt>'$TXDATE'::date
 left join dw_sdata.pcs_006_tb_csm_customer   t2                                 --客户总表
 on        t.asset_cus_id=t2.cus_id
 and       t2.start_dt<='$TXDATE'::date
 and       t2.end_dt>'$TXDATE'::date
 left join dw_sdata.pcs_006_tb_lon_loan       t3                                --贷款台帐主信息表
 on        t.app_id=t3.loan_id
 and       t3.start_dt<='$TXDATE'::date
 and       t3.end_dt>'$TXDATE'::date
 left join dw_sdata.pcs_006_tb_ast_depositcer t4                                --存单
 on        t.asset_id =t4.asset_id
 and       t4.start_dt<='$TXDATE'::date
 and       t4.end_dt>'$TXDATE'::date
 where     t.security_status <>'5'
 and       t.start_dt<='$TXDATE'::date
 and       t.end_dt>'$TXDATE'::date
 union all
 (select
        2                                                                        as  grp_typ
        ,'$TXDATE'::date                                                as  etl_date
        ,coalesce(t.collateral_num ,'')                                          as  pldg_id
        ,coalesce(t1.biz_limit_cont_num ,'')                                     as  guar_contr_agmt_id
        ,'CCS_'||coalesce(t.collateral_type_cd ,'')                                      as  pldg_typ_cd
        ,coalesce(t.collateral_name ,'')                                         as  pldg_nm
        ,coalesce(t.eval_value_currency_cd ,'')                                  as  cur_cd
        ,coalesce(t2.register_value ,0)                                          as  pldg_rgst_val
        ,coalesce(t.eval_value ,0)                                               as  pldg_estim_val
        ,coalesce(t.evaluate_date,'$MAXDATE'::date)                          as  pldg_estim_dt
        ,coalesce((case
                   when t1.guaranty_type_cd='1'
                   and  t.collateral_type_cd='35'
                   then t1.guaranty_use_amt                                      --本次抵质押品占用价值
                   when t1.guaranty_type_cd='2'
                   then t1.guaranty_use_amt                                      --本次抵质押品占用价值
                   else  t1.guaranty_amt                                         --本次担保债权金额
                   end
                   ) ,0)                                                         as  had_mtg_val
        ,coalesce(t.customer_num ,'')                                            as  atrer_id
        ,coalesce(t.customer_name ,'')                                           as  atrer_nm
        ,''                                                                      as  mtg_usg_cd
        ,coalesce(t.ccb_assessed_value ,0)                                       as  PSBC_cfm_val
        ,coalesce(t.evaluate_date,'$MAXDATE'::date)                          as  val_cfm_dt
        ,coalesce(t3.account_num,'')                                             as  cret_dpst_acct_num
        ,'CCS'                                                                   as  sys_src
 from   dw_sdata.ccs_004_tb_grt_collateral t                                     --抵质押物
 inner join dw_sdata.ccs_004_tb_grt_business_relation  t1                       --额度业务合同与担保品关联关系
 on         t.guaranty_id=t1.guaranty_id
 and        t1.valid_ind='1'                                                     --是否有效=1有效
 and        t1.guaranty_relation_type_cd='4'                                     --担保关联类型代码=4从合同关联
 and        t1.start_dt<='$TXDATE'::date
 and        t1.end_dt>'$TXDATE'::date
 left join
          (select
                 guaranty_id
                 ,sum(register_value) as register_value
          from  dw_sdata.ccs_004_tb_grt_collateral_registration              --抵质押物登记信息
          where reg_state='2'
          and   start_dt<='$TXDATE'::date
          and   end_dt>'$TXDATE'::date
          group by guaranty_id) t2
 on         t.guaranty_id=t2.guaranty_id
 left join  dw_sdata.ccs_004_tb_grt_deposit t3                               --存单
 on         t. guaranty_id=t3.guaranty_id
 and        t3.start_dt<='$TXDATE'::date
 and        t3.end_dt>'$TXDATE'::date
 where      t.collateral_status_cd='1'
 and        t.start_dt<='$TXDATE'::date
 and        t.end_dt>'$TXDATE'::date)
 ;
/*数据处理区END*/
 COMMIT;