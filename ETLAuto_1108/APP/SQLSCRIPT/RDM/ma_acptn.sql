/*
Author             :liudongyan
Function           :承兑汇票
Load method        :INSERT
Source table       :f_agt_acptn_info
Destination Table  :ma_acptn   承兑汇票
Frequency          :M
Modify history list:Created by liudongyan 2016/5/18 10:19:34
                   :Modify  by chh 2016/6/13 14:22:46 修改业务编号字段加工规则,业务编号+票号
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
DELETE FROM f_rdm.ma_acptn
WHERE  etl_date=  '$TXDATE'::date ;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_rdm.ma_acptn 
(
       etl_date                     --数据日期
      ,biz_id                        --业务编号
      ,Org_Num                       --机构号
      ,cust_num                      --客户号
      ,Cur_Cd                        --币种
      ,prod_cd                       --产品代码
      ,St_Int_Dt                     --起息日
      ,Due_Dt                        --到期日
      ,Fiv_Cls                       --五级分类
      ,sys_src                       --系统来源
      ,loan_deval_prep_bal           --贷款减值准备余额
      ,loan_deval_prep_amt           --贷款减值准备发生额
      ,acct_stat                     --帐户状态
      ,prin_subj                     --本金科目
      ,acptn_amt                     --承兑汇票金额
      ,acptn_bal                     --承兑汇票余额
      ,mth_day_avg_bal               --月日均余额
      ,yr_day_avg_bal                --年日均余额
      ,prosp_loss                    --预期损失
      ,Crdt_Risk_Econ_Cap            --信用风险经济资本
      ,Mkt_Risk_Econ_Cap             --市场风险经济资本
      ,Opr_Risk_Econ_Cap             --操作风险经济资本
      ,Crdt_Risk_Econ_Cap_Cost       --信用风险经济资本成本
      ,Mkt_Risk_Econ_Cap_Cost        --市场风险经济资本成本
      ,Opr_Risk_Econ_Cap_Cost        --操作风险经济资本成本
      ,margn_acct_num                --保证金账号
 
)
SELECT
       '$TXDATE'::date     as etl_date
      ,a.biz_id||'_'||a.bill_num    as biz_id
      ,a.org_num                      as Org_Num
      ,a.drawr_cust_id                as cust_num
      ,a.cur_cd                       as Cur_Cd
      ,a.prod_cd                      as prod_cd
      ,a.acpt_dt                      as St_Int_Dt                   
      ,b.draw_day                     as Due_Dt
      ,a.fiv_cls_cd                   as Fiv_Cls
      ,a.sys_src                      as sys_src
      ,a.deval_prep_bal               as loan_deval_prep_bal
      ,a.deval_prep_amt               as loan_deval_prep_amt
      ,a.acct_stat_cd                 as acct_stat
      ,a.subj_cd                      as prin_subj
      ,a.acpt_amt                     as acptn_amt
      ,a.acpt_bal                     as acptn_bal
      ,a.mth_day_avg_bal              as mth_day_avg_bal
      ,a.yr_day_avg_bal               as yr_day_avg_bal
      ,0.00                         as prosp_loss
      ,0.00                         as Crdt_Risk_Econ_Cap
      ,0.00                         as Mkt_Risk_Econ_Cap
      ,0.00                         as Opr_Risk_Econ_Cap
      ,0.00                         as Crdt_Risk_Econ_Cap_Cost
      ,0.00                         as Mkt_Risk_Econ_Cap_Cost
      ,0.00                         as Opr_Risk_Econ_Cap_Cost
      ,a.margn_acct_num               as margn_acct_num
FROM  f_fdm.f_agt_acptn_info a -- 关联取 draw_day 起息日
left join (select bill_num,draw_day from f_fdm.f_agt_bill_info
 where bill_stat_cd <>'99'
and etl_date = '$TXDATE'::date group by 1,2 ) b 
 on a.bill_num=b.bill_num 
WHERE a.etl_date = '$TXDATE'::date
;
/*数据处理区END*/

COMMIT;
