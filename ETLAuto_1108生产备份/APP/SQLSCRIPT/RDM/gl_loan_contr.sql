/*
Author             :liudongyan
Function           :贷款合同
Load method        :INSERT
Source table       :f_loan_indv_contr	个人贷款合同信息表
                    f_loan_corp_contr	公司贷款合同信息表
Destination Table  :GL_Loan_Contr  贷款合同
Frequency          :M
Modify history list:Created by liudongyan 2016/5/18 10:19:34
                    Modify by 魏银辉 at 2016-8-8 16:08:11 修改公司贷款合同信息表下的筛选条件
                    modified by wyh at 20161011 增加月末跑批控制语句                 
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
DELETE FROM f_rdm.GL_Loan_Contr
WHERE  etl_date=  '$TXDATE'::date ;
/*数据回退区END*/

/*数据处理区*/
INSERT INTO f_rdm.GL_Loan_Contr
(
     etl_date       --数据日期
     ,contr_id      --合同编号
     ,contr_amt     --合同金额
     ,contr_bal     --合同余额
     ,contr_aval_amt   --合同可用金额
     ,contr_end_prd    --合同止期
     ,sys_src          --来源系统

)
SELECT
    '$TXDATE'::date         as etl_date
     ,agmt_id                as contr_id
     ,contr_amt              as contr_amt
     ,contr_bal              as contr_bal
     ,contr_aval_amt         as contr_aval_amt
     ,due_dt                 as contr_end_prd
     ,sys_src                as sys_src
FROM  f_fdm.f_loan_corp_contr	--公司贷款合同信息表
WHERE agmt_stat_cd in ('3','4','5')
AND etl_date = '$TXDATE'::date
UNION ALL
SELECT
     distinct
     '$TXDATE'::date       as etl_date
     ,agmt_id                as contr_id
     ,contr_amt              as contr_amt
     ,contr_bal              as contr_bal
     ,contr_aval_amt         as contr_aval_amt
     ,due_dt                 as contr_end_prd
     ,sys_src                as sys_src
FROM  f_fdm.f_loan_indv_contr	--个人贷款合同信息表
WHERE etl_date = '$TXDATE'::date
;
/*数据处理区END*/

COMMIT;
