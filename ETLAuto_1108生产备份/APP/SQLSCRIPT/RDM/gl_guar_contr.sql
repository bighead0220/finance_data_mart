/*
Author             :liudongyan
Function           :担保合同
Load method        :INSERT
Source table       :f_loan_guar_contr	贷款担保合同信息表
Destination Table  :GL_Guar_Contr  担保合同
Frequency          :M
Modify history list:Created by liudongyan 2016/5/18 10:19:34
                    Modify by 魏银辉 at 2016-8-8 15:24:45
                        添加筛选条件；
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
DELETE FROM f_rdm.GL_Guar_Contr
WHERE  etl_date=  '$TXDATE'::date ;
/*数据回退区END*/

/*数据处理区*/
INSERT INTO f_rdm.GL_Guar_Contr
(
     etl_date                --数据日期
     ,loan_contr_agmt_id     --贷款合同协议编号
     ,guar_contr_st_dt       --担保合同起期
     ,guar_contr_stp_dt      --担保合同止期
     ,guar_amt               --担保金额
     ,guar_typ_cd            --担保类型代码
     ,guar_agmt_id           --担保协议编号
     ,sys_src                --系统来源

)
SELECT
     '$TXDATE'::date        as etl_date
     ,T.loan_contr_agmt_id           as loan_contr_agmt_id
     ,T.guar_contr_st_dt             as guar_contr_st_dt
     ,T.guar_contr_stp_dt            as guar_contr_stp_dt
     ,T.guar_amt                     as guar_amt
     ,T.guar_typ_cd                  as guar_typ_cd
     ,T.guar_agmt_id                 as guar_agmt_id
     ,T.sys_src                      as sys_src


FROM  f_fdm.f_loan_guar_contr  T	--贷款担保合同信息表
WHERE T.loan_contr_agmt_id IN   (SELECT
                                        contr_id
                                from f_rdm.GL_Loan_Contr
                                where etl_date = '$TXDATE'::date
                                )
AND T.etl_date = '$TXDATE'::date
;
/*数据处理区END*/

COMMIT;
