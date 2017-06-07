/*
Author             :liudongyan
Function           :存款
Load method        :INSERT
Source table       :f_dpst_corp_acct 公司存款账户信息表
                    f_dpst_indv_acct 个人存款账户信息表
Destination Table  :GL_Dpst	存款
Frequency          :
Modify history list:Created by liudongyan 2016/5/18 10:19:34
                    Modify by 魏银辉 at 2016-8-9 15:03:10
                    modified by wyh at 20160830    删除第二组 修改第一组筛选条件
                    modified by wyh at 20160831    增加字段 本金科目
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
DELETE FROM f_rdm.GL_Dpst
WHERE  etl_date=  '$TXDATE'::date ;
/*数据回退区END*/

/*数据处理区*/
INSERT INTO f_rdm.GL_Dpst
(
      etl_date               --数据日期
     ,acct_num               --账户号
     ,sub_acct_num           --子账户号
     ,accti_pltf_subj_num    --会计平台科目号
     ,fst_lvl_brch           --一级分行编码
     ,cust_cd                --客户代码
     ,Cur_Cd                 --币种
     ,book_orgnl_amt         --账面原币金额
     ,stbl_int_rate          --适用利率
     ,sys_src                --来源系统
     ,prin_subj              --本金科目    --modified 20160831
)
SELECT
    '$TXDATE'::date        as etl_date
     ,Cust_Acct_Num                 as acct_num
     ,Sub_Acct                      as sub_acct_num
     ,Prin_Subj                     as accti_pltf_subj_num
     ,Org_Num                       as fst_lvl_brch
     ,Cust_Num                      as cust_cd
     ,Cur_Cd                        as Cur_Cd
     ,Curr_Bal                      as book_orgnl_amt
     ,Exec_Int_Rate                 as stbl_int_rate
     ,Sys_Src                       as sys_src
     ,prin_subj                     as prin_subj
FROM  f_fdm.f_dpst_corp_acct  --公司存款账户信息表
WHERE 
--Curr_Bal > 0 AND                                    modified 20160830 
etl_date = '$TXDATE'::date;
/*UNION ALL                                           modified 20160830
SELECT
      '$TXDATE'::date      as etl_date
     ,Agmt_Id                       as acct_num
     ,''                            as sub_acct_num
     ,Prin_Subj                     as accti_pltf_subj_num
     ,Org_Num                       as fst_lvl_brch
     ,Cust_Num                      as cust_cd
     ,Cur_Cd                        as Cur_Cd
     ,Curr_Bal                      as book_orgnl_amt
     ,Exec_Int_Rate                 as stbl_int_rate
     ,Sys_Src                       as sys_src
FROM  f_fdm.f_dpst_indv_acct	--个人存款账户信息表
WHERE Curr_Bal > 0
AND etl_date = '$TXDATE'::date
*/
/*数据处理区END*/

COMMIT;
