/*
Author             :魏银辉
Function           :同业
Load method        :INSERT
Source table       :f_fdm.F_agt_Cap_Stor
Destination Table  :GL_IBANK 同业
Frequency          :M
Modify history list:Created by 魏银辉 at 2016-8-8 17:07:02
                    modified wyh 20160831 增加字段本金科目 客户编号
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
DELETE FROM f_rdm.GL_IBANK
WHERE  etl_date= '$TXDATE'::date ;
/*数据回退区END*/

/*数据处理区*/
INSERT INTO f_rdm.GL_IBANK
(
        etl_date            --数据日期
        ,bank_num           --银行帐号
        ,bank_sub_acct      --银行子账号
        ,biz_cate           --业务类别
        ,fst_lvl_brch       --一级分行编码
        ,Due_Dt             --到期日
        ,orgnl_cur          --原币币种
        ,orgnl_bal          --帐面原币余额
        ,sys_src            --来源系统
        ,term               --期限
        ,prin_subj          --本金科目    modified 20160831 
        ,Cust_Id            --客户编号    modified 20160831
)
SELECT
        '$TXDATE'::date        AS etl_date
        ,T.Cust_Acct_Num                AS bank_num
        ,T.Agmt_Id                      AS bank_sub_acct
        ,T.Prod_Cd                      AS biz_cate
        ,T.Org_Num                      AS fst_lvl_brch
        ,T.Due_Dt                       AS Due_Dt
        ,T.Cur_Cd                       AS orgnl_cur
        ,T.Curr_Bal                     AS orgnl_bal
        ,T.Sys_Src                      AS sys_src
        ,(CASE
                WHEN T.Due_Dt IS NOT NULL AND T.St_Int_Dt IS NOT NULL
                    THEN T.Due_Dt - T.St_Int_Dt
                ELSE -1
         END
         )                              AS term
        ,prin_subj                      as prin_subj
        ,Cust_Acct_Num                  as Cust_Id
FROM  f_fdm.F_agt_Cap_Stor T
WHERE etl_date = '$TXDATE'::date
;

/*数据处理区END*/

COMMIT;
