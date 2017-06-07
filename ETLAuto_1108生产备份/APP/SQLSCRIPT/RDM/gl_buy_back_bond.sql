/*
Author             :魏银辉
Function           :回购反售-债券
Load method        :INSERT
Source table       :f_fdm.F_agt_Cap_Bond_Buy_Back
Destination Table  :GL_BUY_BACK_BOND 回购反售-债券
Frequency          :M
Modify history list:Created by 魏银辉 at 2016-8-9 10:02:06
                    modified by wyh at 20160831  增加字段 本金科目
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
DELETE FROM f_rdm.GL_BUY_BACK_BOND
WHERE  etl_date= '$TXDATE'::date ;
/*数据回退区END*/

/*数据处理区*/
INSERT INTO f_rdm.GL_BUY_BACK_BOND
(
        etl_date            -- 数据日期
        ,tx_num             -- 交易号
        ,tx_sht_nm          -- 交易工具简称
        ,Due_Dt             -- 到期日
        ,dlvy_amt           -- 交割金额
        ,book_val           -- 账面价值
        ,Cur_Cd             -- 币种
        ,sys_src            -- 来源系统
        ,term               -- 期限
        ,prin_subj          -- 本金科目  modified 20160831
)
SELECT
        '$TXDATE'::date        AS etl_date
        ,T.Agmt_Id                      AS tx_num
        ,T.Prod_Cd                      AS tx_sht_nm
        ,T.Due_Dt                       AS Due_Dt
        ,T.Buy_Back_Amt                 AS dlvy_amt
        ,T.Buy_Back_Amt                 AS book_val
        ,T.Cur_Cd                       AS Cur_Cd
        ,T.Sys_Src                      AS sys_src
        ,(CASE
                WHEN T.Due_Dt IS NOT NULL AND T.St_Int_Dt IS NOT NULL
                    THEN T.Due_Dt - T.St_Int_Dt
                ELSE -1
         END
         )                              AS term
        ,prin_subj                      as prin_subj
FROM f_fdm.F_agt_Cap_Bond_Buy_Back T
where T.Prod_Cd NOT IN ('2910','2911')
AND T.etl_date= '$TXDATE'::date
;

/*数据处理区END*/

COMMIT;
