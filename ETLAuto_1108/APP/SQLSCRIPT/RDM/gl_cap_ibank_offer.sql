/*
Author             :魏银辉
Function           :资金拆借
Load method        :INSERT
Source table       :f_fdm.F_agt_Cap_Offer
Destination Table  :GL_CAP_IBANK_OFFER 资金拆借
Frequency          :M
Modify history list:Created by 魏银辉 at 2016-8-8 17:29:08
                    modified by wyh at 20160831 增加字段本金科目
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
DELETE FROM f_rdm.GL_CAP_IBANK_OFFER
WHERE  etl_date= '$TXDATE'::date ;
/*数据回退区END*/

/*数据处理区*/
INSERT INTO f_rdm.GL_CAP_IBANK_OFFER
(
        etl_date                        --数据日期
        ,tx_num                         --交易号
        ,biz_typ                        --业务类型
        ,tx_tools                       --交易工具
        ,Due_Dt                         --到期日
        ,remn_prin                      --剩余本金
        ,fst_lvl_brch                   --一级分行
        ,orgnl_cur                      --原币币种
        ,orgnl_bal                      --帐面原币余额
        ,sys_src                        --来源系统
        ,term                           --期限
        ,prin_subj                      --本金科目   modified 20160831
)
SELECT
        '$TXDATE'::DATE        AS etl_date
        ,T.Agmt_Id                      AS tx_num
        ,T.Ibank_Offer_Drct_Cd          AS biz_typ
        ,T.Prod_Cd                      AS tx_tools
        ,T.Due_Dt                       AS Due_Dt
        ,T.Curr_Bal                     AS remn_prin
        ,T.Org_Num                      AS fst_lvl_brch
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
FROM  f_fdm.F_agt_Cap_Offer T
WHERE etl_date = '$TXDATE'::date
;

/*数据处理区END*/

COMMIT;
