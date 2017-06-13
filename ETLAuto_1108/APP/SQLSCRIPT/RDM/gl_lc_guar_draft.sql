/*
Author             :魏银辉
Function           :信保汇（信用证 保函 汇票）
Load method        :INSERT
Source table       :f_fdm.f_agt_lc
                    f_fdm.f_fnc_Exchg_Rate
                    f_fdm.f_agt_Guarantee
                    f_fdm.f_agt_acptn_info
Destination Table  :GL_LC_GUAR_DRAFT   信保汇（信用证 保函 汇票）
Frequency          :M
Modify history list:Created by 魏银辉 at 2016-8-8 14:33:52
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
DELETE FROM f_rdm.GL_LC_GUAR_DRAFT
WHERE  etl_date= '$TXDATE'::date ;
/*数据回退区END*/

/*数据处理区*/
INSERT INTO f_rdm.GL_LC_GUAR_DRAFT(
        etl_date                --数据日期
        ,biz_id                 --业务编号
        ,biz_cate               --业务类别
        ,fst_lvl_brch           --一级分行编码
        ,applr_id               --申请人编号
        ,Due_Dt                 --到期日
        ,Cur_Cd                 --币种
        ,end_tm_orgnl_book_amt  --期末原币账面金额
        ,rmb_book_amt           --折合人民币账面金额
        ,sys_src                --来源系统
)
/*信用证*/
SELECT
        '$TXDATE'::date        AS etl_date
        ,T.Agmt_Id                      AS biz_id
        ,'1'                            AS biz_cate
        ,T.Org_Num                      AS fst_lvl_brch
        ,T.Cust_Num                     AS applr_id
        ,T.Due_Dt                       AS Due_Dt
        ,T.Cur_Cd                       AS Cur_Cd
        ,T.Lc_Bal                       AS end_tm_orgnl_book_amt
        ,T1.Exchg_Rate_Val * T.Lc_Bal   AS rmb_book_amt
        ,T.Sys_Src                      AS sys_src
FROM f_fdm.f_agt_lc T
left join f_fdm.f_fnc_Exchg_Rate T1
on T.Cur_Cd = T1.Orgnl_Cur_Cd
and T1.Convt_Cur_Cd = '156' --人民币
and T1.etl_date = '$TXDATE'::date
WHERE T.etl_date = '$TXDATE'::date
/*信用证 END*/
UNION ALL
/*保函*/
SELECT
        '$TXDATE'::date           AS etl_date
        ,T.Agmt_Id                         AS biz_id
        ,'2'                               AS biz_cate
        ,T.Org_Num                         AS fst_lvl_brch
        ,T.Cust_Num                        AS applr_id
        ,T.Due_Dt                          AS Due_Dt
        ,T.Cur_Cd                          AS Cur_Cd
        ,T.Guar_Bal                        AS end_tm_orgnl_book_amt
        ,T1.Exchg_Rate_Val * T.Guar_Bal    AS rmb_book_amt
        ,T.Sys_Src                         AS sys_src
FROM f_fdm.f_agt_Guarantee T
left join f_fdm.f_fnc_Exchg_Rate T1
on T.Cur_Cd = T1.Orgnl_Cur_Cd
and T1.Convt_Cur_Cd = '156' --人民币
and T1.etl_date = '$TXDATE'::date
WHERE T.etl_date = '$TXDATE'::date
/*保函END*/
UNION ALL
/*承兑汇票*/
SELECT
        '$TXDATE'::date           AS etl_date
        ,T.bill_num                         AS biz_id
        ,'3'                               AS biz_cate
        ,T.org_num                         AS fst_lvl_brch
        ,T.drawr_cust_id                   AS applr_id
        ,T.acpt_dt                         AS Due_Dt
        ,T.cur_cd                          AS Cur_Cd
        ,T.acpt_amt                        AS end_tm_orgnl_book_amt
        ,T.acpt_amt                        AS rmb_book_amt
        ,T.Sys_Src                         AS sys_src
FROM (SELECT T.*,ROW_NUMBER() OVER(PARTITION BY T.BILL_NUM ORDER BY T.ACCT_STAT_CD DESC) NUM FROM  f_fdm.f_agt_acptn_info T) T
WHERE T.NUM=1 AND T.etl_date = '$TXDATE'::date 
/*承兑汇票END*/
;
/*数据处理区END*/

COMMIT;
