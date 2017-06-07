/*
Author             :魏银辉
Function           :贷款借据
Load method        :INSERT
Source table       :f_fdm.f_loan_corp_dubil_info
                    f_fdm-f_loan_indv_dubil
                    f_fdm.f_agt_asst_consvs
Destination Table  :GL_LOAN_DUBIL   贷款借据
Frequency          :M
Modify history list:Created by 魏银辉 at 2016-8-8 14:33:52
                    modified by wyh at 20161011 增加月末跑批控制语句
                    modified by wyh 20161013 修改t1表的约束条件
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

DROP TABLE IF EXISTS GL_LOAN_DUBIL_tmp_t2 cascade;

create local temporary table if not exists GL_LOAN_DUBIL_tmp_t2             --本金科目、利息科目、投资收益科目及对应的金额
on commit preserve rows as
SELECT
        org_cd      as level1_org_cd
        ,org_cd     as org_cd
FROM f_fdm.f_org_info
WHERE org_hrcy = '01'
and etl_date = '$TXDATE'::date
UNION ALL
SELECT
        up_org_cd   as level1_org_cd
        ,org_cd     as org_cd
FROM f_fdm.f_org_info
WHERE org_hrcy = '02'
and etl_date = '$TXDATE'::date
UNION ALL
SELECT
        T1.up_org_cd as level1_org_cd
        ,T.org_cd    as org_cd
FROM f_fdm.f_org_info T
LEFT JOIN f_fdm.f_org_info T1
ON T.up_org_cd = T1.org_cd
WHERE T.org_hrcy = '03'
and T.etl_date = '$TXDATE'::date
UNION ALL
SELECT
        T2.up_org_cd  as level1_org_cd
        ,T.org_cd     as org_cd
FROM f_fdm.f_org_info T
LEFT JOIN f_fdm.f_org_info T1
ON T.up_org_cd = T1.org_cd
LEFT JOIN f_fdm.f_org_info T2
ON T1.up_org_cd = T2.org_cd
WHERE T.org_hrcy = '04'
and T.etl_date = '$TXDATE'::date
;
/*临时表创建区END*/

/*数据回退区*/
DELETE FROM f_rdm.GL_LOAN_DUBIL
WHERE  etl_date= '$TXDATE'::date ;
/*数据回退区END*/

/*数据处理区*/
INSERT INTO f_rdm.GL_LOAN_DUBIL
(
        etl_date                            --数据日期
        ,contr_id                           --合同编号
        ,dubil_id                           --借据编号
        ,fst_lvl_brch                       --一级分行编码
        ,Cust_Id                            --客户编号
        ,loan_bal                           --贷款余额
        ,due_dt                             --到期日期
        ,is_nt_ovrd                         --是否逾期
        ,prin_ovrd_days                     --本金逾期天数
        ,int_ovrd_days                      --利息逾期天数
        ,Wrtoff_Dt                          --核销日期
        ,Wrtoff_Prin                        --核销本金
        ,wrtoff_in_bal_int                  --核销表内利息
        ,wrtoff_norm_int                    --核销正常利息
        ,wrtoff_arr_int                     --核销拖欠息
        ,wrtoff_pnsh_int                    --核销罚息
        ,wrtoff_post_retr_dt                --核销后收回日期
        ,wrtoff_post_retr_prin              --核销后收回本金
        ,wrtoff_post_retr_int               --核销后收回利息
        ,wrtoff_post_retr_accrd_int         --核销后收回应计利息
        ,Fiv_Cls                            --五级分类
        ,resv_provs_amt                     --准备金计提金额
        ,Cur_Cd                             --币种
        ,Next_Rprc_Day                      --下次重定价日
        ,loan_typ                           --贷款种类
        ,sys_src                            --来源系统
)
SELECT
        '$TXDATE'::date            AS etl_date
        ,T.Contr_Agmt_Id                    AS contr_id
        ,T.Agmt_Id                          AS dubil_id
        ,T2.level1_org_cd                   AS fst_lvl_brch
        ,T.Cust_Num                         AS Cust_Id
        ,T.Curr_Bal                         AS loan_bal
        ,T.Due_Dt                           AS due_dt
        ,(CASE
                WHEN T.Ovrd_Days = 0 THEN '否'
                ELSE '是'
         END
         )                                  AS is_nt_ovrd
        ,T.Ovrd_Days                        AS prin_ovrd_days
        ,0                                  AS int_ovrd_days
        ,T.Wrtoff_Dt                        AS Wrtoff_Dt
        ,T.Wrtoff_Prin                      AS Wrtoff_Prin
        ,T1.wrtoff_in_bal_int               AS wrtoff_in_bal_int
        ,T1.wrtoff_norm_int                 AS wrtoff_norm_int
        ,T1.wrtoff_arr_int                  AS wrtoff_arr_int
        ,T1.wrtoff_pnsh_int                 AS wrtoff_pnsh_int
        ,T1.wrtoff_post_retr_dt             AS wrtoff_post_retr_dt
        ,T1.wrtoff_post_retr_prin           AS wrtoff_post_retr_prin
        ,T1.wrtoff_post_retr_int            AS wrtoff_post_retr_int
        ,T1.wrtoff_post_retr_accrd_int      AS wrtoff_post_retr_accrd_int
        ,T.Fiv_Cls                          AS Fiv_Cls
        ,T.Loan_Deval_Prep_Bal              AS resv_provs_amt
        ,T.Cur_Cd                           AS Cur_Cd
        ,T.Next_Rprc_Day                    AS Next_Rprc_Day
        ,T.Prod_Cd                          AS loan_typ
        ,T.Sys_Src                          AS sys_src
FROM    f_fdm.f_loan_corp_dubil_info T
left join  (select
                    orgnl_agmt_id
                    ,sum(wrtoff_in_bal_int)             AS wrtoff_in_bal_int
                    ,sum(wrtoff_norm_int)               AS wrtoff_norm_int
                    ,sum(wrtoff_arr_int)                AS wrtoff_arr_int
                    ,sum(wrtoff_pnsh_int)               AS wrtoff_pnsh_int
                    ,MAX(wrtoff_post_retr_dt)           AS wrtoff_post_retr_dt
                    ,sum(wrtoff_post_retr_prin)         AS wrtoff_post_retr_prin
                    ,sum(wrtoff_post_retr_int)          AS wrtoff_post_retr_int
                    ,sum(wrtoff_post_retr_accrd_int)    AS wrtoff_post_retr_accrd_int
            from f_fdm.f_agt_asst_consv
            where sys_src = 'CCS'
            AND SUBSTR(to_char(wrtoff_post_retr_dt,'YYYYMMDD'),1,6) = SUBSTR('$TXDATE',1,6)               --modified by wyh 20161013
            and etl_date = '$TXDATE'::date
            GROUP BY orgnl_agmt_id
           ) T1
on T.Agmt_Id = T1.orgnl_agmt_id
LEFT JOIN GL_LOAN_DUBIL_tmp_t2 T2
ON T.Org_Num = T2.org_cd
WHERE T.etl_date = '$TXDATE'::date
UNION ALL
SELECT
        '$TXDATE'::date            AS etl_date
        ,T.Contr_Agmt_Id                    AS contr_id
        ,T.Agmt_Id                          AS dubil_id
        ,T.Org_Num                          AS fst_lvl_brch
        ,T.Cust_Num                         AS Cust_Id
        ,T.Curr_Bal                         AS loan_bal
        ,T.Due_Dt                           AS due_dt
        ,(CASE
                WHEN T.Ovrd_Days = 0 THEN '否'
                ELSE '是'
         END
         )                                  AS is_nt_ovrd
        ,T.Ovrd_Days                        AS prin_ovrd_days
        ,0                                  AS int_ovrd_days
        ,T.Wrtoff_Dt                        AS Wrtoff_Dt
        ,T.Wrtoff_Prin                      AS Wrtoff_Prin
        ,T1.wrtoff_in_bal_int               AS wrtoff_in_bal_int
        ,T1.wrtoff_norm_int                 AS wrtoff_norm_int
        ,T1.wrtoff_arr_int                  AS wrtoff_arr_int
        ,T1.wrtoff_pnsh_int                 AS wrtoff_pnsh_int
        ,T1.wrtoff_post_retr_dt             AS wrtoff_post_retr_dt
        ,T1.wrtoff_post_retr_prin           AS wrtoff_post_retr_prin
        ,T1.wrtoff_post_retr_int            AS wrtoff_post_retr_int
        ,T1.wrtoff_post_retr_accrd_int      AS wrtoff_post_retr_accrd_int
        ,T.Fiv_Cls                          AS Fiv_Cls
        ,T.Loan_Deval_Prep_Bal              AS resv_provs_amt
        ,T.Cur_Cd                           AS Cur_Cd
        ,T.Next_Rprc_Day                    AS Next_Rprc_Day
        ,T.Prod_Cd                          AS loan_typ
        ,T.Sys_Src                          AS sys_src
FROM     f_fdm.f_loan_indv_dubil T
left join   (select
                    orgnl_agmt_id
                    ,sum(wrtoff_in_bal_int)             AS wrtoff_in_bal_int
                    ,sum(wrtoff_norm_int)               AS wrtoff_norm_int
                    ,sum(wrtoff_arr_int)                AS wrtoff_arr_int
                    ,sum(wrtoff_pnsh_int)               AS wrtoff_pnsh_int
                    ,MAX(wrtoff_post_retr_dt)           AS wrtoff_post_retr_dt
                    ,sum(wrtoff_post_retr_prin)         AS wrtoff_post_retr_prin
                    ,sum(wrtoff_post_retr_int)          AS wrtoff_post_retr_int
                    ,sum(wrtoff_post_retr_accrd_int)    AS wrtoff_post_retr_accrd_int
            from f_fdm.f_agt_asst_consv
            where sys_src = 'PCS'
            AND SUBSTR(to_char(wrtoff_post_retr_dt,'YYYYMMDD'),1,6) = SUBSTR('$TXDATE',1,6)               --modified by wyh 20161013
            and etl_date = '$TXDATE'::date
            GROUP BY orgnl_agmt_id
           ) T1
on T.Agmt_Id = T1.orgnl_agmt_id
WHERE T.etl_date = '$TXDATE'::date
;

/*数据处理区END*/

COMMIT;
