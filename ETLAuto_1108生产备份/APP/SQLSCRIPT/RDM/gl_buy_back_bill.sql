/*
Author             :魏银辉
Function           :回购反售-票据
Load method        :INSERT
Source table       :f_fdm.f_agt_bill_discount
                    f_fdm.F_agt_Cap_Bond_Buy_Back
Destination Table  :GL_BUY_BACK_BILL 回购反售-票据
Frequency          :M
Modify history list:Created by 魏银辉 at 2016-8-9 10:02:06
                    Modified by wyh at 20160830  增加字段  币种
 		    modified by wyh at 20160831  增加字段  本金科目
                    modified by wyh at 20161011 增加月末跑批控制语句modified by wyh at 20161011 增加月末跑批控制语句
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
DELETE FROM f_rdm.GL_BUY_BACK_BILL
WHERE  etl_date= '$TXDATE'::date ;
/*数据回退区END*/

/*数据处理区*/
INSERT INTO f_rdm.GL_BUY_BACK_BILL
(
        etl_date                --数据日期
        ,tx_id                  --交易编号
        ,bill_num               --票号
        ,book_val               --账面价值
        ,TX_Cnt_Pty_Id          --交易对手编号
        ,discnt_cate            --贴现类别
        ,bill_typ               --票据类型
        ,buy_dt                 --买入日期
        ,sell_dt                --卖出日期
        ,buy_back_dt            --回购日期
        ,rtn_sell_dt            --返售日期
        ,sell_int_rate          --卖出利率
        ,buy_int_rate           --买入利率
        ,par_amt                --票面金额
        ,recv_amt               --实收金额
        ,actl_pmt_amt           --实付金额
        ,sys_src                --来源系统
        ,term                   --期限
        ,cur_cd                 --币种                 modified 20160830
        ,prin_subj              --本金科目             modified 20160831 
)
SELECT
        '$TXDATE'::date                                         AS etl_date
        ,T.agmt_id                                                       AS tx_id
        ,T.bill_id                                                       AS bill_num
        ,T.discnt_amt                                                    AS book_val
        ,T.cust_num                                                      AS TX_Cnt_Pty_Id
        ,T.prod_cd                                                       AS discnt_cate
        ,(case
                when T.prod_cd = '0310' then '1' --银承
                else '2' --商承
         end
         )                                                               AS bill_typ
        ,T.discnt_dt                                                     AS buy_dt
        ,'$MINDATE'::date                                                AS sell_dt
        ,'$MINDATE'::date                                                AS buy_back_dt
        ,T.due_dt                                                        AS rtn_sell_dt
        ,0.00                                                            AS sell_int_rate
        ,T.int_rate                                                      AS buy_int_rate
        ,T.discnt_amt                                                    AS par_amt
        ,0.00                                                            AS recv_amt
        ,T.discnt_amt - T.discnt_int                                     AS actl_pmt_amt
        ,T.sys_src                                                       AS sys_src
        ,(CASE
                WHEN T.Due_Dt IS NOT NULL AND T.discnt_dt IS NOT NULL
                    THEN T.due_dt - T.discnt_dt
                ELSE -1
         END
         )                                                               AS term
        ,t.cur_cd                                                        AS cur_cd
        ,t.prin_subj                                                     as prin_subj
FROM    f_fdm.f_agt_bill_discount T
where   T.prod_cd IN ('0310','0311')
AND     T.etl_date= '$TXDATE'::date

UNION ALL

SELECT
        '$TXDATE'::DATE                                        AS etl_date
        ,T.Agmt_Id                                                      AS tx_id
        ,T.Bond_Cd                                                      AS bill_num
        ,T.Buy_Back_Amt                                                 AS book_val
        ,T.TX_Cnt_Pty_Cust_Num                                          AS TX_Cnt_Pty_Id
        ,T.Prod_Cd                                                      AS discnt_cate
        ,'1'                                                            AS bill_typ
        ,(case
                when T.Prod_Cd='2911' then T.TX_Day
                else '$MINDATE'
         end
         )                                                              AS buy_dt
        ,(case
                when T.Prod_Cd='2910' then T.TX_Day
                else '$MINDATE'
         end
         )                                                              AS sell_dt
        ,(case
                when T.Prod_Cd='2910' then T.Due_Dt
                else '$MINDATE'
         end
         )                                                              AS buy_back_dt
        ,(case
                when T.Prod_Cd='2911' then T.Due_Dt
                else '$MINDATE'
         end
         )                                                              AS rtn_sell_dt
        ,(case
                when T.Prod_Cd='2910' then T.Exec_Int_Rate
                else 0
         end
         )                                                              AS sell_int_rate
        ,(case
                when T.Prod_Cd='2911' then T.Exec_Int_Rate
                else 0
         end
         )                                                              AS buy_int_rate
        ,T.Buy_Back_Amt                                                 AS par_amt
        ,(case
                when T.Prod_Cd='2910' then T.Buy_Back_Amt - T.Buy_Back_Int
                else 0
         end
         )                                                              AS recv_amt
        ,(case
                when T.Prod_Cd='2911' then T.Buy_Back_Amt - T.Buy_Back_Int
                else 0
         end
         )                                                              AS actl_pmt_amt
        ,T.Sys_Src                                                      AS sys_src
        ,(CASE
                WHEN T.Due_Dt IS NOT NULL AND T.St_Int_Dt IS NOT NULL
                    THEN T.Due_Dt - T.St_Int_Dt
                ELSE -1
         END
         )                                                              AS term
         ,t.cur_cd                                                      AS cur_cd
         ,t.prin_subj                                                   as prin_subj
FROM f_fdm.F_agt_Cap_Bond_Buy_Back T
where T.prod_cd IN ('2910','2911')
AND T.etl_date= '$TXDATE'::date

;

/*数据处理区END*/

COMMIT;
