/*
Author             :dhy
Function           :个人存款账户信息表
Load method        :INSERT
Source table       :DW_SDATA.LCS_000_PK_CBK_BASE,DW_SDATA.LCS_000_PK_CBK_SUBACC_CLS,DW_SDATA.LCS_000_CARD_SC_BASE,DW_SDATA.LCS_000_CARD_SC_ACCTYPE,DW_SDATA.LCS_000_CARD_SC_CLS,DW_SDATA.LCS_000_CARD_OC_BASE,DW_SDATA.LCS_000_CARD_OC_ACCTYPE,DW_SDATA.LCS_000_CARD_OC_SUBACC_CLS,DW_SDATA.LCS_000_CARD_PUB_PAPER,DW_SDATA.LCS_000_PK_CBK_PAPER,DW_SDATA.ACC_000_T_INT_PRSN_CDM_DTL,DW_SDATA.LCS_000_QRY_CDM_DTL,DW_SDATA.LCS_000_CARD_CDM_LEG,DW_SDATA.LCS_000_PK_CDM_PAPERD,DW_SDATA.ACC_003_T_ACC_CDM_LEDGER,DW_SDATA.LCS_000_CARD_PUB_SVT,DW_SDATA.LCS_000_T_PUB_ITMRATE,DW_SDATA.LCS_000_PARA_RATE,DW_SDATA.LCS_000_SRM_RATE,DW_SDATA.ACC_003_T_ACCDATA_LAST_ITEM_NO,DW_SDATA.LCS_000_CARD_FC_CDM_LEG,DW_SDATA.ACC_003_T_ACC_CDM_FC_LEDGER,DW_SDATA.LCS_000_T_PUB_ITMRATE,DW_SDATA.ACC_003_T_INT_PRSN_CDM_FC_DTL,DW_SDATA.LCS_000_CARD_PUB_FC_SVT,DW_SDATA.LCS_000_PK_FIX_LEG,DW_SDATA.ACC_003_T_ACC_FIX_LEDGER,DW_SDATA.ACC_003_T_INT_PRSN_FIX_DTL,DW_SDATA.LCS_000_TSF_FIX_INT,DW_SDATA.LCS_000_PK_FIX_INT,DW_SDATA.LCS_000_PK_FC_FIX_LEG,DW_SDATA.ACC_003_T_ACC_FIX_FC_LEDGER,DW_SDATA.LCS_000_TSF_FC_FIX_INT,DW_SDATA.LCS_000_PK_FC_FIX_INT,DW_SDATA.LCS_000_PK_FC_FIX_INT,DW_SDATA.ACC_003_T_INT_PRSN_FIX_FC_DTL
Frequency          :D
Modify history list:Created by
                   :Modify  by liuxz 20160714 acc_003_T_INT_PRSN_CDM_DTL  改为 acc_000_T_INT_PRSN_CDM_DTL 
                   :modified by wyh 20160902 添加月积数计算规则
                   :Modify by gln 20160907  修改第一组，对主表进行去重 
                   :MOdify by wyh 20160925 修改lcs_000_pub_itmrate的逻辑,解决主键重复问题
                   :modify by zhangliang 修改组3组4   t3表，以及t5增加日期限制i
                    modified by wyh at 20160930 组别3去除PK逻辑，所有组别利率加拉链
                    modified by zhangliang 20161008 组1,2,3,4增加执行利率的取数规则以及关联表lcs_000_pk_savrate_reg，lcs_000_pk_subcal_reg
                    modified by zhangliang 20161013   将$MINDATE中的-2修改为-1
                    modified by wyh at 20161014  1.修改了组别1,2储种06的科目号、余额的取值规则
                                                 2.修改了组别3  T13的去重复逻辑;ROW_NUMBER()OVER(PARTITION BY A.acc,A.BGN_DATE ORDER BY A.rate DESC)
                    modified by wyh at 20161019 修改ROW_NUMBER逻辑:order by UPDATED_TS desc,last_updated_ts desc
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
个人存款账户信息表 是由 活期本币 活期外币 定期本币 定期外币 四种情况加工而成。
*-------------------------------------------逻辑说明END------------------------------------------
*/

/*数据回退区*/
delete from  f_fdm.F_DPST_INDV_ACCT where etl_date = '$TXDATE'::DATE;
/*数据回退区end*/
/*临时表*/
--- 个人活期本币账户去重
create local temporary table IF NOT EXISTS card_cdm_leg_tmp
 on commit preserve rows as
select * from 
 (
 select 
        acc,
         bgn_int_date,
         SVT_NO,
         b_flag,
         bal,
        row_number()over(partition by acc order by bgn_int_date desc  ) rn 
   from dw_sdata.lcs_000_card_cdm_leg 
  where start_dt<='$TXDATE'::date and '$TXDATE'::date<end_dt 
  )  t where rn = 1
order by acc
segmented by hash(acc) ALL NODES  
;
-----定期分户登记薄（本币）去重处理                                        modified by wyh at 20160930
create local temporary table IF NOT EXISTS pk_fix_leg_tmp
 on commit preserve rows as
select  * from
 (
 select
        *,
        row_number()over(partition by acc order by bgn_int_date desc,bal asc ) rn
   from  dw_sdata.lcs_000_pk_fix_leg  -- 定期分户登记薄（本币）
  where start_dt<='$TXDATE'::date and '$TXDATE'::date<end_dt
  )  t where rn = 1
order by acc
segmented by hash(acc) ALL NODES
;

--- 账号与介质号 对应关系
create local temporary table IF NOT EXISTS TT_acct_med_rel 
  on commit preserve rows as
 select A.sign_id as sign_id ,
        coalesce(b.ecif_cust_no,c.ecif_cust_no) as ecif_cust_no
  from (select party_id,
               sign_id,
               row_number()over(partition by sign_id order by updated_ts desc, last_updated_ts desc) rn 
           from dw_sdata.ecf_001_t02_cust_acct_rel               --modified by wyh at 20161019 
          where start_dt <='$TXDATE'
            and end_dt > '$TXDATE'   
        ) A 
  left join (select party_id,
                    ecif_cust_no,
                    row_number()over(partition by ecif_cust_no order by updated_ts desc, last_updated_ts desc)rn 
               from dw_sdata.ecf_001_t01_cust_info 
              where --updated_ts = '99991231 00:00:00:000000' --modified by wyh at 20161019 
 
                --and 
                start_dt <='$TXDATE'
                and end_dt > '$TXDATE'
             ) B
    on a.party_id = b.party_id
   and b.rn = 1
  left join (select party_id ,
                    ecif_cust_no,
                    row_number()over(partition by ecif_cust_no order by updated_ts desc, last_updated_ts desc) rn
                 from dw_sdata.ecf_004_t01_cust_info
                where --updated_ts = '99991231 00:00:00:000000' --modified by wyh at 20161019 
                  --and 
                  start_dt <='$TXDATE'
                  and end_dt > '$TXDATE'
             ) C
    on a.party_id = c.party_id
   and c.rn = 1 
 where a.rn = 1 
order by sign_id
segmented by hash(sign_id) ALL NODES  
;
--- 账号与介质号 对应关系,组别2,4,处理外币
create local temporary table IF NOT EXISTS TT_acct_med_rel_grp24
  on commit preserve rows as
select * from TT_acct_med_rel t
where exists ( select 1 from (
  select acc 
  from dw_sdata.lcs_000_card_fc_cdm_leg T      --- 活期分户登记表（外币）
  where T.start_dt<='$TXDATE'::DATE 
  and '$TXDATE'::DATE <T.end_dt 
  union
  select acc 
  from dw_sdata.lcs_000_pk_fc_fix_leg   T -- 定期分户登记簿（外币）
  where T.start_dt<='$TXDATE'::DATE
  and '$TXDATE'::DATE <T.end_dt
  )  a where t.sign_id=a.acc ) 
order by sign_id
segmented by hash(sign_id) ALL NODES
;
--- 账号与介质号 对应关系,组别3,处理本币定期
create local temporary table IF NOT EXISTS TT_acct_med_rel_grp3
  on commit preserve rows as
select * from TT_acct_med_rel t
where exists ( select 1 from pk_fix_leg_tmp a where t.sign_id=a.acc) --定期分户登记薄（本币）
order by sign_id
segmented by hash(sign_id) ALL NODES
;

-- 活期计息
create local temporary table IF NOT EXISTS TT_ACC_provs_Int 
  on commit preserve rows as
select ACC,  
       sum(case when ACC_DATE = '$TXDATE' then TRAN_AMT else 0 end ) as TRAN_AMT_D  -- 日
       ,sum(case when substr(ACC_DATE,1,6) = substr('$TXDATE',1,6) and  ACC_DATE <= '$TXDATE'   then TRAN_AMT else 0 end ) as TRAN_AMT_M  -- 月
      -- ,sum(case when  etl_dt<='$TXDATE'   then TRAN_AMT  else 0 end ) TRAN_AMT_SUM --累积
  from dw_sdata.acc_000_T_INT_PRSN_CDM_DTL 
 group by ACC
order by acc
segmented by hash(acc) ALL NODES
;
 -- 活期付息
create local temporary table IF NOT EXISTS TT_ACC_Paid_Int 
  on commit preserve rows as
 SELECT ACC,
        SUM(CASE WHEN ('$MINDATE'::date + tran_date-1)::date  = '$TXDATE'::date THEN amt ELSE 0 END)Today_Int_Pay ,
        SUM(CASE WHEN month(('$MINDATE'::date + tran_date-1)::date )=month('$TXDATE'::date) and  ('$MINDATE'::date + tran_date-1)::date<='$TXDATE'::date  THEN amt else 0 end) CurMth_Paid_Int  
        --SUM(case when etl_dt<='$TXDATE' then amt else 0 end) Accm_Paid_Int
  FROM DW_SDATA.lcs_000_qry_cdm_dtl  --活期交易明细表
 WHERE amt_type in ('3', '4')  --  结息利息
   AND N_TRAN_TYPE = '0' -- 交易类型 正常0
 GROUP BY 1
order by acc
segmented by hash(acc) ALL NODES
;
 -- 活期付息,组别2外币
create local temporary table IF NOT EXISTS TT_ACC_Paid_Int_grp2
  on commit preserve rows as 
select * from TT_ACC_Paid_Int t
where exists  ( select 1 from dw_sdata.lcs_000_card_fc_cdm_leg cdm 
                 where  cdm.start_dt<='$TXDATE'::DATE
                  and   '$TXDATE'::DATE <cdm.end_dt
                  and   cdm.acc = t.acc)       --- 活期分户登记表（外币）
order by  acc
segmented by hash(acc) ALL NODES
;
 -- 获取利率
create local temporary table IF NOT EXISTS TT_pk_savrate_reg
  on commit preserve rows as
select acc, BGN_DATE, rate
  from (select A.acc,
               A.BGN_DATE,
               A.rate,
               ROW_NUMBER() OVER(PARTITION BY A.acc ORDER BY A.BGN_DATE, A.rate DESC) AS Rn
          from dw_sdata.lcs_000_pk_savrate_reg A
          where start_dt <= '$TXDATE' ::date
            and '$TXDATE'::date < end_dt
           and ('$MINDATE' ::date + bgn_date - 1)::date <= '$TXDATE' ::date) T
where T.Rn = 1
order by  acc
segmented by hash(acc) ALL NODES
;
-- 获取利率,外币利率2,4
create local temporary table IF NOT EXISTS TT_pk_savrate_reg_grp24
on commit preserve rows as
select *
  from TT_pk_savrate_reg t 
 where exists
  ( select 1 from (
  select acc
  from dw_sdata.lcs_000_card_fc_cdm_leg T      --- 活期分户登记表（外币）
  WHERE T.start_dt<='$TXDATE'::DATE
   AND '$TXDATE'::DATE <T.end_dt
  union
  select acc
  from dw_sdata.lcs_000_pk_fc_fix_leg   T -- 定期分户登记簿（外币）
  WHERE T.start_dt<='$TXDATE'::DATE
   AND '$TXDATE'::DATE <T.end_dt
  )  a where t.acc=a.acc )
order by acc
segmented by hash(acc) ALL NODES
;
-- 获取利率,人民币定期利率3组
create local temporary table IF NOT EXISTS TT_pk_savrate_reg_grp3
  on commit preserve rows as
select * 
  from TT_pk_savrate_reg t
where exists ( select 1 from pk_fix_leg_tmp a where t.acc=a.acc) --定期分户登记薄（本币）
order by acc
segmented by hash(acc) ALL NODES
;
  
/*临时表 end*/
/*数据处理区*/
  
  -- 本币 活期
insert into f_fdm.F_DPST_INDV_ACCT
(etl_date
,grp_typ
,Agmt_Id
,Cust_Num
,Org_Num
,Cur_Cd
,Prod_Cd
,dpst_cate_cd
,Cash_Ind_Cd
,St_Int_Dt
,Due_Dt
,Open_Acct_Day
,Clos_Acct_Day
,Exec_Int_Rate
,Bmk_Int_Rate
,Basis
,Int_Base_Cd
,Cmpd_Int_Calc_Mode_Cd
,Is_Nt_Int_Ind
,Pre_Chrg_Int
,Int_Rate_Attr_Cd
,Orgnl_Term
,Orgnl_Term_Corp_Cd
,Rprc_Prd
,Rprc_Prd_Corp_Cd
,Last_Rprc_Day
,Next_Rprc_Day
,Is_AutoRnw
,Last_AutoRnw_Dt
,Agmt_Stat_Cd
,Prin_Subj
,Curr_Bal
,Int_Subj
,Today_Provs_Int
,CurMth_Provs_Int
,Accm_Provs_Int
,Today_Int_Pay
,CurMth_Paid_Int
,Accm_Paid_Int
,Int_Adj_Amt
,Mth_Accm
,Yr_Accm
,Mth_Day_Avg_Bal
,Yr_Day_Avg_Bal
,Sys_Src
 )
select '$TXDATE'::date,
       1,
       T.acc,
       --coalesce(a.cust_num,d.cstm_no),
       coalesce(t1.ecif_cust_no,''),
       coalesce(T2.OP_INST,''),
       '156',
       T.SVT_NO,
       coalesce(T3.cal_flag,''),
       '',
       ('$MINDATE'::date+T.BGN_INT_DATE-1)::date,
       '$MAXDATE' :: date ,
       coalesce(T2.OP_DATE::date,'$MINDATE' :: date ),
       coalesce(T2.CLS_DATE::date,'$MINDATE' :: date ),
       CASE WHEN substr(to_char(T.b_flag),5,1) ='1' THEN coalesce(T9.rate,0)
            WHEN substr(to_char(T.b_flag),6,1) ='1' OR   substr(to_char(T.b_flag),7,1) ='1' THEN coalesce(T10.due_rate,0)
       ELSE  coalesce(T11.rate_val,0)
       END,
       0.00,
       0.00,
       CASE WHEN SUBSTR(T3.INT_CAL_FLAG,2,1) ='0' THEN '1' --实际/360
            WHEN SUBSTR(T3.INT_CAL_FLAG,2,1) ='1' THEN '10' --30/360
       ELSE '@'||T3.INT_CAL_FLAG
       END , --360计算利率
       '1', --单利
       '1', --是
       '0', --后收
       '4', --不定期
       0,
       '',
       0,
       '',
        coalesce(('$MINDATE'::date+T5.bgn_date-1)::date,'$MINDATE' :: date ),
       '$TXDATE' ::date + 1,
       '',
       '$MINDATE' :: date ,
       --substr(TO_BITSTRING(HEX_TO_BINARY(TO_HEX(T.B_FLAG::integer))),length(TO_BITSTRING(HEX_TO_BINARY(TO_HEX(T.B_FLAG::integer))))), -- 注意：此字段要先转为二进制，然后从右边起，截取右边第一位，即为状态标识。,
       to_char(mod(t.b_flag,2)),
       (case 
             when T.SVT_NO = '06' then '262556'
             else coalesce(T2.ITM_NO,'')
             end
       ),                                --modified by wyh at 20161014
       --coalesce(T2.BAL,t.bal),           --modified by wyh at 20161014
       (case                  
             when T.SVT_NO = '06' then t.bal
             else coalesce(T2.bal,0)
             end
       ),      -- xsh modify 20161015 
       coalesce(T6.ITM_NO,''),
       coalesce(T7.TRAN_AMT_D,0),
       coalesce(T7.TRAN_AMT_M,0),
       0,
       coalesce(T8.Today_Int_Pay,0),
       coalesce(T8.CurMth_Paid_Int,0),
       0,
       0,
      (case
            when '$TXDATE'= '$MONTHBGNDAY'
            then (case  
             when T.SVT_NO = '06' then t.bal
             else coalesce(T2.bal,0)
             end )
            else (case  
             when T.SVT_NO = '06' then t.bal
             else coalesce(T2.bal,0)
             end ) +coalesce(T_yjs.mth_accm,0)
            end
       )                                                                      as mth_accm  --月积攼0
      ,(case
            when  '$TXDATE' = '$YEARBGNDAY'
            then (case  
             when T.SVT_NO = '06' then t.bal
             else coalesce(T2.bal,0)
             end )
            else (case  
             when T.SVT_NO = '06' then t.bal
             else coalesce(T2.bal,0)
             end )+coalesce(T_yjs.yr_accm,0)
            end
       )                                                                      as yr_accm   --年积攼0
      ,(case
            when '$TXDATE' = '$MONTHBGNDAY'
            then (case  
             when T.SVT_NO = '06' then t.bal
             else coalesce(T2.bal,0)
             end )
            else (case  
             when T.SVT_NO = '06' then t.bal
             else coalesce(T2.bal,0)
             end )+coalesce(T_yjs.mth_accm,0)
            end
       )/('$TXDATE'::date-'$MONTHBGNDAY'::date+1)               as mth_day_avg_bal  --月日均余顾]
      ,(case
           when '$TXDATE' = '$YEARBGNDAY'
           then (case  
             when T.SVT_NO = '06' then t.bal
             else coalesce(T2.bal,0)
             end )
           else (case  
             when T.SVT_NO = '06' then t.bal
             else coalesce(T2.bal,0)
             end )+coalesce(T_yjs.yr_accm,0)
           end
       )/('$TXDATE'::date-'$YEARBGNDAY'::date+1)                    as Yr_Day_Avg_Bal   --年日均余顾]
       ,'LCS'
  from card_cdm_leg_tmp t    --- 活期分户登记表（本币）  
   left join TT_acct_med_rel t1
     on t.acc = t1.sign_id  
  left join dw_sdata.acc_003_t_acc_cdm_ledger T2 ---负债类储蓄本币账户活期分户账
    ON T.ACC = T2.ACC
   and T2.SYS_CODE = '99700010000'
   and T2.start_dt <='$TXDATE'::date
   and T2.end_dt > '$TXDATE' ::date
  left join dw_sdata.lcs_000_card_pub_svt T3 ------储种表 
    ON T.SVT_NO =T3.SVT_NO 
   and T3.start_dt <='$TXDATE'::date
   and T3.end_dt > '$TXDATE' ::Date
 /* left join (select distinct t.ITM_NO,t.rate_no,t.CURR_TYPE,t.RATE_KIND FROM dw_sdata.lcs_000_pub_itmrate t)T4 ---科目利率对照表
    ON T.SVT_NO=T4.ITM_NO
   AND T4.CURR_TYPE='156'
   And T4.RATE_KIND ='0'*/
 left join (
select * from (
select t.ITM_NO
--,t.ITM_NAME
,t.CURR_TYPE
,t.RATE_KIND
,t.RATE_NO
--,t.RATE_LEV
,row_number()over(partition by t.ITM_NO,t.CURR_TYPE,t.RATE_KIND order by t.RATE_LEV desc) NUM
FROM dw_sdata.lcs_000_pub_itmrate t
where T.start_dt <='$TXDATE'::Date
   and T.end_dt > '$TXDATE'::Date
)t
where NUM = 1
)T4 ---科目利率对照表
    ON T.SVT_NO=T4.ITM_NO
   AND T4.CURR_TYPE='156'                                                     
   And T4.RATE_KIND ='0'
  left join  (SELECT A. RATE_NO, A. bgn_date , A.RATE_VAL FROM  dw_sdata.lcs_000_srm_rate  A 
              INNER JOIN
             (SELECT RATE_NO,MAX(bgn_date) AS bgn_date  FROM  dw_sdata.lcs_000_srm_rate
              where  ('$MINDATE'::date + bgn_date-1)::date  <'$TXDATE'::date GROUP  BY RATE_NO  ) B
              ON  A. RATE_NO=B. RATE_NO
              and A. bgn_date =B. bgn_date ) T5 ---利率表
    ON T4.RATE_NO=T5.RATE_NO 
 
  left join dw_sdata.acc_003_t_accdata_last_item_no T6 --科目转换表
    ON T2.ITM_NO = T6.AMT_ITM
   AND T6.FIRST_ITM = '20' --负债类利率支出  
   and T6.start_dt <='$TXDATE'::Date
   and T6.end_dt > '$TXDATE'::Date   
  left join TT_ACC_provs_Int T7 --计提利息
    ON T.ACC = T7.ACC 
  left join TT_ACC_Paid_Int T8 -- 付息
    ON T.ACC = T8.ACC 
left join TT_pk_savrate_reg t9 --利率
/*  (select A.acc,A.BGN_DATE,A.rate from  dw_sdata.lcs_000_pk_savrate_reg A
inner join
           (select  acc,max(BGN_DATE)  as BGN_DATE FROM dw_sdata.lcs_000_pk_savrate_reg
               where   start_dt<='$TXDATE'::date
               and '$TXDATE'::date < end_dt
               and ('$MINDATE'::date+bgn_date-1)::date<='$TXDATE'::date
              group by 1)  B
          on  A.acc=B.acc
          and A.BGN_DATE=B.BGN_DATE
          where   A.start_dt<='$TXDATE'::date
          and '$TXDATE'::date <  A.end_dt) T9
*/
 ON T.acc=T9.acc
left join dw_sdata.lcs_000_pk_subcal_reg T10
       ON T.acc=T10.acc
      and T10.start_dt<='$TXDATE'::date
      and '$TXDATE'::date <T10.end_dt
left join  (SELECT A. RATE_NO, A. bgn_date , A.RATE_VAL FROM  dw_sdata.lcs_000_srm_rate  A 
              INNER JOIN
             (SELECT RATE_NO,MAX(bgn_date) AS bgn_date  FROM  dw_sdata.lcs_000_srm_rate
              where  ('$MINDATE'::date + bgn_date-1)::date  <='$TXDATE'::date GROUP  BY RATE_NO  ) B
              ON  A. RATE_NO=B. RATE_NO
              and A. bgn_date =B. bgn_date ) T11 ---利率表
 ON T4.RATE_NO=T11.RATE_NO 
left join f_fdm.F_DPST_INDV_ACCT T_yjs
on         t.acc= T_yjs.agmt_id
and  T_yjs.etl_date='$TXDATE'::date-1
 ;


-- 外币 活期 
insert into f_fdm.F_DPST_INDV_ACCT
(etl_date
,grp_typ
,Agmt_Id
,Cust_Num
,Org_Num
,Cur_Cd
,Prod_Cd
,dpst_cate_cd
,Cash_Ind_Cd
,St_Int_Dt
,Due_Dt
,Open_Acct_Day
,Clos_Acct_Day
,Exec_Int_Rate
,Bmk_Int_Rate
,Basis
,Int_Base_Cd
,Cmpd_Int_Calc_Mode_Cd
,Is_Nt_Int_Ind
,Pre_Chrg_Int
,Int_Rate_Attr_Cd
,Orgnl_Term
,Orgnl_Term_Corp_Cd
,Rprc_Prd
,Rprc_Prd_Corp_Cd
,Last_Rprc_Day
,Next_Rprc_Day
,Is_AutoRnw
,Last_AutoRnw_Dt
,Agmt_Stat_Cd
,Prin_Subj
,Curr_Bal
,Int_Subj
,Today_Provs_Int
,CurMth_Provs_Int
,Accm_Provs_Int
,Today_Int_Pay
,CurMth_Paid_Int
,Accm_Paid_Int
,Int_Adj_Amt
,Mth_Accm
,Yr_Accm
,Mth_Day_Avg_Bal
,Yr_Day_Avg_Bal
,Sys_Src
 )
select 
    '$TXDATE'::date, 
     2,
    T.ACC,                           -- 协议编号
    coalesce(t1.ecif_cust_no,''),    -- 客户号
    coalesce(T2.ACC_INST,''),        -- 核算机构代码
    T.CURR_TYPE,                     -- 币种
    T.SVT_NO,                        -- 储种
    coalesce(T14.cal_flag,''),       -- 存款类别代码
    T.CH_TYPE,                       -- 资金形态 
    ('$MINDATE'::date+T.BGN_INT_DATE-1)::date,      -- 起息日
    '$MAXDATE' :: date ,                        -- 到期日
    coalesce(T2.OP_DATE::date,'$MINDATE' :: date ),          -- 开户日期
    coalesce(T2.CLS_DATE::date,'$MINDATE' :: date ),         -- 销户日期 
    CASE WHEN substr(to_char(T.b_flag),5,1) ='1' THEN coalesce(T15.rate,0)
         WHEN substr(to_char(T.b_flag),6,1) ='1' OR   substr(to_char(T.b_flag),7,1) ='1' THEN coalesce(T16.due_rate,0)
         ELSE  coalesce(T17.rate_val,0)
         END,        -- 执行利率
    0.00,                -- 基准利率
    0.00,                -- 基差
    CASE WHEN SUBSTR(T14.INT_CAL_FLAG,2,1) ='0' THEN '1' --实际/360
         WHEN SUBSTR(T14.INT_CAL_FLAG,2,1) ='1' THEN '10' --30/360
    ELSE  '@'||T14.INT_CAL_FLAG 
    END,                  -- 计息基差代码
    '1',                  -- 复利计算方式代码 默认单利
    '1',                  -- 是否计息标志   是
    '0',                  -- 是否先收息 后收
    '4',                  -- 利率属性代码 不定期
    0,                    -- 原始期限
    '',                   -- 原始期限单位代码
    0,                    -- 重定价周期
    '',                   -- 重定价周期单位代码
    coalesce(('$MINDATE'::date+T13.bgn_date-1)::date ,'$MINDATE' :: date ),        -- 上次重定价日
    '$TXDATE'::date + 1,                                         -- 下次重定价日
    '',                   -- 是否自动转存
    '$MINDATE' :: date ,        -- 上次自动转存日期 默认空
    to_char(mod(t.b_flag,2)),   -- 此字段要先转为二进制，然后从右边起，截取右边第一位，即为状态标识。
    (case 
             when T.SVT_NO = '06' then '262556'
             else coalesce(T2.ITM_NO,'')
             end
    ),                                  --modified by wyh at 20161014                 
    --coalesce(T2.BAL,T.BAL),             --modified by wyh at 20161014
       (case                  
             when T.SVT_NO = '06' then t.bal
             else coalesce(T2.bal,0)
             end
       ),      -- xsh modify 20161015 
    coalesce(T6.ITM_NO,''),
    coalesce(T7.TRAN_AMT_D,0),
    coalesce(T7.TRAN_AMT_M,0),
    0,
    coalesce(T8.Today_Int_Pay,0),
    coalesce(T8.CurMth_Paid_Int,0),
    0,
    0, -- 默认0
    (case 
            when '$TXDATE'= '$MONTHBGNDAY' 
            then (case   
             when T.SVT_NO = '06' then t.bal 
             else coalesce(T2.bal,0) 
             end ) 
            else (case   
             when T.SVT_NO = '06' then t.bal 
             else coalesce(T2.bal,0) 
             end ) +coalesce(T_yjs.mth_accm,0) 
            end 
       )                                                                      as mth_accm  --月积攼0 
      ,(case 
            when  '$TXDATE' = '$YEARBGNDAY' 
            then (case   
             when T.SVT_NO = '06' then t.bal 
             else coalesce(T2.bal,0) 
             end ) 
            else (case   
             when T.SVT_NO = '06' then t.bal 
             else coalesce(T2.bal,0) 
             end )+coalesce(T_yjs.yr_accm,0) 
            end 
       )                                                                      as yr_accm   --年积攼0 
      ,(case 
            when '$TXDATE' = '$MONTHBGNDAY' 
            then (case   
             when T.SVT_NO = '06' then t.bal 
             else coalesce(T2.bal,0) 
             end ) 
            else (case   
             when T.SVT_NO = '06' then t.bal 
             else coalesce(T2.bal,0) 
             end )+coalesce(T_yjs.mth_accm,0) 
            end 
       )/('$TXDATE'::date-'$MONTHBGNDAY'::date+1)               as mth_day_avg_bal  --月日均余顾] 
      ,(case 
           when '$TXDATE' = '$YEARBGNDAY' 
           then (case   
             when T.SVT_NO = '06' then t.bal 
             else coalesce(T2.bal,0) 
             end ) 
           else (case   
             when T.SVT_NO = '06' then t.bal 
             else coalesce(T2.bal,0) 
             end )+coalesce(T_yjs.yr_accm,0) 
           end 
       )/('$TXDATE'::date-'$YEARBGNDAY'::date+1)                    as Yr_Day_Avg_Bal   --年日均余顾]
    ,'LCS' 
  from dw_sdata.lcs_000_card_fc_cdm_leg T      --- 活期分户登记表（外币） 
    left join TT_acct_med_rel_grp24 t1
     on t.acc = t1.sign_id  
  left join dw_sdata.acc_003_t_acc_cdm_fc_ledger  T2 ---负债类储蓄外币活期分户账		
    ON T.ACC=T2.ACC 
   and T.CURR_TYPE=T2.CURR_TYPE 
   and T2.SYS_CODE='99700010000' 
   and T2.start_dt<='$TXDATE'::date
   and '$TXDATE'::date <T2.end_dt
 /* left join (select distinct t.ITM_NO,t.RATE_NO,t.CURR_TYPE,t.RATE_KIND FROM dw_sdata.lcs_000_pub_itmrate t) T3 ---科目利率对照表		
    ON T.SVT_NO=T3.ITM_NO
   AND T.CURR_TYPE=T3.CURR_TYPE
   And T3.RATE_KIND ='0'*/
left join (
select * from (
select t.ITM_NO
--,t.ITM_NAME
,t.CURR_TYPE
,t.RATE_KIND
,t.RATE_NO
--,t.RATE_LEV
,row_number()over(partition by t.ITM_NO,t.CURR_TYPE,t.RATE_KIND order by t.RATE_LEV desc) NUM
FROM dw_sdata.lcs_000_pub_itmrate t
where T.start_dt <='$TXDATE'::date
   and T.end_dt > '$TXDATE'::Date
)t
where NUM = 1
) T3 ---科目利率对照表         
    ON T.SVT_NO=T3.ITM_NO
   AND T.CURR_TYPE=T3.CURR_TYPE
   And T3.RATE_KIND ='0'
  left join dw_sdata.acc_003_t_accdata_last_item_no T6 --科目转换表	
    ON T2.ITM_NO = T6.AMT_ITM
   AND T6.FIRST_ITM = '20' --负债类利率支出  
   and T6.start_dt<='$TXDATE'::date
   and '$TXDATE'::date<T6.end_dt
  left join  (select ACC,
                     sum(case when ACC_DATE = '$TXDATE' then TRAN_AMT else 0 end) as TRAN_AMT_D  ,-- 日
                     sum(case when SUBSTR(ACC_DATE,1,6)  = SUBSTR('$TXDATE',1,6)  and ACC_DATE <='$TXDATE' then TRAN_AMT else 0 end) as TRAN_AMT_M  -- 月
                    -- sum(case when etl_dt<='$TXDATE' then  TRAN_AMT  else 0 end) TRAN_AMT_SUM --累积
                from dw_sdata.acc_000_T_INT_PRSN_CDM_FC_DTL 
	       group by ACC 
	      )T7--储蓄外币活期计息明细登记簿	
	ON T.ACC=T7.ACC
  left join TT_ACC_Paid_Int_grp2 t8 
     on t.acc = t8.acc  
  left join (SELECT A. RATE_NO, A. bgn_date , A.RATE_VAL FROM  dw_sdata.lcs_000_srm_rate  A 
              INNER JOIN
             (SELECT RATE_NO,MAX(bgn_date) AS bgn_date  FROM  dw_sdata.lcs_000_srm_rate
              where  ('$MINDATE'::date + bgn_date-1)::date  <'$TXDATE'::date GROUP  BY RATE_NO  ) B
              ON  A. RATE_NO=B. RATE_NO
              and A. bgn_date =B. bgn_date) T13 ---利率表	
    ON T3.RATE_NO=T13.RATE_NO 
  left join dw_sdata.lcs_000_card_pub_fc_svt T14 ------储种表外币		
    ON T.SVT_NO =T14.SVT_NO 
    and T.CURR_TYPE=t14.curr_type
   and T14.start_dt<='$TXDATE'::DATE 
   AND '$TXDATE'::DATE <T14.end_dt 
left join TT_pk_savrate_reg_grp24 t15 --利率
/*
(select A.acc,A.BGN_DATE,A.rate from  dw_sdata.lcs_000_pk_savrate_reg A
        inner join
        (select  acc,max(BGN_DATE)  as BGN_DATE FROM dw_sdata.lcs_000_pk_savrate_reg
          where   start_dt<='$TXDATE'::date
            and '$TXDATE'::date < end_dt
            and ('$MINDATE'::date+bgn_date-1)::date<='$TXDATE'::date
            group by 1) B
        on  A.acc=B.acc
       and A.BGN_DATE=B.BGN_DATE
       where   A.start_dt<='$TXDATE'::date
         and '$TXDATE'::date <  A.end_dt) T15
*/
 ON T.acc=T15.acc
left join dw_sdata.lcs_000_pk_subcal_reg T16
       ON T.acc=T16.acc
      and T16.start_dt<='$TXDATE'::date
      and '$TXDATE'::date <T16.end_dt
left join  (SELECT A. RATE_NO, A. bgn_date , A.RATE_VAL FROM  dw_sdata.lcs_000_srm_rate  A 
              INNER JOIN
             (SELECT RATE_NO,MAX(bgn_date) AS bgn_date  FROM  dw_sdata.lcs_000_srm_rate
              where  ('$MINDATE'::date + bgn_date-1)::date  <='$TXDATE'::date GROUP  BY RATE_NO  ) B
              ON  A. RATE_NO=B. RATE_NO
              and A. bgn_date =B. bgn_date ) T17 ---利率表
 ON T3.RATE_NO=T17.RATE_NO 
left join f_fdm.F_DPST_INDV_ACCT T_yjs
on         t.acc= T_yjs.agmt_id
and  T_yjs.etl_date='$TXDATE'::date-1
 where T.start_dt<='$TXDATE'::DATE
   AND '$TXDATE'::DATE <T.end_dt
;
--

-- 定期本币
insert into f_fdm.F_DPST_INDV_ACCT
(etl_date
,grp_typ
,Agmt_Id
,Cust_Num
,Org_Num
,Cur_Cd
,Prod_Cd
,dpst_cate_cd
,Cash_Ind_Cd
,St_Int_Dt
,Due_Dt
,Open_Acct_Day
,Clos_Acct_Day
,Exec_Int_Rate
,Bmk_Int_Rate
,Basis
,Int_Base_Cd
,Cmpd_Int_Calc_Mode_Cd
,Is_Nt_Int_Ind
,Pre_Chrg_Int
,Int_Rate_Attr_Cd
,Orgnl_Term
,Orgnl_Term_Corp_Cd
,Rprc_Prd
,Rprc_Prd_Corp_Cd
,Last_Rprc_Day
,Next_Rprc_Day
,Is_AutoRnw
,Last_AutoRnw_Dt
,Agmt_Stat_Cd
,Prin_Subj
,Curr_Bal
,Int_Subj
,Today_Provs_Int
,CurMth_Provs_Int
,Accm_Provs_Int
,Today_Int_Pay
,CurMth_Paid_Int
,Accm_Paid_Int
,Int_Adj_Amt
,Mth_Accm
,Yr_Accm
,Mth_Day_Avg_Bal
,Yr_Day_Avg_Bal
,Sys_Src
 )
select 
      '$TXDATE'::DATE,
      3,
      T.ACC,
      coalesce(t1.ecif_cust_no,''),
      coalesce(T2.OP_INST,''), 
      '156',
      T.SVT_NO,
      coalesce(T15.cal_flag,''),
      '',
      ('$MINDATE'::date+T.BGN_INT_DATE-1)::date,
      ('$MINDATE'::date+T.DUE_DATE-1)::date,
      coalesce(T2.OP_DATE::date,'$MINDATE' :: date ),
      coalesce(T2.CLS_DATE::date,'$MINDATE' :: date ),
      CASE WHEN substr(to_char(T.b_flag),5,1) ='1' THEN coalesce(T13.rate,0)
           WHEN substr(to_char(T.b_flag),6,1) ='1' OR   substr(to_char(T.b_flag),7,1) ='1' THEN coalesce(T14.due_rate,0)
           ELSE  coalesce(T16.rate_val,0)
      END  ,
      0.00,
      0.00,
      CASE WHEN SUBSTR(T15.INT_CAL_FLAG,2,1) ='0' THEN '1' --实际/360
           WHEN SUBSTR(T15.INT_CAL_FLAG,2,1) ='1' THEN '10' --30/360
      ELSE '@'|| T15.INT_CAL_FLAG END,
      '1', --单利
      '1', --是
      '0', --后收
      '1', --固定
      T.DUE_DATE-T.BGN_INT_DATE,
      'D',     
      CASE WHEN T.DUE_DATE IS NULL  or ('$MINDATE'::date+ T.DUE_DATE-1)::date<='$TXDATE'::DATE THEN  NULL 
           ELSE T.DUE_DATE - T.BGN_INT_DATE 
      END ,
      'D',
      coalesce(('$MINDATE'::date+t4.bgn_date-1)::date,'$MINDATE' :: date ),      
      CASE WHEN T.DUE_DATE IS NULL  THEN '$MINDATE' :: date 
      WHEN T.DUE_DATE IS NOT NULL AND   ('$MINDATE'::date+ T.DUE_DATE-1)::date <= '$TXDATE'::DATE THEN '$TXDATE'::DATE+1
      ELSE  ('$MINDATE'::date+ T.DUE_DATE-1)::date
      END ,
      T.AUTO_TSF_FLAG,
      coalesce(('$MINDATE'::date+ T5.TRAN_DATE-1)::date,'$MINDATE' :: date ),
      to_char(mod(t.b_flag,2)),--T.B_FLAG 备注：先转二进制，然后截取右边第一位，即为账户状态
      coalesce(T2.ITM_NO,'') ,
      coalesce(T2.BAL,0),
      coalesce(T6.ITM_NO,''),
      coalesce(T7.TRAN_AMT,0),
      coalesce(T12.TRAN_AMT_M,0),
      0,
      case when T8.INTS is not null then T8.INTS else T9.INTS end, 
      coalesce(T10.INTS,0),
      0,
      0,
      (case
            when '$TXDATE'= '$MONTHBGNDAY'
            then coalesce(T2.BAL,0)
            else coalesce(T2.BAL,0)+coalesce(T_yjs.mth_accm,0)
            end
       )                                                                      as mth_accm  --月积攼0
      ,(case
            when  '$TXDATE' = '$YEARBGNDAY'
            then coalesce(T2.BAL,0)
            else coalesce(T2.BAL,0)+coalesce(T_yjs.yr_accm,0)
            end
       )                                                                      as yr_accm   --年积攼0
      ,(case
            when '$TXDATE' = '$MONTHBGNDAY'
            then coalesce(T2.BAL,0)
            else coalesce(T2.BAL,0)+coalesce(T_yjs.mth_accm,0)
            end
       )/('$TXDATE'::date-'$MONTHBGNDAY'::date+1)               as mth_day_avg_bal  --月日均余顾]
      ,(case
           when '$TXDATE' = '$YEARBGNDAY'
           then coalesce(T2.BAL,0)
           else coalesce(T2.BAL,0)+coalesce(T_yjs.yr_accm,0)
           end
       )/('$TXDATE'::date-'$YEARBGNDAY'::date+1)                    as Yr_Day_Avg_Bal   --年日均余顾]
      ,'LCS'
  from pk_fix_leg_tmp T  -- 定期分户登记薄（本币）
left join DW_SDATA.lcs_000_card_pub_svt T15 ------麓垄讝卤霝?             
      ON T.SVT_NO =T15.SVT_NO
     and T15.start_dt<='$TXDATE'::DATE
     AND '$TXDATE'::DATE <T15.end_dt
  left join TT_acct_med_rel_grp3 t1
    on t.acc = t1.sign_id
left join dw_sdata.acc_003_t_acc_fix_ledger  T2 ---负债类储蓄本币账户定期分户账	
  ON T.ACC=T2.ACC and T2.SYS_CODE='99700010000' 
 and T2.start_dt<='$TXDATE'::DATE
 AND '$TXDATE'::DATE<T2.end_dt
left join dw_sdata.lcs_000_pub_itmrate  T3        --科目利率对照表   
on T.SVT_NO =T3.itm_NO
and t15.term=t3.rate_lev
and t3.CURR_TYPE='156'
and t3.rate_kind=(case when ('$MINDATE'::date + T.DUE_DATE-1)::date>='$TXDATE'::date then '0' else '1' end)
and T3.start_dt<='$TXDATE'::DATE
and '$TXDATE'::DATE <T3.end_dt
/*left join (select distinct t.ITM_NO,t.RATE_NO,t.CURR_TYPE,t.RATE_KIND FROM dw_sdata.lcs_000_pub_itmrate t) T3 ---科目利率对照表		
  ON  T.SVT_NO=T3.ITM_NO
AND T3.CURR_TYPE='156'
And T3.RATE_KIND ='0'
left join (
select * from (
select t.ITM_NO
--,t.ITM_NAME
,t.CURR_TYPE
,t.RATE_KIND
,t.RATE_NO
--,t.RATE_LEV
,row_number()over(partition by t.ITM_NO,t.CURR_TYPE,t.RATE_KIND order by t.RATE_LEV desc) NUM
FROM dw_sdata.lcs_000_pub_itmrate t
)t
where NUM = 1
) T3 ---科目利率对照表               
  ON  T.SVT_NO=T3.ITM_NO
AND T3.CURR_TYPE='156'
And T3.RATE_KIND ='0'
*/
left join (SELECT A. RATE_NO, A. bgn_date , A.RATE_VAL FROM  dw_sdata.lcs_000_srm_rate  A 
              INNER JOIN
             (SELECT RATE_NO,MAX(bgn_date) AS bgn_date  FROM  dw_sdata.lcs_000_srm_rate
              where  ('$MINDATE'::date + bgn_date-1)::date  <'$TXDATE'::date GROUP  BY RATE_NO  ) B
              ON  A. RATE_NO=B. RATE_NO
              and A. bgn_date =B. bgn_date) T4 ---利率表	
ON T3.RATE_NO=T4.RATE_NO
left join (SELECT ACC,MAX(TRAN_DATE) as TRAN_DATE FROM dw_sdata.lcs_000_tsf_fix_int where ('$MINDATE'::date + tran_date-1)::date  <'$TXDATE'::date group by 1) T5 ---定期自动转存利息登记簿（本币）		
   ON T.ACC=T5.ACC
left join dw_sdata.acc_003_t_accdata_last_item_no T6   --科目转换表
   ON T2.ITM_NO= T6.AMT_ITM
  AND T6.FIRST_ITM='20'     --负债类利率支出 
  and T6.start_dt<='$TXDATE'::DATE
  AND '$TXDATE'::DATE <T6.end_dt
left join(select ACC,
                 sum(TRAN_AMT) as TRAN_AMT 
            from dw_sdata.acc_000_T_INT_PRSN_FIX_DTL 
           where ACC_DATE ='$TXDATE'
	  group by ACC 
	 ) T7--储蓄人民币定期计息明细登记簿 
	ON T.ACC=T7.ACC
left join dw_sdata.lcs_000_tsf_fix_int T8  --定期自动转存利息登记簿（本币） 	
    ON T.ACC=T8.acc 
    and ('$MINDATE'::date + T8.tran_date-1)::DATE= '$TXDATE'::DATE
left join dw_sdata.lcs_000_pk_fix_int T9 --定期利息登记簿(本币)	
    ON T.ACC=T9.acc 
   and ('$MINDATE'::date + T9.tran_date-1)::DATE='$TXDATE'::DATE   
left join (select A.acc, 
                  sum(A.ints) as ints  
            from  (select acc,tran_date,ints from DW_sdata.lcs_000_pk_fix_int 
                    where month(('$MINDATE'::date + tran_date-1)::DATE) = month('$TXDATE'::date)  and ('$MINDATE'::date + tran_date-1)::DATE <='$TXDATE'::date
                    union all
                   select acc,tran_date,ints from DW_sdata.lcs_000_tsf_fix_int 
                   where month(('$MINDATE'::date + tran_date-1)::DATE) = month('$TXDATE'::date)  and ('$MINDATE'::date + tran_date-1)::DATE <='$TXDATE'::date
                  )  A
            group by acc
           ) T10
 	ON T.ACC=T10.acc  	
/* left join (select A.acc,
                  sum(A.ints)  as ints
             from  (select acc, tran_date,ints from DW_sdata.lcs_000_pk_fix_int where  etl_dt<='$TXDATE'::date
                    union all
                    select acc,tran_date,ints from DW_sdata.lcs_000_tsf_fix_int where etl_dt<='$TXDATE'::date ) A 
            group by acc
          ) T11
   ON T.ACC=T11.acc*/ 	

left join (select acc,
                  sum(case when substr(tran_time,1,6)=substr('$TXDATE',1,6)  and substr(tran_time,1,8) <='$TXDATE' then TRAN_AMT else 0 end ) TRAN_AMT_M
                 -- sum(case when etl_dt<='$TXDATE'::date then TRAN_AMT  else 0 end) TRAN_AMT_S
             from dw_sdata.acc_000_T_INT_PRSN_FIX_DTL 
            group by 1 
          ) T12
      on  T.ACC=T12.ACC
left join TT_pk_savrate_reg_grp3 t13
/*
 (select acc,BGN_DATE,rate from (
                    select A.acc,A.BGN_DATE,A.rate ,ROW_NUMBER()OVER(PARTITION BY A.acc,A.BGN_DATE ORDER BY A.rate DESC) AS Rn
                    from  dw_sdata.lcs_000_pk_savrate_reg A
                    inner join
                            (select  acc,max(BGN_DATE)  as BGN_DATE FROM dw_sdata.lcs_000_pk_savrate_reg
                             where   start_dt<='$TXDATE'::date
                             and '$TXDATE'::date < end_dt
                             and ('$MINDATE'::date+bgn_date-1)::date<='$TXDATE'::date
                             group by 1) B
                              on  A.acc=B.acc
                             and A.BGN_DATE=B.BGN_DATE
                             where   A.start_dt<='$TXDATE'::date
                             and '$TXDATE'::date <  A.end_dt
                            )  T                                ----modified by wyh at 20161014
                   where T.Rn = 1
           ) T13 
*/
ON T.acc=T13.acc
left join dw_sdata.lcs_000_pk_subcal_reg T14
       ON T.acc=T14.acc
      and T14.start_dt<='$TXDATE'::date
      and '$TXDATE'::date <T14.end_dt
left join  (SELECT A. RATE_NO, A. bgn_date , A.RATE_VAL FROM  dw_sdata.lcs_000_srm_rate  A 
              INNER JOIN
             (SELECT RATE_NO,MAX(bgn_date) AS bgn_date  FROM  dw_sdata.lcs_000_srm_rate
              where  ('$MINDATE'::date + bgn_date-1)::date  <='$TXDATE'::date GROUP  BY RATE_NO  ) B
              ON  A. RATE_NO=B. RATE_NO
              and A. bgn_date =B. bgn_date ) T16 ---利率表
 ON T3.RATE_NO=T16.RATE_NO 
left join f_fdm.F_DPST_INDV_ACCT T_yjs
on         t.acc= T_yjs.agmt_id
and  T_yjs.etl_date='$TXDATE'::date-1
where T.start_dt<='$TXDATE'::DATE
  AND '$TXDATE'::DATE <T.end_dt
  ; 

-- 定期外币
insert into f_fdm.F_DPST_INDV_ACCT
(etl_date
,grp_typ
,Agmt_Id
,Cust_Num
,Org_Num
,Cur_Cd
,Prod_Cd
,dpst_cate_cd
,Cash_Ind_Cd
,St_Int_Dt
,Due_Dt
,Open_Acct_Day
,Clos_Acct_Day
,Exec_Int_Rate
,Bmk_Int_Rate
,Basis
,Int_Base_Cd
,Cmpd_Int_Calc_Mode_Cd
,Is_Nt_Int_Ind
,Pre_Chrg_Int
,Int_Rate_Attr_Cd
,Orgnl_Term
,Orgnl_Term_Corp_Cd
,Rprc_Prd
,Rprc_Prd_Corp_Cd
,Last_Rprc_Day
,Next_Rprc_Day
,Is_AutoRnw
,Last_AutoRnw_Dt
,Agmt_Stat_Cd
,Prin_Subj
,Curr_Bal
,Int_Subj
,Today_Provs_Int
,CurMth_Provs_Int
,Accm_Provs_Int
,Today_Int_Pay
,CurMth_Paid_Int
,Accm_Paid_Int
,Int_Adj_Amt
,Mth_Accm
,Yr_Accm
,Mth_Day_Avg_Bal
,Yr_Day_Avg_Bal
,Sys_Src
 )select '$TXDATE'::DATE ,
         4,
         T.ACC,
        coalesce(t1.ecif_cust_no,''),
         coalesce(T2.ACC_INST,''),
         T.CURR_TYPE,
         T.SVT_NO,
         coalesce(T15.cal_flag,''),
         T.CH_TYPE,
         ('$MINDATE'::date + T.BGN_INT_DATE-1)::date,
         ('$MINDATE'::date + T.DUE_DATE-1)::date,
         coalesce(T2.OP_DATE::date,'$MINDATE' :: date ),
         coalesce(T2.CLS_DATE::date,'$MINDATE' :: date ),
         CASE WHEN substr(to_char(T.b_flag),5,1) ='1' THEN coalesce(T13.rate,0)
              WHEN substr(to_char(T.b_flag),6,1) ='1' OR   substr(to_char(T.b_flag),7,1) ='1' THEN coalesce(T14.due_rate,0)
              ELSE  coalesce(T16.rate_val,0)
          END,
         0.00,
         0.00,
         CASE WHEN SUBSTR(T15.INT_CAL_FLAG,2,1) ='0' THEN '1' --实际/360
              WHEN SUBSTR(T15.INT_CAL_FLAG,2,1) ='1' THEN '10' --30/360
         ELSE '@'||T15.INT_CAL_FLAG END,
         '1',   --单利
         '1',   --是
         '0',   --后收
         '1',   --固定
         T.DUE_DATE-T.BGN_INT_DATE,
         'D',
         CASE WHEN T.DUE_DATE IS NULL  or ('$MINDATE'::date+ T.DUE_DATE-1)::date<='$TXDATE'::DATE THEN  NULL 
             ELSE T.DUE_DATE - T.BGN_INT_DATE 
         END ,          
         'D',
          coalesce(('$MINDATE'::date + t4.bgn_date-1)::date,'$MINDATE' :: date ),
          CASE WHEN T.DUE_DATE IS NULL  THEN '$MINDATE' :: date 
               WHEN T.DUE_DATE IS NOT NULL AND   ('$MINDATE'::date+ T.DUE_DATE-1)::date <= '$TXDATE'::DATE THEN '$TXDATE'::DATE+1
          ELSE  ('$MINDATE'::date+ T.DUE_DATE-1)::date
          END ,
         T.AUTO_TSF_FLAG,
         coalesce(('$MINDATE'::date+ T5.TRAN_DATE-1)::date,'$MINDATE' :: date ),
         to_char(mod(t.b_flag,2)), --T.B_FLAG 备注：先转二进制，然后截取右边第一位，即为账户状态
         coalesce(T2.ITM_NO,''),
         coalesce(T2.BAL,0),
         coalesce(T6.ITM_NO,''),
         coalesce(T7.TRAN_AMT,0),
         coalesce(T12.TRAN_AMT_m,0),
         0,
         case when T8.INTS is not null then T8.INTS else T9.INTS end ,
         coalesce(T10.INTS,0),
         0,
         0,   
         (case
            when '$TXDATE'= '$MONTHBGNDAY'
            then coalesce(T2.BAL,0)
            else coalesce(T2.BAL,0)+coalesce(T_yjs.mth_accm,0)
            end
       )                                                                      as mth_accm  --月积攼0
      ,(case
            when  '$TXDATE' = '$YEARBGNDAY'
            then coalesce(T2.BAL,0)
            else coalesce(T2.BAL,0)+coalesce(T_yjs.yr_accm,0)
            end
       )                                                                      as yr_accm   --年积攼0
      ,(case
            when '$TXDATE' = '$MONTHBGNDAY'
            then coalesce(T2.BAL,0)
            else coalesce(T2.BAL,0)+coalesce(T_yjs.mth_accm,0)
            end
       )/('$TXDATE'::date-'$MONTHBGNDAY'::date+1)               as mth_day_avg_bal  --月日均余顾]
      ,(case
           when '$TXDATE' = '$YEARBGNDAY'
           then coalesce(T2.BAL,0)
           else coalesce(T2.BAL,0)+coalesce(T_yjs.yr_accm,0)
           end
       )/('$TXDATE'::date-'$YEARBGNDAY'::date+1)                    as Yr_Day_Avg_Bal   --年日均余顾]
        , 'LCS'
   from dw_sdata.lcs_000_pk_fc_fix_leg   T -- 定期分户登记簿（外币）
    left join TT_acct_med_rel_grp24 t1
    on t.acc = t1.sign_id
left join dw_sdata.lcs_000_card_pub_fc_svt T15 ------储种表外币         
  ON T.SVT_NO =T15.SVT_NO
 and t.curr_type=t15.curr_type
 and T15.start_dt<='$TXDATE'::DATE
 and '$TXDATE'::DATE<T15.end_dt
left join dw_sdata.acc_003_t_acc_fix_fc_ledger T2 --- 负债类储蓄外币定期分户账 
    ON T.ACC=T2.ACC 
   and T.CURR_TYPE=T2.CURR_TYPE 
   and T2.SYS_CODE='99700010000'
   and T2.start_dt<='$TXDATE'::DATE
   AND '$TXDATE'::DATE<T2.end_dt
left join dw_sdata.lcs_000_pub_itmrate t3            -----科目利率对照表
on t.svt_no=t3.itm_no
and t15.term=t3.rate_lev
and t.curr_type=t3.curr_type
and t3.rate_kind=(case when ('$MINDATE'::date + T.DUE_DATE-1)::date>='$TXDATE'::date then '0' else '1' end)
and T3.start_dt<='$TXDATE'::DATE
   AND '$TXDATE'::DATE<T3.end_dt
 /* left join (select distinct t.ITM_NO,t.RATE_NO,t.CURR_TYPE,t.RATE_KIND FROM dw_sdata.lcs_000_pub_itmrate t) T3 ---科目利率对照表		
    ON  T.SVT_NO=T3.ITM_NO
   AND T.CURR_TYPE=T3.CURR_TYPE
   And T3.RATE_KIND ='0'
left join (
select * from (
select t.ITM_NO
--,t.ITM_NAME
,t.CURR_TYPE
,t.RATE_KIND
,t.RATE_NO
--,t.RATE_LEV
,row_number()over(partition by t.ITM_NO,t.CURR_TYPE,t.RATE_KIND order by t.RATE_LEV desc) NUM
FROM dw_sdata.lcs_000_pub_itmrate t
)t
where NUM = 1
) T3 ---科目利率对照表             
    ON  T.SVT_NO=T3.ITM_NO
   AND T.CURR_TYPE=T3.CURR_TYPE
   And T3.RATE_KIND ='0'
*/
   left join (SELECT A. RATE_NO, A. bgn_date , A.RATE_VAL FROM  dw_sdata.lcs_000_srm_rate  A 
              INNER JOIN
             (SELECT RATE_NO,MAX(bgn_date) AS bgn_date  FROM  dw_sdata.lcs_000_srm_rate
              where  ('$MINDATE'::date + bgn_date-1)::date  <'$TXDATE'::date GROUP  BY RATE_NO  ) B
              ON  A. RATE_NO=B. RATE_NO
              and A. bgn_date =B. bgn_date) T4 ---利率表	
    ON T3.RATE_NO=T4.RATE_NO
 left join (SELECT ACC,MAX(TRAN_DATE) as TRAN_DATE FROM DW_SDATA.lcs_000_tsf_fc_fix_int where ('$MINDATE'::date + tran_date-1)::date  <'$TXDATE'::date  group by 1 ) T5 ---定期自动转存利息登记簿（外币）	
  ON T.ACC=T5.ACC
 left join dw_sdata.acc_003_t_accdata_last_item_no T6--科目转换表	
  ON T2.ITM_NO= T6.AMT_ITM
 AND T6.FIRST_ITM='20' --负债类利率支出 
 and T6.start_dt<='$TXDATE'::DATE
 AND '$TXDATE'::DATE<T6.end_dt
left join (select ACC,sum(TRAN_AMT) as TRAN_AMT 
           from DW_SDATA.acc_000_T_INT_PRSN_FIX_FC_DTL 
           where  ACC_DATE ='$TXDATE'
           group by ACC ) T7--储蓄外币定期计息明细登记簿	
 	ON T.ACC=T7.ACC
 left join  dw_sdata.lcs_000_tsf_fc_fix_int T8--定期自动转存利息登记簿（外币） 
  ON T.ACC=T8.acc 
and ('$MINDATE'::date+T8.tran_date-1)::DATE='$TXDATE'::DATE  

left join dw_sdata.lcs_000_pk_fc_fix_int T9 --定期利息登记簿（外币） 
  ON T.ACC=T9.acc 
  and ('$MINDATE'::date + T9.tran_date-1)::DATE='$TXDATE'::DATE   
    
 left join (select A.acc, 
                  sum(A.ints) as ints  
            from  (select acc,tran_date,ints from DW_sdata.lcs_000_tsf_fc_fix_int
                    where month(('$MINDATE'::date + tran_date-1)::DATE) = month('$TXDATE'::date)  and ('$MINDATE'::date + tran_date-1)::DATE <='$TXDATE'::date
                    union all
                   select acc,tran_date,ints from DW_sdata.lcs_000_pk_fc_fix_int 
                   where month(('$MINDATE'::date + tran_date-1)::DATE) = month('$TXDATE'::date)  and ('$MINDATE'::date + tran_date-1)::DATE <='$TXDATE'::date
                  )  A
            group by acc
           ) T10
 	ON T.ACC=T10.acc

/* left join (select A.acc,
                  sum(A.ints)  as ints
             from  (select acc, tran_date,ints from DW_sdata.lcs_000_tsf_fc_fix_int where  etl_dt<='$TXDATE'::date
                    union all
                    select acc,tran_date,ints from DW_sdata.lcs_000_pk_fc_fix_int where etl_dt<='$TXDATE'::date ) A 
            group by acc
          ) T11
   ON T.ACC=T11.acc */ 	
   	 
 left join (select acc,
                  sum(case when substr(tran_time,1,6)=substr('$TXDATE',1,6)  and substr(tran_time,1,8) <='$TXDATE' then TRAN_AMT else 0 end ) TRAN_AMT_M
                 -- sum(case when etl_dt<='$TXDATE'::date then TRAN_AMT  else 0 end) TRAN_AMT_S
             from dw_sdata.acc_000_T_INT_PRSN_FIX_DTL 
            group by 1 
          ) T12
      on  T.ACC=T12.ACC
left join TT_pk_savrate_reg_grp24 t13 --利率
/*
 (select A.acc,A.BGN_DATE,A.rate from  dw_sdata.lcs_000_pk_savrate_reg A
           inner join
           (select  acc,max(BGN_DATE)  as BGN_DATE FROM dw_sdata.lcs_000_pk_savrate_reg
            where   start_dt<='$TXDATE'::date
            and '$TXDATE'::date < end_dt
            and ('$MINDATE'::date+bgn_date-1)::date<='$TXDATE'::date
            group by 1) B
           on  A.acc=B.acc
           and A.BGN_DATE=B.BGN_DATE
           where   A.start_dt<='$TXDATE'::date
           and '$TXDATE'::date <  A.end_dt) T13  
*/
ON T.acc=T13.acc
left join dw_sdata.lcs_000_pk_subcal_reg T14
       ON T.acc=T14.acc
      and T14.start_dt<='$TXDATE'::date
      and '$TXDATE'::date <T14.end_dt
left join  (SELECT A. RATE_NO, A. bgn_date , A.RATE_VAL FROM  dw_sdata.lcs_000_srm_rate  A 
              INNER JOIN
             (SELECT RATE_NO,MAX(bgn_date) AS bgn_date  FROM  dw_sdata.lcs_000_srm_rate
              where  ('$MINDATE'::date + bgn_date-1)::date  <='$TXDATE'::date GROUP  BY RATE_NO  ) B
              ON  A. RATE_NO=B. RATE_NO
              and A. bgn_date =B. bgn_date ) T16 ---利率表
 ON T3.RATE_NO=T16.RATE_NO 
left join f_fdm.F_DPST_INDV_ACCT T_yjs
on         t.acc= T_yjs.agmt_id
and  T_yjs.etl_date='$TXDATE'::date-1
where T.start_dt<='$TXDATE'::DATE
 and '$TXDATE'::DATE<T.end_dt
;
/*数据处理区end*/

commit;

