/*
Author             :dhy
Function           :
Load method        :
Source table       :
Destination Table  :f_fdm.f_acct_crdt_card_info
Frequency          :D
Modify history list:Created by dhy 2016/4/26 14:54:38
                   :Modify  byliudongyan 2016/8/11 修改T9表 见变更记录160
                    modify by zmx 修改--此处限制当月“数据日期”201602为当月日期日期^M
                    modified by lxz 20160928 T9�条�放�T1关��
                    modified by zmx 20161014 修改表名�[6~
*/

-------------------------------------------逻辑说明---------------------------------------------
/*
建3张临时表 
*/
-------------------------------------------逻辑说明END------------------------------------------

/*临时表创建区*/
CREATE Local TEMP TABLE IF NOT EXISTS  TT_6
ON COMMIT PRESERVE ROWS as 
SELECT T.XACCOUNT,
       T.BAL_NOINT + T.STM_NOINT + T.STM_BALORI + T.BAL_CMPINT +
       T.MP_REM_PPL + T.MP_AUTHS + T.AUTHS_AMT + 
          GREATEST(DECODE(T.STMBALINTFLAG,'-',-1,1)*T.STM_BALINT+T.STM_BALFRE+T.BAL_FREE+DECODE(T.BAL_INTFLAG,'-',-1,1)*BAL_INT
                     +DECODE(T.BAL_MPFLAG,'-',-1,1)*BAL_MP+DECODE(T.STM_BMFLAG,'-',-1,1)*stm_balmp,0)  AS RMBSUM
       ,t.TEMP_LIMIT
       ,t.TLMT_BEG
       ,t.TLMT_END
       ,t.CRED_LIMIT
       ,t.BANK
  FROM  dw_sdata.ccb_000_acct  t
  where T.start_dt<='$TXDATE'::date
  AND '$TXDATE'::date<T.end_dt
order by XACCOUNT
unsegmented all nodes 
--SEGMENTED BY hash(XACCOUNT) ALL NODES 
;

create local temp table tt_2 
(
 ECIF_CUST_NO varchar(28),
 CERT_NO  varchar(38),
 PARTY_NAME varchar(60)
)
on commit preserve rows  
order by CERT_NO,PARTY_NAME
--SEGMENTED BY hash(CERT_NO,PARTY_NAME) ALL NODES
unsegmented all nodes
;
insert into tt_2 
select ECIF_CUST_NO 
       ,CERT_NO
       ,PARTY_NAME
  from (SELECT ECIF_CUST_NO,
               CERT_NO,
               PARTY_NAME,
               IS_VIP_FLAG,
               start_dt,
               end_dt,
               row_number() over(partition by CERT_NO, PARTY_NAME order by IS_VIP_FLAG desc) as num2
          FROM (select *
                  from (select ECIF_CUST_NO,
                               CERT_NO,
                               PARTY_NAME,
                               IS_VIP_FLAG,
                               start_dt,
                               end_dt,
                               row_number() over(partition by CERT_NO, PARTY_NAME order by cert_due_date desc) as num
                          from dw_sdata.ecf_001_t01_cust_info
                         where UPDATED_TS = '99991231 00:00:00:000000'
                           and start_dt <= '$TXDATE' ::date
                           and '$TXDATE' ::date < end_dt) a
                 where a.num = 1
                union all
                select *
                  from (select ECIF_CUST_NO,
                               CERT_NO,
                               PARTY_NAME,
                               IS_VIP_FLAG,
                               start_dt,
                               end_dt,
                               row_number() over(partition by CERT_NO, PARTY_NAME order by cert_due_date desc) as num
                          from dw_sdata.ecf_004_t01_cust_info
                         where UPDATED_TS = '99991231 00:00:00:000000'
                           and start_dt <= '$TXDATE' ::date
                           and '$TXDATE' ::date < end_dt) b
                 where b.num = 1) c
             ) d
 where d.num2 = 1
   and d.CERT_NO||d.PARTY_NAME in (select CUSTR_NBR||SURNAME from dw_sdata.ccb_000_custr where start_dt<='$TXDATE'::date and '$TXDATE'::date<end_dt  )
;

CREATE Local TEMP TABLE IF NOT EXISTS  TT_7
(
 XACCOUNT varchar(38),
 WBSUM     numeric(24,2)
)ON COMMIT PRESERVE ROWS
order by XACCOUNT
--SEGMENTED BY hash(XACCOUNT) ALL NODES
unsegmented all nodes
;
CREATE Local TEMP TABLE IF NOT EXISTS  TT_8
ON COMMIT  PRESERVE ROWS
 as 
SELECT  
 T.CARD_NBR ,SUM(CASE WHEN T.TRANS_TYPE IN ('2000','2010','2040','2042','2050','2060','2070','2072','2090','2092','2094','2100'
 ,'2110','2140','2150','2160','2170','2180','2182','2184','2300','2310','2340','2342','2350','2360','2370','2372','2400','2410','2440'
 ,'2450','2460','2470','2480','2482','2484','8110','8112','8114','8116','8118','8120','8170') THEN T.BILL_AMT ELSE 0 END) AS QXSUM-- 当月取现金额:包含：一般取现、购汇取现、售汇取现、转账转出、司法扣划、销户取现
 ,SUM(CASE WHEN T.TRANS_TYPE IN ('1000','1002','1004','1010','1012','1016','1030','1032','1040','1102','1110','1132','1180','1184'
 ,'6010','6016','6020','6102','6110','6180','6184','8100','8102','8104','8106','8108') THEN T.BILL_AMT ELSE 0 END )  AS XFSUM-- 当月消费金额：包含：POS消费（一般POS/POS退货/POS误抛/IC卡圈存消费）、网上消费、积分消费
 ,SUM(CASE WHEN T.TRANS_TYPE IN ('7000','7010','7012','7014','7020','7030','7036','7040','7050','7054','7058','7060','7062','7070'
 ,'7080','7082','7084','7092','7094','7098','8140')  THEN T.BILL_AMT ELSE 0 END) AS HKSUM-- 当月还款金额：包含：本行柜面存现/本行ATM存现/他行柜面存现/他行ATM存现/购汇还款/售汇还款/批量还款/转账转入/手工还款撤销/其他现金还款/IC卡圈提
 ,SUM(CASE WHEN T.TRANS_TYPE IN ('3000'/*年费*/ ,'8022'/*调帐-调减年费收入*/) THEN T.BILL_AMT ELSE 0 END ) AS NF_FEE_SUM -- 当月年费收入金额 
 ,SUM(CASE WHEN T.TRANS_TYPE IN ('4010','4060','4110','4120','4122','4160','4180','4182','4184','4310','4360','4410','4460','4480' ,'4482','4484')THEN T.BILL_AMT ELSE 0 END) AS ATM_FEE_SUM-- 当月ATM取款手续费收入金额
 ,SUM(CASE WHEN T.TRANS_TYPE IN ('3250','3252','8128','8030','8044') THEN T.BILL_AMT ELSE 0 END)  AS FQ_FEE_SUM  ------分期手续费
 ,SUM(CASE WHEN T.TRANS_TYPE IN ('3190') THEN T.BILL_AMT ELSE 0 END)AS GX_FEE_SUM -- 当月挂失手续费收入金额
 ,SUM(CASE WHEN T.TRANS_TYPE IN ('3120')THEN T.BILL_AMT ELSE 0 END) BK_FEE_SUM -- 当月补换卡手续费收入金额
 ,SUM(CASE WHEN T.TRANS_TYPE IN ('4000','4050','4100','4150','4300','4350','4400','4450')THEN T.BILL_AMT ELSE 0 END)AS GM_FEE_SUM -- 当月柜面取现手续费收入金额
 ,SUM(CASE WHEN T.TRANS_TYPE IN ('3066','3068','3110','3112','3120','3130','3140','3150','3160','3170','3180','3200','3202','3210','3230'
 ,'3242','3260','3262','3280','3510','3700','3800','4040','4042','4070','4072','4140','4170','4340','4342','4370','4372','4440','4470','8020','8172')THEN T.BILL_AMT ELSE 0 END)AS QT_FEE_SUM -- 当月其他手续费收入金额
 ,SUM(CASE WHEN T.TRANS_TYPE IN ('3242','4180','4182','4184','4480','4482','4484')THEN T.BILL_AMT ELSE 0 END)AS JW_FEE_SUM   -- 当月跨国交易服务费收入金额
 ,SUM(CASE WHEN T.TRANS_TYPE IN ('3500','8028')THEN T.BILL_AMT ELSE 0 END) AS ZNJ_FEE_SUM-- 当月滞纳金手续费收入金额
 ,SUM(CASE WHEN T.TRANS_TYPE IN ('4000','4010','4050','4060','4100','4110','4120','4122','4150','4160','4180','4182','4184','4300','4310','4350'
 ,'4360','4400','4410','4450','4460','4480','4482','4484') THEN T.BILL_AMT ELSE 0 END)AS QX_FEE_SUM   ---取现手续费
 FROM dw_sdata.CCB_000_EVENT T WHERE  SUBSTR(TO_CHAR(T.INP_DATE),1,6)=substr(to_char('$TXDATE'),1,6)  --此处限制当月“数据日期”
 GROUP BY T.CARD_NBR        --卡号
unsegmented all nodes
;               
delete from f_fdm.f_acct_crdt_card_info where etl_date = '$TXDATE'::date 
;
-- 加工临�

insert into TT_7
SELECT T.XACCOUNT,
       --T.BAL_NOINT,
       (T.STM_NOINT + T.STM_BALORI + T.BAL_CMPINT + T.MP_REM_PPL + MP_AUTHS + AUTHS_AMT + 
        GREATEST(decode(T.STMBALINTFLAG,'-',-1,1)*T.STM_BALINT+T.STM_BALFRE+T.BAL_FREE+decode(T.BALINTFLAG,'-',-1,1)*T.BAL_INT+
		         decode(T.STM_BALFLAG,'-',-1,1)*T.STM_BALMP,0)
		)*COALESCE(b.Exchg_Rate_Val::numeric,6)
		AS RMBSUM
  FROM dw_sdata.ccb_000_accx T 
left join f_fdm.f_fnc_Exchg_Rate b
on t.CURR_NUM=b.orgnl_cur_cd
and b.Convt_Cur_Cd='156'
and b.etl_date='$TXDATE'::date
where T.start_dt<='$TXDATE'::date 
and  '$TXDATE'::date<T.end_dt 
		
;

-- 插入目标表
INSERT INTO f_fdm.f_acct_crdt_card_info
	(grp_typ
	,ETL_Date
	,Card_Num
	,Pri_Card_Card_Num
	,Rmb_Acct_Num
	,Cust_Num
	,Actv_Ind
	,Chg_Card_Ind
	,Crdt_Lmt
	,Card_Typ_Cd
	,Pri_Sec_Card_Ind
	,Card_Stat_Cd
	,Issu_Card_Dt
	,Ltst_Actv_Dt
	,Pin_Card_Dt
	,Crdt_Card_Appl_Stat_Cd
	,Crdt_Card_Chk_Reasn_Cd
	,Fst_Swipe_Dt
	,Apprv_Dt
	,Aval_Lmt
	,CurMth_Cash_Amt --当月取现金额
	,CurMth_Consm_Amt  --当月消费金额
	,CurMth_Repay_Amt  --当月还款金额
	,CurMth_Annl_Fee_Incom_Amt --当月年费收入金额
	,CurMth_ATM_Draw_Comm_Fee_Incom_Amt --当月ATM取款手续费收入金额
	,CurMth_Amtbl_Pay_Comm_Fee_Incom_Amt --当月分期付款手续费收入金额
	,CurMth_Loss_Comm_Fee_Incom_Amt --当月挂失手续费收入金额
	,CurMth_Replm_Card_Comm_Fee_Incom_Amt --当月补换卡手续费收入金额
	,CurMth_Cntr_Cash_Comm_Fee_Incom_Amt
	,CurMth_Oth_Comm_Fee_Incom_Amt
	,CurMth_Cro_Cnr_TX_Serv_Cost_Incom_Amt
	,CurMth_Late_Chrg_Comm_Fee_Incom_Amt
	,CurMth_Oth_Cash_Comm_Fee_Incom_Amt
	,Prmt_Org_Cd
	,Sys_Src
	)
 select /*+ SYNTACTIC_JOIN */ 
        '1'
        ,'$TXDATE'::DATE
        ,COALESCE(T.CARD_NBR,'')
		    ,COALESCE(T.MASTER_NBR, '')
		    ,COALESCE(T.XACCOUNT,0)
		    ,COALESCE(T2.ECIF_CUST_NO,T2.CERT_NO)
		    ,CASE WHEN T.ACTIVE_DAY=0 THEN 0 ELSE 1 END 
		    ,CASE WHEN T.ISSUE_NBR<>0 THEN 1 ELSE 0 END
	   	,CASE WHEN T.CARDHOLDER = 1 OR( COALESCE(T3.TEMP_LIMIT,0) <> 0 AND $TXDATE >= T3.TLMT_BEG  AND $TXDATE < T3.TLMT_END )
		      THEN (COALESCE(T3.CRED_LIMIT,0) + COALESCE(T3.TEMP_LIMIT,0) + ABS(COALESCE(T3.CRED_LIMIT,0) - COALESCE(T3.TEMP_LIMIT,0)))/2 
			  ELSE COALESCE(T.CLIMIT,0)
		 END      AS Crdt_Lmt 
		,COALESCE(T.PRODUCT,0) 
		,CASE T.CARDHOLDER WHEN 1 THEN 1 ELSE 0 END   --1为主卡,0为附属卡
		,COALESCE(T.CANCL_CODE,'')
		,COALESCE(to_date(to_char(T.ISSUE_DAY),'yyyymmdd'),'$MINDATE'::DATE)
		,COALESCE(to_date(to_char(T.ACTIVE_DAY),'yyyymmdd'),'$MINDATE'::DATE)
		,COALESCE(to_date(to_char(T.CANCL_DAY),'yyyymmdd'),'$MINDATE'::DATE)
		,FIRST_VALUE(T5.DECCAN_CDE)OVER(ORDER BY T5.DECCAN_CDE DESC) 
    ,FIRST_VALUE(T5.DECCAN_REA)OVER(ORDER BY T5.DECCAN_REA DESC)
    ,COALESCE(to_date(to_char(T.MAILER_1ST),'yyyymmdd'),'$MINDATE'::DATE)
    ,COALESCE(to_date(to_char(FIRST_VALUE(T5.APPDEC_DAY)OVER(ORDER BY T5.APP_SEQ DESC)),'yyyymmdd'),'$MINDATE'::DATE)
		 ,GREATEST((CASE WHEN T.CARDHOLDER = 1 OR( COALESCE(T3.TEMP_LIMIT,0) <> 0 AND $TXDATE >= T3.TLMT_BEG  AND $TXDATE < T3.TLMT_END )
		      THEN (COALESCE(T3.CRED_LIMIT,0) + COALESCE(T3.TEMP_LIMIT,0) + ABS(COALESCE(T3.CRED_LIMIT,0) - COALESCE(T3.TEMP_LIMIT,0)))/2 
			  ELSE COALESCE(T.CLIMIT,0) END )-T3.RMBSUM - T7.WBSUM ,0)
		,COALESCE(T8.QXSUM,0)    as CurMth_Cash_Amt --当月取现金额
		,COALESCE(T8.XFSUM,0)    as CurMth_Consm_Amt  --当月消费金额
		,COALESCE(T8.HKSUM,0)    as CurMth_Repay_Amt  --当月还款金额
		,COALESCE(T8.NF_FEE_SUM,0) as CurMth_Annl_Fee_Incom_Amt --当月年费收入金额
		,COALESCE(T8.ATM_FEE_SUM,0) as CurMth_ATM_Draw_Comm_Fee_Incom_Amt --当月ATM取款手续费收入金额
		,COALESCE(T8.FQ_FEE_SUM,0)  as CurMth_Amtbl_Pay_Comm_Fee_Incom_Amt --当月分期付款手续费收入金额
		,COALESCE(T8.GX_FEE_SUM,0)  as CurMth_Loss_Comm_Fee_Incom_Amt --当月挂失手续费收入金额
		,COALESCE(T8.BK_FEE_SUM,0)  as CurMth_Replm_Card_Comm_Fee_Incom_Amt --当月补换卡手续费收入金额
		,COALESCE(T8.GM_FEE_SUM,0)
		,COALESCE(T8.QT_FEE_SUM,0)
		,COALESCE(T8.JW_FEE_SUM,0)
		,COALESCE(T8.ZNJ_FEE_SUM,0)
		,COALESCE(T8.QX_FEE_SUM,0) - COALESCE(T8.GM_FEE_SUM,0) - COALESCE(T8.ATM_FEE_SUM,0)
		,COALESCE(T9.BRH_CODE,'')
		,'CBS' 
  from dw_sdata.ccb_000_card T  --卡片资料表
  LEFT JOIN  dw_sdata.ccb_000_custr T1 /*+ PROJS('DW_SDATA.CCB_000_CUSTR_B0') */ 
    on T.CUSTR_NBR=T1.CUSTR_NBR
   and T1.start_dt<='$TXDATE'::date 
   and '$TXDATE'::date<T1.end_dt     
  LEFT JOIN  tt_2 T2
    ON T1.CUSTR_NBR=T2.CERT_NO 
   AND T1.SURNAME=T2.PARTY_NAME 
  LEFT JOIN /*+JTYPE(FM)*/ tt_6  T3
    ON T.BANK=T3.BANK 
   AND T.XACCOUNT=T3.XACCOUNT
  LEFT JOIN dw_sdata.ccb_000_apcd T4  
    ON T.CARD_NBR=T4.CARD_NBR
   AND T.CUSTR_NBR=T4.CUSTR_NBR 
   AND T4.start_dt<='$TXDATE'::date 
   AND '$TXDATE'::date<T4.end_dt
  LEFT JOIN dw_sdata.ccb_000_apma T5
    ON T4.APP_JDAY=T5.APP_JDAY
   AND T4.APP_SEQ=T5.APP_SEQ
   AND T5.DECCAN_CDE='A' 
   AND T5.start_dt<='$TXDATE'::date 
   AND  '$TXDATE'::date<T5.end_dt 
  LEFT JOIN TT_7 T7
    ON  T7.XACCOUNT=T3.XACCOUNT
  LEFT JOIN TT_8 T8
    ON  T.CARD_NBR=T8.CARD_NBR 
LEFT JOIN
       (SELECT DISTINCT COALESCE(T1.BRH_CODE,substr(trim(T.AB_PHONE),2)) AS BRH_CODE,T.APP_DAY, T.APP_SEQ 
        FROM dw_sdata.CCB_000_INTR T  --MODIFIED BY ZMX 20161014  
        LEFT JOIN dw_sdata.ogs_000_tbl_new_old_rel T1 
        ON substr(trim(T.AB_PHONE),2)=T1.BRH_SV_NEW_CODE 
        AND T1.sys_type='99700010000'
        AND T1.use_flag='02'
        AND T1.START_DT<='$TXDATE'::date --modified by lxz 20160928
        AND '$TXDATE'::date<T1.END_DT
        WHERE T.START_DT<='$TXDATE'::date
        AND  '$TXDATE'::date<T.END_DT) T9
ON  T5.APP_DAY=T9.APP_DAY 
AND T5.APP_SEQ=T9.APP_SEQ
WHERE T.start_dt<='$TXDATE'::date
AND '$TXDATE'::date <T.end_dt
;
/*数据处理区END*/

COMMIT;
