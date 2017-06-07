/*
Author             :刘东燕
Function           :中间业务收入信息表
Load method        :INSERT
Source table       :dw_sdata.acc_004_t_accdata_main,dw_sdata.acc_004_t_accdata_dtl,dw_sdata.cbs_001_jnl,dw_sdata.cbs_001_ammst_corp,dw_sdata.cbs_001_ammst_secu,f_fdm.f_dpst_indv_acct    
Destination Table  :f_evt_comfee_commsn  中间业务收入信息表
Frequency          :D
Modify history list:Created by刘东燕2016年5月04日16:05:55
                   :Modify  by LIUDONGYAN at 20160912 修改组别3的客户号的映射规则。见变更记录201
		   modified by liuxz 20160928 optimized T2&T3 into f_fdm.f_dpst_indv_acct_yht2t3
                   modified by liudongyan at 20160928 修��别 4 T2 表 
                   modified by liudongyan at 20161012 ����2�T2������
                   modified by zhangliang 20161006  删除临时表f_dpst_indv_acct_yh,并修改组2关联表t3
                   modified by ldy 20161018 修改组别3的 取数逻辑
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/
/*数据回退区*/
Delete /* +direct */  from  f_fdm.f_evt_comfee_commsn
where etl_date='$TXDATE'::date
;
/*数据回退区END*/
/*Temporary table area*/
create local temporary table if not exists  f_dpst_indv_acct_yh_grp2
on commit preserve rows as 
select * from f_fdm.f_dpst_indv_acct t3 where t3.AGMT_ID in 
(select (CASE
              WHEN  T2.FEE_GROUPQTY IN (0,1)  THEN T2.FEE_TX_ACCOUNT
              WHEN T2.FEE_GROUPQTY >1  THEN   SUBSTR(T2.FEE_TX_ACCOUNT,0,INSTR(T2.FEE_TX_ACCOUNT,'|'))
           END
           )
  from  dw_sdata.lcs_000_jnl_acc_chg t2
  where etl_dt = '$TXDATE'::DATE
)
AND T3.ETL_DATE = '$TXDATE'::DATE
order by AGMT_ID 
segmented by hash(AGMT_ID) all nodes 
;
create local temporary table if not exists f_dpst_indv_acct_yh_grp3 
on commit preserve rows as
select T3.etl_date,T3.cust_num as ECIF_CUST_NO,T3.CERT_NUM from f_fdm.f_cust_indv T3
where T3.CERT_NUM in (select T2.CUSTR_NBR from dw_sdata.ccb_000_acct T2);
/*Temporary table area END*/
/*数据处理区*/
INSERT INTO f_fdm.f_evt_comfee_commsn
(
           grp_typ                                                                           --组别
          ,etl_date                                                                          --数据日期
          ,TX_Dt                                                                             --交易日期
          ,Acct_Num                                                                          --账号
          ,Org_Num                                                                           --机构号
          ,Cur_Cd                                                                            --货币代码
          ,Chrg_Mode                                                                         --收费方式
          ,TX_Chnl                                                                           --交易渠道
          ,Cust_Num                                                                          --客户号
          ,Chrg_Proj                                                                         --收费项目
          ,Comm_Fee_Subj                                                                     --手续费科目
          ,Comm_Fee_Amt                                                                      --手续费金额
          ,TX_Cnt                                                                            --交易笔数
          ,Sys_Src                                                                           --系统来源
)
SELECT
            '1'                                                                                  as grp_typ
            ,'$TXDATE'::date                                                            as etl_date
            ,to_date(SUBSTR(T1.TRAN_TIME,1,8),'yyyymmdd')        as TX_Dt
            ,coalesce(T2.ACCOUNT,'')                                                             as Acct_Num
            ,coalesce(T.INST_NO,'')                                                              as Org_Num
            ,coalesce(T1.CURR_TYPE,'')                                                           as Cur_Cd
            ,''                                                                                  as Chrg_Mode
            ,coalesce(T2.CHNL_CODE,'')                                                           as TX_Chnl
            ,coalesce(corp.cust_id,secu.cust_id,indv.cust_id,'')                                                             as Cust_Num
            ,coalesce(T1.ENTRY_CODE,'')                                                          as Chrg_Proj
            ,coalesce(T.ITM_NO,'')                                                               as Comm_Fee_Subj
            ,coalesce(SUM(T.AMT),0)                                                              as Comm_Fee_Amt
            ,coalesce(COUNT(1),0)                                                                as TX_Cnt
            ,'CBS'                                                                               as Sys_Src
FROM     dw_sdata.acc_004_t_accdata_dtl             AS T --标准账务数据明细表
INNER JOIN dw_sdata.acc_004_t_accdata_main           AS T1--标准账务数据主表
ON       T.CLT_SEQNO=T1.CLT_SEQNO 
AND      T.ENTRY_SERNO=T1.ENTRY_SERNO
AND      T.ACC_DATE=T1.ACC_DATE
AND      T1.ETL_dt='$TXDATE'::date  
LEFT JOIN dw_sdata.cbs_001_jnl                      AS T2 --中心流水表
ON       T1.ACC_DATE=T2.CLR_DATE   
AND      LPAD(SUBSTR(T1.HOST_SEQNO,1,8),8,'0')=LPAD(TO_CHAR(T2.CEN_SERIAL_NO),8,'0')
AND     T2.etl_dt='$TXDATE'::date 
LEFT  JOIN 
     (SELECT T.ACCOUNT,T.CUST_ID,T.SUBACCT
         FROM dw_sdata.cbs_001_ammst_corp T
         WHERE T.start_dt<='$TXDATE'::date 
         AND   T.end_dt>'$TXDATE'::date  
      ) corp      
      ON       T2.ACCOUNT=corp.ACCOUNT
      AND      T2.SUBACCT=corp.SUBACCT
left join 
      (SELECT T.ACCOUNT,T.CUST_ID,T.SUBACCT  
        FROM dw_sdata.cbs_001_ammst_secu T
        WHERE T.start_dt<='$TXDATE'::date 
        AND   T.end_dt>'$TXDATE'::date ) secu  
      ON T2.ACCOUNT=secu.ACCOUNT 
      AND      T2.SUBACCT=secu.SUBACCT
left join 
      (SELECT T.AGMT_ID as ACCOUNT,T.CUST_NUM as CUST_ID,0 as SUBACCT  
         FROM f_fdm.f_dpst_indv_acct T
        where t.etl_date ='$TXDATE'::date
      ) indv  
      ON       T2.ACCOUNT=indv.ACCOUNT 
      AND      T2.SUBACCT=indv.SUBACCT
WHERE    (T.ITM_NO LIKE '5325%' OR T.ITM_NO LIKE  '5326%' OR SUBSTR(T.ITM_NO,1,2) IN ('54','55','56')) 
AND      T.SYSTEM_ID='99200000000' 
AND      T.ETL_dt='$TXDATE'::date 
GROUP BY SUBSTR(T1.TRAN_TIME,1,8)
,T2.ACCOUNT
,T.INST_NO
,T1.CURR_TYPE
,T2.CHNL_CODE
,coalesce(corp.cust_id,secu.cust_id,indv.cust_id,'')
,T1.ENTRY_CODE
,T.ITM_NO;
INSERT INTO f_fdm.f_evt_comfee_commsn
(
           grp_typ                                                                           --组别
          ,etl_date                                                                          --数据日期
          ,TX_Dt                                                                             --交易日期
          ,Acct_Num                                                                          --账号
          ,Org_Num                                                                           --机构号
          ,Cur_Cd                                                                            --货币代码
          ,Chrg_Mode                                                                         --收费方式
          ,TX_Chnl                                                                           --交易渠道
          ,Cust_Num                                                                          --客户号
          ,Chrg_Proj                                                                         --收费项目
          ,Comm_Fee_Subj                                                                     --手续费科目
          ,Comm_Fee_Amt                                                                      --手续费金额
          ,TX_Cnt                                                                            --交易笔数
          ,Sys_Src                                                                           --系统来源
)
SELECT
         '2'                                                                                as  grp_typ
         ,'$TXDATE'::date                                                          as etl_date
         ,TO_DATE(SUBSTR(T1.TRAN_TIME,1,8),'YYYYmmDD')                                     as TX_Dt
         ,(CASE  
                 WHEN  T2.FEE_GROUPQTY IN (0,1)  THEN T2.FEE_TX_ACCOUNT
                 WHEN  T2.FEE_GROUPQTY >1  THEN   SUBSTR(T2.FEE_TX_ACCOUNT,0,INSTR(T2.FEE_TX_ACCOUNT,'|'))  
           END
           )                                                                                as Acct_Num
         ,coalesce(T.INST_NO,'')                                                            as Org_Num
          ,coalesce(T1.CURR_TYPE,'')                                                        as Cur_Cd
          ,coalesce(T2.HAND_FEE_MODE,'')                                                    as Chrg_Mode
          ,''                                                                               as TX_Chnl
         ,coalesce(T3.Cust_Num,'')                                                          as Cust_Num
         ,coalesce(T1.ENTRY_CODE,'')                                                        as Chrg_Proj
         ,coalesce(T.ITM_NO,'')                                                             as Comm_Fee_Subj
         ,coalesce(SUM(T.AMT),0)                                                            as Comm_Fee_Amt
         ,coalesce(COUNT(1),0)                                                              as TX_Cnt
         ,'LCS'                                                                             as Sys_Src
FROM  dw_sdata.acc_004_t_accdata_dtl          AS T --标准账务数据明细表
INNER JOIN dw_sdata.acc_004_t_accdata_main     AS  T1 --标准账务数据主表
ON       T.ACC_DATA_ID=T1.ACC_DATA_ID 
AND      T.ENTRY_SERNO=T1.ENTRY_SERNO 
AND      T.ACC_DATE=T1.ACC_DATE
AND      T1.etl_dt='$TXDATE'::date
LEFT JOIN
        (select A.CEN_SERIAL_NO,A.CLI_SERIAL_NO,A.FEE_GROUPQTY,A.FEE_TX_ACCOUNT,A.HAND_FEE_MODE,A.CLR_DATE,A.etl_dt,ROW_NUMBER() OVER(PARTITION BY A.CEN_SERIAL_NO,A.CLI_SERIAL_NO ORDER BY A.TIME_STAMP DESC) AS NM
        FROM  dw_sdata.lcs_000_jnl_acc_chg A)     AS T2 --负债业务账务变动类
ON       T1.ACC_DATE=T2.CLR_DATE 
AND      T1.HOST_SEQNO = T2.CEN_SERIAL_NO
AND      T2.etl_dt='$TXDATE'::date
LEFT JOIN f_dpst_indv_acct_yh_grp2  AS T3 
ON  
         (CASE   
                WHEN  T2.FEE_GROUPQTY IN (0,1)  THEN T2.FEE_TX_ACCOUNT
                WHEN T2.FEE_GROUPQTY >1  THEN   SUBSTR(T2.FEE_TX_ACCOUNT,0,INSTR(T2.FEE_TX_ACCOUNT,'|'))  
           END
           ) =T3.AGMT_ID 
/*
left join f_fdm.f_dpst_indv_acct as t3    ---------------20161006 hp测试临时表没有明显效果
ON  
         (CASE   
                WHEN  T2.FEE_GROUPQTY IN (0,1)  THEN T2.FEE_TX_ACCOUNT
                WHEN T2.FEE_GROUPQTY >1  THEN   SUBSTR(T2.FEE_TX_ACCOUNT,0,INSTR(T2.FEE_TX_ACCOUNT,'|'))  
           END
           ) =T3.AGMT_ID
*/
and t3.etl_date='$TXDATE'::date
WHERE    (T.ITM_NO LIKE '5325%' OR T.ITM_NO LIKE  '5326%' OR SUBSTR(T.ITM_NO,1,2) IN ('54','55','56')) 
AND      T.SYSTEM_ID='99700010000' 
AND      T.etl_dt='$TXDATE'::date
GROUP BY SUBSTR(T1.TRAN_TIME,1,8)
 ,CASE   WHEN  T2.FEE_GROUPQTY IN (0,1)  THEN T2.FEE_TX_ACCOUNT
 WHEN T2.FEE_GROUPQTY >1  THEN   SUBSTR(T2.FEE_TX_ACCOUNT,0,INSTR(T2.FEE_TX_ACCOUNT,'|'))  END 
,T.INST_NO
,T1.CURR_TYPE
,T2.HAND_FEE_MODE
,T3.CUST_NUM
,T1.ENTRY_CODE
,T.ITM_NO 
;
 INSERT INTO f_fdm.f_evt_comfee_commsn
(
           grp_typ                                                                           --组别
          ,etl_date                                                                          --数据日期
          ,TX_Dt                                                                             --交易日期
          ,Acct_Num                                                                          --账号
          ,Org_Num                                                                           --机构号
          ,Cur_Cd                                                                            --货币代码
          ,Chrg_Mode                                                                         --收费方式
          ,TX_Chnl                                                                           --交易渠道
          ,Cust_Num                                                                          --客户号
          ,Chrg_Proj                                                                         --收费项目
          ,Comm_Fee_Subj                                                                     --手续费科目
          ,Comm_Fee_Amt                                                                      --手续费金额
          ,TX_Cnt                                                                            --交易笔数
          ,Sys_Src                                                                           --系统来源
)       

SELECT
       '3'                                                             as grp_typ
       ,'$TXDATE'::date                                       as etl_date
       ,coalesce(TO_DATE(TO_CHAR(T.REL_DAY),'YYYYmmDD'),'$MINDATE'::date)                       as TX_Dt
       ,coalesce(T.XACCOUNT,0)                                         as Acct_Num
       ,'1199523Q'                                                     as Org_Num
       ,coalesce(T.CURR_NUM,0)                                         as Cur_Cd
       ,''                                                             as Chrg_Mode
       ,coalesce(T.PIPE,'')                                            as TX_Chnl
       ,coalesce(T3.CUST_ID,'')                                   as Cust_Num
       ,coalesce(T.TRANS_TYPE,0)                                       as Chrg_Proj
       ,(CASE
        WHEN SUBSTR(BANKACCT,1,2) IN ('54','55','56') THEN BANKACC1 
        ELSE  BANKACCT 
        END
         )                                                             as Comm_Fee_Subj
        , coalesce(SUM(CASE WHEN T.O_R_FLAG='R' THEN -1.00*T.TRAN_AMT ELSE T.TRAN_AMT END),0.00)                              as Comm_Fee_Amt
        ,coalesce(COUNT(1),0)                                          as TX_Cnt
        ,'CCB'                                                         as Sys_Src 
FROM     dw_sdata.ccb_000_jorj  AS  T  --会计分录明细表                              
LEFT JOIN dw_sdata.ccb_000_acct T2	
ON      T.XACCOUNT=T2.XACCOUNT 
AND     T2.start_dt<='$TXDATE'::date 
AND     T2.end_dt>'$TXDATE'::date 
LEFT JOIN F_FDM.F_ACCT_CRDT_INFO  T3
ON  T.XACCOUNT=T3.AGMT_ID  
AND T3.CUR_CD='156'
AND T3.ETL_DATE='$TXDATE'::date
      -- (SELECT T.ECIF_CUST_NO,T.CERT_NO,start_dt,end_dt FROM dw_sdata.ecf_001_t01_cust_info T
       --UNION
       --SELECT  T.ECIF_CUST_NO,T. CERT_NO,start_dt,end_dt  FROM dw_sdata.ecf_004_t01_cust_info T) T3
--ON       T2.CUSTR_NBR = T3.CERT_NO  
--AND      T3.start_dt<='$TXDATE'::date 
--AND      T3.end_dt>'$TXDATE'::date 
WHERE    T.TRANS_TYPE IN ('8128','8172','8030','8022','8020','4484','4482','4480','4470','4460','4450','4440','4410'
,'4400','4372','4370','4360','4350','4342','4340','4310','4300','4184','4182','4180','4170','4160','4150','4140','4122','4120','4110','4100','4072'
,'4070','4060','4050','4042','4040','4010','4000','3800','3700','3510','3500','3280','3262','3260','3252','3250','3242','3230','3210','3202','3200'
,'3190','3180','3170','3160','3150','3140','3130','3120','3112','3110','3068','3030','3020','3010','3000','3068') 
AND     T.etl_dt='$TXDATE'::date 
GROUP BY T.REL_DAY
,T.XACCOUNT
,T.CURR_NUM
,T.PIPE
,T3.CUST_ID
,T.TRANS_TYPE
,CASE WHEN SUBSTR(BANKACCT,1,2) IN ('54','55','56') THEN BANKACC1 ELSE BANKACCT END 
;
INSERT INTO f_fdm.f_evt_comfee_commsn
(
           grp_typ                                                                           --组别
          ,etl_date                                                                          --数据日期
          ,TX_Dt                                                                             --交易日期
          ,Acct_Num                                                                          --账号
          ,Org_Num                                                                           --机构号
          ,Cur_Cd                                                                            --货币代码
          ,Chrg_Mode                                                                         --收费方式
          ,TX_Chnl                                                                           --交易渠道
          ,Cust_Num                                                                          --客户号
          ,Chrg_Proj                                                                         --收费项目
          ,Comm_Fee_Subj                                                                     --手续费科目
          ,Comm_Fee_Amt                                                                      --手续费金额
          ,TX_Cnt                                                                            --交易笔数
          ,Sys_Src                                                                           --系统来源
)          

SELECT
          '4'                                                        as grp_typ
          ,'$TXDATE'::date                                  as etl_date
          ,TO_DATE(SUBSTR(T1.TRAN_TIME,1,8),'YYYYmmDD')              as TX_Dt
          ,coalesce(T2.ACCT_NO,'')                                   as Acct_Num
          ,coalesce(T.INST_NO,'')                                    as Org_Num
          ,coalesce(T1.CURR_TYPE,'')                                 as Cur_Cd
          ,''                                                        as Chrg_Mode
          ,''                                                        as TX_Chnl
          ,coalesce(T3.CUSTOMER_ID,'')                               as Cust_Num
          ,coalesce(T1.ENTRY_CODE,'')                                as Chrg_Proj
          ,coalesce(T.ITM_NO,'')                                     as Comm_Fee_Subj
          ,coalesce(SUM(T.AMT),0)                                    as Comm_Fee_Amt
          ,coalesce(COUNT(1),0)                                      as TX_Cnt
          ,'GTS'                                                     as Sys_Src 
FROM     dw_sdata.acc_004_t_accdata_dtl          AS T --标准账务数据明细表
INNER JOIN dw_sdata.acc_004_t_accdata_main       AS T1 --标准账务数据主表	
ON        T.ACC_DATA_ID=T1.ACC_DATA_ID 
AND       T.ENTRY_SERNO=T1.ENTRY_SERNO 
AND       T.ACC_DATE=T1.ACC_DATE
AND       T1.etl_dt='$TXDATE'::date
LEFT JOIN 
        (select ACCT_NO,CLR_DATE,ID,ETL_DT
        from dw_sdata.gts_000_cpds_flow
        where etl_dt='$TXDATE'::date
        union
       select ACCT_NO,CLR_DATE,ID,ETL_DT 
        from dw_sdata.gts_000_his_cpds_flow
        where etl_dt='$TXDATE'::date)    AS T2
ON       T2.CLR_DATE=T.ACC_DATE 
AND      T2.ID=T1.HOST_SEQNO
AND      T2.ACCT_NO IS NOT NULL
AND      T2.ACCT_NO<>''
AND      T2.etl_dt='$TXDATE'::date
LEFT JOIN dw_sdata.gts_000_t_pim_customer_info   AS T3
ON        T3.GOLD_EXCH_NO=T2.ACCT_NO
AND       T3.start_dt<='$TXDATE'::date
AND       T3.end_dt>'$TXDATE'::date 
WHERE    (T.ITM_NO LIKE '5325%' OR T.ITM_NO LIKE  '5326%' OR SUBSTR(T.ITM_NO,1,2) IN ('54','55','56')) 
AND       T.SYSTEM_ID='99550000000' 
AND       T.etl_dt='$TXDATE'::date
GROUP BY SUBSTR(T1.TRAN_TIME,1,8)
,T2.ACCT_NO
,T.INST_NO
,T1.CURR_TYPE
,T3.CUSTOMER_ID
,T1.ENTRY_CODE
,T.ITM_NO  
;
INSERT INTO f_fdm.f_evt_comfee_commsn
(
           grp_typ                                                                           --组别
          ,etl_date                                                                          --数据日期
          ,TX_Dt                                                                             --交易日期
          ,Acct_Num                                                                          --账号
          ,Org_Num                                                                           --机构号
          ,Cur_Cd                                                                            --货币代码
          ,Chrg_Mode                                                                         --收费方式
          ,TX_Chnl                                                                           --交易渠道
          ,Cust_Num                                                                          --客户号
          ,Chrg_Proj                                                                         --收费项目
          ,Comm_Fee_Subj                                                                     --手续费科目
          ,Comm_Fee_Amt                                                                      --手续费金额
          ,TX_Cnt                                                                            --交易笔数
          ,Sys_Src                                                                           --系统来源
)         

SELECT
         '5'                                                      as grp_typ
         ,'$TXDATE'::date                                as etl_date
         ,coalesce(TO_DATE(T1.TX_TIMESTAMP,'YYYYmmDD'),'$MINDATE'::date)            as TX_Dt
         ,coalesce(T1.FEE_TX_ACCOUNT,'')                          as Acct_Num
         ,coalesce(T.ASSG_INST_NO,'')                             as Org_Num
         ,coalesce(T.FEE_CURR_TYPE,'')                            as Cur_Cd
         ,coalesce(T1.HAND_FEE_MODE,'')                           as Chrg_Mode
         ,coalesce(T.CHAN_CODE,'')                                as TX_Chnl
         ,''                                                      as Cust_Num
         ,coalesce(T.FEE_TYPE,'')                                 as Chrg_Proj
         ,''                                                      as Comm_Fee_Subj
         ,coalesce(SUM(T.ASSG_AMT),0)                             as Comm_Fee_Amt
         ,coalesce(COUNT(1),0)                                    as TX_Cnt
         ,'BLP'                                                   as Sys_Src 
FROM      dw_sdata.blp_000_clr_fee_detail   AS T --手续费处理明细信息表
LEFT JOIN dw_sdata.blp_000_clr_fee_aggre   AS T1 --手续费归集汇总表
ON       T.CLR_FEE_AGGRE_ID=T1.CLR_FEE_AGGRE_ID
AND      T1.etl_dt='$TXDATE'::date
LEFT JOIN dw_sdata.acc_004_t_accdata_fee_rule   AS T2 --手续费核算规则登记簿
ON       T.FEE_TYPE=T2.FEE_CODE
AND      T2.start_dt<='$TXDATE'::date
AND      T2.end_dt>'$TXDATE'::date
WHERE    T.etl_dt='$TXDATE'::date
GROUP BY TO_DATE(T1.TX_TIMESTAMP,'YYYYmmDD'),
T1.FEE_TX_ACCOUNT ,
T.ASSG_INST_NO,
T.FEE_CURR_TYPE,
T1.HAND_FEE_MODE ,
T.CHAN_CODE ,
T.FEE_TYPE
;
INSERT INTO f_fdm.f_evt_comfee_commsn
(
           grp_typ                                                                           --组别
          ,etl_date                                                                          --数据日期
          ,TX_Dt                                                                             --交易日期
          ,Acct_Num                                                                          --账号
          ,Org_Num                                                                           --机构号
          ,Cur_Cd                                                                            --货币代码
          ,Chrg_Mode                                                                         --收费方式
          ,TX_Chnl                                                                           --交易渠道
          ,Cust_Num                                                                          --客户号
          ,Chrg_Proj                                                                         --收费项目
          ,Comm_Fee_Subj                                                                     --手续费科目
          ,Comm_Fee_Amt                                                                      --手续费金额
          ,TX_Cnt                                                                            --交易笔数
          ,Sys_Src                                                                           --系统来源
)       
SELECT
     
         '6'                                                        as grp_typ
         ,'$TXDATE'::date                                  as etl_date
         ,to_date(SUBSTR(T1.TRAN_TIME,1,8),'YYYYmmDD')              as TX_Dt
         ,coalesce(T2.FEETXACCOUNT,'')                              as Acct_Num
         ,coalesce(T.INST_NO,'')                                    as Org_Num
         ,coalesce(T1.CURR_TYPE,'')                                 as Cur_Cd
         ,coalesce(T2.HANDFEEMODE,'')                               as Chrg_Mode
         ,''                                                        as TX_Chnl
         ,coalesce(T5.ECIFCUSTOMERNO,'')                            as Cust_Num
         ,coalesce(T2.FEETYPE,'')                                   as Chrg_Proj
         ,coalesce(T.ITM_NO,'')                                     as Comm_Fee_Subj
         ,coalesce(SUM(T.AMT),0)                                    as Comm_Fee_Amt
         ,coalesce(COUNT(1),0)                                      as TX_Cnt
         ,'FSS'                                                     as Sys_Src 
 FROM   dw_sdata.acc_004_t_accdata_dtl     AS T --标准账务数据明细表                                        
INNER JOIN dw_sdata.acc_004_t_accdata_main  AS T1 --标准账务数据主表
ON       T.CLT_SEQNO=T1.CLT_SEQNO 
AND      T.ENTRY_SERNO=T1.ENTRY_SERNO 
AND      T.ACC_DATE=T1.ACC_DATE
AND      T1.etl_dt='$TXDATE'::date
LEFT JOIN dw_sdata.fss_001_fd_acctchangetemp  AS T2 --资金结算明细登记簿流水表
ON       T1.HOST_SEQNO=T2.CENSERIALNO 
AND      T1.CLT_SEQNO=T2.CLISERIALNO
AND      T2.etl_dt='$TXDATE'::date
LEFT JOIN dw_sdata.fss_001_fd_checklist  AS  T3 --明细对账表	
ON       T2.SYSSERIALNO=T3.EXTERNSERIAL 
AND      T2.CLRDATE=T3.EXTERNDATE
AND      T3.etl_dt='$TXDATE'::date
LEFT JOIN dw_sdata.fss_001_fd_fee T4--手续费表	
ON       T3.ORGANCODE=T4.ORGANCODE 
AND      T3.OPERCODE=T4.OPERCODE 
AND      T3.LIQUIDATE=T4.LIQUIDATE 
AND      T3.ACCTSERIAL=T4.ACCTSERIAL
AND      T4.etl_dt='$TXDATE'::date
LEFT JOIN dw_sdata.fss_001_fd_customerinfo AS T5	--客户基本资料表
ON       T4.CUSTOMERID=T5.CUSTOMERID  
and      T5.start_dt<='$TXDATE'::date
AND      T5.end_dt>'$TXDATE'::date
WHERE    (T.ITM_NO LIKE '5325%' OR T.ITM_NO LIKE  '5326%' OR SUBSTR(T.ITM_NO,1,2) IN ('54','55','56')) 
AND      T.SYSTEM_ID='99370000000' 
AND      T.etl_dt<='$TXDATE'::date
GROUP BY SUBSTR(T1.TRAN_TIME,1,8)
,T2.FEETXACCOUNT
,T.INST_NO
,T1.CURR_TYPE
,T2.HANDFEEMODE
,T5.ECIFCUSTOMERNO
,T2.FEETYPE
,T.ITM_NO 
;
 /*数据回退区END*/                   
commit;
