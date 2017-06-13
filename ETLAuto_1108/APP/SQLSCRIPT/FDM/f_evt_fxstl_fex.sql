/*
Author             :XMH
Function           :结售汇及外汇买卖交易信息表
Load method        :INSERT
Source table       :dw_sdata.iss_001_fi_dealspfeinfo,dw_sdata.iss_001_bu_transactioninfo,dw_sdata.ogs_000_tbl_new_old_rel,dw_sdata.iss_001_fi_exchangedeliveryinfo,dw_sdata.iss_001_fi_remoteexchangeinfo,dw_sdata.Ibs_001_T_Feb_Exch_Rel,DW_SDATA.ecf_001_t01_cust_info,DW_SDATA.ecf_004_t01_cust_info
Frequency          :D
Modify history list:Created by徐铭浩2016年5月13日10:05:55
                   :Modify  by徐铭浩20160811 第一组第二组T2:ogs_000_tbl_new_old_rel并且关联条件加上：
                   T1.TRANSACTORGNO = T2.BRH_SV_NEW_CODE and T2.sys_type='99700010000' 
                  modified bu liudongyan 组别3的主表拉链改为流水
                  modified by zmx 1.字段买入币种，买入金额，卖出币种，卖出金额的字段值与字段不对应2.删掉left join t3内容 3,修改null为''4,添加买入币种，卖出币种需转换 
                  modified by zmx 删除临时表TT_TEMP 20160920
                  modified by wyh at 20160925 处理组别3下交易流水号的取数规则
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
*-------------------------------------------逻辑说明END------------------------------------------
*/

/* 临时表创建区 */
 /*
CREATE LOCAL TEMP TABLE TT_TEMP 
on commit preserve rows  
AS 
            SELECT T.SELL_PRC,
                   T1.MID_PRC,
                   T.TRAN_TIME,
                   T1.EFFECT_TIME,
                   ROW_NUMBER()OVER(PARTITION BY T.SELL_PRC ORDER BY T1.EFFECT_TIME DESC) NUM,
                   T.etl_dt
              FROM DW_SDATA.ibs_001_t_feb_exch_reg T 
         LEFT JOIN DW_SDATA.ibs_001_t_srm_prc T1 
                ON T.SELL_PRC=T1.CURR_TYPE 
               AND T.TRAN_DATE=T1.EFFECT_DATE
               AND T.TRAN_TIME=T1.EFFECT_TIME
               AND T1.start_dt<='$TXDATE'::date 
               and '$TXDATE'::date<T1.end_dt
             WHERE T.TYPE='04'
               AND T.etl_dt='$TXDATE'::date 
;
*/
/* 临时表创建区 end */

/*数据回退区*/
Delete  from  f_fdm.f_evt_fxstl_fex
where etl_date='$TXDATE'::date
--truncate table f_fdm.f_evt_fxstl_fex ;
;
/*数据回退区end*/

/*数据处理区 */
insert into f_fdm.f_evt_fxstl_fex
(
        etl_date,                             --数据日期            
        grp_typ,                              --组别        
        TX_Dt,                                --交易日期        
        Ent_Acct_Acct_Num,                    --入账账号        
        Out_Acct_Acct_Num,                    --出账账号        
        TX_Org,                               --交易机构        
        Cust_Num,                             --客户号          
        Prod_Cd,                              --产品代码        
        TX_Seq_Num,                           --交易流水号      
        TX_Typ,                               --交易类型        
        Buy_Cur,                              --买入币种        
        Buy_Amt,                              --买入金额        
        Sell_Cur,                             --卖出币种        
        Sell_Amt,                             --卖出金额        
        Exchg_Rate,                           --汇率            
        Buy_Amt_To_Rmb,                       --买入金额折人民币
        Sell_Amt_To_Rmb,                      --卖出金额折人民币
        Biz_Prc_Diff,                         --买卖价差        
        Sight_Fwd_Ind,                        --即远期标志      
        TX_Chnl_Cd,                           --交易渠道代码    
        Sys_Src                               --系统来源        
)
select 
        '$TXDATE' :: date                                                  as   etl_date,                   
        '1'                                                                         as   grp_typ,                   
        case when T.TRADEDATE is null then T.TRADEDATE::date 
             else T.TRADEDATE::date
        end   as   TX_Dt,                     
        coalesce(T.TOACCTNO,'')                                                     as   Ent_Acct_Acct_Num,         
        coalesce(T.FROMACCTNO ,'')                                                  as   Out_Acct_Acct_Num,         
        COALESCE(T2.BRH_CODE,T1.TRANSACTORGNO)                                      as   TX_Org,                    
        coalesce(T.TRADEOBJNO ,'')                                                  as   Cust_Num,                  
        coalesce(T.SPFLAG  ,'')                                                     as   Prod_Cd,                   
        coalesce(T.TXNSERIALNO,'')                                                  as   TX_Seq_Num,                
        coalesce(T.TRADETYPE ,'')                                                   as   TX_Typ,                    
        coalesce(T3.TGT_CD,'@'||T.TOCUR)                                             as   Buy_Cur,      --zmx  20160909             
        coalesce(T.TOAMT,0)                                                       as   Buy_Amt,      --zmx             
        coalesce(T4.TGT_CD,'@'||T.FROMCUR)                                        as   Sell_Cur,   --zmx               
        coalesce(T.FROMAMT,0)                                                         as   Sell_Amt,   --zmx               
        coalesce(T.EXCHANGERATE ,0)                                                 as   Exchg_Rate,                
        CASE WHEN T.SPFLAG=0 THEN T.FROMAMT*T.EXCHANGERATE
             WHEN T.SPFLAG=1 THEN T.FROMAMT
             WHEN T.SPFLAG=2 THEN T.TOAMT*T.EXCHANGERATE
        END    as   Buy_Amt_To_Rmb,            
        CASE WHEN T.SPFLAG=0 THEN T.FROMAMT*T.EXCHANGERATE
             WHEN T.SPFLAG=1 THEN T.FROMAMT
             WHEN T.SPFLAG=2 THEN T.FROMAMT*T.EXCHANGERATE
        END    as   Sell_Amt_To_Rmb,           
        CASE WHEN T.SPFLAG=0 THEN T.FROMAMT*T.EXCHANGERATE
             WHEN T.SPFLAG=1 THEN T.FROMAMT
             WHEN T.SPFLAG=2 THEN T.TOAMT*T.EXCHANGERATE
        END
         -
         CASE WHEN T.SPFLAG=0 THEN T.FROMAMT*T.EXCHANGERATE
         WHEN T.SPFLAG=1 THEN T.FROMAMT
         WHEN T.SPFLAG=2 THEN T.FROMAMT*T.EXCHANGERATE
         END                                                                   as   Biz_Prc_Diff,              
        '0'                                                                 as   Sight_Fwd_Ind,             
        '01'                                                                 as   TX_Chnl_Cd,                
        'ISS'                                                                  as   Sys_Src                    
 from dw_sdata.iss_001_fi_dealspfeinfo T
 LEFT JOIN dw_sdata.iss_001_bu_transactioninfo  T1
        on T.TXNSERIALNO = T1.TXNSERIALNO
        AND T1.start_dt<='$TXDATE'::date and '$TXDATE'::date<T1.end_dt
 LEFT JOIN dw_sdata.ogs_000_tbl_new_old_rel T2
        on T1.TRANSACTORGNO = T2.BRH_SV_NEW_CODE 
        and T2.sys_type='99700010000'
       and t2.use_flag='02'
        AND T2.start_dt<='$TXDATE'::date and '$TXDATE'::date<T2.end_dt
 left join F_FDM.cd_RF_STD_CD_TRAN_REF T3 
ON T.TOCUR = T3.Src_Cd
AND  T3.DATA_PLTF_SRC_TAB_NM = 'ISS_001_FI_DEALSPFEINFO'
AND  T3.Data_Pltf_Src_Fld_Nm = 'TOCUR'
left join F_FDM.cd_RF_STD_CD_TRAN_REF T4 
ON T.FROMCUR = T4.Src_Cd
AND  T4.DATA_PLTF_SRC_TAB_NM = 'ISS_001_FI_DEALSPFEINFO'
AND  T4.Data_Pltf_Src_Fld_Nm = 'FROMCUR' 
WHERE T.etl_dt='$TXDATE'::date;



/*zhushi */

insert into f_fdm.f_evt_fxstl_fex
(
        etl_date,                                 --数据日期            
        grp_typ,                                  --组别        
        TX_Dt,                                    --交易日期        
        Ent_Acct_Acct_Num,                        --入账账号        
        Out_Acct_Acct_Num,                        --出账账号        
        TX_Org,                                   --交易机构        
        Cust_Num,                                 --客户号          
        Prod_Cd,                                  --产品代码        
        TX_Seq_Num,                               --交易流水号      
        TX_Typ,                                   --交易类型        
        Buy_Cur,                                  --买入币种        
        Buy_Amt,                                  --买入金额        
        Sell_Cur,                                 --卖出币种        
        Sell_Amt,                                 --卖出金额        
        Exchg_Rate,                               --汇率            
        Buy_Amt_To_Rmb,                           --买入金额折人民币
        Sell_Amt_To_Rmb,                          --卖出金额折人民币
        Biz_Prc_Diff,                             --买卖价差        
        Sight_Fwd_Ind,                            --即远期标志      
        TX_Chnl_Cd,                               --交易渠道代码    
        Sys_Src                                   --系统来源        
)
select 
        '$TXDATE' :: date                                 as   etl_date,                   
        '2'                                                        as   grp_typ,                   
        case when T.deliveryovertime is null then '$MINDATE'::date else T.deliveryovertime::date  end as   TX_Dt,                     
        coalesce(T.INCREDITNO1  ,'')                               as   Ent_Acct_Acct_Num,         
        coalesce(T.OUTCREDITNO1 ,'')                               as   Out_Acct_Acct_Num,         
        COALESCE(T2.BRH_CODE,T1.TRANSACTORGNO)                      as   TX_Org,                    
        coalesce(T.CLIENTNO,'')             as   Cust_Num,                  
        CASE WHEN T.MATURETYPE =0 THEN '0' WHEN T.MATURETYPE =1 THEN '1' END                                 as   Prod_Cd,    --zmx 20160909               
        coalesce(T.TXNSERIALNO,'')          as   TX_Seq_Num,                
        ''                                 as   TX_Typ,                    
       coalesce(T3.TGT_CD,'@'||T.INCREDITCUR)            as   Buy_Cur,                   
        coalesce(T.INCREDITAMT1+T.INCREDITAMT2,0)                 as   Buy_Amt,                   
       coalesce(T4.TGT_CD,'@'||T.OUTCREDITCUR)                                 as   Sell_Cur,                  
        coalesce(T.OUTCREDITAMT1+T.OUTCREDITAMT2 ,0)              as   Sell_Amt,                  
        coalesce(T.CONTRACTRATE,0)                                 as   Exchg_Rate,                
        CASE T.MATURETYPE WHEN 0 THEN T.OUTCREDITAMT1 WHEN 1 THEN T.INCREDITAMT1 
         END   as   Buy_Amt_To_Rmb,            
        CASE T.MATURETYPE WHEN 0 THEN T.OUTCREDITAMT1 WHEN 1 THEN T.INCREDITAMT2 
         END   as   Sell_Amt_To_Rmb,           
        CASE T.MATURETYPE WHEN 0 THEN T.OUTCREDITAMT1 WHEN 1 THEN T.INCREDITAMT1 
         END 
           -
         CASE T.MATURETYPE WHEN 0 THEN T.OUTCREDITAMT1 WHEN 1 THEN T.INCREDITAMT2 END 
          as   Biz_Prc_Diff,              
        '1'                                           as   Sight_Fwd_Ind,             
        '01'                                           as   TX_Chnl_Cd,                
        'ISS'                                            as   Sys_Src                    
from dw_sdata.iss_001_fi_exchangedeliveryinfo T  
 LEFT JOIN dw_sdata.iss_001_bu_transactioninfo T1
      on T.TXNSERIALNO = T1.TXNSERIALNO
     AND T1.start_dt<='$TXDATE'::date 
     and '$TXDATE'::date<T1.end_dt
 LEFT JOIN dw_sdata.ogs_000_tbl_new_old_rel T2
     on T1.TRANSACTORGNO = T2.BRH_SV_NEW_CODE 
     and T2.sys_type='99700010000'
     and t2.use_flag='02'
     AND T2.start_dt<='$TXDATE'::date 
     and '$TXDATE'::date<T2.end_dt
left join F_FDM.cd_RF_STD_CD_TRAN_REF T3 
ON T.INCREDITCUR = T3.Src_Cd
AND  T3.DATA_PLTF_SRC_TAB_NM = 'ISS_001_FI_EXCHANGEDELIVERYINFO'
AND  T3.Data_Pltf_Src_Fld_Nm = 'INCREDITCUR'
left join F_FDM.cd_RF_STD_CD_TRAN_REF T4 
ON T.OUTCREDITCUR = T4.Src_Cd
AND  T4.DATA_PLTF_SRC_TAB_NM = 'ISS_001_FI_EXCHANGEDELIVERYINFO'
AND  T4.Data_Pltf_Src_Fld_Nm = 'OUTCREDITCUR'
 WHERE T.start_dt<='$TXDATE'::date
     and '$TXDATE'::date<T.end_dt;
/* zhushi   */

insert into f_fdm.f_evt_fxstl_fex
(
        etl_date,                                --数据日期            
        grp_typ,                                 --组别        
        TX_Dt,                                   --交易日期        
        Ent_Acct_Acct_Num,                       --入账账号        
        Out_Acct_Acct_Num,                       --出账账号        
        TX_Org,                                  --交易机构        
        Cust_Num,                                --客户号          
        Prod_Cd,                                 --产品代码        
        TX_Seq_Num,                              --交易流水号      
        TX_Typ,                                  --交易类型        
        Buy_Cur,                                 --买入币种        
        Buy_Amt,                                 --买入金额        
        Sell_Cur,                                --卖出币种        
        Sell_Amt,                                --卖出金额        
        Exchg_Rate,                              --汇率            
        Buy_Amt_To_Rmb,                          --买入金额折人民币
        Sell_Amt_To_Rmb,                         --卖出金额折人民币
        Biz_Prc_Diff,                            --买卖价差        
        Sight_Fwd_Ind,                           --即远期标志      
        TX_Chnl_Cd,                              --交易渠道代码    
        Sys_Src                                  --系统来源        
)
select 
       '$TXDATE' :: date                       as   etl_date,                   
        '3'                                             as   grp_typ,                   
        case when substr(T.TRAN_TIME,1,8) is null then '$MINDATE'::date   else substr(T.TRAN_TIME,1,8) ::date  end as   TX_Dt,                     
        coalesce(T.IN_ACC        ,'')                    as   Ent_Acct_Acct_Num,         
        coalesce(T.OUT_ACC       ,'')                    as   Out_Acct_Acct_Num,         
        coalesce(T.TRAN_INST     ,'')                    as   TX_Org,                    
     -- coalesce(T1.ECIF_CUST_NO ,'')                    as   Cust_Num,                  
        coalesce(T1.cust_num     ,'')                    as   Cust_Num,
        coalesce(T.TYPE          ,'')                    as   Prod_Cd,                   
        substr(T.TRAN_TIME,1,8)||'_'||coalesce(T.CLT_SEQNO     ,'')                    as   TX_Seq_Num,                
        coalesce(T.TRAN_NO       ,'')                    as   TX_Typ,                    
        coalesce(T.BUY_CURR_TYPE ,'')                    as   Buy_Cur,                   
        coalesce(T.BUY_AMT       ,0)                     as   Buy_Amt,                   
        coalesce(T.SELL_CURR_TYPE,'')                    as   Sell_Cur,                  
        coalesce(T.SELL_AMT      ,0)                     as   Sell_Amt,    
        coalesce(T.BUY_PRC     ,0)/DECODE(T.SELL_PRC,0,1)as   Exchg_Rate,                
        CASE WHEN T.TYPE = '01'  --结汇
             THEN T.BUY_AMT*(T.BUY_PRC/T.SELL_PRC)
             WHEN T.TYPE IN ('02','03')  --售汇
             THEN T.BUY_AMT
             WHEN T.TYPE= '04'   --外汇买卖
             THEN T.BUY_AMT
             END                                         as   Buy_Amt_To_Rmb,            
        CASE WHEN T.TYPE = '01'  --结汇
             THEN T.SELL_AMT
             WHEN T.TYPE IN ('02','03')  --售汇
             THEN T.SELL_AMT*(T.SELL_PRC/T.BUY_PRC)
             WHEN T.TYPE= '04'   --外汇买卖
             THEN T.SELL_AMT
             END                                         as   Sell_Amt_To_Rmb,           
          CASE WHEN T.TYPE = '01'  --结汇
             THEN T.BUY_AMT*(T.BUY_PRC/T.SELL_PRC)
             WHEN T.TYPE IN ('02','03')  --售汇
             THEN T.BUY_AMT
             WHEN T.TYPE= '04'   --外汇买卖
             THEN T.BUY_AMT
             END           --买入金额折人民币-卖出金额折人民      
             -
              CASE WHEN T.TYPE = '01'  --结汇
             THEN T.SELL_AMT
             WHEN T.TYPE IN ('02','03')  --售汇
             THEN T.SELL_AMT*(T.SELL_PRC/T.BUY_PRC)
             WHEN T.TYPE= '04'   --外汇买卖
             THEN T.SELL_AMT
             END       -- 卖出金额折人民                      
                                                          as   Biz_Prc_Diff,              
        '0'                                               as   Sight_Fwd_Ind,             
        CASE SUBSTR(T.CLT_SEQNO,0,2) WHEN '70' THEN '12'  WHEN '29' THEN '99' ELSE '01' END                    as   TX_Chnl_Cd,                
        'IBS'                                             as   Sys_Src                    
from dw_sdata.Ibs_001_T_Feb_Exch_Reg T
LEFT JOIN 
f_fdm.f_cust_indv t1
on t1.Cert_Typ_Cd = T.PAPER_TYPE
and T1.Cert_Num = T.paper_no
AND T1.etl_date = '$TXDATE'::date      
-- left join TT_TEMP T2
--        on T.SELL_PRC=T2.SELL_PRC 
--       AND T2.NUM=1 
     where T.STAT='0'
       and T.etl_dt='$TXDATE'::date
;
/* 数据处理区end */
commit;



