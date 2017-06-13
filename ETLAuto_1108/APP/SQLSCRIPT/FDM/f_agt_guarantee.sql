/*
Author             :XMH
Function           :保函信息表
Load method        :INSERT
Source table       :dw_sdata.iss_001_im_lgissueinfo,DW_SDATA.iss_001_bu_transactioninfo,DW_SDATA.ogs_000_tbl_new_old_rel,DW_SDATA.ISS_001_BU_DEPUSED,F_FDM.CD_RF_STD_CD_TRAN_REF
Modify history list:Created by徐铭浩2016年5月5日10:05:55
                   :Modify  by徐铭浩2016年7月15日11:28:05
                   :Modify  by xsh 20160715 将coalesce(T.LGTYPE,'') as Prod_Cd 修改为coalesce(T.LGTYPE ::varchar,'') as Prod_Cd 
                    Modify  by徐铭浩20160726 T2: DW_SDATA.iss_001_PA_ORG 
                    Modify  by徐铭浩20160811 T2: DW_SDATA.ogs_000_tbl_new_old_rel
                    Modify by liudongyan20160901 增加T2.use_flag='02' 
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
*-------------------------------------------逻辑说明END------------------------------------------
*/

/*数据回退区*/
Delete /* +direct */ from  f_fdm.f_agt_Guarantee
where etl_date='$TXDATE'::date
/*数据回退区end*/
;
/*数据处理区*/
INSERT INTO  f_fdm.f_agt_Guarantee
(       
        etl_date                  -- 数据日期        
        ,grp_typ                  -- 组别     
        ,Agmt_Id                  -- 协议编号    
        ,Cust_Num                 -- 客户号      
        ,Org_Num                  -- 机构号      
        ,Cur_Cd                   -- 货币代码    
        ,Prod_Cd                  -- 产品代码    
        ,Loan_Contr_Id            -- 贷款合同编号
        ,Open_Dt                  -- 开立日期    
        ,Due_Dt                   -- 到期日期    
        ,Subj_Cd                  -- 科目代码    
        ,Guar_Amt                 -- 保函金额    
        ,Guar_Bal                 -- 保函余额    
        ,Agmt_Stat_Cd             -- 协议状态代码
        ,Guar_Mode_Cd             -- 担保方式代码
        ,Guar_Typ_Cd              -- 保函类型代码
        ,Guar_Charc_Cd            -- 保函性质代码
        ,Margn_Ratio              -- 保证金比例  
        ,Margn_Acct_Num           -- 保证金帐号  
        ,Margn_Amt                -- 保证金金额  
        ,Fill_Tab_Dept            -- 填表部门    
        ,Mth_Accm                 -- 月积数      
        ,Yr_Accm                  -- 年积数      
        ,Mth_Day_Avg_Bal          -- 月日均余额  
        ,Yr_Day_Avg_Bal           -- 年日均余额  
        ,Sys_Src                  -- 系统来源    
)
SELECT    
          '$TXDATE' :: date                                 as etl_date                     
          ,'1'                                                       as grp_typ  
          ,coalesce(T.LGNO,'')                                       as Agmt_Id              
          ,coalesce(T.APPNO,'')                                      as Cust_Num             
          ,COALESCE(T2.BRH_CODE,T1.TRANSACTORGNO)                     as Org_Num              
          ,coalesce(T4.TGT_Cd,'@'||T.LGCURSIGN)                                    as Cur_Cd              
          ,coalesce(T.LGTYPE ::varchar,'')                           as Prod_Cd              
          ,''                                                      as Loan_Contr_Id        --暂空
          ,coalesce(T.ISSUEDATE,to_date('','yyyymmdd'))              as Open_Dt              
          ,coalesce(T.MATURITYDATE,to_date('','yyyymmdd'))           as Due_Dt               
          ,''                                                      as Subj_Cd              --暂空
          ,coalesce(T.LGAMT,0)                                       as Guar_Amt             
          ,(CASE 
                WHEN T.DRAFTDAYS<1 THEN T.LGAMT-T.PAYMENTAMT  
                ELSE T.LGAMT-T.LCACCEPTAMT 
            end)   as Guar_Bal             
          ,(CASE WHEN '$TXDATE' :: date>T.MATURITYDATE THEN '2' ELSE '1'  
            END)   as Agmt_Stat_Cd         
          ,'GUAR'                                                   as Guar_Mode_Cd         
          ,T.LGTYPE                          as Guar_Typ_Cd          
          ,(CASE WHEN INOUTFLAG ='0'  AND ISLG='1' THEN '1' 
          	 WHEN INOUTFLAG ='0'  AND ISLG='0' THEN '2' 
                 WHEN  INOUTFLAG ='1'  AND ISLG='1' THEN '3'  
                 WHEN  INOUTFLAG ='1'  AND ISLG='0' THEN '4' 
             else '4' 
            end)                                                    as Guar_Charc_Cd        
          ,(CASE WHEN REPLACE(T3.ratio,';','') is null OR REPLACE(T3.ratio,';','') ='' 
                then 0.00  else (REPLACE(T3.ratio,';','')::NUMBER)
            END )                                      as Margn_Ratio          
          ,(CASE WHEN replace(T3.depacctno,';','') IS NULL
                 THEN '' ELSE replace(T3.depacctno,';','')
            END )
                                                       as Margn_Acct_Num       
          ,(CASE WHEN replace(T3.depamt,';','') IS NULL OR replace(T3.depamt,';','')=''
		 THEN 0.00 ELSE (replace(T3.depamt,';','')::NUMBER)
	     END )                                     as Margn_Amt            
          ,0                                        as Fill_Tab_Dept        --暂空
          ,0                                        as Mth_Accm             --暂空
          ,0                                        as Yr_Accm              --暂空
          ,0                                        as Mth_Day_Avg_Bal      --暂空
          ,0                                        as Yr_Day_Avg_Bal       --暂空
          ,'ISS'                                       as Sys_Src              --暂空    
from DW_SDATA.iss_001_im_lgissueinfo T	
LEFT JOIN DW_SDATA.iss_001_bu_transactioninfo T1		
on T.TXNSERIALNO=T1.TXNSERIALNO    
AND T1.start_dt<='$TXDATE'::date and '$TXDATE'::date<T1.end_dt
LEFT JOIN DW_SDATA.ogs_000_tbl_new_old_rel T2	
on T1.TRANSACTORGNO = T2.BRH_SV_NEW_CODE 
AND T2.sys_type='99700010000'
AND T2.use_flag='02'
AND T2.start_dt<='$TXDATE'::date and '$TXDATE'::date<T2.end_dt
LEFT JOIN  DW_SDATA.ISS_001_cr_lginfosucc T3
ON T.LGNO = T3.LGNO AND T3.start_dt<='$TXDATE'::date and '$TXDATE'::date<T3.end_dt 
LEFT JOIN  F_FDM.CD_RF_STD_CD_TRAN_REF T4 
     ON  T.LGCURSIGN=T4.SRC_CD                      
    AND  T4.DATA_PLTF_SRC_TAB_NM = 'ISS_001_IM_LGISSUEINFO'
    AND  T4.Data_Pltf_Src_Fld_Nm ='LGCURSIGN'   
WHERE  T.start_dt<='$TXDATE'::date and '$TXDATE'::date<T.end_dt;

 /*月积数、年积数、月日均余额、年日均余额临时表创建区*/
 
create local temporary table tt_f_agt_Guarantee_yjs
on commit preserve rows as
select 
      t.Agmt_Id
      
      ,(case 
            when '$TXDATE'= '$MONTHBGNDAY' 
            then t.Guar_Bal
            else t.Guar_Bal+coalesce(t1.mth_accm,0)
            end
       )                                                                      as mth_accm  --月积数
      ,(case 
            when  '$TXDATE' = '$YEARBGNDAY' 
            then t.Guar_Bal
            else t.Guar_Bal+coalesce(t1.yr_accm,0)
            end
       )                                                                      as yr_accm   --年积数
      ,(case 
            when '$TXDATE' = '$MONTHBGNDAY' 
            then t.Guar_Bal
            else t.Guar_Bal+coalesce(t1.mth_accm,0)
            end
       )/('$TXDATE'::date-'$MONTHBGNDAY'::date+1)               as mth_day_avg_bal  --月日均余额
      ,(case 
           when '$TXDATE' = '$YEARBGNDAY' 
           then t.Guar_Bal
           else t.Guar_Bal+coalesce(t1.yr_accm,0)
           end
       )/('$TXDATE'::date-'$YEARBGNDAY'::date+1)                    as Yr_Day_Avg_Bal   --年日均余额
from  f_fdm.f_agt_guarantee     t
left join f_fdm.f_agt_guarantee t1
on         t.Agmt_Id=t1.Agmt_Id
 
and       t1.etl_date='$TXDATE'::date-1
where     t.etl_date='$TXDATE'::date
;
/*月积数、年积数、月日均余额、年日均余额临时表创建区END*/
/*更新月积数、年积数、月日均余额、年日均余额*/
update f_fdm.f_agt_guarantee    t
set mth_accm=t1.mth_accm 
,yr_accm=t1.yr_accm
,mth_day_avg_bal=t1.mth_day_avg_bal
,Yr_Day_Avg_Bal=t1.Yr_Day_Avg_Bal
from  tt_f_agt_Guarantee_yjs  t1
where t.Agmt_Id=t1.Agmt_Id
and   t.etl_date='$TXDATE'::date
;
/*数据处理区end*/ 
COMMIT;
