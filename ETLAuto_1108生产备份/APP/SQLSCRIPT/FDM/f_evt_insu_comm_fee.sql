/*
Author             :liudongyan
Function           :保险公司手续费信息表
Load method        :INSERT
Source table       :
Frequency          :D
Modify history list:Created by刘东燕2016年8月19日12:05:55

-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
*-------------------------------------------逻辑说明END------------------------------------------
*/

/*数据回退区*/
Delete /* +direct */ from  f_fdm.f_evt_insu_comm_fee
where etl_date='$TXDATE'::date
/*数据回退区end*/
;
/*数据处理区*/

insert into f_fdm.f_evt_insu_comm_fee
(
        grp_typ	                    --组别
       ,etl_date	            --数据日期
       ,insu_corp_cd	            --保险公司代码
       ,life_insur_comm_fee	    --寿险手续费
       ,prop_insur_comm_fee	    --财险手续费
       ,renewal_comm_fee	    --续期手续费
       ,realtm_clltn_comm_fee       --实时代收手续费  
       ,bat_comm_fee	            --批量手续费 
       ,oth_comm_fee	            --其他手续费
       ,comm_fee_sum	            --手续费合计
       ,sys_src	                    --系统来源
  
 )
 select
       '1'                                             as grp_typ	      --组别                       
       ,'$TXDATE'::date                       as etl_date	    --数据日期                                                                                     
       ,coalesce(T.INSU_CODE,'')	               as insu_corp_cd	--保险公司代码
       ,coalesce(T.SXSUM,0)	                       as life_insur_comm_fee	--寿险手续费
       ,coalesce(T.CXSUM,0)	                       as prop_insur_comm_fee	--财险手续费
       ,coalesce(T1.XQSUM,0)	                       as renewal_comm_fee	--续期手续费
       ,coalesce(T1.SDSUM,0)	                       as realtm_clltn_comm_fee	--实时代收手续费  
       ,coalesce(T1.PLSUM,0)	                       as bat_comm_fee	--批量手续费 
       ,coalesce(T1.QTSUM,0)	                       as oth_comm_fee	--其他手续费
       ,T.SXSUM+T.CXSUM+T1.XQSUM+T1.SDSUM+T1.PLSUM+T1.QTSUM	as comm_fee_sum	--手续费合计
       ,'ICS'                                    	as sys_src	--系统来源
FROM 
    (SELECT   T.INSU_CODE,T.CLR_DATE,T.ETL_DT 
     ,SUM(CASE WHEN T1.INSU_PROD_TYPE='1' THEN T.FEE_AMT ELSE 0 END) AS SXSUM--寿险金额
     ,SUM(CASE WHEN T1.INSU_PROD_TYPE='2' THEN T.FEE_AMT ELSE 0 END) AS CXSUM--财险金额
     FROM dw_sdata.ICS_001_CLDTL_TX_FEE T  
     JOIN dw_sdata.ICS_001_smctl_insu_prod_code T1 
     ON T.INSU_CODE=T1.INSU_CODE 
     AND T.INSU_PROD_CODE=T1.INSU_PROD_CODE 
     AND T.VALID_FLAG=1
     WHERE T1.start_dt <= '$TXDATE'::date
     AND   T1.end_dt > '$TXDATE'::date
     GROUP BY  T.INSU_CODE,CLR_DATE,T.ETL_DT ) T
LEFT JOIN 
     (SELECT   T.INSU_CODE,T.CLR_DATE,T.ETL_DT
     ,SUM(CASE  WHEN T. TX_CODE in ('580002','580982')  THEN T.FEE_AMT ELSE 0 END) AS XQSUM--续期手续费
     ,0 AS SDSUM --待定 SUM(CASE  WHEN T. TX_CODE  THEN T.FEE_AMT ELSE 0 END) AS SXSUM--实时代收手续费 
     ,SUM(CASE  WHEN T. TX_CODE in ('584301','584302')  THEN T.FEE_AMT ELSE 0 END) AS PLSUM--批量手续费 
     ,SUM(CASE  WHEN T. TX_CODE NOT IN ('580002','580982','584301','584302')   THEN T.FEE_AMT ELSE 0 END) AS QTSUM--其他手续费
      FROM dw_sdata.ICS_001_CLDTL_TX_FEE T  
      WHERE    T.VALID_FLAG=1
      GROUP BY  T.INSU_CODE,CLR_DATE,T.ETL_DT ) T1
ON    T.INSU_CODE=T1.INSU_CODE
AND   T.CLR_DATE=T1.CLR_DATE
AND   T.ETL_DT=T1.ETL_DT
AND   T1.ETL_DT='$TXDATE'::date
WHERE T.ETL_DT='$TXDATE'::date

;
/*数据处理区end*/
commit;
