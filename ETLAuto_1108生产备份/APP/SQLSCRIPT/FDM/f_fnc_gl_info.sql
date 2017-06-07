/*
Author             :XMH
Function           :总账信息
Load method        :INSERT
Source table       :dw_sdata.acc_002_t_acc_inst_dvlp
Frequency          :D
Modify history list:Created by
                   :Modify  by xmh gengxin  bizhong guize 
                    MODIFIED BY ZMX 20160901  修改表结构，修改映射规则`
                    modified by zmx 修改筛选条件
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/

/*数据回退区*/
Delete /* +direct */ from  f_fdm.f_fnc_gl_info
where etl_date='$TXDATE'::date
;
/*数据回退区end*/
/*数据处理区*/
 
insert into f_fdm.f_fnc_gl_info
(
         grp_typ                     --组别                     
        ,etl_date                     --数据日期          
        ,org_cd                      --机构代码            
        ,cur_cd                      --币种      
        ,duty_ctr_cd                 --责任中心代码  
        ,ACCOUNT_cd                  --自然科目代码      
        ,dtl_subj_cd                 --明细科目代码      
        ,prod_cd                     --产品代码      
        ,biz_line_cd                 --业务条线代码          
        ,inn_reco_cd                 --内部往来代码      
        ,BUSINESS_UNIT_cd            --事业部代码      
        ,SPARE_cd                    --备用段代码      
        ,today_debit_amt             --当日借方发生额        
        ,today_crdt_amt              --当日贷方发生额    
        ,curmth_accm_debit_amt       --本月累计借方发生额    
        ,curmth_accm_crdt_amt        --本月累计贷方发生额
        ,debit_bal                   --借方余额
        ,crdt_bal                    --贷方余额
        ,curr_debit_avg_bal          --本期借方平均余頿
        ,curr_crdt_avg_bal           --本期贷方平均余额
        ,term_ind                    --期限标志  
        ,Sys_Src                     --系统来源          
 )
 select  DISTINCT 1                                 As   grp_typ                     --组别                  
        ,'$TXDATE' :: date       As   etl_date                     --数据日期              
        ,T.INST_NO                        AS   org_cd                      --机构代码              
        -- ,(CASE WHEN T.CURR_TYPE='RMB'  THEN '156'
        --       WHEN T.CURR_TYPE='USD'  THEN '840'
        --  ELSE T.CURR_TYPE END )  
        ,T.CURR_TYPE                      AS   cur_cd                      --币种                  
        ,''                               AS   duty_ctr_cd                 --责任中心代码          
        ,T.ITM_NO                         AS   ACCOUNT_cd                  --自然科目代码          
        ,''                               AS   dtl_subj_cd                 --明细科目代码          
        ,''                               AS   prod_cd                     --产品代码              
        ,''                               AS   biz_line_cd                 --业务条线代码          
        ,''                               AS   inn_reco_cd                 --内部往来代码          
        ,''                               AS   BUSINESS_UNIT_cd            --事业部代码            
        ,''                               AS   SPARE_cd                    --备用段代码            
        ,0.00                             AS   today_debit_amt             --当日借方发生额        
        ,0.00                             AS   today_crdt_amt              --当日贷方发生额        
        ,T.DR_AMT_MON                     AS   curmth_accm_debit_amt       --本月累计借方发生额    
        ,T.CR_AMT_MON                     AS   curmth_accm_crdt_amt        --本月累计贷方发生额    
        ,T.DR_CRT_BAL                     AS   debit_bal                   --借方余额              
        ,T.CR_CRT_BAL                     AS   crdt_bal                    --贷方余额              
        ,0.00                             AS   curr_debit_avg_bal          --本期借方平均余頿      
        ,0.00                             AS   curr_crdt_avg_bal           --本期贷方平均余额      
        ,0.00                             AS   term_ind                    --期限标志              
        ,'ACC'                            AS   Sys_Src                     --系统来源            
 FROM DW_SDATA.acc_002_t_acc_inst_dvlp T
where T.CURR_TYPE NOT IN ('RMB','USD')-- AND T.SUM_DATE::DATE=START_DT
AND  T.start_dt<='$TXDATE'::date and'$TXDATE'::date<T.end_dt ;
/*数据处理区end*/ 
COMMIT
