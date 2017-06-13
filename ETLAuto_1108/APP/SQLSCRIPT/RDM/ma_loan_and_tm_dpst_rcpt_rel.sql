/*
Author             :liudongyan
Function           :贷款与定期存单关系
Load method        :INSERT
Source table       :f_loan_indv_dubil,f_loan_corp_dubil_info,f_loan_mrtg_prop,f_loan_guar_contr                
Destination Table  :ma_loan_and_tm_dpst_rcpt_rel 贷款与定期存单关系
Frequency          :M
Modify history list:Created by liudongyan 2016/5/18 10:19:34
                   :Modify  by
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
DELETE FROM f_rdm.ma_loan_and_tm_dpst_rcpt_rel
WHERE  etl_date='$TXDATE'::date ;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_rdm.ma_loan_and_tm_dpst_rcpt_rel
(
      etl_date                              --数据日期
     ,loan_acct                             --贷款账号
     ,impw_tm_dpst_rcpt_acct_num            --质押定期存单账号
     ,sys_src                               --系统来源 
)
 
SELECT  
     '$TXDATE'::date             as etl_date
     ,NVL(T2.Agmt_Id ,T3.Agmt_Id)         as loan_acc
     ,T.Cret_Dpst_Acct_Num                as impw_tm_dpst_rcpt_acct_num
     ,T.sys_src                           as sys_src 
FROM 
(select guar_contr_agmt_id,Cret_Dpst_Acct_Num,sys_src from 
 f_fdm.f_loan_mrtg_prop 
 WHERE etl_date= '$TXDATE'::date 
  and  Cret_Dpst_Acct_Num <> ''
 group by 1,2,3) T               --贷款抵质押物信息
INNER JOIN  f_fdm.f_loan_guar_contr T1 --贷款担保合同信息                    
   ON T.guar_contr_agmt_id =T1.guar_agmt_id    
  AND T1.loan_contr_agmt_id<>''
  AND T1.etl_date= '$TXDATE'::date  
 LEFT JOIN f_fdm.f_loan_indv_dubil T2  --个人贷款借据信息表                        
   ON T1.loan_contr_agmt_id=T2.Contr_Agmt_Id          
  AND T2.etl_date= '$TXDATE'::date                        
 LEFT JOIN f_fdm.f_loan_corp_dubil_info  T3  --公司贷款借据信息表                       
   ON T1.loan_contr_agmt_id=T3.Contr_Agmt_Id              
  AND T3.etl_date= '$TXDATE'::date  
WHERE 
  NVL(T2.Agmt_Id ,T3.Agmt_Id) is not null
                      
;
/*数据处理区END*/

COMMIT;
