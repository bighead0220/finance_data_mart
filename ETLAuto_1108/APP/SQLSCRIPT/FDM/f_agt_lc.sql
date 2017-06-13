/*
Author             :Liuxz
Function           :信用证信息表
Load method        :
Source table       : dw_sdata.iss_001_im_lcissueinfo,dw_sdata.iss_001_bu_transactioninfo,dw_sdata.iss_001_PA_ORG,dw_sdata.iss_001_im_lccorpinfo,dw_sdata.iss_001_bu_depused
Destination Table  :f_fdm.f_agt_lc
Frequency          :D
Modify history list:Created by Liuxz at 2016-05-18 10:47:45.732000
                   :Modify  by Liuxz 2016-05-25 修改贴源层表名，开发组别2
                   modify by liuxz 20160616 修改保证金账号、保证金金额逻辑 （变更记录29）添加组别3部分取数逻辑（变更记录35）
                   modify by liuxz 20160629 修改机构号取数逻辑 （变更记录80）
                   modified by liuxz 20160704 组别1,2货币代码需转换代码开发
                   modified by Liuxz 20160725 组别1,2更改T和T3表的关联条件(变更记录124)
                   modified by Liuxz 201607259 更新机构号取数逻辑（变更记录131）
                   modified by zhangliang 20160817 将关联的iss_001_PA_ORG修改为ogs_000_tb1_new_old_rel
                   modified by zhangliang 20160824 1、修改组1关联表t3 为ccs_004_tb_con_lc_detail ; t4为ccs_004_tb_con_biz_detail;增加关联表t6 为iss_001_cr_lcinfosucc
                                                      修改合同编号，保证金比例，保证金账号，保证金金额映射规则
                                                   2、修改组2关联表t3 为ccs_004_tb_con_lc_detail ; t4为ccs_004_tb_con_biz_detail;增加关联表t6 为iss_001_cr_lcinfosucc
                                                      修改合同编号，保证金比例，保证金账号，保证金金额映射规则
                   modified by zhangliang 20160829 1、将组1,组2关联表t3,t4,t5日期限制注释
                   modified by zhangliang 20160630 修改组3关联表t
                   modified by liudongyan 20160901 新增组一组二T2.use_flag='02'
                   modified by wyh 20160921 增加月积数的逻辑;第3组之前Null值字段(保证金比例  保证金帐号  保证金金额  填表部门  担保方式代码)改为对应字段类型的空(''或0)
*/


-------------------------------------------逻辑说明---------------------------------------------
/*业务逻辑说明
f_agt_lc 

*/
-------------------------------------------逻辑说明END------------------------------------------
/*数据回退区*/
delete from f_fdm.f_agt_lc
where etl_date='$TXDATE'::date
;
/*数据回退区END*/
insert into f_fdm.f_agt_lc
(
grp_typ                                          --组别
,etl_date                                        --数据日期
,Agmt_Id                                         --协议编号
,Cust_Num                                        --客户号
,Org_Num                                         --机构号
,Cur_Cd                                          --货币代码
,Prod_Cd                                         --产品代码
,Loan_Contr_Id                                   --贷款合同编号
,Happ_Dt                                         --发生日期
,Due_Dt                                          --到期日期
,Subj_Cd                                         --科目代码
,Lc_Amt                                          --信用证金额
,Lc_Bal                                          --信用证余额
,Agmt_Stat_Cd                                    --协议状态代码
,Contr_Id                                        --合同编号
,Issu_Bank_Cd                                    --开证行代码
,Issu_Bank_Nm                                    --开证行名称
,Advis_Bank_Cd                                   --通知行代码
,Advis_Bank_Nm                                   --通知行名称
,Nego_Pay_Line_Cd                                --议付行代码
,Nego_Pay_Line_Nm                                --议付行名称
,Margn_Ratio                                     --保证金比例
,Margn_Acct_Num                                  --保证金帐号
,Margn_Amt                                       --保证金金额
,Fill_Tab_Dept                                   --填表部门
,Guar_Mode_Cd                                    --担保方式代码
,Mth_Accm                                        --月积数
,Yr_Accm                                         --年积数
,Mth_Day_Avg_Bal                                 --月日均余额
,Yr_Day_Avg_Bal                                  --年日均余额
,Sys_Src                                         --系统来源
)
select
'1'                                               as grp_typ          --组别
,'$TXDATE'::date                         as etl_date         --数据日期
,T.LCNO                                           as Agmt_Id          --协议编号
,T.APPNO                                          as Cust_Num         --客户号
,COALESCE(T2.BRH_CODE,T1.TRANSACTORGNO)           as Org_Num          --机构号
,coalesce(T5.TGT_CD,'@'||T.LCCURSIGN)             as Cur_Cd           --货币代码
,'1'                                               as Prod_Cd          --产品代码
,''                                               as Loan_Contr_Id    --贷款合同编号
,T.LCISSUINGDATE                                  as Happ_Dt          --发生日期
,T.EXPIRYDATE                                     as Due_Dt           --到期日期
,''                                               as Subj_Cd          --科目代码
,T.LCAMT                                          as Lc_Amt           --信用证金额
,(CASE 
		   WHEN T.DRAFTDAYS<1 OR T.SPECIALLCTYPE=4 THEN LCNOTPAYAMT 
		   ELSE (CASE 
		   				    WHEN T.LCAMT-T.LCACCEPTAMT>=0 THEN T.LCAMT-T.LCACCEPTAMT 
		   			 ELSE 0 
		   			 END
		   			 ) 
   END
  )                                               as Lc_Bal           --信用证余额
,(CASE 
       WHEN T.ISCLOSE='N' THEN '1' 
       ELSE '2' 
  END
  )                                               as Agmt_Stat_Cd     --协议状态代码
,coalesce(T4.contract_num,'')                                    as Contr_Id         --合同编号
,T.OPENAPPBANKNO                                  as Issu_Bank_Cd     --开证行代码
,T.OPENAPPBANKNAMEADDR                            as Issu_Bank_Nm     --开证行名称
,T.ADVBANKNO                                      as Advis_Bank_Cd    --通知行代码
,T.ADVBANKNAMEADDR                                as Advis_Bank_Nm    --通知行名称
,T.NEGOBANKNO                                     as Nego_Pay_Line_Cd --议付行代码
,T.NEGOBANKNAMEADDR                               as Nego_Pay_Line_Nm --议付行名称
,case when replace(t6.ratio,';','')='' then 0
else
coalesce(replace(t6.ratio,';','')::numeric,0) 
end                                    as Margn_Ratio      --保证金比例
  ,replace(t6.depacctno,';','')                                 as Margn_Acct_Num   --保证金帐号
,case when replace(t6.depamt,';','')='' then 0
else
coalesce(replace(t6.depamt,';','0')::numeric,0)
end  as Margn_Amt        --保证金金额
,''                                               as Fill_Tab_Dept    --填表部门
,(CASE 
    WHEN T.SPECIALLCTYPE = 2 THEN 'COLL' 
    ELSE 'GUAR'          
    END)                                          as Guar_Mode_Cd     --担保方式代码
,0.00                                             as Mth_Accm         --月积数
,0.00                                             as Yr_Accm          --年积数
,0.00                                             as Mth_Day_Avg_Bal  --月日均余额
,0.00                                             as Yr_Day_Avg_Bal   --年日均余额
,'ISS'                                            as Sys_Src          --系统来源
from
dw_sdata.iss_001_im_lcissueinfo T
LEFT JOIN   dw_sdata.iss_001_bu_transactioninfo  as T1
ON      T.TXNSERIALNO = T1.TXNSERIALNO
AND     T1.START_DT<='$TXDATE'::date
AND     T1.END_DT>'$TXDATE'::date
LEFT JOIN dw_sdata.ogs_000_tbl_new_old_rel     as T2    
ON      T1.TRANSACTORGNO = T2.BRH_SV_NEW_CODE 
and     T2.sys_type='99700010000'
AND    T2.use_flag='02'
AND     T2.START_DT<='$TXDATE'::date
AND     T2.END_DT>'$TXDATE'::date
left join (select t.contract_biz_detail_id,t.lc_num,t.start_dt,t.end_dt,row_number()over(partition by t.lc_num order by t.fxbs_time_mark desc) as num 
   from dw_sdata.ccs_004_tb_con_lc_detail t) t3
on        T.lcno=T3.lc_num
and t3.num=1
and t3.start_dt<='$TXDATE'::date
AND T3.end_dt>'$TXDATE'::date
left join (select t.contract_num,t.contract_biz_detail_id,t.biz_detail_id,t.start_dt,t.end_dt,row_number()over(partition by t.biz_detail_id order by t.time_mark desc) as num
   from dw_sdata.ccs_004_tb_con_biz_detail t) t4
on        t3.contract_biz_detail_id=t4.contract_biz_detail_id
and t4.num=1
and t4.start_dt<='$TXDATE'::date
AND T4.end_dt>'$TXDATE'::date
left join f_fdm.CD_RF_STD_CD_TRAN_REF T5
on       T.LCCURSIGN=T5.SRC_CD
and      T5.DATA_PLTF_SRC_TAB_NM = 'ISS_001_GN_LCISSUEINFO' 
and      T5.Data_Pltf_Src_Fld_Nm ='LCCURSIGN'
left join dw_sdata.iss_001_cr_lcinfosucc T6
on t.lcno=t6.lcno
and t6.currentstatus=2
and t6.start_dt<='$TXDATE'::date
AND T6.end_dt>'$TXDATE'::date
WHERE   T.START_DT<='$TXDATE'::date
AND     T.END_DT>'$TXDATE'::date
;
insert into f_fdm.f_agt_lc
(
grp_typ                                          --组别
,etl_date                                        --数据日期
,Agmt_Id                                         --协议编号
,Cust_Num                                        --客户号
,Org_Num                                         --机构号
,Cur_Cd                                          --货币代码
,Prod_Cd                                         --产品代码
,Loan_Contr_Id                                   --贷款合同编号
,Happ_Dt                                         --发生日期
,Due_Dt                                          --到期日期
,Subj_Cd                                         --科目代码
,Lc_Amt                                          --信用证金额
,Lc_Bal                                          --信用证余额
,Agmt_Stat_Cd                                    --协议状态代码
,Contr_Id                                        --合同编号
,Issu_Bank_Cd                                    --开证行代码
,Issu_Bank_Nm                                    --开证行名称
,Advis_Bank_Cd                                   --通知行代码
,Advis_Bank_Nm                                   --通知行名称
,Nego_Pay_Line_Cd                                --议付行代码
,Nego_Pay_Line_Nm                                --议付行名称
,Margn_Ratio                                     --保证金比例
,Margn_Acct_Num                                  --保证金帐号
,Margn_Amt                                       --保证金金额
,Fill_Tab_Dept                                   --填表部门
,Guar_Mode_Cd                                    --担保方式代码
,Mth_Accm                                        --月积数
,Yr_Accm                                         --年积数
,Mth_Day_Avg_Bal                                 --月日均余额
,Yr_Day_Avg_Bal                                  --年日均余额
,Sys_Src                                         --系统来源
)
SELECT 
'2'                                                               as   grp_typ                     --组别    
,'$TXDATE'::date                                         as   etl_date                    --数据日期  
,T.LCNO                                                           as   Agmt_Id                     --协议编号  
,T.APPNO                                                          as   Cust_Num                    --客户号   
,COALESCE(T2.BRH_CODE,T1.TRANSACTORGNO)
,coalesce(T5.TGT_CD,'@'||T.LCCURSIGN)                             as   Cur_Cd                      --货币代码  
,'2'                                                              as   Prod_Cd                     --产品代码  
,''                                                               as   Loan_Contr_Id               --贷款合同编号
,T.LCISSUINGDATE                                                  as   Happ_Dt                     --发生日期  
,T.EXPIRYDATE                                                     as   Due_Dt                      --到期日期  
,''                                                               as   Subj_Cd                     --科目代码  
,T.LCAMT                                                          as   Lc_Amt                      --信用证金额 
,(CASE 
      WHEN T.LCPAYTYPE =0 THEN YULCAMT 
      ELSE (CASE 
      				   WHEN T.LCAMT-T.LCACCEPTAMT>=0 THEN T.LCAMT-T.LCACCEPTAMT 
      				   ELSE 0 
      		  END
      		 ) 
   END
  )                                                              as   Lc_Bal                      --信用证余额 
,(CASE  
		   WHEN T.ISCLOSE='N' THEN '1' 
		   ELSE '2' 
  END
 )                                                                as   Agmt_Stat_Cd                --协议状态代码
,coalesce(T4.contract_num,'')                                                    as   Contr_Id                    --合同编号  
,T.OPENAPPBANKNO                                                  as   Issu_Bank_Cd                --开证行代码 
,T.OPENAPPBANKNAMEADDR                                            as   Issu_Bank_Nm                --开证行名称 
,T.ADVBANKNO                                                      as   Advis_Bank_Cd               --通知行代码 
,T.ADVBANKNAMEADDR                                                as   Advis_Bank_Nm               --通知行名称 
,T.NEGOBANKNO                                                     as   Nego_Pay_Line_Cd            --议付行代码 
,T.NEGOBANKNAMEADDR                                               as   Nego_Pay_Line_Nm            --议付行名称 
,case when replace(T6.ratio,';','') = '' then 0
 else 
coalesce(replace(T6.ratio,';','')::numeric,0)
end                                                    as   Margn_Ratio                 --保证金比例 
,replace(T6.depacctno,';','')                                        as   Margn_Acct_Num              --保证金帐号 
,case when replace(t6.depamt,';','') ='' then 0
else 
coalesce(replace(t6.depamt,';','')::numeric,0)
end                                                   as   Margn_Amt                   --保证金金额 
,''                                                               as   Fill_Tab_Dept               --填表部门  
,''                                                               as   Guar_Mode_Cd                --担保方式代码
,0.00                                                             as   Mth_Accm                    --月积数   
,0.00                                                             as   Yr_Accm                     --年积数   
,0.00                                                             as   Mth_Day_Avg_Bal             --月日均余额 
,0.00                                                             as   Yr_Day_Avg_Bal              --年日均余额 
,'ISS'                                                            as   Sys_Src                     --系统来源  
from      dw_sdata.iss_001_gn_lcissueinfo T
left join dw_sdata.iss_001_bu_transactioninfo  T1
on      T.TXNSERIALNO = T1.TXNSERIALNO
AND     T1.START_DT<='$TXDATE'::date
AND     T1.END_DT>'$TXDATE'::date
left join dw_sdata.ogs_000_tbl_new_old_rel T2
on      T1.TRANSACTORGNO =T2.BRH_SV_NEW_CODE 
 and T2.sys_type='99700010000'
AND    T2.use_flag='02'
AND     T2.START_DT<='$TXDATE'::date
AND     T2.END_DT>'$TXDATE'::date
left join (select t.contract_biz_detail_id,t.lc_num,t.start_dt,t.end_dt,row_number()over(partition by t.lc_num order by t.fxbs_time_mark desc) as num 
   from dw_sdata.ccs_004_tb_con_lc_detail t) t3
on        t.lcno=t3.lc_num
and t3.num=1
and t3.start_dt<='$TXDATE'::date
AND T3.end_dt>'$TXDATE'::date
left join (select t.contract_num,t.contract_biz_detail_id,t.biz_detail_id,t.start_dt,t.end_dt,row_number()over(partition by t.biz_detail_id order by t.time_mark desc) as num
   from dw_sdata.ccs_004_tb_con_biz_detail t) t4
on        t3.contract_biz_detail_id=t4.contract_biz_detail_id
and t4.num=1
and t4.start_dt<='$TXDATE'::date
AND T4.end_dt>'$TXDATE'::date 
left join f_fdm.CD_RF_STD_CD_TRAN_REF T5
on       T.LCCURSIGN=T5.SRC_CD
and      T5.DATA_PLTF_SRC_TAB_NM = 'ISS_001_GN_LCISSUEINFO' 
and      T5.Data_Pltf_Src_Fld_Nm ='LCCURSIGN'
left join dw_sdata.iss_001_cr_lcinfosucc T6
on t.lcno=t6.lcno
and t6.currentstatus=2
and t6.start_dt<='$TXDATE'::date
AND T6.end_dt>'$TXDATE'::date
WHERE   T.START_DT<='$TXDATE'::date
AND     T.END_DT>'$TXDATE'::date
;
insert into f_fdm.f_agt_lc
(
grp_typ                                          --组别
,etl_date                                        --数据日期
,Agmt_Id                                         --协议编号
,Cust_Num                                        --客户号
,Org_Num                                         --机构号
,Cur_Cd                                          --货币代码
,Prod_Cd                                         --产品代码
,Loan_Contr_Id                                   --贷款合同编号
,Happ_Dt                                         --发生日期
,Due_Dt                                          --到期日期
,Subj_Cd                                         --科目代码
,Lc_Amt                                          --信用证金额
,Lc_Bal                                          --信用证余额
,Agmt_Stat_Cd                                    --协议状态代码
,Contr_Id                                        --合同编号
,Issu_Bank_Cd                                    --开证行代码
,Issu_Bank_Nm                                    --开证行名称
,Advis_Bank_Cd                                   --通知行代码
,Advis_Bank_Nm                                   --通知行名称
,Nego_Pay_Line_Cd                                --议付行代码
,Nego_Pay_Line_Nm                                --议付行名称
,Margn_Ratio                                     --保证金比例
,Margn_Acct_Num                                  --保证金帐号
,Margn_Amt                                       --保证金金额
,Fill_Tab_Dept                                   --填表部门
,Guar_Mode_Cd                                    --担保方式代码
,Mth_Accm                                        --月积数
,Yr_Accm                                         --年积数
,Mth_Day_Avg_Bal                                 --月日均余额
,Yr_Day_Avg_Bal                                  --年日均余额
,Sys_Src                                         --系统来源
)
SELECT distinct 3                                  --组别      
,'$TXDATE'::date                                   --数据日期    
,T.LCNO                                            --协议编号    
,T.APPNO                                           --客户号     
,''                                              --机构号     
,''                                              --货币代码    
,'3'                                               --产品代码    
,''                                              --贷款合同编号  
,'$MINDATE'::date                                              --发生日期    
,T.EXPIRYDATE                                      --到期日期    
,''                                              --科目代码    
,T.LCAMT                                           --信用证金额   
,0                                              --信用证余额   
,''                                              --协议状态代码  
,''                                              --合同编号    
,T.ISSUINGBANKNO                                   --开证行代码   
,T.ISSUINGBANKNAME                                 --开证行名称   
,T.AGENTBANKSWFCODE                                --通知行代码   
,T.AGENTBANKNAME                                   --通知行名称   
,T.NEGBANKNO                                       --议付行代码   
,T.NEGBANKNAMEADDR                                 --议付行名称   
,0                                              --保证金比例   
,''                                              --保证金帐号   
,0                                              --保证金金额   
,''                                              --填表部门    
,''                                              --担保方式代码  
,0                                              --月积数     
,0                                              --年积数     
,0                                              --月日均余额   
,0                                              --年日均余额   
,'ISS'                                             --系统来源    
from (SELECT T.LCNO
,T.APPNO
,T.EXPIRYDATE
,T.LCAMT
,T.ISSUINGBANKNO
,T.ISSUINGBANKNAME
,T.AGENTBANKSWFCODE
,T.AGENTBANKNAME
,T.NEGBANKNO
,T.NEGBANKNAMEADDR,T.start_dt,T.end_dt,ROW_NUMBER()
OVER(PARTITION BY T.lcno ORDER BY T.txnserialno DESC) NUM  FROM DW_SDATA.iss_001_ex_advissueinfo T ) T
WHERE   T.START_DT<='$TXDATE'::date
and t.num=1
AND     T.END_DT>'$TXDATE'::date
; 

create local temporary table if not exists f_agt_lc_tmp_yjs
on commit preserve rows as
select
      t.agmt_id
      ,(case
            when '$TXDATE'= '$MONTHBGNDAY'
            then t.Lc_Bal
            else t.Lc_Bal+coalesce(t1.mth_accm,0)
            end
       )                                                                      as mth_accm  --月积数
      ,(case
            when  '$TXDATE' = '$YEARBGNDAY'
            then t.Lc_Bal
            else t.Lc_Bal+coalesce(t1.yr_accm,0)
            end
       )                                                                      as yr_accm   --年积数
      ,(case
            when '$TXDATE' = '$MONTHBGNDAY'
            then t.Lc_Bal
            else t.Lc_Bal+coalesce(t1.mth_accm,0)
            end
       )/('$TXDATE'::date-'$MONTHBGNDAY'::date+1)               as mth_day_avg_bal  --月日均余额
      ,(case
           when '$TXDATE' = '$YEARBGNDAY'
           then t.Lc_Bal
           else t.Lc_Bal+coalesce(t1.yr_accm,0)
           end
       )/('$TXDATE'::date-'$YEARBGNDAY'::date+1)                    as Yr_Day_Avg_Bal   --年日均余额
from  f_fdm.f_agt_lc     t
left join f_fdm.f_agt_lc t1
on         t.agmt_id= t1.agmt_id
and        t1.etl_date='$TXDATE'::date-1
where     t.etl_date='$TXDATE'::date
;

update f_fdm.f_agt_lc   t
set mth_accm=t1.mth_accm
,yr_accm=t1.yr_accm
,mth_day_avg_bal=t1.mth_day_avg_bal
,Yr_Day_Avg_Bal=t1.Yr_Day_Avg_Bal
from  f_agt_lc_tmp_yjs    t1
where t.agmt_id= t1.agmt_id
and   t.etl_date='$TXDATE'::date
;
/*数据处理区END*/
commit;
