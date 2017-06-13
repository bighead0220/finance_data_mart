/*
Author             :XMH
Function           :公司存款账户信息表
Load method        :INSERT
Source table       :dw_sdata.cbs_001_ammst_corp,f_fdm.CD_RF_STD_CD_TRAN_REF,DW_SDATA.cbs_001_pmctl_irate_code,DW_SDATA.cbs_001_fsrgt_product,DW_SDATA.acc_003_t_acc_cdm_com_ledger,DW_SDATA.acc_003_t_acc_fix_com_ledger,DW_SDATA.acc_003_t_accdata_last_item_no,dw_sdata.cbs_001_amdtl_prep_int,DW_SDATA.cbs_001_amrgt_cal_int,DW_SDATA.cbs_001_pmctl_kind_type,DW_SDATA.cbs_001_pmctl_cur_code,DW_SDATA.cbs_001_pmctl_kind_type,dw_sdata.cbs_001_pmctl_cur_code,dw_sdata.cbs_001_amdtl_prep_int
Modify history list:Created by徐铭浩2016年5月5日10:05:55
                   :Modify  by 刘东燕2016年6月29日 插入代码表
                   modified by liuxz 2016年7月14日17:47:27 删除第1组T9表cbs_001_amrgt_cal_int的“and   start_dt<=ETL加载日期<end_dt”第2组T5表cbs_001_amdtl_prep_int“and   start_dt<=ETL加载日期<end_dt
                   modified by xumh 2016年7月25日11:47:27 删除第1组T10,T13表"and start_dt<=ETL加载日期<end_dt”第2组T7,T8,T11表"and start_dt<=ETL加载日期<end_dt"
                   modified by zhangliang 20160920 将cbs_001_amrgt_cal_int关联表对应的start_dt 修改为etl_dt
                   modified by wyh 20160925 修改1,2组下的当日计提利息，累计已收息;
*/
-------------------------------------------逻辑说明---------------------------------------------
/* 业务逻辑说明 */
-------------------------------------------逻辑说明END------------------------------------------
/*数据回退区*/
Delete /* +direct */ from  f_fdm.f_dpst_corp_acct
where etl_date='$TXDATE'::date;
/*数据回退区end*/
/*临时表区*/
 create local temp table tt_f_dpst_corp_acct_yjs
 on commit preserve rows as
select *
  from f_fdm.f_dpst_corp_acct
  where 1=2 
;
/*临时表区end*/
insert into f_fdm.f_dpst_corp_acct
(
       etl_date,                                  --数据日期       
       grp_typ,                                   --组别         
       Cust_Acct_Num,                             --客户账号          
       Sub_Acct,                                  --子账号            
       Org_Num,                                   --机构号            
       Cur_Cd,                                    --货币代码          
       Cust_Num,                                  --客户号            
       Prod_Cd,                                   --产品代码 
       dpst_cate_cd,                              --存款类别代码         
       St_Int_Dt,                                 --起息日            
       Due_Dt,                                    --到期日            
       Open_Acct_Day,                             --开户日            
       Clos_Acct_Day,                             --销户日            
       Exec_Int_Rate,                             --执行利率          
       Bmk_Int_Rate,                              --基准利率          
       Basis,                                     --基差              
       Int_Rate_Adj_Mode_Cd,                      --利率调整方式代码  
       Adj_Prd_Typ_Cd,                            --调整周期类型代码  
       Int_Base_Cd,                               --计息基础代码      
       Cmpd_Int_Calc_Mode_Cd,                     --复利计算方式代码  
       Pre_Chrg_Int,                              --计息方式代码      
       Int_Rate_Attr_Cd,                          --利率属性代码      
       Orgnl_Term,                                --原始期限          
       Orgnl_Term_Corp_Cd,                        --原始期限单位代码  
       Rprc_Prd,                                  --重定价周期        
       Rprc_Prd_Corp_Cd,                          --重定价周期单位代码
       Last_Rprc_Day,                             --上次重定价日      
       Next_Rprc_Day,                             --下次重定价日      
       Is_AutoRnw,                                --是否自动转存      
       Last_AutoRnw_Dt,                           --上次自动转存日期  
       Is_Cash_Mgmt_Acct,                         --是否现金管理账户  
       Agmt_Stat_Cd,                              --协议状态代码      
       Prin_Subj,                                 --本金科目          
       Curr_Bal,                                  --当前余额          
       Int_Subj,                                  --利息科目          
       Today_Provs_Int,                           --当日计提利息      
       CurMth_Provs_Int,                          --当月计提利息      
       Accm_Provs_Int,                            --累计计提利息      
       Today_Int_Pay,                             --当日付息          
       CurMth_Paid_Int,                           --当月已付息        
       Accm_Paid_Int,                             --累计已付息        
       Int_Adj_Amt,                               --利息调整金额      
       Mth_Accm,                                  --月积数            
       Yr_Accm,                                   --年积数            
       Mth_Day_Avg_Bal,                           --月日均余额        
       Yr_Day_Avg_Bal,                            --年日均余额        
       Sys_Src                                    --系统来源          
 )
 select
       '$TXDATE' :: date                                           as  etl_date,                             
       1                                                                    as   grp_typ,                   
       T.account                                                            as   Cust_Acct_Num,             
       T.subacct                                                            as   Sub_Acct,                  
       T.open_unit                                                          as   Org_Num,                   
       T.cur_code                                                           as   Cur_Cd,                    
       T.cust_id                                                            as   Cust_Num,                  
       T.dep_type                                                           as   Prod_Cd, 
       coalesce(T11.flag,'')                                                as   dpst_cate_cd,
       T.int_start_date:: date                                              as   St_Int_Dt,                 
       T.due_date:: date                                                    as   Due_Dt,                    
       T.open_date:: date                                                   as   Open_Acct_Day,             
       T.close_date:: date                                                  as   Clos_Acct_Day,             
       T.irate                                                              as   Exec_Int_Rate,             
       coalesce(T1.irate1,0)                                                as   Bmk_Int_Rate,              
       coalesce(T1.irate1,0)-T.irate                                        as   Basis,                     
       coalesce(T14.TGT_CD,'@'||T.irate_adjust_way)                         as   Int_Rate_Adj_Mode_Cd,      
       T.adj_cycle_type                                                     as   Adj_Prd_Typ_Cd,            
       (CASE WHEN T11.flag ='1'  THEN '6' --活期存款 实际/实际 
             ELSE '10'    --定期存款30/360   
        END)                                                                as  Int_Base_Cd,                                             
       '1'                                                                  as  Cmpd_Int_Calc_Mode_Cd, 
       '0'                                                                  as  Pre_Chrg_Int,           
       (CASE 
           WHEN T.irate_adjust_way IN  ('1','2') then '3'  
           WHEN T.irate_adjust_way='0' THEN '1' else '' 
        end)                                                                as  Int_Rate_Attr_Cd,                                                                                 
       coalesce(T.dep_term,0)                                               as   Orgnl_Term,         
       'D'                                                                  as   Orgnl_Term_Corp_Cd,    
       (CASE 
           WHEN T.due_date is null or T.due_date:: date<= '$TXDATE':: date then  0 
           else T.due_date:: date-T.int_start_date:: date  
        end)    as   Rprc_Prd,  
       'D'                                                                  as  Rprc_Prd_Corp_Cd,  
       (case 
           when T.irate_adj_date:: date is not null then T.irate_adj_date:: date 
           else  T.int_start_date:: date 
        end)    as  Last_Rprc_Day,    
       (Case 
           when T11.flag ='1' --普通活期
           then  '$TXDATE':: date+1
        Else 
           (Case when T.due_date is null then '$MINDATE' :: date 
            When T.due_date:: date<= '$TXDATE':: date then '$TXDATE':: date+1
            Else T.due_date :: date 
            end )
        End)     as   Next_Rprc_Day,            
       NVL(T15.TGT_CD,'@'||T.auto_flag)                                    as   Is_AutoRnw,          
       '$MINDATE' :: date                                              as   Last_AutoRnw_Dt,     
       (case 
            when T2.account is not null then '1' else '0'
        end)     as   Is_Cash_Mgmt_Acct,            
       T.acct_state                                                        as   Agmt_Stat_Cd,                 
       COALESCE(T3.ITM_NO,T4.ITM_NO)                                       as   Prin_Subj,                    
       COALESCE(T3.BAL,T4.BAL)                                             as   Curr_Bal,                     
       COALESCE(T5.ITM_NO,T6.ITM_NO)                                       as   Int_Subj,                     
       coalesce(T16.cur_cope_int ,0)                                       as   Today_Provs_Int,              
       coalesce(T7.cur_cope_int ,0)                                        as   CurMth_Provs_Int,             
       coalesce(T13.cur_face_int,0)                                        as   Accm_Provs_Int,               
       coalesce(T8.interest,0)                                      as   Today_Int_Pay,                
       coalesce(T9.interest,0)                                             as   CurMth_Paid_Int,              
       0                                                                   as   Accm_Paid_Int,                
       0.00                                                                as   Int_Adj_Amt,                  
       0.00                                                                as    Mth_Accm,                    
       0.00                                                                as    Yr_Accm,                     
       0.00                                                                as    Mth_Day_Avg_Bal,             
       0.00                                                                as    Yr_Day_Avg_Bal,              
       'CBS'                                                               as   Sys_Src                
from   DW_SDATA.cbs_001_ammst_corp T---单位分户账
LEFT JOIN  f_fdm.CD_RF_STD_CD_TRAN_REF T14 --需转换代码表
       ON  T.irate_adjust_way=T14.SRC_CD --源代码值相同
      AND  T14.DATA_PLTF_SRC_TAB_NM = 'CBS_001_AMMST_CORP' --数据平台源表主干名
      AND  T14.Data_Pltf_Src_Fld_Nm ='IRATE_ADJUST_WAY' 
LEFT JOIN  f_fdm.CD_RF_STD_CD_TRAN_REF T15 --需转换代码表
       ON  T.auto_flag=T15.SRC_CD  --源代码值相同
      AND  T15.DATA_PLTF_SRC_TAB_NM = 'CBS_001_AMMST_CORP' --数据平台源表主干名 
      AND  T15.Data_Pltf_Src_Fld_Nm ='AUTO_FLAG'               --数据平台源字段名
Left join
  (select  A.irate_code, A.bgn_date,A.irate1,A.cur_code
     from  DW_SDATA.cbs_001_pmctl_irate_code   A
inner join 
  (select irate_code ,max(bgn_date) as bgn_date 
     from DW_SDATA.cbs_001_pmctl_irate_code
    where start_dt<='$TXDATE'::date and '$TXDATE'::date<end_dt
 group by irate_code
  ) B
       on A.irate_code=B.irate_code
      and A.bgn_date=B.bgn_date
    where A.start_dt<='$TXDATE'::date and '$TXDATE'::date<A.end_dt
  ) T1
       ON T.irate_code=T1.irate_code    --利率代码
      and T.cur_code =T1.cur_code       --币种代码
left join 
  (select account||subacct as account  
     from  DW_SDATA.cbs_001_fsrgt_product
    where start_dt<='$TXDATE'::date and '$TXDATE'::date<end_dt
 group by account||subacct) T2
       ON T.account||T.subacct=T2.account
left join DW_SDATA.acc_003_t_acc_cdm_com_ledger   T3---负债类对公账户活期分户账
       ON T.account=T3.ACC and T3.SYS_CODE='99200000000'
      and T3.start_dt<='$TXDATE'::date and '$TXDATE'::date<T3.end_dt
left join DW_SDATA.acc_003_t_acc_fix_com_ledger T4---负债类对公账户定期分户账
       ON T.account||T.SUBACCT=T4.ACC  AND T4.SYS_CODE='99200000000'
      and T4.start_dt<='$TXDATE'::date and '$TXDATE'::date<T4.end_dt
left join DW_SDATA.acc_003_t_accdata_last_item_no T5---科目转换表
       ON T3.ITM_NO=T5.AMT_ITM AND T5.FIRST_ITM='20'  --负债类
      and T5.start_dt<='$TXDATE'::date and '$TXDATE'::date<T5.end_dt
left join DW_SDATA.acc_003_t_accdata_last_item_no T6---科目转换表
       ON T4.ITM_NO=T6.AMT_ITM AND T6.FIRST_ITM='20' --负债类
      and T6.start_dt<='$TXDATE'::date and '$TXDATE'::date<T6.end_dt
left join 
  (select account,subacct ,sum(cur_cope_int)  as cur_cope_int 
     from dw_sdata.cbs_001_amdtl_prep_int
    where substr(draw_date,1,6)=substr('$TXDATE' ,1,6)
       and draw_date::date <= '$TXDATE'::date
   --and  start_dt<='$TXDATE'::date and '$TXDATE'::date<end_dt
group by account,subacct )  T7 ---账户利息预提清单
      on T.account=T7.account  and T.SUBACCT=T7.subacct 
left join (select account,subacct,sum(interest)  as interest from DW_SDATA.cbs_001_amrgt_cal_int
where cal_int_date= '$TXDATE' group by account,subacct)  T8---账户结息登记簿
      ON T.account=T8.account  and T.subacct=T8.subacct  
   --and T8.start_dt<='$TXDATE'::date and '$TXDATE'::date<T8.end_dt
left join (select account,subacct,sum(interest) as interest
    from dw_sdata.cbs_001_amrgt_cal_int   
   where substr(cal_int_date,1,6)=substr('$TXDATE',1,6)
   and cal_int_date::date <= '$TXDATE'::date 
group by account,subacct) T9
      ON T.account=T9.account AND T.subacct=T9.subacct
/*
left join 
 (select account,subacct,sum(interest) as interest 
    from DW_SDATA.cbs_001_amrgt_cal_int 
   where etl_dt<='$TXDATE'::date
group by account,subacct) T10
      ON T.account=T10.account AND T.subacct=T10.subacct
*/
left join
DW_SDATA.cbs_001_pmctl_kind_type  T11---存款类别参数表
      ON T.dep_type=T11.dep_code   
     and T11.start_dt<='$TXDATE'::date and '$TXDATE'::date<T11.end_dt

left join 
 ( select * from 
 (
 select distinct
        account
        ,subacct,
        cur_cope_int,cur_face_int
        ,row_number()over(partition by account,subacct order by draw_date desc ,cur_face_int desc ) rn 
   from dw_sdata.cbs_001_amdtl_prep_int
  where draw_date::date <='$TXDATE'::date
  )  t where rn = 1
         )T13
      on T.account=T13.account  and T.SUBACCT=T13.subacct 
left join
  (select account,subacct ,sum(cur_cope_int)  as cur_cope_int
     from dw_sdata.cbs_001_amdtl_prep_int
       where  draw_date::date = '$TXDATE'::date
group by account,subacct )  T16 ---账户利息预提清单
      on T.account=T16.account  and T.SUBACCT=T16.subacct
   where T.start_dt<='$TXDATE'::date and '$TXDATE'::date<T.end_dt
     and  T.subacct<>'0'
 
;
-- 第二段
insert into f_fdm.f_dpst_corp_acct
(
       etl_date,                                  --数据日期       
       grp_typ,                                   --组别         
       Cust_Acct_Num,                             --客户账号          
       Sub_Acct,                                  --子账号            
       Org_Num,                                   --机构号            
       Cur_Cd,                                    --货币代码          
       Cust_Num,                                  --客户号            
       Prod_Cd,                                   --产品代码
       dpst_cate_cd,                              --存款类别代码          
       St_Int_Dt,                                 --起息日            
       Due_Dt,                                    --到期日            
       Open_Acct_Day,                             --开户日            
       Clos_Acct_Day,                             --销户日            
       Exec_Int_Rate,                             --执行利率          
       Bmk_Int_Rate,                              --基准利率          
       Basis,                                     --基差              
       Int_Rate_Adj_Mode_Cd,                      --利率调整方式代码  
       Adj_Prd_Typ_Cd,                            --调整周期类型代码  
       Int_Base_Cd,                               --计息基础代码      
       Cmpd_Int_Calc_Mode_Cd,                     --复利计算方式代码  
       Pre_Chrg_Int,                              --计息方式代码      
       Int_Rate_Attr_Cd,                          --利率属性代码      
       Orgnl_Term,                                --原始期限          
       Orgnl_Term_Corp_Cd,                        --原始期限单位代码  
       Rprc_Prd,                                  --重定价周期        
       Rprc_Prd_Corp_Cd,                          --重定价周期单代码
       Last_Rprc_Day,                             --上次重定价日      
       Next_Rprc_Day,                             --下次重定价日      
       Is_AutoRnw,                                --是否自动转存      
       Last_AutoRnw_Dt,                           --上次自动转存日期  
       Is_Cash_Mgmt_Acct,                         --是否现金管理账户  
       Agmt_Stat_Cd,                              --协议状态代码      
       Prin_Subj,                                 --本金科目          
       Curr_Bal,                                  --当前余额          
       Int_Subj,                                  --利息科目          
       Today_Provs_Int,                           --当日计提利息      
       CurMth_Provs_Int,                          --当月计提利息      
       Accm_Provs_Int,                            --累计计提利息      
       Today_Int_Pay,                             --当日付息          
       CurMth_Paid_Int,                           --当月已付息        
       Accm_Paid_Int,                             --累计已付息        
       Int_Adj_Amt,                               --利息调整金额      
       Mth_Accm,                                  --月积数            
       Yr_Accm,                                   --年积数            
       Mth_Day_Avg_Bal,                           --月日均余额        
       Yr_Day_Avg_Bal,                            --年日均余额        
       Sys_Src                                    --系统来源          
 )
 select
       '$TXDATE' :: date                                          as  etl_date,                             
       2                                                                   as   grp_typ,                   
       T.account                                                           as   Cust_Acct_Num,             
       T.subacct                                                           as   Sub_Acct,                  
       T.open_unit                                                         as   Org_Num,                   
       T.cur_code                                                          as   Cur_Cd,                    
       T.cust_id                                                           as   Cust_Num,                  
       T.dep_type                                                          as   Prod_Cd,
       coalesce(T9.flag,'')                                                as   dpst_cate_cd,                   
       T.int_start_date::date                                              as St_Int_Dt,                 
       T.due_date::date                                                    as Due_Dt,                    
       T.open_date::date                                                   as Open_Acct_Day,             
       T.close_date :: date                                                as Clos_Acct_Day,             
       coalesce(T.irate,0)                                                 as   Exec_Int_Rate,             
       coalesce(T1.irate1,0)                                               as   Bmk_Int_Rate,              
       coalesce(T1.irate1,0)-T.irate                                       as   Basis,                     
       coalesce(T14.TGT_CD,'@'||T.irate_adjust_way)                        as   Int_Rate_Adj_Mode_Cd,      
       T.adj_cycle_type                                                    as   Adj_Prd_Typ_Cd,            
       (CASE WHEN T9.flag ='3'  THEN '6' --保证金活期 实际/实际 
             ELSE '10' --保证金定期30/360  
        END)                                                              as  Int_Base_Cd,
       '1'                                                                AS Cmpd_Int_Calc_Mode_Cd, 
       '0'                                                                AS Pre_Chrg_Int,           
       (CASE WHEN T.irate_adjust_way IN  ('1','2') then '3' 
             WHEN  T.irate_adjust_way='0' THEN '1' else ''
        end)                                                              as   Int_Rate_Attr_Cd,                                                                                 
       coalesce(T.dep_term,0)                                             as   Orgnl_Term,            
       'D'                                                                as   Orgnl_Term_Corp_Cd,    
       (CASE
            WHEN T.due_date is null or T.due_date:: date<= '$TXDATE':: date then  0
            else T.due_date:: date-T.int_start_date:: date 
       end)     as   Rprc_Prd,  
       'D'                                                                as  Rprc_Prd_Corp_Cd,
       (case 
            when T.irate_adj_date is not null then T.irate_adj_date :: date 
            else T.due_date :: date 
        end)                                                              as  Last_Rprc_Day,       
       (Case when T9.flag ='3' --保证金活期
        then  '$TXDATE':: date+1
        Else 
           (Case when T.due_date is null then '$MINDATE' :: date 
            When T.due_date:: date<= '$TXDATE':: date then '$TXDATE':: date+1
            Else T.due_date :: date end )
        End)                                                             as    Next_Rprc_Day,            
       NVL(T15.TGT_CD,'@'||T.auto_flag)                                  as   Is_AutoRnw,          
       '$MINDATE' :: date                                            as   Last_AutoRnw_Dt,     
       (case when T2.account is not null then '1' else '0' 
        end)   as   Is_Cash_Mgmt_Acct,            
       T.acct_state                                                      as   Agmt_Stat_Cd,                 
       coalesce(T3.ITM_NO   ,'')                                         as   Prin_Subj,                    
       coalesce(T3.BAL      ,0)                                          as   Curr_Bal,                     
       coalesce(T4.ITM_NO   ,'')                                         as   Int_Subj,                     
       coalesce(T16.cur_cope_int ,0)                                     as   Today_Provs_Int,              
       coalesce(T5.cur_cope_int ,0)                                      as   CurMth_Provs_Int,             
       coalesce(T11.cur_face_int,0)                                      as   Accm_Provs_Int,               
       coalesce(T6.interest,0)                                           as   Today_Int_Pay,                
       coalesce(T7.interest     ,0)                                      as   CurMth_Paid_Int,              
       0                                                                 as   Accm_Paid_Int,                
       0.00                                                              as   Int_Adj_Amt,                  
       0                                                                 as    Mth_Accm,                    
       0                                                                 as    Yr_Accm,                     
       0                                                                 as    Mth_Day_Avg_Bal,             
       0                                                                 as    Yr_Day_Avg_Bal,              
       'CBS'                                                             as    Sys_Src   
from  dw_sdata.cbs_001_ammst_secu T ---保证金分户账
LEFT JOIN  f_fdm.CD_RF_STD_CD_TRAN_REF T14 --需转换代码表
       ON  T.irate_adjust_way=T14.SRC_CD  --源代码值相同
      AND  T14.DATA_PLTF_SRC_TAB_NM = 'CBS_001_AMMST_SECU' --数据平台源表主干名
      AND  T14.Data_Pltf_Src_Fld_Nm ='IRATE_ADJUST_WAY'  --数据平台源字段名
LEFT JOIN  f_fdm.CD_RF_STD_CD_TRAN_REF T15 --需转换代码表
       ON  T.auto_flag=T15.SRC_CD  --源代码值相同
      AND  T15.DATA_PLTF_SRC_TAB_NM = 'CBS_001_AMMST_SECU' --数据平台源表主干名
      AND  T15.Data_Pltf_Src_Fld_Nm ='AUTO_FLAG'               --数据平台源字段名
left join  (select 
                  A.irate_code
                  , A.bgn_date
                  ,A.irate1
                  ,A.cur_code 
     from   dw_sdata.cbs_001_pmctl_irate_code  A
inner join (select 
                  irate_code 
                  ,max(bgn_date) as bgn_date 
     from  dw_sdata.cbs_001_pmctl_irate_code  
    where  start_dt<='$TXDATE'::date and '$TXDATE'::date<end_dt
 group by  irate_code) B
       on  A.irate_code=B.irate_code
      and  A.bgn_date=B.bgn_date
    where  A.start_dt<='$TXDATE'::date and '$TXDATE'::date<A.end_dt) T1
       ON  T.irate_code=T1.irate_code    --利率代码
      AND  T.cur_code =T1.cur_code       --币种代码
left join (select 
                 account||subacct as account  
     from  dw_sdata.cbs_001_fsrgt_product
    where  start_dt<='$TXDATE'::date and '$TXDATE'::date<end_dt
 group by  account||subacct) T2
       ON  T.account||T.subacct=T2.account
left join  (select 
                 ACC
                 ,curr_type
                 ,bal
                 ,ITM_NO      
     from  dw_sdata.ACC_003_T_ACC_CDM_COM_LEDGER  --负债类对公账户活期分户账
    where  sys_code='99200000000'    
      and  start_dt<='$TXDATE'::date and '$TXDATE'::date<end_dt   
    union  select  
                 ACC
                 ,curr_type
                 ,bal
                 ,ITM_NO  
     from  dw_sdata.ACC_003_T_ACC_FIX_COM_LEDGER --负债类对公账户定期分户账
    where  sys_code='99200000000' 
      and  start_dt<='$TXDATE'::date and '$TXDATE'::date<end_dt
            ) T3
       on  T.account||lpad(to_char(T.subacct),6,'0')=T3.ACC
left join  dw_sdata.ACC_003_T_ACCDATA_LAST_ITEM_NO T4 --科目转换表
       ON  T3.ITM_NO=T4.AMT_ITM
      AND  T4.FIRST_ITM='20'  --负债类
      and  T4.start_dt<='$TXDATE'::date and '$TXDATE'::date<T4.end_dt
left join (select 
                 account
                 ,subacct 
                 ,sum(cur_cope_int)  as cur_cope_int  
     from dw_sdata.cbs_001_amdtl_prep_int
    where substr(draw_date,1,6)=substr('$TXDATE' ,1,6)
      and draw_date::date <= '$TXDATE'::date
 group by account,subacct )  T5 ---账户利息预提清单
       on T.account=T5.account  and T.SUBACCT=T5.subacct 
left join (select account,subacct,sum(interest)as interest from DW_SDATA.cbs_001_amrgt_cal_int                
where cal_int_date= '$TXDATE'  group by account,subacct)  T6---账户结息登记簿
      ON T.account=T6.account  and T.subacct=T6.subacct  
      --and T6.start_dt<='$TXDATE'::date and '$TXDATE'::date<T6.end_dt
left join (select
                 account
                 ,subacct
                 ,sum(interest) as interest 
    from  dw_sdata.cbs_001_amrgt_cal_int--账户结息登记簿 
   where substr(cal_int_date,1,6)=substr('$TXDATE' ,1,6)
    and cal_int_date::date <= '$TXDATE'::date 
group by account,subacct) T7
      ON T.account=T7.account AND T.subacct=T7.subacct
/*left join  (select account,subacct,sum(interest) as interest from     dw_sdata.cbs_001_amrgt_cal_int 
   where etl_dt<='$TXDATE'::date 
group by account,subacct) T8
      ON T.account=T8.account AND T.subacct=T8.subacct */
left join  DW_SDATA.cbs_001_pmctl_kind_type T9---存款类别参数表
      ON T.dep_type=T9.dep_code  
     and T9.start_dt<='$TXDATE'::date and '$TXDATE'::date<T9.end_dt 

 left join (select * from 
              (
        select distinct
        account
        ,subacct,
        cur_cope_int,cur_face_int
        ,row_number()over(partition by account,subacct order by draw_date desc ,cur_face_int desc ) rn 
   from dw_sdata.cbs_001_amdtl_prep_int
  where draw_date::date <='$TXDATE'::date
  )  t where rn = 1 
             )T11
      on T.account=T11.account  
     and T.SUBACCT=T11.subacct 
left join (select
                 account
                 ,subacct
                 ,sum(cur_cope_int)  as cur_cope_int
     from dw_sdata.cbs_001_amdtl_prep_int
      where  draw_date::date = '$TXDATE'::date
 group by account,subacct )  T16 ---账户利息预提清单
       on T.account=T16.account  and T.SUBACCT=T16.subacct
   where T.start_dt<='$TXDATE'::date 
     and '$TXDATE'::date<T.end_dt
     and  T.subacct<>'0' 
;
 

insert into tt_f_dpst_corp_acct_yjs 
(Cust_Acct_Num ,
 Sub_Acct ,
 mth_accm ,
 yr_accm ,
 mth_day_avg_bal,
 Yr_Day_Avg_Bal
)
select 
       t.Cust_Acct_Num
      ,t.Sub_Acct
      ,(case 
            when '$TXDATE'= '$MONTHBGNDAY' 
            then t.Curr_Bal
            else t.Curr_Bal+coalesce(t1.mth_accm,0)
            end
       )                                                                      as mth_accm  --月积数
      ,(case 
            when  '$TXDATE' = '$YEARBGNDAY' 
            then t.Curr_Bal
            else t.Curr_Bal+coalesce(t1.yr_accm,0)
            end
       )                                                                      as yr_accm   --年积数
      ,(case 
            when '$TXDATE' = '$MONTHBGNDAY' 
            then t.Curr_Bal
            else t.Curr_Bal+coalesce(t1.mth_accm,0)
            end
       )/('$TXDATE'::date-'$MONTHBGNDAY'::date+1)               as mth_day_avg_bal  --月日均余额
      ,(case 
           when '$TXDATE' = '$YEARBGNDAY' 
           then t.Curr_Bal
           else t.Curr_Bal+coalesce(t1.yr_accm,0)
           end
       )/('$TXDATE'::date-'$YEARBGNDAY'::date+1)                    as Yr_Day_Avg_Bal   --年日均余额
from  f_fdm.f_dpst_corp_acct     t
left join f_fdm.f_dpst_corp_acct t1
       on t.Cust_Acct_Num=t1.Cust_Acct_Num
      and t.Sub_Acct =t1.Sub_Acct 
      and t1.etl_date='$TXDATE'::date-1
    where t.etl_date='$TXDATE'::date
;

/*月积数、年积数、月日均余额、年日均余额临时表创建区END*/
/*更新月积数、年积数、月日均余额、年日均余额*/
update f_fdm.f_dpst_corp_acct    t
   set mth_accm=t1.mth_accm 
       ,yr_accm=t1.yr_accm
       ,mth_day_avg_bal=t1.mth_day_avg_bal
       ,Yr_Day_Avg_Bal=t1.Yr_Day_Avg_Bal
  from tt_f_dpst_corp_acct_yjs   t1
 where t.Cust_Acct_Num=t1.Cust_Acct_Num
   and t.Sub_Acct =t1.Sub_Acct 
   and t.etl_date='$TXDATE'::date
;

/*数据处理区end*/
commit;

