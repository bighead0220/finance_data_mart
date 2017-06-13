/*
Author             :XMH
Function           :国债代销账户信息
Load method        :INSERT
Source table       :
Frequency          :D
Modify history list:Created by徐铭浩2016年5月13日10:05:55
                   :Modify  by lxz 20160614 修改月积数等字段
                   :Modify  by xsh 20160715 在表f_acct_tbond_agn_yjs_tmp前面增加schema前缀f_fdm醉
                   :modified by zhangliang 20160831 将组1中t2,t4关联表删除，并修改"客户经理编号"映射规则；将组2中t5关联表删除，并修改"客户经理编号"映射规则
                   modified by liudongyan 20160909 将组别2Tbond_Cust_Typ_Cd的映射规则由Y改为1,（脚本和映射不一致）
                   modified by zhangliang 20160920 将 fss_001_cd_aheadcashcom_note,fss_001_cd_cash_note,fss_001_cd_aheadcash_note,fss_001_cd_cashcom_not拉链表修改为流水表并修改逻辑
                   modified by liudongyan at 20161010  �޸����2 T6��
                   MODIFIED BY ZMX 20161017 修改t3
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
*-------------------------------------------逻辑说明END------------------------------------------
create local temp table tt_f_acct_tbond_agn_temp
 on commit preserve rows as
select *
  from f_fdm.f_acct_tbond_agn
  where 1=2
;
/*临时表区end */
/*数据回退区*/
Delete /* +direct */ from  f_fdm.f_acct_tbond_agn
where etl_date='$TXDATE'::date
/*数据回退区end*/
;
/*数据处理区*/
/*
INSERT INTO f_fdm.f_acct_tbond_agn
(
         etl_date                                                          --组别            
        ,grp_typ                                                           --数据日期        
        ,Agmt_Id                                                           --协议编号        
        ,Tbond_Cd                                                          --国债代码        
        ,Cust_Num                                                          --客户号          
        ,Org_Num                                                           --机构号          
        ,Cur_Cd                                                            --货币代码        
        ,Subj_Cd                                                           --科目代码        
        ,Buy_Amt                                                           --购买金额        
        ,Curr_Bal                                                          --当前余额        
        ,Actl_Pmt_Int                                                      --实付利息        
        ,Tbond_Int_Rate                                                    --国债利率        
        ,Int_Rate_Attr_Cd                                                  --利率属性代码    
        ,Term_Prd_Cnt                                                      --期限周期数      
        ,Term_Prd_Typ_Cd                                                   --期限周期种类代码
        ,Tbond_Cust_Typ_Cd                                                 --国债客户类型代码
        ,Is_Adv_Cash_Ind                                                   --是否提前兑付标志
        ,Bond_Typ_Cd                                                       --债券种类代码    
        ,Agmt_Stat_Cd                                                      --协议状态代码    
        ,St_Int_Dt                                                         --起息日期        
        ,Open_Dt                                                           --开立日期        
        ,Due_Dt                                                            --到期日期        
        ,Termn_Dt                                                          --终止日期        
        ,Cust_Mgr_Id                                                       --客户经理编号    
        ,Mth_Accm                                                          --月积数          
        ,Yr_Accm                                                           --年积数          
        ,Mth_Day_Avg_Bal                                                   --月日均余额      
        ,Yr_Day_Avg_Bal                                                    --年日均余额      
        ,Sys_Src                                                           --系统来源        
)
select 
        '$TXDATE' :: date                                         as  etl_date   
        ,'1'                                                               as  grp_typ 
        ,coalesce(T.ACCOUNTCODE||'_'||T.openorgan,'')                                    as  Agmt_Id              
        ,coalesce(T.KINDCODE       ,'')                                    as  Tbond_Cd             
        ,coalesce(T1.ECIFCUSTOMERNO,'')                                    as  Cust_Num             
        ,coalesce(T.OPENORGAN      ,'')                                    as  Org_Num              
        ,'156'                                                             as  Cur_Cd               
        ,null                                                              as  Subj_Cd              
        ,coalesce(T.BOOKAMT,0)                                             as  Buy_Amt              
        ,coalesce(T.BOOKAMT,0)                                             as  Curr_Bal             
        ,coalesce(T5.INTEREST,0)                                           as  Actl_Pmt_Int         
        ,coalesce(T.JELILV ,0)                                             as  Tbond_Int_Rate       
        ,'1'                                                               as  Int_Rate_Attr_Cd      
        ,coalesce(T3.LIMITEDPERIOD,0)                                      as  Term_Prd_Cnt         
        ,'3'                                                               as  Term_Prd_Typ_Cd      
        ,coalesce(T.SELLFLAG ,'')                                          as  Tbond_Cust_Typ_Cd    
        ,'1'                                                               as  Is_Adv_Cash_Ind      
        ,coalesce(T3.KINDTYPE,'')                                          as  Bond_Typ_Cd          
        ,coalesce(T.ACCTFLAG  ,'')                                         as  Agmt_Stat_Cd         
        ,case when T.QXDATE=null then to_date(T.QXDATE,'yyyymmdd') else to_date(T.QXDATE,'yyyymmdd') end as St_Int_Dt         
        ,case when T.OPENDATE=null then to_date(T.OPENDATE,'yyyymmdd')   else to_date(T.OPENDATE,'yyyymmdd') end as   Open_Dt           
        ,coalesce(to_date(T.DELDATE,'yyyymmdd'))                            as  Due_Dt            
        ,coalesce(to_date(T.DELDATE,'yyyymmdd'))                            as   Termn_Dt               
        ,coalesce(T.CUSTOMERMGRCODE ,'')                                   as  Cust_Mgr_Id          
        ,0.00                                                              as  Mth_Accm             
        ,0.00                                                              as  Yr_Accm              
        ,0.00                                                              as  Mth_Day_Avg_Bal      
        ,0.00                                                              as  Yr_Day_Avg_Bal       
        ,'FSS'                                                             as  Sys_Src        
from dw_sdata.fss_001_cd_account_note T-- (凭证国债分户表)         
LEFT JOIN  dw_sdata.fss_001_fd_customerinfo T1 --(客户基本资料表)
on T.CUSTOMERID=T1.CUSTOMERID  
AND T1.start_dt<='$TXDATE'::date and '$TXDATE'::date<T1.end_dt 
--left join  dw_sdata.fss_001_cd_current_rate T2 --(利率表)
--on T.KINDCODE=T2.KINDCODE
--AND T2.start_dt<='$TXDATE'::date and '$TXDATE'::date<T2.end_dt 
--AND BEGINDATE='$MAXDATE'
--AND $TXDATE -T.QXDATE=T2.RATELEVEL
LEFT JOIN  dw_sdata.fss_001_cd_kind_main T3-- (凭证国债券种主表)
on T.KINDCODE=T3.KINDCODE
AND T3.start_dt<='$TXDATE'::date and '$TXDATE'::date<T3.end_dt
--LEFT JOIN  dw_sdata.fss_001_fd_custtomgr T4-- (客户理财经理对照表)
--on T.CUSTOMERID=T4.CUSTOMERID
--AND T4.start_dt<='$TXDATE'::date and '$TXDATE'::date<T4.end_dt 
left join                                                                                                                                    
     (
      select sum(t.YJACCRUAL-t.YKACCRUAL+t.yjinterest) AS interest,t.CUSTOMERID,t.KINDCODE                                                                    
      from (                                                                                                                                                  
            select t.CUSTOMERID
                   ,t.KINDCODE
                   ,0 as YJACCRUAL
                   ,0 as YKACCRUAL
                   ,t6.INTEREST as yjinterest                                                                  
             from
                 ( 
                 select * 
                  from dw_sdata.fss_001_cd_account_note 
                 where start_dt<='$TXDATE' ::date 
                   and end_dt>'$TXDATE'::date
                 ) T--凭证国债分户表                                         
inner join 
      (select ACCOUNTCODE,KINDCODE,sum(INTEREST) as INTEREST 
       from dw_sdata.fss_001_cd_cashcom_note 
       where etl_dt<='$TXDATE'::date 
        and state='0'
        group by 1,2
      )T6-- 凭证国债到期兑付登记簿对公        
ON T.CUSTOMERID=T6.CUSTOMERID                                                                                                                           
AND T.KINDCODE =T6.KINDCODE                                                                                                                             
union                                                                                                                                                   
select 
      t.CUSTOMERID
      ,t.KINDCODE
      ,t7.INTERESTPLUS as YJACCRUAL
      , t7.INTERESTSUB as YKACCRUAL
      , 0 as yjinterest                                               
from
     (select * 
     from dw_sdata.fss_001_cd_account_note  
     where start_dt<='$TXDATE' ::date 
      and end_dt>'$TXDATE'::date
     )T--凭证国债分户表                                      
inner join 
     (select  ACCOUNTCODE,KINDCODE,sum(INTERESTSUB) as INTERESTSUB
      from dw_sdata.fss_001_cd_aheadcashcom_note  
      where etl_dt<='$TXDATE' ::date 
      and state='0'
      group by 1,2
     ) T7-- 凭证国债提前兑付登记簿对公
ON T.CUSTOMERID=T7.CUSTOMERID                                                                                                                           
AND T.KINDCODE =T7.KINDCODE                                                                                                                             
union                                                                                                                                                   
select t.CUSTOMERID
       ,t.KINDCODE
       ,0 as YJACCRUAL
       , 0 as YKACCRUAL
       , t8.INTEREST as yjinterest                                                                
 from 
      (
      select * 
       from dw_sdata.fss_001_cd_account_note  
       where start_dt<='$TXDATE' ::date 
       and end_dt>'$TXDATE'::date
      )  T--凭证国债分户表                                      
inner join 
      (
       select ACCOUNTCODE,KINDCODE,sum(INTEREST) as INTEREST 
       from dw_sdata.fss_001_cd_cash_note  
       where etl_dt<='$TXDATE'::date 
       and state='0'
       group by 1,2
      ) T8-- 凭证国债到期兑付登记簿             
ON T.CUSTOMERID=T8.CUSTOMERID                                                                                                                           
AND T.KINDCODE =T8.KINDCODE                                                                                                                             
union                                                                                                                                                   
select 
      t.CUSTOMERID
      ,t.KINDCODE
      ,t9.INTERESTPLUS as YJACCRUAL
      , t9.INTERESTSUB as YKACCRUAL
      ,0 as yjinterest                                                
 from 
      (select * 
       from dw_sdata.fss_001_cd_account_note  
       where start_dt<='$TXDATE'::date 
       and end_dt>'$TXDATE'::date
      )  T--凭证国债分户表                                      
inner join 
      (
       select ACCOUNTCODE,KINDCODE,sum(INTERESTSUB) as INTERESTSUB 
       from dw_sdata.fss_001_cd_aheadcash_note  
       where etl_dt<='$TXDATE'::date 
        and state='0'
     group by 1,2
       ) T9-- 凭证国债到期兑付登记簿        
ON T.CUSTOMERID=T9.CUSTOMERID                                                                                                                           
AND T.KINDCODE =T9.KINDCODE                                                                                                                             
) t                                                                                                                                                     
group by t.CUSTOMERID,t.KINDCODE) T5 --利息临时表  
on T.CUSTOMERID=T5.CUSTOMERID AND T.KINDCODE=T5.KINDCODE   
WHERE  T.start_dt<='$TXDATE'::date 
and '$TXDATE'::date<T.end_dt
;        
commit;
*/


INSERT INTO f_fdm.f_acct_tbond_agn
(
         etl_date                         --组别            
        ,grp_typ                          --数据日期        
        ,Agmt_Id                          --协议编号        
        ,Tbond_Cd                         --国债代码        
        ,Cust_Num                         --客户号          
        ,Org_Num                          --机构号          
        ,Cur_Cd                           --货币代码        
        ,Subj_Cd                          --科目代码        
        ,Buy_Amt                          --购买金额        
        ,Curr_Bal                         --当前余额        
        ,Actl_Pmt_Int                     --实付利息        
        ,Tbond_Int_Rate                   --国债利率        
        ,Int_Rate_Attr_Cd                 --利率属性代码    
        ,Term_Prd_Cnt                     --期限周期数      
        ,Term_Prd_Typ_Cd                  --期限周期种类代码
        ,Tbond_Cust_Typ_Cd                --国债客户类型代码
        ,Is_Adv_Cash_Ind                  --是否提前兑付标志
        ,Bond_Typ_Cd                      --债券种类代码    
        ,Agmt_Stat_Cd                     --协议状态代码    
        ,St_Int_Dt                        --起息日期        
        ,Open_Dt                          --开立日期        
        ,Due_Dt                           --到期日期        
        ,Termn_Dt                         --终止日期        
        ,Cust_Mgr_Id                      --客户经理编号    
        ,Mth_Accm                         --月积数          
        ,Yr_Accm                          --年积数          
        ,Mth_Day_Avg_Bal                  --月日均余额      
        ,Yr_Day_Avg_Bal                   --年日均余额      
        ,Sys_Src                          --系统来源        
)
select 
        '$TXDATE' :: date                                 as  etl_date   
        ,'2'                                                       as  grp_typ 
        ,coalesce(T.CUSTOMERID||'_'||T.savingbondacct,'')                            as  Agmt_Id              
        ,coalesce(T.KINDCODE       ,'')                            as  Tbond_Cd             
        ,coalesce(T1.ECIFCUSTOMERNO,'')                            as  Cust_Num             
        ,coalesce(T2.ORGANCODE     ,'')                            as  Org_Num              
        ,'156'                                                      as  Cur_Cd               
        ,null                                                      as  Subj_Cd               
        ,coalesce(T.LEAVAMT   ,0)                                  as  Buy_Amt              
        ,coalesce(T.LEAVAMT   ,0)                                  as  Curr_Bal             
        ,coalesce(T6.INTEREST ,0)                                  as  Actl_Pmt_Int         
        ,coalesce(T4.FXGDRATEDEBTYEARRATE ,0)                      as  Tbond_Int_Rate      
        ,'1'                                                       as  Int_Rate_Attr_Cd     
        ,case when T4.TIMELIMIT=null then T4.TIMELIMIT::numeric else T4.TIMELIMIT::numeric  end as  Term_Prd_Cnt         
        ,'3'                                                       as  Term_Prd_Typ_Cd      
        ,coalesce(T1.CUSTOMERKIND,'')                              as  Tbond_Cust_Typ_Cd    
        ,'1'                                                       as  Is_Adv_Cash_Ind      
        ,coalesce(T4.KINDTYPE,'')                                   as  Bond_Typ_Cd          
        ,coalesce(T.ACCTSTATE,'')                                  as  Agmt_Stat_Cd          
        ,case when T.QXDATE=null then T.QXDATE::date  else  T.QXDATE::date end as St_Int_Dt         
        ,case when T.OPENDATE=null then T.OPENDATE::date  else T.OPENDATE::date  end as   Open_Dt           
        ,case when T.JEDATE=null then T.JEDATE::date  else T.JEDATE::date  end as   Due_Dt            
        ,case when T.JEDATE=null then T.JEDATE::date else T.JEDATE::date end  as   Termn_Dt                
        ,coalesce(T3.CUSTOMERMGRCODE,'')                           as  Cust_Mgr_Id          
        ,0.00                                                       as  Mth_Accm             
        ,0.00                                                      as  Yr_Accm              
        ,0.00                                                      as  Mth_Day_Avg_Bal      
        ,0.00                                                      as  Yr_Day_Avg_Bal       
        ,'FSS'                                                     as  Sys_Src       
from dw_sdata.fss_001_sd_account_note T --(储蓄国债分户表)     
LEFT JOIN  dw_sdata.fss_001_fd_customerinfo T1-- (客户基本资料表)
on T.CUSTOMERID=T1.CUSTOMERID
AND T1.start_dt<='$TXDATE'::date and '$TXDATE'::date<T1.end_dt   
left join  dw_sdata.fss_001_sd_acctopen_note T2 --(储蓄国债托管账户开户登记簿)
on T.CUSTOMERID=T2.CUSTOMERID
AND T2.start_dt<='$TXDATE'::date and '$TXDATE'::date<T2.end_dt 
LEFT JOIN (SELECT CUSTOMERID,SAVINGBONDACCT,KINDCODE,CUSTOMERMGRCODE, ROW_NUMBER() OVER(PARTITION BY  CUSTOMERID,SAVINGBONDACCT,KINDCODE ORDER BY acctserial DESC) NUM  
FROM dw_sdata.fss_001_sd_takeup_note 
where  start_dt<='$TXDATE'::date and '$TXDATE'::date<end_dt
)T3   --MODIFIED BY ZMX 20161017
--dw_sdata.fss_001_sd_takeup_note T3 --(储蓄国债认购登记簿)----null。。。。。。。。。。。。。。。。。。。
on T.CUSTOMERID=T3.CUSTOMERID
AND T.SAVINGBONDACCT=T3.SAVINGBONDACCT
AND T.KINDCODE=T3.KINDCODE
AND T3.NUM = 1
LEFT JOIN  dw_sdata.fss_001_sd_kind_main T4 --(储蓄国债劵种主表)
on T.KINDCODE=T4.KINDCODE
AND T4.start_dt<='$TXDATE'::date and '$TXDATE'::date<T4.end_dt 
--left join  dw_sdata.fss_001_fd_custtomgr T5 --(客户理财经理对照表）
--on T.CUSTOMERID=T5.CUSTOMERID 
left join (
select sum(t.YJACCRUAL-t.YKACCRUAL+t.yjinterest)  as INTEREST
       ,t.CUSTOMERID as CUSTOMERID
       ,t.KINDCODE as KINDCODE
     --,t.CUSTOMERID
     --,t.SAVINGBONDACCT
     --,t.KINDCODE
      --,t.YJACCRUAL
      --,t.YKACCRUAL
      --,t.yjinterest
       --,t.YJACCRUAL-t.YKACCRUAL+t.yjinterest
from 
      (
      select t.CUSTOMERID
             ,t.SAVINGBONDACCT
             ,t.KINDCODE
             ,t6.YJACCRUAL
             ,t6.YKACCRUAL,
             0 as yjinterest
       from
            ( 
             select * 
             from  dw_sdata.fss_001_sd_account_note 
             where start_dt<='$TXDATE'::date 
             and end_dt>'$TXDATE'::date
            ) T--储蓄国债分户表
            inner join 
                     (select 
                            * 
                       from  dw_sdata.fss_001_sd_aheadcash_note 
                      where start_dt<='$TXDATE'::date  
                       and end_dt>'$TXDATE'::date
                     )T6-- 储蓄国债提前兑付登记簿
                 ON T.CUSTOMERID=T6.CUSTOMERID 
                 AND T.SAVINGBONDACCT=T6.SAVINGBONDACCT 
                 AND T.KINDCODE =T6.KINDCODE
          union 
          select 
                t.CUSTOMERID
                ,t.SAVINGBONDACCT
                ,t.KINDCODE
                ,0 as YJACCRUAL
                , 0 as YKACCRUAL
                , t7.YJINTEREST as yjinterest
          from 
              (
               select * 
                from  dw_sdata.fss_001_sd_account_note  
                where start_dt<='$TXDATE'::date 
                and end_dt>'$TXDATE'::date
              )  T--储蓄国债分户表
inner join 
          (
           select * 
           from dw_sdata.fss_001_sd_cash_note  
           where start_dt<='$TXDATE'::date and end_dt>'$TXDATE'::date
         ) T7-- 储蓄国债到期   兑付和定期付息登记簿 
         ON T.CUSTOMERID=T7.CUSTOMERID 
         AND T.SAVINGBONDACCT=T7.SAVINGBONDACCT 
         AND T.KINDCODE =T7.KINDCODE
) t
         group by t.CUSTOMERID,t.KINDCODE/*,t.SAVINGBONDACCT,t.YJACCRUAL,t.YKACCRUAL,t.yjinterest */ )T6 --实付利息临时表
         on T.CUSTOMERID=T6.CUSTOMERID AND T.KINDCODE=T6.KINDCODE
         WHERE  T.start_dt<='$TXDATE'::date and '$TXDATE'::date<T.end_dt
;  

/*数据处理区END*/

/*计算月积数等字段*/
      
create local temporary table tt_f_acct_tbond_agn_yjs 
on commit preserve rows as 
select t.Agmt_Id
      ,t.Tbond_Cd
      ,(case 
            when '$TXDATE'= '$MONTHBGNDAY' then t.Curr_Bal
            else t.Curr_Bal+coalesce(t1.Mth_Accm,0)
        end
       )                                                                              as Mth_Accm  --月积数
      ,(case 
            when  '$TXDATE' = '$YEARBGNDAY' then t.Curr_Bal
            else t.Curr_Bal+coalesce(t1.Yr_Accm,0)
        end
       )                                                                              as Yr_Accm  --年积数
       ,(case 
            when '$TXDATE' = '$MONTHBGNDAY' then t.Curr_Bal
            else t.Curr_Bal+coalesce(t1.Mth_Accm,0)
        end
        )/('$TXDATE'::date-'$MONTHBGNDAY'::date+1)                      as Mth_Day_Avg_Bal  --月日均余额
       ,(case 
            when '$TXDATE' = '$YEARBGNDAY' then t.Curr_Bal
            else t.Curr_Bal+coalesce(t1.Yr_Accm,0)
        end
        )/('$TXDATE'::date-'$YEARBGNDAY'::date+1)                           as Yr_Day_Avg_Bal   --年日均余额
from f_fdm.f_acct_tbond_agn t
left join f_fdm.f_acct_tbond_agn t1
on t.Agmt_Id=t1.Agmt_Id
and t.Tbond_Cd=t1.Tbond_Cd
and t1.etl_date='$TXDATE'::date-1
where t.etl_date='$TXDATE'::date
;
/*计算月积数等字段END*/

/*更新目标表月积数等字段*/
update f_fdm.f_acct_tbond_agn t
set Mth_Accm=t1.Mth_Accm
   ,Yr_Accm=t1.Yr_Accm
   ,Mth_Day_Avg_Bal=t1.Mth_Day_Avg_Bal
   ,Yr_Day_Avg_Bal=t1.Yr_Day_Avg_Bal
from tt_f_acct_tbond_agn_yjs t1
where t.Agmt_Id=t1.Agmt_Id
and   t.Tbond_Cd=t1.Tbond_Cd
and   t.etl_date='$TXDATE'::date 
;
/*更新目标表月积数等字段END*/

commit;                                                                                        
