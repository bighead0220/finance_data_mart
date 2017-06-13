/*
Author             :zhangliang
Function           :资金存放信息表
Load method        :
Source table       :dw_sdata.cos_000_deals              T   --交易信息主表
                    dw_sdata.cos_000_mmdeals            T1  --货币市场交易表
                    dw_sdata.cos_000_cflows                 --清算信息主表
                    f_fdm.cd_cd_table                   T2  --代码表
                    dw_sdata.ecf_002_t01_cust_info_T    T3  --同业客户基本信息
                    dw_sdata.cos_000_gl_entry           gl  --账务信息主表，存放交易会计分录信息
                    dw_sdata.cos_000_chart_acc         acc  --科目表，业务人员人工录入
                    f_fdm.cd_rf_std_cd_tran_ref             --需转换代码表
Destination Table  :f_fdm.f_agt_cap_stor
Frequency          :D
Modify history list:Created by zhangliang at 2016-8-3 14:35 v1.0
                    Modify by  zhangliang at 2016-817 11:40 1、将b.etl_date='$TXDATE'::date
                                                            2、将276行注释
                    :Modify by zhangliang at 2016-8-18 14:50 将98行prepaid修改为PREPAID
                     modified by wyh 20160912 修改机构代码，交易对手客户号
                    modified  by zhangliang 20161009   �����¼223���޸�t5
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/


/*数据回退区*/
delete /* +direct */ from f_fdm.f_agt_cap_stor
where etl_date = '$TXDATE'::date
;
/*数据回退区END*/

/*数据处理区*/
insert /* +direct */  into f_fdm.f_agt_cap_stor
      (grp_typ                                               --组别       
       ,etl_date                                              --数据日期     
       ,Agmt_Id                                               --协议编号     
       ,TX_Cnt_Pty_Cust_Num                                   --交易对手客户号  
       ,Org_Num                                               --机构号      
       ,Cust_Acct_Num                                         --客户账号     
       ,Acct_Stat_Cd                                          --帐户状态代码   
       ,Cur_Cd                                                --货币代码     
       ,Prod_Cd                                               --产品代码
       ,TX_Comb_Cd                                            --交易组合码
       ,St_Int_Dt                                             --起息日      
       ,Due_Dt                                                --到期日      
       ,Int_Base_Cd                                           --计息基础代码   
       ,Cmpd_Int_Calc_Mode_Cd                                 --复利计算方式代码 
       ,Pre_Chrg_Int                                          --是否先收息    
       ,Int_Rate_Attr_Cd                                      --利率属性代码   
       ,Rprc_Prd                                              --重定价周期    
       ,Rprc_Prd_Corp_Cd                                      --重定价周期单位代码
       ,Last_Rprc_Day                                         --上次重定价日   
       ,Next_Rprc_Day                                         --下次重定价日   
       ,Int_Pay_Freq                                          --付息频率     
       ,Orgnl_Term                                            --原始期限     
       ,Orgnl_Term_Corp_Cd                                    --原始期限单位代码 
       ,Bmk_Int_Rate                                          --基准利率     
       ,Curr_Int_Rate                                         --当前利率     
       ,Basis                                                 --基差       
       ,Prin_Subj                                             --本金科目     
       ,Curr_Bal                                              --当前余额     
       ,Int_Subj                                              --利息科目     
       ,Today_Provs_Int                                       --当日计提利息   
       ,CurMth_Provs_Int                                      --当月计提利息   
       ,Accm_Provs_Int                                        --累计计提利息   
       ,Today_Acpt_Pay_Int                                    --当日收付息    
       ,CurMth_Recvd_Int_Pay                                  --当月已收付息   
       ,Accm_Recvd_Int_Pay                                    --累计已收付息   
       ,Int_Adj_Subj                                          --利息调整科目   
       ,Int_Adj_Amt                                           --利息调整金额   
       ,Mth_Accm                                              --月积数      
       ,Yr_Accm                                               --年积数      
       ,Mth_Day_Avg_Bal                                       --月日均余额    
       ,Yr_Day_Avg_Bal                                        --年日均余额    
       ,Sys_Src                                               --系统来源     
      )
select                                                                
      1                                                                            as        grp_typ                   --组别        
      ,'$TXDATE'::date                                                    as        etl_date                  --数据日期      
      ,t.deal_no                                                                   as        Agmt_Id                   --协议编号                                                                       
      ,coalesce(t3.ecif_cust_no,'@'||t.cparty)                                                as        TX_Cnt_Pty_Cust_Num       --交易对手客户号                                                                         
      ,coalesce(T_org_2.name,'')
                                                                  as        Org_Num                   --机构号                                                                     
      ,coalesce(t4.bank_acc,'')                                                    as        Cust_Acct_Num             --客户账号                                                                                  
      ,case                                                                          
           when t.in_use = 'Y' 
           and substr(t.cur_mat_dt,1,8) > to_char('$TXDATE'::date,'yyyymmdd') 
           then '0'   --正常
           else '3'   --已销户 
           end                                                                     as         Acct_Stat_Cd              --帐户状态代码               
      ,coalesce(t6.tgt_cd,'@' || t.ccy)                                            as         Cur_Cd                    --货币代码                                                                         
      ,coalesce(t.sectype,'')                                                      as         Prod_Cd                   --产品代码                                                                             
      ,coalesce(t.entity,'')                                                       as         TX_Comb_Cd                                                                                                                                
      ,to_date(t.settle_dt,'yyyymmdd')                                             as         St_Int_Dt                 --起息日                                                                                                         
      ,to_date(t.cur_mat_dt,'yyyymmdd')                                            as         Due_Dt                    --到期日                                            
      ,coalesce(t7.tgt_cd,'@' || t1.INT_DAYS)                                      as         Int_Base_Cd               --计息基础代码    
      ,1                                                                           as         Cmpd_Int_Calc_Mode_Cd     --复利计算方式代码     
      ,case                                                                            
           when t1.int_timing = 'PERIODIC' then '0'    --按周期                                  
           when t1.int_timing = 'PREPAID'  then '1'    --预付
           when t1.int_timing = ''         then ''
           else '@' || t1.int_timing                  --映射表有错
           end                                                                     as         Pre_Chrg_Int              --是否先收息
      ,case                                                                        
           when t1.fix_float = 'FIXED'     then '1'
           when t1.fix_float = 'FLOATING'
           and  t1.review_frq <> ''        then '3'    --按定期浮动利率
           else '4'                                    --按不定期浮动利率  
           end                                                                     as         Int_Rate_Attr_Cd          --利率属性代码      
      ,case   when t1.fix_float = 'FLOATING' then (case                                                                      
           when t1.review_frq in('DAILY','MONTHLY','ANNUAL')  then 1
           when t1.review_frq = 'SEMI ANNUAL'   then 6
           when t1.review_frq = 'QUARTERLY' then 3
           when t1.review_frq = 'WEEKLY' then 7
           else 0
           end)
           else 0
           end                                                                     as         Rprc_Prd                   --重定价周期
      ,case                                                                        
           when t1.fix_float = 'FLOATING'
           then (case when t1.review_frq in ('DAILY','WEEKLY') then 'D'
                      when t1.review_frq in ('MONTHLY','QUARTERLY','SEMI ANNUAL') then 'M'
                      when t1.review_frq='ANNUAL'  then 'Y'
                      else ''
                      end)
           else ''
           end                                                                     as         Rprc_Prd_Corp_Cd           --重定价周期单位代码
      ,coalesce(to_date(t1.review_dt,'yyyymmdd'),'$MINDATE'::date)         as         Last_Rprc_Day              --上次重定价日      
      ,(case 
         when T1.fix_float='FIXED' and to_date(T.cur_mat_dt,'yyyymmdd') > '$TXDATE'::date then to_date(T.cur_mat_dt,'YYYYMMDD')
         when T1.fix_float='FLOATING'  and T1.review_frq<>'' and to_date(T.cur_mat_dt,'YYYYMMDD')>'$TXDATE'::date
         then (case
                 when T1.review_frq='DAILY' then '$TXDATE'::date+1
                 when T1.review_frq='WEEKLY' then (case 
                                                        when to_date(T1.review_dt,'yyyymmdd')+7*ceil(('$TXDATE'::date-to_date(T1.review_dt,'yyyymmdd'))/7)>'$TXDATE'::date
                                                        then to_date(T1.review_dt,'yyyymmdd')+7*ceil(('$TXDATE'::date-to_date(T1.review_dt,'yyyymmdd'))/7)
                                                        else to_date(T1.review_dt,'yyyymmdd')+7*ceil(('$TXDATE'::date-to_date(T1.review_dt,'yyyymmdd'))/7)+7
                                                   end
                                                  )
                 when T1.review_frq='MONTHLY' then add_months(to_date(T1.review_dt,'yyyymmdd'),datediff('month',to_date(T1.review_dt,'yyyymmdd'),'$TXDATE'::date))
                 when T1.review_frq='QUARTERLY' then (case when add_months(to_date(T1.review_dt,'yyyymmdd'),3*ceil(datediff('month',to_date(T1.review_dt,'yyyymmdd'),'$TXDATE'::date)/3)::integer)>'$TXDATE'::date
                                                           then add_months(to_date(T1.review_dt,'yyyymmdd'),3*ceil(datediff('month',to_date(T1.review_dt,'yyyymmdd'),'$TXDATE'::date)/3)::integer)
                                                           else add_months(to_date(T1.review_dt,'yyyymmdd'),3*ceil(datediff('month',to_date(T1.review_dt,'yyyymmdd'),'$TXDATE'::date)/3)::integer+3)
                                                      end
                                                      )
                 when T1.review_frq='SEMI ANNUAL' then (case when add_months(to_date(T1.review_dt,'yyyymmdd'),6*ceil(datediff('month',to_date(T1.review_dt,'yyyymmdd'),'$TXDATE'::date)/6)::integer)>'$TXDATE'::date
                                                             then add_months(to_date(T1.review_dt,'yyyymmdd'),6*ceil(datediff('month',to_date(T1.review_dt,'yyyymmdd'),'$TXDATE'::date)/6)::integer)
                                                             else add_months(to_date(T1.review_dt,'yyyymmdd'),6*ceil(datediff('month',to_date(T1.review_dt,'yyyymmdd'),'$TXDATE'::date)/6)::integer+6)
                                                        end
                                                        )
                 when T1.review_frq='ANNUAL' then (case when add_months(to_date(T1.review_dt,'yyyymmdd'),12*ceil(datediff('month',to_date(T1.review_dt,'yyyymmdd'),'$TXDATE'::date)/12)::integer)>'$TXDATE'::date
                                                   then add_months(to_date(T1.review_dt,'yyyymmdd'),12*ceil(datediff('month',to_date(T1.review_dt,'yyyymmdd'),'$TXDATE'::date)/12)::integer)
                                                   else add_months(to_date(T1.review_dt,'yyyymmdd'),12*ceil(datediff('month',to_date(T1.review_dt,'yyyymmdd'),'$TXDATE'::date)/12)::integer+12)
                                                  end
                                                  )
                end
               )
                 else '$TXDATE'::date + 1
         end
        )                                                                          as         Next_Rprc_Day              --下次重定价日            
      ,case                                                                        
           when t1.pay_frq <> '' then coalesce(t8.tgt_cd,'@' || t1.PAY_FRQ)
           else '2'   --按季
           end                                                                     as         Int_Pay_Freq                  --付息频率
      ,to_date(t.mature_dt,'yyyymmdd') - to_date(t.settle_dt,'yyyymmdd')           as         Orgnl_Term                    --原始期限
      ,'D'                                                                         as         Orgnl_Term_Corp_Cd            --原始期限单位代码
      ,0                                                                           as         Bmk_Int_Rate                  --基准利率  
      ,coalesce(t1.int_rate,0)                                                                 as         Curr_Int_Rate                 --当前利率  
      ,0                                                                           as         Basis                         --基差    
      ,coalesce(t5.prin_acc,'')                                                                 as         Prin_Subj                     --本金科目  
      ,coalesce(abs(t5.prin_amount),0)                                                      as         Curr_Bal                      --当前余额  
      ,coalesce(t5.accr_acc,'')                                                                 as         Int_Subj                      --利息科目  
      ,coalesce(abs(t5.accr_amount_d),0)                                                       as         Today_Provs_Int               --当日计提利息          
      ,coalesce(abs(t5.accr_amount_m),0)                                                       as         CurMth_Provs_Int              --当月计提利息          
      ,coalesce(abs(t5.accr_amount_t),0)                                                       as         Accm_Provs_Int                --累计计提利息          
      ,coalesce(abs(t5.intr_amount_d),0)                                                       as         Today_Acpt_Pay_Int            --当日收付息            
      ,coalesce(abs(t5.intr_amount_m),0)                                                       as         CurMth_Recvd_Int_Pay          --当月已收付息           
      ,coalesce(abs(t5.intr_amount_t),0)                                                       as         Accm_Recvd_Int_Pay            --累计已收付息           
      ,coalesce(t5.intr_dis_acc,'')                                                as         Int_Adj_Subj                  --利息调整科目           
      ,coalesce(abs(t5.intr_dis_amount),0)                                                     as         Int_Adj_Amt                   --利息调整金额           
      ,0.00                                                                        as         Mth_Accm                      --月积数   
      ,0.00                                                                        as         Yr_Accm                       --年积数   
      ,0.00                                                                        as         Mth_Day_Avg_Bal               --月日均余额 
      ,0.00                                                                        as         Yr_Day_Avg_Bal                --年日均余额 
      ,'COS'                                                                       as         Sys_Src                       --系统来源  
from         dw_sdata.cos_000_deals             t    --交易信息主表
inner join   dw_sdata.cos_000_mmdeals           t1   --货币市场交易表
on           t1.deal_no = t.deal_no            
and          t1.start_dt <= '$TXDATE'::date 
and          t1.end_dt >= '$TXDATE'::date
inner join   f_fdm.cd_cd_table                   t2    --代码表 
on           t.sectype = t2.cd
and          t2.cd_typ_encd = 'FDM082'  
left join    dw_sdata.ecf_002_t01_cust_info_t    t3  --同业客户基本信息
on           t.cparty = t3.trans_emt_no
and          t3.start_dt <= '$TXDATE'::date
and          t3.end_dt >= '$TXDATE'::date  
left join    (select distinct deal_no,bank_acc
              from dw_sdata.cos_000_cflows            
              where (deal_no,flow_no) in (select b.deal_no,max(b.flow_no)
                                           from   dw_sdata.cos_000_cflows b
                                           where  b.start_dt <= '$TXDATE'::date
                                           and    b.end_dt>'$TXDATE'::date
                                           group by b.deal_no
                                              )
                                          and start_dt <= '$TXDATE'::date
                                          and end_dt>'$TXDATE'::date
              ) t4
on            t1.deal_no = t4.deal_no
left join (
       select   deal_no
                --1005\1015\1020\102102001\150505001\2115
                ,max(case when map_code like '1005%' or map_code like '1015%' or map_code like '1020%' or map_code like '2115%' 
                               or map_code in ('102102001','150505001')
                          then map_code end)    as prin_acc  --�����Ŀ
                ,sum(case when map_code like '1005%' or map_code like '1015%' or map_code like '1020%' or map_code like '2115%' 
                               or map_code in ('102102001','150505001')
                          then amount else 0 end)                    as prin_amount --��ǰ���
                --5205\6205
                ,max(case when map_code like '5205%' or map_code like '6205%' 
                          then map_code end)    as intr_acc   --��Ϣ��Ŀ����Ϣ����/��Ϣ֧��
                ,sum(case when (map_code like '5205%' or map_code like '6205%') and gl_date='$TXDATE'::DATE
                          then amount else 0 end)                    as intr_amount_d --�����ո�Ϣ
                ,sum(case when (map_code like '5205%' or map_code like '6205%') and to_char(gl_date,'yyyymm')=to_char('$TXDATE'::date,'yyyymm')
                          then amount else 0 end)                    as intr_amount_m --�����ո�Ϣ
                ,sum(case when  map_code like '5205%' or map_code like '6205%'
                          then amount else 0 end)                    as intr_amount_t --�ۼ��ո�Ϣ
                --1305\1330\2505                
                ,max(case when (map_code like '1305%' or map_code like '1330%' or map_code like '2505%' )  
                          then map_code end )   as accr_acc     --������Ϣ��Ŀ
                ,sum(case when (map_code like '1305%' or map_code like '1330%' or map_code like '2505%') and gl_date='$TXDATE'::DATE 
                          then amount else 0 end )                   as accr_amount_d --���ռ�����Ϣ
                ,sum(case when (map_code like '1305%' or map_code like '1330%' or map_code like '2505%') and to_char(gl_date,'yyyymm')=to_char('$TXDATE'::date,'yyyymm')
                          then amount else 0 end )                   as accr_amount_m --���¼�����Ϣ
                ,sum(case when map_code like '1305%' or map_code like '1330%' or map_code like '2505%'
                          then amount else 0 end )                   as accr_amount_t --�ۼƼ�����Ϣ       
                --150505002\102102002
                 ,max(case when map_code in ('150505002','102102002')
                           then map_code end )  as intr_dis_acc   --��Ϣ������Ŀ
                 ,sum(case when map_code in ('150505002','102102002')
                           then amount else 0 end )                  as intr_dis_amount --��Ϣ�������
     from      dw_sdata.cos_000_qta_gl_accounting --������Ϣ��������Ž��׻�Ʒ�¼��Ϣ
     where     ret_code='000000'  --��ƴ���ƽ̨�����ɹ�
       and     gl_date<='$TXDATE'::DATE
    group by   deal_no ) t5
on t1.deal_no=t5.deal_no
left join f_fdm.cd_rf_std_cd_tran_ref   t6                   --需转换代码表
on t.ccy = t6.src_cd                                         
and  t6.Data_Pltf_Src_Tab_Nm = 'COS_000_DEALS'               --源表名
and  t6.Data_Pltf_Src_Fld_Nm = 'CCY'                         --对应字段名                                                             
left join  f_fdm.cd_rf_std_cd_tran_ref  t7                   --需转换代码表
           on  t1.int_days = t7.src_cd                       --源代码值相同
          and  t7.Data_Pltf_Src_Tab_Nm = 'COS_000_MMDEALS'   --源表名
          and  t7.Data_Pltf_Src_Fld_Nm = 'INT_DAYS'
left join  f_fdm.cd_rf_std_cd_tran_ref  t8                   --需转换代码表
      on  t1.pay_frq = t8.src_cd                             --源代码值相同
     and  t8.Data_Pltf_Src_Tab_Nm = 'COS_000_MMDEALS'        --源表名
     and  t8.Data_Pltf_Src_Fld_Nm = 'PAY_FRQ'                --源表对应的字段名
left join dw_sdata.cos_000_bustruct T_org_1
on t.entity = T_org_1.thekey
and T_org_1.start_dt <= '$txdate'::date
and T_org_1.end_dt > '$txdate'::date
left join dw_sdata.cos_000_anacode T_org_2
on T_org_1.analyse04 = T_org_2.thekey 
and T_org_1.start_dt <= '$txdate'::date
and T_org_1.end_dt > '$txdate'::date       --modified 20160912
where  t.start_dt <=  '$TXDATE'::date
and    t.end_dt >  '$TXDATE'::date          

;

/*月积数、年积数、月日均余额、年日均余额临时表创建区*/ 
create local temporary table IF NOT EXISTS f_agt_cap_stor_temp_1
on commit preserve rows as
select t.agmt_id
       ,(case 
            when '$TXDATE'= '$MONTHBGNDAY' 
            then t.Curr_Bal
            else t.curr_bal+coalesce(t1.Mth_Accm,0)
            end
       )                                                                      as mth_accm  --月积数
       ,(case 
            when  '$TXDATE' = '$YEARBGNDAY' 
            then t.curr_bal
            else t.curr_bal+coalesce(t1.yr_accm,0)
            end
       )                                                                      as yr_accm   --年积数
       ,(case 
            when '$TXDATE' = '$MONTHBGNDAY' 
            then t.curr_bal
            else t.curr_bal+coalesce(t1.mth_accm,0)
            end
       )/('$TXDATE'::date-'$MONTHBGNDAY'::date+1)               as mth_day_avg_bal  --月日均余额
       ,(case 
           when '$TXDATE' = '$YEARBGNDAY' 
           then t.curr_bal
           else t.curr_bal+coalesce(t1.yr_accm,0)
           end
       )/('$TXDATE'::date-'$YEARBGNDAY'::date+1)                    as Yr_Day_Avg_Bal   --年日均余额
from      f_fdm.f_agt_cap_stor t
left join f_fdm.f_agt_cap_stor t1
on        t.agmt_Id=t1.agmt_Id
and       t1.etl_date='$TXDATE'::date-1
where     t.etl_date='$TXDATE'::date
;
/*月积数、年积数、月日均余额、年日均余额临时表创建区END*/
/*更新月积数、年积数、月日均余额、年日均余额*/
update f_fdm.f_agt_cap_stor t
set  mth_accm=t1.mth_accm 
    ,yr_accm=t1.yr_accm
    ,mth_day_avg_bal=t1.mth_day_avg_bal
    ,Yr_Day_Avg_Bal=t1.Yr_Day_Avg_Bal
from   f_agt_cap_stor_temp_1 t1
where  t.agmt_Id=t1.agmt_Id
and    t.etl_date='$TXDATE'::date
;
/*数据处理区END*/
 COMMIT;                                                                                            
