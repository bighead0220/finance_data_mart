/*
Author                       :zhangliang
Function                     :资金拆借信息表
Load method                  :
Source table                 :dw_sdata.cos_000_deals           T   --交易信息主表
                              dw_sdata.cos_000_mmdeals         T1  --货币市场交易表
                              f_fdm.cd_cd_table                T2  --代码表（财务数据集市基础层）
                              dw_sdata.ecf_002_t01_cust_info_T T3  --同业客户基本信息
                              dw_sdata.cos_000_cflows              --清算信息主表
                              dw_sdata.cos_000_gl_entry  gl        --账务信息主表，存放交易会计分录信息
                              dw_sdata.cos_000_chart_acc acc       --科目表，业务人员人工录入
                              f_fdm.cd_rf_std_cd_tran_ref          --需转换代码表
Destination table            :f_fdm.f_agt_cap_offer
Frequency                    :D
Modify history list          :Created by zhangliang at 2016-8-3 20:36 
                             :Modify  by  zhangliang at 2016-8-17 11:50  1、将etl_date='$TXDATE'::date 修改为start_dt<= '$TXDATE'::date and end_dt>'$TXDATE'::date
                                                                         2、将and gl.etl_dt='$TXDATE'::date注释
                              modified by wyh 20160912 修改机构代码，交易对手客户号
                              modified by zhangliang at 20161009 �����¼223
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/

/*临时表创建区*/
/*临时表创建区END*/

/*数据回退区*/
delete /* +direct */ from f_fdm.f_agt_cap_offer
where etl_date = '$TXDATE'::date
;
/*数据回退区END*/

/*数据处理区*/
insert into f_fdm.f_agt_cap_offer
(        grp_typ                                    --组别              
         ,etl_date                                   --数据日期          
         ,Agmt_Id                                    --协议编号          
         ,TX_Cnt_Pty_Cust_Num                        --交易对手客户号    
         ,Ibank_Offer_Drct_Cd                        --拆借方向代码      
         ,Prod_Cd                                    --产品代码          
         ,TX_Comb_Cd                                 --交易组合代码
         ,St_Int_Dt                                  --起息日            
         ,Due_Dt                                     --到期日            
         ,Int_Base_Cd                                --计息基础代码      
         ,Cmpd_Int_Calc_Mode_Cd                      --复利计算方式代码  
         ,Int_Rate_Attr_Cd                           --利率属性代码      
         ,Orgnl_Term                                 --原始期限          
         ,Orgnl_Term_Corp_Cd                         --原始期限单位代码  
         ,Rprc_Prd                                   --重定价周期        
         ,Rprc_Prd_Corp_Cd                           --重定价周期单位代码
         ,Org_Num                                    --机构号            
         ,Cust_Acct_Num                              --客户账号          
         ,Cur_Cd                                     --货币代码          
         ,Curr_Int_Rate                              --当前利率          
         ,Bmk_Int_Rate                               --基准利率          
         ,Basis                                      --基差              
         ,Last_Rprc_Day                              --上次重定价日    * 
         ,Next_Rprc_Day                              --下次重定价日      
         ,Prin_Subj                                  --本金科目          
         ,Curr_Bal                                   --当前余额          
         ,Int_Subj                                   --利息科目          
         ,Today_Provs_Int                            --当日计提利息      
         ,CurMth_Provs_Int                           --当月计提利息      
         ,Accm_Provs_Int                             --累计计提利息      
         ,Today_Acpt_Pay_Int                         --当日收付息        
         ,CurMth_Recvd_Int_Pay                       --当月已收付息      
         ,Accm_Recvd_Int_Pay                         --累计已收付息      
         ,Mth_Accm                                   --月积数            
         ,Yr_Accm                                    --年积数            
         ,Mth_Day_Avg_Bal                            --月日均余额        
         ,Yr_Day_Avg_Bal                             --年日均余额        
         ,Sys_Src                                    --系统来源          
)
select     
         1                                                                   as     grp_typ                 --组别          
         ,'$TXDATE'::date                                           as     etl_date                --数据日期      
         ,t.deal_no                                                         as     Agmt_Id                 --协议编号
         ,coalesce(t3.ecif_cust_no,'@'||t.cparty)                                       as     TX_Cnt_Pty_Cust_Num     --交易对手客户号
         ,coalesce(t1.buy_sell,'')                                           as     Ibank_Offer_Drct_Cd     --拆借方向代码
         ,coalesce(t.sectype,'')                                             as     Prod_Cd                 --产品代码
         ,coalesce(T.entity,'')                                              as     TX_Comb_Cd              --交易组合代码
         ,coalesce(to_date(T.settle_dt,'YYYYMMDD'),'$MINDATE'::date)         as     St_Int_Dt               --起息日修改最小日期变量 by liudongyan
         ,coalesce(to_date(T.cur_mat_dt,'YYYYMMDD'),'$MINDATE'::date)        as     Due_Dt                  --到期日 修改最小日期变量 by liudongyan 
         ,nvl(t7.tgt_cd,'@' || t1.int_days)                                  as     SInt_Base_Cd            --计息基础代码    
         ,'1'                                                                as     Cmpd_Int_Calc_Mode_Cd   --复利计算方式代码
         ,case when T1.fix_float='FIXED' THEN '1' 
               WHEN T1.FIX_FLOAT='FLOATING' AND T1.review_frq <> '' 
               THEN '3'  --按定期浮动利率
               else '4'  --按不定期浮动利率
           end                                                               as     Int_Rate_Attr_Cd        --利率属性代码    
         ,to_date(T.mature_dt,'YYYYMMDD')-to_date(T.settle_dt,'YYYYMMDD')    as     Orgnl_Term              --原始期限        
         ,'D'                                                                as     Orgnl_Term_Corp_Cd      --原始期限单位代码
         ,case when T1.FIX_FLOAT = 'FLOATING'  
               then case when T1.review_frq in ('DAILY','MONTHLY','ANNUAL') then 1
                         when T1.review_frq = 'SEMI ANNUAL' then 6
                         when T1.review_frq = 'QUARTERLY' then 3
                         when T1.review_frq = 'WEEKLY' then 7
                         else 0
                         end 
               ELSE 0 
          END                                                                as     Rprc_Prd                 --重定价周期        
         ,case when T1.FIX_FLOAT='FLOATING'  
               THEN (case when T1.review_frq in ('DAILY','WEEKLY') then 'D'
                          when T1.review_frq in ('MONTHLY','QUARTERLY','SEMI ANNUAL') then 'M'
                          when T1.review_frq='ANNUAL'  then 'Y'
                          else ''
                      end)
               else ''
               end                                                           as     Rprc_Prd_Corp_Cd         --重定价周期单位代码
         ,coalesce(T_org_2.name,'')                                                         as     Org_Num                  --机构号        
         ,coalesce(t4.bank_acc,'')                                           as     Cust_Acct_Num            --客户账号      
         ,nvl(t6.tgt_cd,'@' || t.ccy)                                        as     Cur_Cd                   --货币代码      
         ,t1.int_rate                                                        as     Curr_Int_Rate            --当前利率     
         ,0                                                                  as     Bmk_Int_Rate             --基准利率     
         ,0                                                                  as     Basis                    --基差         
         ,coalesce(to_date(t1.review_dt,'YYYYMMDD'),'$MINDATE'::date)                                   as     Last_Rprc_Day            --上次重定价日 
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
                       when T1.review_frq='QUARTERLY' then (case when add_months(to_date(T1.review_dt,'yyyymmdd'),3*ceil(datediff(M,to_date(T1.review_dt,'yyyymmdd'),'$TXDATE'::date)/3)::integer)>'$TXDATE'::date
                                                                 then add_months(to_date(T1.review_dt,'yyyymmdd'),3*ceil(datediff(M,to_date(T1.review_dt,'yyyymmdd'),'$TXDATE'::date)/3)::integer)
                                                                 else add_months(to_date(T1.review_dt,'yyyymmdd'),3*ceil(datediff(M,to_date(T1.review_dt,'yyyymmdd'),'$TXDATE'::date)/3)::integer+3)
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
               )                                             as     Next_Rprc_Day            --下次重定价日 
         ,coalesce(t5.prin_acc,'')                           as     Prin_Subj                --本金科目     
         ,coalesce(abs(t5.prin_amount),0)                   as     Curr_Bal                 --当前余额     
         ,coalesce(t5.accr_acc,'')                           as     Int_Subj                 --利息科目     
         ,coalesce(abs(T5.accr_amount_d),0)                                   as     Today_Provs_Int          --当日计提利息 
         ,coalesce(abs(T5.accr_amount_m),0)                                   as     CurMth_Provs_Int         --当月计提利息 
         ,coalesce(abs(T5.accr_amount_t),0)                                   as     Accm_Provs_Int           --累计计提利息 
         ,coalesce(abs(T5.intr_amount_d),0)                                   as     Today_Acpt_Pay_Int       --当日收付息   
         ,coalesce(abs(T5.intr_amount_m),0)                                   as     CurMth_Recvd_Int_Pay     --当月已收付息 
         ,coalesce(abs(T5.intr_amount_t),0)                                   as     Accm_Recvd_Int_Pay       --累计已收付息 
         ,0                                                  as     Mth_Accm                 --月积数       
         ,0                                                  as     Yr_Accm                  --年积数       
         ,0                                                  as     Mth_Day_Avg_Bal          --月日均余额   
         ,0                                                  as     Yr_Day_Avg_Bal           --年日均余额
         ,'COS'                                              as     Sys_Src                  --系统来源
from          dw_sdata.cos_000_deals           t  --交易信息主表
inner join    dw_sdata.cos_000_mmdeals         t1 --货币市场交易表
on            t1.deal_no = t.deal_no
and           t1.start_dt <= '$TXDATE'::date
and           t1.end_dt > '$TXDATE'::date

inner join    f_fdm.cd_cd_table                t2 --代码表
on t.sectype = t2.cd 
and t2.cd_typ_encd = 'FDM083'

left join     dw_sdata.ecf_002_t01_cust_info_T t3 --同业客户基本信息
on            t.cparty = t3.trans_emt_no
and           t3.start_dt <= '$TXDATE'::date
and           t3.end_dt > '$TXDATE'::date   
left join     (select distinct deal_no,bank_acc
               from dw_sdata.cos_000_cflows
               where (deal_no,flow_no) in (select deal_no,max(flow_no) 
                                             from dw_sdata.cos_000_cflows
                                             where start_dt <= '$TXDATE'::date
                                             and   end_dt>'$TXDATE'::date
                                             group by deal_no)
                 and start_dt <= '$TXDATE'::date
                 and end_dt>'$TXDATE'::date
               )                              t4 --现金流量表
on            T1.DEAL_NO=T4.DEAL_NO
left join    (
select
                deal_no
                --101501/1020/2110/92621000/310603001
                ,max(case when map_code like '101501%' or map_code like '1020%' or map_code like '2110%'
                               or map_code in ('92621000','310603001')
                          then map_code end)    as prin_acc  --�����Ŀ
                ,sum(case when map_code like '101501%' or map_code like '1020%' or map_code like '2110%'
                               or map_code in ('92621000','310603001')
                          then amount else 0 end)                    as prin_amount --��ǰ���
                --5205/6205/580709/630709
                ,max(case when map_code like '5205%' or map_code like '6205%' or map_code in ('580709','630709')
                          then map_code end)    as intr_acc   --��Ϣ��Ŀ����Ϣ����/��Ϣ֧��
                ,sum(case when (map_code like '5205%' or map_code like '6205%' or map_code in ('580709','630709'))
                               and gl_date='$TXDATE'::date
                          then amount else 0 end)                    as intr_amount_d --�����ո�Ϣ
                ,sum(case when  (map_code like '5205%' or map_code like '6205%' or map_code in ('580709','630709'))
                                and to_char(gl_date,'yyyymm')=to_char('$TXDATE'::date,'yyyymm')
                          then amount else 0 end)                    as intr_amount_m --�����ո�Ϣ
                ,sum(case when map_code like '5205%' or map_code like '6205%' or map_code in ('580709','630709')
                          then amount else 0 end)                    as intr_amount_t --�ۼ��ո�Ϣ
                --1305/1330/2505/310603002
                ,max(case when map_code like '1305%' or map_code like '1330%' or map_code like '2505%' or map_code ='310603002'
                          then map_code end )   as accr_acc     --������Ϣ��Ŀ
                ,sum(case when (map_code like '1305%' or map_code like '1330%' or map_code like '2505%' or map_code ='310603002')
                               and to_char(gl_date,'yyyymm')=to_char('$TXDATE'::date,'yyyymm')
                          then amount else 0 end )                   as accr_amount_d --���ռ�����Ϣ
                ,sum(case when (map_code like '1305%' or map_code like '1330%' or map_code like '2505%' or map_code ='310603002')
                               and to_char(gl_date,'yyyymm')=to_char('$TXDATE'::date,'yyyymm')
                          then amount else 0 end )                   as accr_amount_m --���¼�����Ϣ
                ,sum(case when map_code like '1305%' or map_code like '1330%' or map_code like '2505%'  or map_code ='310603002'
                          then amount else 0 end )                   as accr_amount_t --�ۼƼ�����Ϣ
      from      dw_sdata.cos_000_qta_gl_accounting --������Ϣ��������Ž��׻�Ʒ�¼��Ϣ
     where     ret_code='000000'  --��ƴ���ƽ̨�����ɹ�
       and     gl_date<='$TXDATE'::date
    group by   deal_no
               )       t5 --本金科目、利息科目、计提利息科目、利息调整科目及对应的金额
on T.DEAL_NO=T5.DEAL_NO
left join  f_fdm.cd_rf_std_cd_tran_ref  t6               --需转换代码表
          on  t.ccy = t6.src_cd                            --源代码值相同
         and  t6.Data_Pltf_Src_Tab_Nm = 'COS_000_DEALS'     --源表名
         and  t6.Data_Pltf_Src_Fld_Nm = 'CCY'
left join  f_fdm.cd_rf_std_cd_tran_ref  t7               --需转换代码表
           on  t1.int_days = t7.src_cd                       --源代码值相同
          and  t7.Data_Pltf_Src_Tab_Nm = 'COS_000_MMDEALS'   --源表名
          and  t7.Data_Pltf_Src_Fld_Nm = 'INT_DAYS'
left join dw_sdata.cos_000_bustruct T_org_1
on t.entity = T_org_1.thekey
and T_org_1.start_dt <= '$txdate'::date
and T_org_1.end_dt > '$txdate'::date
left join dw_sdata.cos_000_anacode T_org_2
on T_org_1.analyse04 = T_org_2.thekey 
and T_org_1.start_dt <= '$txdate'::date
and T_org_1.end_dt > '$txdate'::date                  --modified 20160912
where  t.start_dt <=  '$TXDATE'::date
and    t.end_dt >  '$TXDATE'::date
;
 
 /*月积数、年积数、月日均余额、年日均余额临时表创建区*/ 
create local temporary table IF NOT EXISTS f_agt_cap_offer_temp_1
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
from      f_fdm.f_agt_cap_offer t
left join f_fdm.f_agt_cap_offer t1
on        t.agmt_Id=t1.agmt_Id
and       t1.etl_date='$TXDATE'::date-1
where     t.etl_date='$TXDATE'::date
;
/*月积数、年积数、月日均余额、年日均余额临时表创建区END*/
/*更新月积数、年积数、月日均余额、年日均余额*/
update f_fdm.f_agt_cap_offer a 
set  mth_accm=t1.mth_accm 
    ,yr_accm=t1.yr_accm
    ,mth_day_avg_bal=t1.mth_day_avg_bal
    ,Yr_Day_Avg_Bal=t1.Yr_Day_Avg_Bal
from   f_agt_cap_offer_temp_1 t1
where  a.agmt_Id=t1.agmt_Id
and    a.etl_date='$TXDATE'::date
;

/*更新月积数、年积数、月日均余额、年日均余额END*/ 
 
 
/*数据处理区END*/

COMMIT;

