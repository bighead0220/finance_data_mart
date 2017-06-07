/*
Author             :朱明香
Function           :资金债券回购业务信息表
Load method        :INSERT
Source table       :cos_000_deals           交易信息主表  
                    cos_000_mmdeals         货币市场交易表
                    cd_cd_table             代码表（财务数据集市基础层） 
                    ecf_002_t01_cust_info_T 同业客户基本信息表                                  
Destination Table  :F_agt_Cap_Bond_Buy_Back  资金债券回购业务信息表
Frequency          :D
Modify history list:Created by 朱明香 2016年8月3日14:57:00
                   :modified by wyh 20160912 修改机构代码，交易对手客户号
                    modified by zmx 20161009 �����¼223
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/
/*临时表创建区*/

/*临时表创建区END*/
/*数据回退区*/
DELETE FROM  f_fdm.F_agt_Cap_Bond_Buy_Back
WHERE  etl_date = '$TXDATE'::date
;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_fdm.F_agt_Cap_Bond_Buy_Back
        (grp_typ                                                        --组别
        ,etl_date                                                       --数据日期
        ,Agmt_Id                                                        --协议编号
        ,Org_Num                                                        --机构号
        ,Bond_Cd																					      --债券代码
        ,Cur_Cd                                                         --货币代码
        ,Prod_Cd                                                        --产品代码
        ,TX_Comb_Cd                                                     --交易组合代码
        ,TX_Cnt_Pty_Cust_Num                                            --交易对手客户号
        ,Biz_Drct_Ind                                                   --买卖方向标志
        ,TX_Day                                                         --交易日
        ,St_Int_Dt                                                      --起息日
        ,Due_Dt                                                         --到期日
        ,Exec_Int_Rate                                                  --执行利率
        ,Prin_Subj                                                      --本金科目
        ,Buy_Back_Amt                                                   --回购金额
        ,Int_Subj                                                       --利息科目
        ,Buy_Back_Int                                                   --回购利息
        ,Self_Biz_Agent_Cust_Ind                                        --自营代客标志
        ,Mth_Accm																					        --月积数
        ,Yr_Accm                                                        --年积数
        ,Mth_Day_Avg_Bal                                                --月日均余额
        ,Yr_Day_Avg_Bal                                                 --年日均余额
        ,Sys_Src                                                        --系统来源
        )
SELECT  1                                                               as grp_typ                 --质押式回购
        ,'$TXDATE'::date                                        				as etl_date
        ,T.DEAL_NO                                         							as Agmt_Id 
        ,coalesce(T_org_2.name,'')                                                     as Org_Num
        ,T.ticket_no                                                    as Bond_Cd 
        ,NVL(T4.TGT_CD,'@'||T.ccy)                                      as Cur_Cd
        ,T.sectype                                        						  as Prod_Cd
        ,T.entity                                                       as TX_Comb_Cd
        ,coalesce(T3.ECIF_CUST_NO,'@'||t.cparty)                                                as TX_Cnt_Pty_Cust_Num
        ,T1.BUY_SELL                                                    as Biz_Drct_Ind
        ,to_date(T.deal_dt,'YYYYMMDD')                                	as TX_Day
        ,to_date(T.settle_dt,'YYYYMMDD')                                as St_Int_Dt
        ,to_date(T.cur_mat_dt,'YYYYMMDD')                               as Due_Dt
        ,T1.int_rate                                        						as Exec_Int_Rate
        ,coalesce(T5.intr_acc,'')                                                    as Prin_Subj
        ,ABS(coalesce(T1.settlement,0))																	as Buy_Back_Amt
        ,coalesce(T5.intr_acc,'') 																			as Int_Subj
        ,ABS(coalesce(T1.int_amt,0))																		as Buy_Back_Int
        ,''																					    	      as Self_Biz_Agent_Cust_Ind
        ,0.00                         																	as Mth_Accm
        ,0.00																						      as Yr_Accm
        ,0.00                                   												as Mth_Day_Avg_Bal
        ,0.00                                  												  as Yr_Day_Avg_Bal
        ,'COS'                                                          as Sys_Src

        
FROM  dw_sdata.cos_000_deals  AS  T
LEFT JOIN  f_fdm.CD_RF_STD_CD_TRAN_REF T4         --需转换代码表
  ON  T.ccy = T4.SRC_CD                             --源代码值相同
 AND  T4.DATA_PLTF_SRC_TAB_NM = upper('cos_000_deals')    --数据平台源表主干名
 AND  T4.Data_Pltf_Src_Fld_Nm =upper('ccy')               --数据平台源字段名
INNER JOIN dw_sdata.cos_000_mmdeals             AS  T1
  ON        T1.DEAL_NO = T.DEAL_NO
 AND T1.start_dt <= '$TXDATE'::date
 AND  T1.end_dt > '$TXDATE'::date 
INNER JOIN f_fdm.cd_cd_table                 AS T2
  ON   T.sectype = T2.Cd 
 AND T2.Cd_Typ_Encd = 'FDM084'                --资金债券回购产品代码
LEFT JOIN dw_sdata.ecf_002_t01_cust_info_T           AS T3
ON         T.cparty = T3.trans_emt_no        --交易对手编号
AND T3.start_dt <= '$TXDATE'::date
AND  T3.end_dt > '$TXDATE'::date    
LEFT JOIN 
    ( select deal_no
     --��Ѻʽ���ع���2430% ��Ѻʽ��ع���1415%
     ,max(case when  (map_code like '2430%' or  map_code like '1415%' or  map_code like '274004' )  then map_code end ) prin_acc   --�����Ŀ
     --��Ѻʽ���ع���2505% ��Ѻʽ��ع���1305%
     ,max(case when  (map_code like '2505%' or map_code like '1305%')  then map_code end ) intr_acc   --��Ϣ��Ŀ
from  dw_sdata.cos_000_qta_gl_accounting --������Ϣ��������Ž��׻�Ʒ�¼��Ϣ
where  ret_code='000000'  --��ƴ���ƽ̨�����ɹ�
 and   gl_date<='$TXDATE'::date
 group by   deal_no)  AS  T5          --本金科目、利息科目
  ON  T.DEAL_NO = T5.DEAL_NO     
left join dw_sdata.cos_000_bustruct T_org_1
on t.entity = T_org_1.thekey
and T_org_1.start_dt <= '$txdate'::date
and T_org_1.end_dt > '$txdate'::date
left join dw_sdata.cos_000_anacode T_org_2
on T_org_1.analyse04 = T_org_2.thekey 
and T_org_1.start_dt <= '$txdate'::date
and T_org_1.end_dt > '$txdate'::date                  --modified 20160912
WHERE T.start_dt <= '$TXDATE'::date 
 AND T.end_dt > '$TXDATE'::date                                                                                                                                               
;


INSERT INTO f_fdm.F_agt_Cap_Bond_Buy_Back
        (grp_typ                                                        --组别
        ,etl_date                                                       --数据日期
        ,Agmt_Id                                                        --协议编号
        ,Org_Num                                                        --机构号
        ,Bond_Cd																					      --债券代码
        ,Cur_Cd                                                         --货币代码
        ,Prod_Cd                                                        --产品代码
        ,TX_Comb_Cd                                                     --交易组合代码
        ,TX_Cnt_Pty_Cust_Num                                            --交易对手客户号
        ,Biz_Drct_Ind                                                   --买卖方向标志
        ,TX_Day                                                         --交易日
        ,St_Int_Dt                                                      --起息日
        ,Due_Dt                                                         --到期日
        ,Exec_Int_Rate                                                  --执行利率
        ,Prin_Subj                                                      --本金科目
        ,Buy_Back_Amt                                                   --回购金额
        ,Int_Subj                                                       --利息科目
        ,Buy_Back_Int                                                   --回购利息
        ,Self_Biz_Agent_Cust_Ind                                        --自营代客标志
        ,Mth_Accm																					        --月积数
        ,Yr_Accm                                                        --年积数
        ,Mth_Day_Avg_Bal                                                --月日均余额
        ,Yr_Day_Avg_Bal                                                 --年日均余额
        ,Sys_Src                                                        --系统来源
        )
SELECT  2                                                               as grp_typ           --买断式/开放式回购
        ,'$TXDATE'::date                                               as etl_date
        ,T.DEAL_NO                                        						  as Agmt_Id 
        ,coalesce(T_org_2.name,'')                                                     as Org_Num
        ,T.ticket_no                                                    as Bond_Cd
        ,NVL(T4.TGT_CD,'@'||T.ccy)                                      as Cur_Cd
        ,T.sectype                                        						  as Prod_Cd
        ,T.entity                                                       as TX_Comb_Cd
        ,coalesce(T3.ECIF_CUST_NO,'@'||t.cparty)                                                as TX_Cnt_Pty_Cust_Num
        ,(case 
               when T1.bor_invest='BORROWING' then 'SELL'
               when T1.bor_invest='INVESTMENT' then 'BUY' 
               else ''
         end)                                                           as Biz_Drct_Ind
        ,coalesce(to_date(T.deal_dt,'YYYYMMDD'),'$MINDATE'::date)                                 as TX_Day
        ,coalesce(to_date(T.settle_dt,'YYYYMMDD'),'$MINDATE'::date)                                as St_Int_Dt
        ,coalesce(to_date(T.cur_mat_dt,'YYYYMMDD'),'$MINDATE'::date)                               as Due_Dt
        ,T1.repo_rate                                                   as Exec_Int_Rate
        ,coalesce(T5.prin_acc,'')                                                    as Prin_Subj
        ,T.face_value																					            as Buy_Back_Amt
        ,coalesce(T5.intr_acc,'') 																		              	as Int_Subj
        ,T1.repo_amt																			  as Buy_Back_Int
        ,''																					    	      as Self_Biz_Agent_Cust_Ind
        ,0.00                         																	as Mth_Accm
        ,0.00																						      as Yr_Accm
        ,0.00                                   												as Mth_Day_Avg_Bal
        ,0.00                                  												  as Yr_Day_Avg_Bal
        ,'COS'                                                          as Sys_Src
FROM  dw_sdata.cos_000_deals                       AS T
LEFT JOIN  f_fdm.CD_RF_STD_CD_TRAN_REF T4         --需转换代码表
  ON  T.ccy = T4.SRC_CD                             --源代码值相同
 AND  T4.DATA_PLTF_SRC_TAB_NM = upper('cos_000_deals')    --数据平台源表主干名
 AND  T4.Data_Pltf_Src_Fld_Nm =upper('ccy')               --数据平台源字段名
INNER JOIN dw_sdata.cos_000_radeals             AS T1
  ON        T1.DEAL_NO = T.DEAL_NO
 AND T1.start_dt <= '$TXDATE'::date
 AND  T1.end_dt > '$TXDATE'::date 
INNER JOIN  f_fdm.cd_cd_table                 AS T2
  ON   T.sectype = T2.Cd 
 AND T2.Cd_Typ_Encd = 'FDM084'                --资金拆借产品代码
LEFT JOIN dw_sdata.ecf_002_t01_cust_info_T          AS T3
ON         T.cparty = T3.trans_emt_no        --交易对手编号
AND T3.start_dt <= '$TXDATE'::date
AND  T3.end_dt > '$TXDATE'::date  

LEFT JOIN ( 
select deal_no
        --1015/1415/2430/1405
        ,max(case when (map_code like '1015%' or map_code like '1415%' or map_code like '2430%' or  map_code like '1405%' )  then map_code end ) prin_acc   --�����Ŀ
        --1335/1305/2505
        ,max(case when  (map_code like '2505%' or map_code like '1305%' or map_code like '1335%')  then map_code end ) intr_acc   --��Ϣ��Ŀ
from   dw_sdata.cos_000_qta_gl_accounting --������Ϣ��������Ž��׻�Ʒ�¼��Ϣ
where  ret_code='000000'  --��ƴ���ƽ̨�����ɹ�
 and   gl_date<='$TXDATE'::date
 group by   deal_no
) AS  T5           --本金科目、利息科目
  ON  T.DEAL_NO = T5.DEAL_NO     
left join dw_sdata.cos_000_bustruct T_org_1
on t.entity = T_org_1.thekey
and T_org_1.start_dt <= '$txdate'::date
and T_org_1.end_dt > '$txdate'::date
left join dw_sdata.cos_000_anacode T_org_2
on T_org_1.analyse04 = T_org_2.thekey 
and T_org_1.start_dt <= '$txdate'::date
and T_org_1.end_dt > '$txdate'::date                  --modified 20160912
WHERE T.start_dt <= '$TXDATE'::date 
  AND T.end_dt > '$TXDATE'::date                                                                                                                              
;




 /*月积数、年积数、月日均余额、年日均余额临时表创建区*/
--drop table if exists F_agt_Cap_Bond_Buy_Back_tmp cascade;
create local temp table F_agt_Cap_Bond_Buy_Back_tmp
on commit preserve rows as
select	t.Agmt_Id
   ,(case                                                                                                             
         when '$TXDATE'= '$MONTHBGNDAY' then T.Buy_Back_Amt                                                 
         else T.Buy_Back_Amt+coalesce(T1.Mth_Accm,0)                                                                      
     end                                                                                                              
    )                                                                              as Mth_Accm  --月积数                 
   ,(case                                                                                                             
         when  '$TXDATE' = '$YEARBGNDAY' then T.Buy_Back_Amt                                                    
         else T.Buy_Back_Amt+coalesce(T1.Yr_Accm,0)                                                                       
     end                                                                                                              
    )                                                                              as Yr_Accm  --年积数                  
    ,(case                                                                                                            
         when '$TXDATE' = '$MONTHBGNDAY' then T.Buy_Back_Amt                                                
         else T.Buy_Back_Amt+coalesce(T1.Mth_Accm,0)                                                                      
     end                                                                                                              
     )/('$TXDATE'::date-'$MONTHBGNDAY'::date+1)                      as Mth_Day_Avg_Bal  --月日均余额        
    ,(case                                                                                                            
         when '$TXDATE' = '$YEARBGNDAY' then T.Buy_Back_Amt                                                     
         else T.Buy_Back_Amt+coalesce(T1.Yr_Accm,0)                                                                       
     end                                                                                                              
     )/('$TXDATE'::date-'$YEARBGNDAY'::date+1)                           as Yr_Day_Avg_Bal   --年日均余额        
   
   
from  f_fdm.F_agt_Cap_Bond_Buy_Back     T
left join f_fdm.F_agt_Cap_Bond_Buy_Back  T1
on         T.Agmt_Id= T1.Agmt_Id
and       t1.etl_date='$TXDATE'::date-1
where     T.etl_date='$TXDATE'::date
;




/*月积数、年积数、月日均余额、年日均余额临时表创建区END*/
/*更新月积数、年积数、月日均余额、年日均余额*/
update f_fdm.F_agt_Cap_Bond_Buy_Back   T
set Mth_Accm=T1.Mth_Accm 
,Yr_Accm=T1.Yr_Accm
,Mth_Day_Avg_Bal=T1.Mth_Day_Avg_Bal
,Yr_Day_Avg_Bal=T1.Yr_Day_Avg_Bal
from  F_agt_Cap_Bond_Buy_Back_tmp   T1
where T.Agmt_Id= T1.Agmt_Id
and   T.etl_date='$TXDATE'::date
;
/*数据处理区END*/
COMMIT;
