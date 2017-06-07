/*
Author             :ch
Function           :资金业务-拆出
Load method        :INSERT
Source table       :F_AGT_CAP_OFFER
Destination Table  :MA_LENDING
Frequency          :M
Modify history list:Created by ch 2016/5/18 10:19:34
                   :Modify  by liudongyan at 20160925 将利息调整金额的映射规则改为0 
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
DELETE FROM f_rdm.MA_LENDING WHERE  etl_date =  '$TXDATE'::date ;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_rdm.MA_LENDING
(
       etl_date                   --数据日期      
      ,ibank_offer_id             --拆借编号      
      ,Org_Num                    --机构号       
      ,tx_comb                    --交易组合      
      ,cust_num                   --客户号       
      ,acct_typ                   --账户类型      
      ,Cur_Cd                     --币种        
      ,prod_cd                    --产品代码      
      ,St_Int_Dt                  --起息日       
      ,Due_Dt                     --到期日       
      ,Curr_Int_Rate              --当前利率      
      ,Bmk_Int_Rate               --基准利率      
      ,Basic_Diff                 --基差        
      ,Int_Days                   --计息天数      
      ,Cmpd_Int_Calc_Mode_Cd      --复利计算方式代码  
      ,Int_Rate_Attr_Cd           --固定浮动属性    
      ,Orgnl_Term                 --原始期限      
      ,Orgnl_Term_Corp_Cd         --原始期限单位    
      ,Rprc_Prd                   --重定价周期     
      ,Rprc_Prd_Corp_Cd           --重定价周期单位   
      ,Last_Rprc_Day              --上次重定价日    
      ,Next_Rprc_Day              --下次重定价日    
      ,sys_src                    --系统来源      
      ,prin_subj                  --本金科目      
      ,Curr_Bal                   --当前余额      
      ,int_subj                   --利息科目      
      ,Today_Provs_Int            --当日计提利息    
      ,CurMth_Provs_Int           --当月计提利息    
      ,Accm_Provs_Int             --累计计提利息    
      ,Today_Chrg_Int             --当日收息      
      ,CurMth_Recvd_Int           --当月已收息     
      ,Accm_Recvd_Int             --累计已收付息    
      ,int_adj_amt                --利息调整金额    
      ,mth_day_avg_bal            --月日均余额     
      ,yr_day_avg_bal             --年日均余额     
      ,FTP_Price                  --FTP价格     
      ,ftp_tranfm_expns           --FTP转移支出   
      ,prosp_loss                 --预期损失      
      ,deval_prep_bal             --减值准备余额    
      ,deval_prep_amt             --减值准备发生额   
      ,Crdt_Risk_Econ_Cap         --信用风险经济资本  
      ,Mkt_Risk_Econ_Cap          --市场风险经济资本  
      ,Opr_Risk_Econ_Cap          --操作风险经济资本  
      ,Crdt_Risk_Econ_Cap_Cost    --信用风险经济资本成本
      ,Mkt_Risk_Econ_Cap_Cost     --市场风险经济资本成本
      ,Opr_Risk_Econ_Cap_Cost     --操作风险经济资本成本
      ,exchg_prft_loss_subj       --汇兑损益科目    
      ,Fair_Val_Chg_Subj               --公允价值变动科目  

)
SELECT '$TXDATE'::date      as etl_date               
      ,T.Agmt_Id                     as ibank_offer_id         
      ,T.Org_Num                     as Org_Num                
      ,T.TX_Comb_Cd                  as tx_comb                
      ,T.TX_Cnt_Pty_Cust_Num         as cust_num               
      ,''                            as acct_typ               
      ,T.Cur_Cd                      as Cur_Cd                 
      ,T.Prod_Cd                     as prod_cd                
      ,T.St_Int_Dt                   as St_Int_Dt              
      ,T.Due_Dt                      as Due_Dt                 
      ,T.Curr_Int_Rate               as Curr_Int_Rate          
      ,T.Bmk_Int_Rate                as Bmk_Int_Rate           
      ,T.Basis                       as Basic_Diff             
      ,T.Int_Base_Cd                 as Int_Days               
      ,T.Cmpd_Int_Calc_Mode_Cd       as Cmpd_Int_Calc_Mode_Cd  
      ,T.Int_Rate_Attr_Cd            as Int_Rate_Attr_Cd       
      ,T.Orgnl_Term                  as Orgnl_Term             
      ,T.Orgnl_Term_Corp_Cd          as Orgnl_Term_Corp_Cd     
      ,T.Rprc_Prd                    as Rprc_Prd               
      ,T.Rprc_Prd_Corp_Cd            as Rprc_Prd_Corp_Cd       
      ,T.Last_Rprc_Day               as Last_Rprc_Day          
      ,T.Next_Rprc_Day               as Next_Rprc_Day          
      ,T.Sys_Src                     as sys_src                
      ,T.Prin_Subj                   as prin_subj              
      ,T.Curr_Bal                    as Curr_Bal               
      ,T.Int_Subj                    as int_subj               
      ,T.Today_Provs_Int             as Today_Provs_Int        
      ,T.CurMth_Provs_Int            as CurMth_Provs_Int       
      ,T.Accm_Provs_Int              as Accm_Provs_Int         
      ,T.Today_Acpt_Pay_Int          as Today_Chrg_Int         
      ,T.CurMth_Recvd_Int_Pay        as CurMth_Recvd_Int       
      ,T.Accm_Recvd_Int_Pay          as Accm_Recvd_Int         
      ,0                             as int_adj_amt            
      ,T.Mth_Day_Avg_Bal             as mth_day_avg_bal        
      ,T.Yr_Day_Avg_Bal              as yr_day_avg_bal         
      ,T1.Adj_Post_FTP_Prc           as FTP_Price              
      ,T1.Adj_Post_FTP_Tran_Incom_Expns as ftp_tranfm_expns       
      ,0.00                        as prosp_loss             
      ,0.00                        as deval_prep_bal         
      ,0.00                        as deval_prep_amt         
      ,0.00                        as Crdt_Risk_Econ_Cap     
      ,0.00                        as Mkt_Risk_Econ_Cap      
      ,0.00                        as Opr_Risk_Econ_Cap      
      ,0.00                        as Crdt_Risk_Econ_Cap_Cost
      ,0.00                        as Mkt_Risk_Econ_Cap_Cost 
      ,0.00                        as Opr_Risk_Econ_Cap_Cost 
      ,''                        as exchg_prft_loss_subj   
      ,''                        as Fair_Val_Chg_Subj           
FROM  f_fdm.F_AGT_CAP_OFFER T
left join f_fdm.f_agt_ftp_info T1 --FTP信息表
on T.Agmt_Id= substr(T1.Acct_Num,1,length(T1.Acct_Num)-2) 
and T1.DATA_SOURCE='99700100000'  
and T1.etl_date ='$TXDATE'::date
where  T.etl_date='$TXDATE'::date
and T.Ibank_Offer_Drct_Cd='BUY' 
;

/*数据处理区END*/

COMMIT;
