/*
Author             :Liuxz
Function           :FTP信息表
Load method        :
Source table       :dw_sdata.frs_001_app_tpdm_rst_ftp T
Destination Table  :f_fdm.f_agt_ftp_info
Frequency          :D
Modify history list:Created by Liuxz at 20160719
                   :Modify  by liuxz 20160729 增加字段 "数据来源" （变更记录139）’
                    Modify  by zmx 20160804 修改字段值：调整前FTP转移收支取数逻辑’
                    modified by wyh at 20160930 增加主�
                    modified by lxz at 20161008 add condition on temporary table to filter the records where cur_book_bal = 0
                    modified by wyh at 20161009 P.M. add column "ftp_date"
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/

/*临时表创建区*/
create local temporary table IF NOT EXISTS frs_000_app_tpdm_rst_ftp_tmp
 on commit preserve rows as
select * from 
(select *,row_number()over(partition by ACCOUNT_NUMBER order by AS_OF_DATE desc ,cur_book_bal desc ) Rn
from dw_sdata.frs_000_app_tpdm_rst_ftp T
where T.start_dt<='$TXDATE'::date
and   '$TXDATE'::date<T.end_dt
and   cur_book_bal <> 0    -- modified by lxz 20161008 
) t1
where Rn = 1;
----------------------------------------modified by wyh at 20160930 增加主表PK

/*临时表创建区END*/

/*数据回退区*/
delete /* +direct */ from f_fdm.f_agt_ftp_info
where  etl_date = '$TXDATE'::date
;
/*数据回退区END*/

/*数据处理区*/

insert /* +direct */  into f_fdm.f_agt_ftp_info
(
grp_typ                                  		--组别           
,etl_date                                               --数据日期         
,Acct_num                                               --帐号           
,Org_Num                                                --机构号          
,Cust_Num                                               --客户号          
,Cur_Cd                                                 --货币代码         
,Prod_Cd                                                --产品代码         
,Prin_Subj                                              --本金科目         
,Adj_Befr_FTP_Prc                                       --调整前FTP价格     
,Adj_Befr_FTP_Tran_Incom_Expns                          --调整前FTP转移收支   
,Adj_Post_FTP_Prc                                       --调整后FTP价格     
,Adj_Post_FTP_Tran_Incom_Expns                          --调整后FTP转移收支  
,Data_Source    					--数据来源 
,Sys_Src                                                --系统来源 
,ftp_date        
)
select
'1'							  --组别
,'$TXDATE'::date                                 --数据日期      
,T.ACCOUNT_NUMBER                                         --帐号        
,T.ORG_UNIT_ID                                            --机构号       
,T.CIF_KEY                                                --客户号       
,T.ISO_CURRENCY_CD                                        --货币代码      
,T.FTP_PRODUCT_ID                                         --产品代码      
,T.GL_ACCOUNT_ID                                          --本金科目      
,T.FTP_RATE                                               --调整前FTP价格  
,T.CUR_BOOK_BAL * T.FTP_RATE/360                          --调整前FTP转移收支
,T.TRANSFER_RATE_AJUST                                    --调整后FTP价格  
,T.FTP_INT_DAY_AJUST                                      --调整后FTP转移收支
,T.DATA_SOURCE 					          --数据来源
,'FRS'                                                    --系统来源
,T.AS_OF_DATE
from frs_000_app_tpdm_rst_ftp_tmp t 
;
/*数据处理区END*/
commit;
                                                          
