/*
Author             :刘潇泽
Function           :资产保全信息表
Load method        :INSERT
Source table       :pcs_004_tb_sup_write_off_info
                   :ccs_006_rapf80
                   :ccs_006_rapf90
Destination Table  :f_agt_asst_consv  资产保全信息表
Frequency          :D
Modify history list:Created by 刘潇泽
                   :Modified  by wyh at 20161012  基础层映射文档修改记录224:修改1,2组的取数逻辑
                    Modified  by wyh at 20161013  基础层映射文档修改记录226:增加组别3
                    modified  by wyh at 20161014  对组别1下t1表做ROW_NUMBER处理;修改了组别1,3的asst_consv_id的逻辑
                    modified by liudongyan at 20161014 �޸����2��������Ϊ��ˮ�����3�޸�Э������ȡ������
----------------------------------问题说明--------------------------

-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/
/*临时表创建区*/
/*临时表创建区END*/
/*数据回退区*/
DELETE/* +direct */ from f_fdm.f_agt_asst_consv
where etl_date='$TXDATE'::date
;
/*数据回退区END*/
/*数据处理区*/
/*组别1*/
INSERT INTO f_fdm.f_agt_asst_consv
(
         grp_typ                      --组别
        ,etl_date                    --数据日期
        ,asst_consv_id             	--资产保全编号
        ,orgnl_agmt_id             	--原协议编号
        ,cust_id                   	--客户编号
        ,org_id                    	--机构编号
        ,wrtoff_dt                 	--核销日期
        ,wrtoff_prin               	--核销本金
        ,wrtoff_in_bal_int         	--核销表内利息
        ,wrtoff_norm_int           	--核销正常利息
        ,wrtoff_arr_int            	--核销拖欠息
        ,wrtoff_pnsh_int           	--核销罚息
        ,wrtoff_post_retr_dt       	--核销后收回日期
        ,wrtoff_post_retr_prin       --核销后收回本金
        ,wrtoff_post_retr_int        --核销后收回利息
        ,wrtoff_post_retr_accrd_int	--核销后收回应计利息
        ,sys_src                     --系统来源
)
SELECT '1'
      ,'$TXDATE'::date
			,t.due_num||'_'||t.rcv_date||'_'||COALESCE(T1.RCV_DATE,'')||'_'||COALESCE(T1.NUM,0)
			,t.due_num
			,''
			,T.tal_dep
			,T.rcv_date::date
			,T.oft_prin
			,0.00
			,t.oft_nor_itr_in + t.oft_nor_itr_out
			,t.oft_dft_itr_in + t.oft_dft_itr_out
			,t.oft_pns_itr_in + t.oft_pns_itr_out
			,to_date(T1.RCV_DATE,'YYYYMMDD')
			,0
			,COALESCE(T1.NOR_ITR_IN,0) + COALESCE(T1.DFT_ITR_IN,0) + COALESCE(T1.PNS_ITR_IN,0) + COALESCE(T1.NOR_ITR_OUT,0) + COALESCE(T1.DFT_ITR_OUT,0) + COALESCE(T1.PNS_ITR_OUT,0)
			,COALESCE(T1.OFT_ACR_ITR_BAL,0)
			,'PCS'
FROM DW_SDATA.PCS_004_TB_SUP_WRITE_OFF_INFO T
--LEFT JOIN DW_SDATA.PCS_005_TB_SUP_INTR_INFO T1
LEFT JOIN (
     SELECT ROW_NUMBER() OVER (PARTITION BY T.DUE_NUM) AS NUM
     ,RCV_DATE
     ,NOR_ITR_IN
     ,DFT_ITR_IN
     ,PNS_ITR_IN
     ,NOR_ITR_OUT
     ,DFT_ITR_OUT
     ,PNS_ITR_OUT
     ,OFT_ACR_ITR_BAL
     ,DUE_NUM
     ,ETL_DT
     FROM DW_SDATA.PCS_005_TB_SUP_INTR_INFO T
     ) T1
ON    T.DUE_NUM = T1.DUE_NUM
AND   T1.RCV_DATE > T.RCV_DATE
and   T1.ETL_DT = '$TXDATE'::date
WHERE T.START_dt<='$TXDATE'::date
AND   T.END_DT>'$TXDATE'::date
;
commit;
/*组别1END*/
/*组别2*/

INSERT INTO f_fdm.f_agt_asst_consv
(
         grp_typ                            --组别
        ,etl_date                          --数据日期
        ,asst_consv_id             				--资产保全编号
        ,orgnl_agmt_id             				--原协议编号
        ,cust_id                   				--客户编号
        ,org_id                    				--机构编号
        ,wrtoff_dt                 				--核销日期
        ,wrtoff_prin               				--核销本金
        ,wrtoff_in_bal_int         				--核销表内利息
        ,wrtoff_norm_int           				--核销正常利息
        ,wrtoff_arr_int            				--核销拖欠息
        ,wrtoff_pnsh_int           				--核销罚息
        ,wrtoff_post_retr_dt       				--核销后收回日期
        ,wrtoff_post_retr_prin     			  --核销后收回本金
        ,wrtoff_post_retr_int      		    --核销后收回利息
        ,wrtoff_post_retr_accrd_int 				--核销后收回应计利息
        ,sys_src                   			  --系统来源
)
SELECT '2'
      ,'$TXDATE'::date
      ,t.ra80duebno||'_'||t.ra80date||'_'||COALESCE(t1.rd90date,'') 
      ,t.ra80duebno
      ,''
      ,t.ra80dpnoa
      ,t.ra80date ::Date
      ,t.ra80bjam
      ,t.ra80bnxamt
      ,t.ra80bnxamt+t.ra80bwxamt
      ,t.ra80bntamt+t.ra80bwtamt
      ,t.ra80bnfamt+t.ra80bwfamt
      ,to_date(t1.rd90date,'YYYYMMDD')
      ,COALESCE(t1.rd90hxsh1,0)
      ,COALESCE(t1.rd90hxsh2,0)
      ,0.00
      ,'CCS'
FROM  dw_sdata.ccs_006_rapf80 T
LEFT JOIN dw_sdata.ccs_006_rdpf90 T1
on t.ra80duebno=t1.rd90duebno
and t1.rd90date > t.ra80date
and t1.etl_dt='$TXDATE'::date
;
commit;
/*组别2END*/



/*组别3*/

INSERT INTO f_fdm.f_agt_asst_consv
(
         grp_typ                            --组别
        ,etl_date                          --数据日期
        ,asst_consv_id             				--资产保全编号
        ,orgnl_agmt_id             				--原协议编号
        ,cust_id                   				--客户编号
        ,org_id                    				--机构编号
        ,wrtoff_dt                 				--核销日期
        ,wrtoff_prin               				--核销本金
        ,wrtoff_in_bal_int         				--核销表内利息
        ,wrtoff_norm_int           				--核销正常利息
        ,wrtoff_arr_int            				--核销拖欠息
        ,wrtoff_pnsh_int           				--核销罚息
        ,wrtoff_post_retr_dt       				--核销后收回日期
        ,wrtoff_post_retr_prin     			  --核销后收回本金
        ,wrtoff_post_retr_int      		    --核销后收回利息
        ,wrtoff_post_retr_accrd_int 				--核销后收回应计利息
        ,sys_src                   			  --系统来源
)
SELECT 
        '3'
        ,'$TXDATE'::date
        ,T.XACCOUNT||'_'||(case when T.WROFF_CHDY = 0 OR T.WROFF_CHDY IS NULL
              THEN '$MINDATE' 
              ELSE TO_CHAR(T.WROFF_CHDY) 
        END)||'_'||COALESCE(T1.REL_DAY,0)||'_'||COALESCE(T1.SERIAL_NO,0)||'_'||COALESCE(T2.SERIAL_NO,0)     --COALESCE(T1.INP_TIME,0)  modified by wyh at 20161014
        ,T.XACCOUNT
        ,''
        ,''
        ,case when T.WROFF_CHDY = 0 OR T.WROFF_CHDY IS NULL
              THEN '$MINDATE'::DATE
              ELSE TO_CHAR(T.WROFF_CHDY)::DATE
        END
        ,0
        ,0
        ,0
        ,0
        ,0
        ,to_date(to_char(T1.REL_DAY),'YYYYMMDD')
        ,COALESCE(T1.TRAN_AMT,0)
        ,COALESCE(T2.TRAN_AMT,0)
        ,0
        ,'CCB'
FROM      DW_SDATA.CCB_000_ACCT T
LEFT JOIN DW_SDATA.CCB_000_JORJ T1
ON  T.XACCOUNT  = T1.XACCOUNT
AND T1.BANKACC1 = '92550500001R'
AND T1.ETL_DT   = '$TXDATE'::date
LEFT JOIN DW_SDATA.CCB_000_JORJ T2
ON  T.XACCOUNT  = T2.XACCOUNT
AND T2.BANKACC1 = '92600500001R'
AND T2.ETL_DT   = '$TXDATE'::date
WHERE T.WROF_FLAG = '1'
AND T.START_DT <= '$TXDATE'::date
AND T.END_DT   >  '$TXDATE'::date
;

COMMIT;
/*组别3END*/

