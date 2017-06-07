/*
Author             :刘潇泽
Function           :公司贷款借据信息表
Load method        :INSERT
Source table       :dw_sdata.ccs_006_rapfn0,dw_sdata.ccs_004_tb_con_payout_info_detail,dw_sdata.ccs_004_tb_con_payout_int_rate,dw_sdata.ccs_004_tb_con_biz_detail,dw_sdata.ccs_004_tb_con_payout_int_rate,dw_sdata.ccs_004_tb_con_repay_plan,dw_sdata.ccs_004_tb_con_contract,dw_sdata.ccs_004_tb_con_borr_acct_summary,dw_sdata.acc_003_t_acc_assets_ledger,dw_sdata.ccs_006_aapf10,dw_sdata.acc_003_t_accdata_last_item_no,dw_sdata.ccs_006_japf10,dw_sdata.ccs_006_rdpf90,dw_sdata.ccs_004_tb_con_extend_term_info
Destination Table  :f_fdm.f_loan_corp_dubil_info
Frequency          :D
Modify history list:Created by刘东燕2016年4月19日10:05:55
                   :Modify  by liuxz 20160613 修改Repay_Prd_corp_Cd为Repay_Prd_Cd
                   修改当月计提利息逻辑 T10.ja10amt 为T10.jc10amt+T9.ja10amt
                   修改累计计提利息逻辑 T11.ja10amt 为T11.jc10amt+T9.ja10amt 
                   新增浮动比例逻辑  T2.ir_nego_rate
                   修改原始期限单位代码取数逻辑，M和D位置互换
                   修改是否先收息逻辑 else '2'为'0'
                   修改利率属性代码逻辑 then 2,0,0 else 1为 then 4,1,1 else 3
                   修改重定价周期、重定价周期代码、上次重定价日逻辑
                   T9表
                   modify by liuxz 20160614 修改 T15.aa10type in ('03','04')为 T15.aa10type in ('03'),添加月积数逻辑
                   modify by liuxz 20160616 1、修改字段‘客户编号’取数规则 
                                            2、修改字段‘贷款减值准备余额’来源表关联条件：T17.aa10type='14'--贷款减值准备 
                                            3、修改字段‘计息基础代码’映射规则 （变更记录64）
                                            1、修改字段‘贷款减值准备发生额’取数规则 ，来源表发生变化 （变更记录66）
                                            1、修改字段‘客户号’、‘基准利率’、‘浮动比例’、‘逾期利率’取数规则 ；(变更记录73）
                   modified by liuxz 20160630 货币代码,还款方式代码,协议状态代码,利率调整方式代码 代码转换开发
                   Modify  by xsh 20160715 在表f_loan_corp_dubil_info_yjs_tmp前面增加schema前缀f_fdm
	           modified by liuxz 20160718 月积数临时表创建位置，所有临时表更名
                   modified by gln 20160905 修改T9分录流水表（当日）替换成分录流水表（历史）  
                   modified by gln 20160906 注销T16表借款展期明细信息表                             
                  modified by gln 20160921 修改累计计提利息、累计已收息默认为0 
                   modified by liudongyan 20160922 修�日���问�
                   modified by wyh at 20160930 修�T15,T17去除PKT20+SUM
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/

/*SQL函数创建区
CREATE OR REPLACE FUNCTION f_fdm.Next_Rprc_Day(etl_date date,start_date date,end_date date,change_month varchar)   --下次重定价日函数  #change_month 步长
RETURN date
AS BEGIN
      RETURN (case when add_months(start_date,(ceil(datediff('month',start_date,etl_date)/change_month)*change_month::integer)::integer)<end_date
                      then add_months(start_date,(ceil(datediff('month',start_date,etl_date)/change_month)*change_month::integer)::integer)
                      else end_date
                      end);
END
;                                                           

CREATE OR REPLACE FUNCTION f_fdm.max_month(etl_date date)   --计算当前季度最大月的函数
RETURN varchar
AS BEGIN
     RETURN (case when extract(quarter from etl_date)=1 then '03'
                  when extract(quarter from etl_date)=2 then '06'
                  when extract(quarter from etl_date)=3 then '09'
                  when extract(quarter from etl_date)=4 then '12'
             else '0'
            end);
END
;                                                           
SQL函数创建区END*/

/*临时表创建区*/

--临时表1--

create local temporary table if not exists tt_f_loan_corp_dubil_info_min_relsdate     --下次重定价日的第二种累加情况,最小发放贷款日期临时表
on commit preserve rows as
select  ran0contno,min(ran0dateb) as min_ran0dateb
from   dw_sdata.ccs_006_rapfn0
where  start_dt<='$TXDATE'::date
AND    end_dt>'$TXDATE'::date  
group by 1
;

--临时表2--

create local temporary table if not exists tt_f_loan_corp_dubil_info_Next_Rprc_Day   --下次重定价日临时表
on commit preserve rows as
select
     T.ran0duebno   --贷款主资料表.借据编号
	 ,(case
		    when T.ran0stat='3' or T.ran0datee='' then '$maxdate'::date    --ran0stat 贷款状态 修改最大变量 by liudongyan
			when T.ran0stat in ('7','9') then T.ran0datee::date   --贷款到期日期
			else 
				(case
				when T2.ir_adjust_cyc='01' AND T.ran0cur='01' AND '$TXDATE'::date+1<T.ran0datee::date THEN '$TXDATE'::date+1
				--when T2.ir_adjust_cyc='03' AND T.ran0cur='01'  T1.adjust_date='1'
				when T2.ir_adjust_cyc='04' AND T.ran0cur='01' AND to_date(to_char('$TXDATE'::date,'yyyy')+1||'0101','yyyymmdd')<T.ran0datee::date THEN to_date(to_char('$TXDATE'::date,'yyyy')+1||'0101','yyyymmdd')
				when T2.ir_adjust_cyc='08' AND T.ran0cur='01' AND T2.change_date_type ='01' then f_fdm.Next_Rprc_Day('$TXDATE'::date,T4.start_date::date,T.ran0datee::date,T2.rate_change_month)
				when T2.ir_adjust_cyc='08' AND T.ran0cur='01' AND T2.change_date_type ='02' then f_fdm.Next_Rprc_Day('$TXDATE'::date,T5.min_ran0dateb::date,T.ran0datee::date,T2.rate_change_month)
				when T2.ir_adjust_cyc='08' AND T.ran0cur='01' AND T2.change_date_type ='03' then f_fdm.Next_Rprc_Day('$TXDATE'::date,T.ran0dateb::date,T.ran0datee::date,T2.rate_change_month)
				when T2.ir_adjust_cyc='08' AND T.ran0cur='01' AND T2.change_date_type ='04' then f_fdm.Next_Rprc_Day('$TXDATE'::date,T2.rate_adjust_date::date,T.ran0datee::date,T2.rate_change_month)
				when T2.ir_adjust_cyc='03' then (case 
				                                     when T.ran0cur='01' AND T1.if_adjust_date='1' AND (substr('$TXDATE',1,4)||f_fdm.max_month('$TXDATE')|| T1.adjust_date)::date  --当前季度最大月拼固定付息日
				                                     < T.ran0datee::date then (substr('$TXDATE',1,4)||f_fdm.max_month('$TXDATE')|| T1.adjust_date)::date
				                                     else (case 
				                                               when (substr('$TXDATE',1,4)||f_fdm.max_month('$TXDATE')||substr(T.ran0dateb,7,2))::date
				                                               < T. ran0datee::date then (substr('$TXDATE',1,4)||f_fdm.max_month('$TXDATE')||substr(T.ran0dateb,7,2))::date
				                                               else T.ran0datee::date
				                                           end) 
				                                  end)
			    when T2.ir_adjust_cyc='02' then (case 
				                                      when T.ran0cur='02' AND T1.if_adjust_date='1' AND (substr('$TXDATE',1,4)||f_fdm.max_month('$TXDATE')|| T1.adjust_date)::date 
				                                      < T.ran0datee::date then (substr('$TXDATE',1,4)||f_fdm.max_month('$TXDATE')|| T1.adjust_date)::date
				                                      else (case 
				                                                when (substr('$TXDATE',1,4)||f_fdm.max_month('$TXDATE')||substr(T.ran0dateb,7,2))::date
				                                                < T. ran0datee::date then T.ran0dateb::date 
				                                                else T.ran0datee::date
				                                           end)
				                                  end)
			    when T2.ir_adjust_cyc='03' AND T.ran0cur='02' AND to_date(to_char('$TXDATE'::date,'yyyy')+1||'0101','yyyymmdd')<T.ran0datee::date THEN to_date(to_char('$TXDATE'::date,'yyyy')+1||'0101','yyyymmdd')
				when T2.ir_adjust_cyc='04' AND T.ran0cur='02' AND T2.change_date_type ='01' then f_fdm.Next_Rprc_Day('$TXDATE'::date,T4.start_date::date,T.ran0datee::date,T2.rate_change_month)      
				when T2.ir_adjust_cyc='04' AND T.ran0cur='02' AND T2.change_date_type ='02' then f_fdm.Next_Rprc_Day('$TXDATE'::date,T5.min_ran0dateb::date,T.ran0datee::date,T2.rate_change_month)   
				when T2.ir_adjust_cyc='04' AND T.ran0cur='02' AND T2.change_date_type ='03' then f_fdm.Next_Rprc_Day('$TXDATE'::date,T.ran0dateb::date,T.ran0datee::date,T2.rate_change_month)        
				when T2.ir_adjust_cyc='04' AND T.ran0cur='02' AND T2.change_date_type ='04' then f_fdm.Next_Rprc_Day('$TXDATE'::date,T2.rate_adjust_date::date,T.ran0datee::date,T2.rate_change_month)
			    else T.ran0datee::date
			end
			) 
			end
			)                                                       as Next_Rprc_Day
			from dw_sdata.ccs_006_rapfn0 T   --贷款主资料表			
			left join 
                         (select borrow_num,adjust_date,payout_info_detail_id,if_adjust_date
                          from (
                               select borrow_num
                               ,if_adjust_date
                               ,adjust_date
                               ,payout_info_detail_id
                               ,row_number()over(partition by borrow_num order by time_mark desc  ) rn
                               from dw_sdata.ccs_004_tb_con_payout_info_detail ---支用申请明细表
                               where  start_dt<='$TXDATE'::date  and end_dt>'$TXDATE'::date
                             )  a
                         where rn = 1
                          ) T1
                        on  T.ran0duebno=T1.borrow_num
        		left join dw_sdata.ccs_004_tb_con_payout_int_rate T2        --支用申请利率结构
			    on  T1.payout_info_detail_id=T2.payout_info_detail_id
			    AND  T2.start_dt<='$TXDATE'::date
                            AND  T2.end_dt>'$TXDATE'::date
			left join dw_sdata.ccs_004_tb_con_contract T4     --信贷合同
			    on T.ran0contno=T4.contract_num
			    AND  T4.start_dt<='$TXDATE'::date
                            AND  T4.end_dt>'$TXDATE'::date
			left join tt_f_loan_corp_dubil_info_min_relsdate T5   --首次放款日期临时表
			    on T.ran0contno=T5.ran0contno			   
                         where T.start_dt<='$TXDATE'::date
                         AND T.end_dt>'$TXDATE'::date   
			--(select ran0contno,min(ran0dateb) from  ccs_006_rapfn0 where start_dt<='$TXDATE'::date AND  end_dt>'$TXDATE'::date group by ran0contno) t7 
			--	on T4.contract_num=t7.ran0contno

;

--临时表3-- 

create local temporary table if not exists tt_f_loan_corp_dubil_info_Last_Rprc_Day   --上次重定价日临时表（通过T，C，T1，T2的关联得到下次重定价日和重定价周期。相减得到上次重定价日）
on commit preserve rows as
select  ran0duebno
        ,Next_Rprc_Day
        ,Rprc_Prd
        ,Rprc_Prd_Corp_Cd
        ,ran0dateb
        ,(case
                when Next_Rprc_Day='$MAXDATE'::date then ran0dateb::date --or Next_Rprc_Day='' 修改日期变量 by liudongyan at 20160922
                when Rprc_Prd_Corp_Cd='M' then add_months(Next_Rprc_Day,-Rprc_Prd)
                when Rprc_Prd_Corp_Cd='Y'  then add_months(Next_Rprc_Day,-(Rprc_Prd*12))
                when Rprc_Prd_Corp_Cd='D'  then Next_Rprc_Day-Rprc_Prd
                --when Rprc_Prd_Corp_Cd=''     then ran0dateb::date
         end
         )                                                  as Last_Rprc_Day --上次重定价日
from
(select T.ran0duebno
      ,C.Next_Rprc_Day
      --,Rprc_Prd
      ,T.ran0dateb
      ,(case 
                when T2.ir_adjust_cyc='01' AND T.ran0cur='01' then '0'
                when T2.ir_adjust_cyc='03' AND T.ran0cur='01' then '3'
                when T2.ir_adjust_cyc='04' AND T.ran0cur='01' then '1'
                when T2.ir_adjust_cyc='08' AND T.ran0cur='01' then T2.rate_change_month::integer
                when T2.ir_adjust_cyc='02' AND T.ran0cur<>'01' then '3'
                when T2.ir_adjust_cyc='03' AND T.ran0cur<>'01' then '1'
                when T2.ir_adjust_cyc='04' AND T.ran0cur<>'01' then T2.rate_change_month::integer
           else 
               (case 
                     when T.ran0datee='' then '0'
                else (T.ran0datee::date-T.ran0dateb::date)+1
                end
                   )
          end
          )                                                                         as Rprc_Prd  ---重定价周期  
        ,(case 
               when T2.ir_adjust_cyc='03' AND T.ran0cur='01' then 'M'
               when T2.ir_adjust_cyc='04' AND T.ran0cur='01' then 'Y'
               when T2.ir_adjust_cyc='08' AND T.ran0cur='01' then 'M'
               when T2.ir_adjust_cyc='02' AND T.ran0cur<>'01' then 'M'
               when T2.ir_adjust_cyc='03' AND T.ran0cur<>'01' then 'Y'
               when T2.ir_adjust_cyc='04' AND T.ran0cur<>'01' then 'M'
          else 
              (case 
                   when  T.ran0datee='' then ''
               else 'D'
               end
                   )
          end
              )                                                                         as Rprc_Prd_Corp_Cd ---重定价周期单位代码
             
from dw_sdata.ccs_006_rapfn0   AS T  
left join tt_f_loan_corp_dubil_info_Next_Rprc_Day AS C  
ON C.ran0duebno=T.ran0duebno
LEFT JOIN  (select borrow_num,payout_info_detail_id
            from (
                 select borrow_num
                        ,payout_info_detail_id
                        ,row_number()over(partition by borrow_num order by time_mark desc  ) rn
                  from dw_sdata.ccs_004_tb_con_payout_info_detail ---支用申请明细表
                  where  start_dt<='$TXDATE'::date  and end_dt>'$TXDATE'::date
                 )  a
            where rn = 1
           ) T1
ON T.ran0duebno=T1.borrow_num                                     
LEFT JOIN dw_sdata.ccs_004_tb_con_payout_int_rate                                                  AS T2                                                          
ON        T1.payout_info_detail_id=T2.payout_info_detail_id
AND       T2.start_dt<='$TXDATE'::date
AND       T2.end_dt>'$TXDATE'::date
where     T.start_dt<='$TXDATE'::date
AND   T.end_dt>'$TXDATE'::date
) as table1
;

--临时表4--

create local temporary table tt_f_loan_corp_dubil_info_yjs   --月积数，年积数，月日均余额，年日均余额临时表
on commit preserve rows as
select * 
from f_fdm.f_loan_corp_dubil_info 
where 1=2;

/*临时表创建区END*/

/*数据回退区*/
DELETE FROM f_fdm.f_loan_corp_dubil_info
WHERE etl_date='$TXDATE'::date
;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_fdm.f_loan_corp_dubil_info
(
           Grp_Typ                                                             --组别
           ,ETL_Date                                                            --数据日期
           ,Agmt_Id                                                             --协议编号
           ,Cust_Num                                                            --客户号
           ,Org_Num                                                             --机构号
           ,Cur_Cd                                                              --货币代码
           ,Prod_Cd                                                             -- 产品代码
           ,Distr_Dt                                                            --放款日期
           ,St_Int_Dt                                                           --起息日
           ,Due_Dt                                                              --到期日
           ,Payoff_Dt                                                           --结清日期
           ,Wrtoff_Dt                                                           --核销日期
           ,Loan_Orgnl_Amt                                                      --贷款原始金额
           ,Exec_Int_Rate                                                       --执行利率
           ,Bmk_Int_Rate                                                        --基准利率
           ,Flt_Ratio                                                           --浮动比例
           ,Basis                                                               --基差
           ,Ovrd_Int_Rate                                                       --逾期利率
           ,Int_Base_Cd                                                         --计息基础代码
           ,Cmpd_Int_Calc_Mode_Cd                                               --复利计算方式代码
           ,Pre_Chrg_Int                                                         --是否先收息
          ,Int_Rate_Attr_Cd                                                     --利率属性代码
          ,Int_Rate_Adj_Mode_Cd                                                 --利率调整方式代码
          ,Amtbl_Loan_Ind                                                       --分期贷款标志
          ,Repay_Mode_Cd                                                        --还款方式代码
          ,Repay_Prd_Cd                                                         --还款周期代码
          ,Orgnl_Term                                                           --原始期限
          ,Orgnl_Term_Corp_Cd                                                   --原始期限单位代码
          ,Rprc_Prd                                                             --重定价周期
          ,Rprc_Prd_Corp_Cd                                                     --重定价周期单位代码
          ,Last_Rprc_Day                                                        --上次重定价日
          ,Next_Rprc_Day                                                        --下次重定价日
          ,Next_Pay_Amt                                                         --下次付款金额
          ,Last_Pay_Day                                                         --上次付款日
          ,Next_Pay_Day                                                         --下次付款日
          ,Four_Cls_Cls                                                         --四级分类
          ,Fiv_Cls                                                              --五级分类
          ,Agmt_Stat_Cd                                                         --协议状态代码
          ,Contr_Agmt_Id                                                        --合同协议编号
          ,Asst_Secu_Ind                                                        --资产证券化标识
          ,Prin_Subj                                                            --本金科目
          ,Curr_Bal                                                             --当前余额
          ,Norm_Bal                                                             --正常余额
          ,Slug_Bal                                                             --呆滞余额
          ,Bad_Debt_Bal                                                         --呆账余额
          ,Wrtoff_Prin                                                          --核销本金
          ,Int_Subj                                                             --利息科目
         ,Today_Provs_Int                                                       --当日计提利息
         ,CurMth_Provs_Int                                                      --当月计提利息
         ,Accm_Provs_Int                                                        --累计计提利息
         ,Today_Chrg_Int                                                        --当日收息
         ,CurMth_Recvd_Int                                                      --当月已收息
         ,Accm_Recvd_Int                                                        --累计已收息
         ,Int_Adj_Amt                                                           --利息调整金额
         ,Mth_Accm                                                              --月积数
         ,Yr_Accm                                                               --年积数
         ,Mth_Day_Avg_Bal                                                       --月日均余额
         ,Yr_Day_Avg_Bal                                                        --年日均余额
         ,Opr_Org_Num                                                           --经办机构号
         ,Opr_Tellr_Num                                                         --经办柜员
         ,Is_Corp_Cnstr_Hous_Loan                                               --是否单位构建房贷款
         ,Free_Int_Ind                                                          --免息标志
         ,Free_Int_Prd                                                          --免息周期
         ,Expd_Ind                                                              --展期标志
         ,Expd_Due_Dt                                                           --展期到期日
         ,Loan_Deval_Prep_Bal                                                   --贷款减值准备余额
         ,Loan_Deval_Prep_Amt                                                   --贷款减值准备发生额
         ,Ovrd_Days                                                             --逾期天数
         ,Ovrd_Prin                                                             --逾期本金
         ,Ovrd_Int                                                              --逾期利息
         ,Adv_Money_Ind                                                         --垫款标志
         ,Adv_Money_Amt                                                         --垫款金额
         ,Adv_Money_Bal                                                         --垫款余额
         ,Sys_Src                                                               --系统来源

)
SELECT
       '1'                                                                              as Grp_Typ
       ,'$TXDATE'::date                                                        as ETL_Date      
       ,T.ran0duebno                                                                    as Agmt_Id
       ,COALESCE(T1.customer_num,T.ran0bl1)                                             as Cust_Num
       ,T.ran0dpnok                                                                     as Org_Num
       ,coalesce(T21.TGT_CD,'@'||T.ran0cur)                                             as Cur_Cd
       ,coalesce(T1.credit_product_cd ,'')                                              as Prod_Cd
       ,T.ran0dateb::date                                                               as Distr_Dt
       ,T.ran0dateb::date                                                               as St_Int_Dt
       ,T.ran0datee::date                                                               as Due_Dt
       ,T.ran0dated::date                                                               as Payoff_Dt
       ,T.ran0dated::date                                                               as Wrtoff_Dt
       ,T.ran0amt                                                                       as Loan_Orgnl_Amt
       ,T.ran0itrtn                                                                     as Exec_Int_Rate
       ,coalesce(T2.benchmark_ir_year_rate*100,0)                                       as Bmk_Int_Rate
       ,coalesce(T2.ir_nego_rate*100,0)                                                 as Flt_Ratio
       ,coalesce(T2.benchmark_ir_year_rate*100,0) -T.ran0itrtn                              as Basis
       ,coalesce(T2.ovdue_ir_year_rate*100,0)                                           as Ovrd_Int_Rate
       ,(case 
             when T.ran0itrtdp='1' and T.ran0paytyp in ('01','02','03','04') then '10'  --30/360 
             when T.ran0itrtdp='1' and T.ran0paytyp not in ('01','02','03','04') then '1'  --实际/360
             when T.ran0itrtdp='2' and T.ran0paytyp in ('01','02','03','04') then '11'  --30/365
             else '4' --实际/365
        end
        )                                                                     as Int_Base_Cd
       ,'1'                                                                             as Cmpd_Int_Calc_Mode_Cd
       ,(case
               when T.ran0paytyp in ('21','32') then '1'
          else '0'
          end 
         )                                                                             as Pre_Chrg_Int
        ,(case 
               when T.ran0cur ='01'AND T2.ir_adjust_cyc ='01'then '4'
               when T.ran0cur ='01' AND T2.ir_adjust_cyc ='07'then '1'
               when T.ran0cur <>'01'AND T2.ir_adjust_cyc ='02'then '1'  
               else '3' 
               end  
               )                                                                        as Int_Rate_Attr_Cd
         ,(case 
                when T.ran0cur ='01' then coalesce(T24.tgt_cd,'@'||T2.ir_adjust_cyc)
                else coalesce(T25.tgt_cd,'@'||T2.ir_adjust_cyc)     
           end
          )                                                                             as Int_Rate_Adj_Mode_Cd
         ,T.ran0lntype                                                                  as Amtbl_Loan_Ind
         ,coalesce(T22.TGT_CD,'@'||T.ran0paytyp)                                        as Repay_Mode_Cd
         ,T.ran0caspan                                                                  as Repay_Prd_Cd
         ,T.ran0term                                                                    as Orgnl_Term
         ,(case
                when T3.contract_term_unit_cd='1' then 'Y'
                when T3.contract_term_unit_cd='2' then 'M'
                when T3.contract_term_unit_cd='3' then 'D'  
          end
          )                                                                       as Orgnl_Term_Corp_Cd
         ,coalesce(B.Rprc_Prd,0)                                                              as Rprc_Prd  ---重定价周期  
         ,coalesce(B.Rprc_Prd_Corp_Cd,'')                                                     as Rprc_Prd_Corp_Cd ---重定价周期单位代码
         ,coalesce(B.Last_Rprc_Day ,'$MINDATE' :: date)                                   as Last_Rprc_Day  ---上次重定价日  --暂时默认为最大日期
         ,coalesce(B.Next_Rprc_Day,'$MAXDATE' :: date)                                    as Next_Rprc_Day---下次重定价日
         ,(CASE 
                WHEN T.ran0paytyp in ( '01','02','03','04') THEN T19.ra91bqtsbj+T19.ra91bqtslx
                WHEN T.ran0paytyp IN ('14','15','16') THEN T20.ra92bjamt
                ELSE 0 END)                                                                        as Next_Pay_Amt ---下次付款金额
         ,to_date(T.ran0datepr,'yyyymmdd')                                                         as Last_Pay_Day
         ,to_date(T.ran0dateca,'yyyymmdd')                                                         as Next_Pay_Day
         ,coalesce(T5.four_sort,'')                                                                 as Four_Cls_Cls
         ,T.ran0fivecl                                                             as Fiv_Cls
         ,coalesce(T23.TGT_CD,'@'||T.ran0stat)                                     as Agmt_Stat_Cd
         ,coalesce(T4.contract_num,'')                                             as Contr_Agmt_Id
         ,''                                                                       as Asst_Secu_Ind
         ,coalesce(T6.ITM_NO,'')                                                                 as Prin_Subj
         ,coalesce(T6.BAL,0)                                                                    as Curr_Bal
         ,coalesce(T5.in_gear_balance,0)                                                        as Norm_Bal
         ,coalesce(T5.primness_balance,0)                                                       as Slug_Bal
         ,coalesce(T5.bad_debt_balance,0)                                                       as Bad_Debt_Bal
         ,coalesce(T7.aa10bal,0)                                                                as Wrtoff_Prin
         ,coalesce(T8.ITM_NO ,'')                                                               as Int_Subj
         ,coalesce(T9.jc10amt,0)                                                                as Today_Provs_Int
         ,coalesce(T10.jc10amt,0)                                                               as CurMth_Provs_Int
         ,0                                                               as Accm_Provs_Int
         ,coalesce(T12.AMT,0)                                                                   as Today_Chrg_Int
         ,coalesce(T13.AMT,0)                                                                   as CurMth_Recvd_Int
         ,0                                                               as Accm_Recvd_Int
            ,coalesce(T15.aa10bal,0)                                                                as Int_Adj_Amt
            ,0.00           as Mth_Accm ---月积数
            ,0.00           as Yr_Accm  ---年积数
            ,0.00           as Mth_Day_Avg_Bal ---月日均余额
            ,0.00           as Yr_Day_Avg_Bal ---年日均余额
           ,coalesce(T4.hANDling_org_cd,'')                                                          as Opr_Org_Num
           ,coalesce(T4.hANDling_user_num,'')                                                        as Opr_Tellr_Num
           ,coalesce(T4.if_corp_houst_loan,'')                                                       as Is_Corp_Cnstr_Hous_Loan
           ,coalesce(T2.ir_free_ind ,'')                                                             as Free_Int_Ind --modify 
           ,coalesce(T2.ir_free_term,0)                                                              as Free_Int_Prd
           ,coalesce(T4.postponement_ind,'')                                                         as Expd_Ind
          -- ,coalesce(T16.add_period_end_date ,'$MINDATE' :: date )                                 as Expd_Due_Dt
           ,'$MAXDATE' :: date                                                                      as Expd_Due_Dt
           ,0-coalesce(T17.aa10bal,0)                                                               as Loan_Deval_Prep_Bal
           ,coalesce(T18.jc10amt,0)                                                                 as Loan_Deval_Prep_Amt
           ,coalesce(T5.last_ovdue_days ,0)                                                         as Ovrd_Days
           ,T.ran0yqbal                                                                             as Ovrd_Prin
           ,coalesce(T5.overdue_balance,0)-T.ran0yqbal                                              as Ovrd_Int
           ,(case 
                   when T.ran0lncls in ('08','09','10','15') then '1' 
             else '0' 
             end  
             )                                                                           as Adv_Money_Ind
             ,0.00                                                                         as Adv_Money_Amt  --垫款金额
             ,0.00                                                                         as Adv_Money_Bal  --垫款余额
            ,'CCS'                                                                       as Sys_Src


         
FROM    dw_sdata.ccs_006_rapfn0                                                            AS T   
left join tt_f_loan_corp_dubil_info_Last_Rprc_Day AS B
on        T.ran0duebno=B.ran0duebno                                                 
LEFT JOIN  (
						select borrow_num,customer_num,credit_product_cd,payout_info_detail_id 
						from (
 								   select borrow_num
        							  	,customer_num
        							  	,credit_product_cd
        							  	,payout_info_detail_id
        							  	,row_number()over(partition by borrow_num order by time_mark desc  ) rn 
   						     from dw_sdata.ccs_004_tb_con_payout_info_detail ---支用申请明细表
                   where  start_dt<='$TXDATE'::date 
                   and end_dt>'$TXDATE'::date
                 )  a 
            where rn = 1 
            )																																				AS 	T1
on T.ran0duebno=T1.borrow_num  
--          dw_sdata.ccs_004_tb_con_payout_info_detail                                               AS T1                                                   
--ON        T.ran0duebno=T1.borrow_num
--AND       T1.start_dt<='$TXDATE'::date
--AND       T1.end_dt>'$TXDATE'::date
LEFT JOIN      
          dw_sdata.ccs_004_tb_con_payout_int_rate                                                  AS T2                                                          
ON        T1.payout_info_detail_id=T2.payout_info_detail_id
AND       T2.start_dt<='$TXDATE'::date
AND       T2.end_dt>'$TXDATE'::date
LEFT JOIN dw_sdata.ccs_004_tb_con_biz_detail                                                        AS  T3                                                            
ON        T.ran0contno=T3.contract_num
AND       T3.start_dt<='$TXDATE'::date
AND       T3.end_dt>'$TXDATE'::date 
LEFT JOIN dw_sdata.ccs_004_tb_con_contract                                                          AS  T4 
                                                                             
ON        T.ran0contno=T4.contract_num
AND       T4.start_dt<='$TXDATE'::date
AND       T4.end_dt>'$TXDATE'::date
LEFT JOIN dw_sdata.ccs_004_tb_con_borr_acct_summary                                                 AS  T5 
ON        T.ran0duebno=T5.borrow_num 
AND       T5.start_dt<='$TXDATE'::date
AND       T5.end_dt>'$TXDATE'::date
LEFT JOIN dw_sdata.acc_003_t_acc_assets_ledger                                                     AS   T6         
ON         T.ran0duebno=T6.ACC
AND       T6.start_dt<='$TXDATE'::date
AND        T6.end_dt>'$TXDATE'::date   
AND        T6.SYS_CODE ='99460000000'
LEFT JOIN (select * from (
select *,row_number()over(partition by AA10DUEBNO order by start_dt desc) Rn
 from
dw_sdata.ccs_006_aapf10
where start_dt<='$TXDATE'::date
AND   end_dt>'$TXDATE'::date
AND   aa10type ='12'
)t
where rn =1)                                                                  AS T7
ON        T.ran0duebno=T7.aa10duebno
LEFT JOIN dw_sdata.acc_003_t_accdata_last_item_no                                                      AS T8
ON       T6.ITM_NO= T8.AMT_ITM
AND       T8.start_dt<='$TXDATE'::date
AND       T8.end_dt>'$TXDATE'::date
AND      T8.FIRST_ITM='18' 
LEFT JOIN  (select A.jc10duebno
                 ,sum(A.jc10amt) as jc10amt
           from (select jc10duebno
                       ,jc10amt 
                 from dw_sdata.ccs_006_jcpf10
                 where jc10type ='05'
                 AND  jc10dc='D'
                 AND jc10date::date='$TXDATE'::date
                -- AND etl_dt='$TXDATE'::date                                                                                                                          
                 union  all  
                 select jc10duebno
                       ,jc10amt 
                 from dw_sdata.ccs_006_jcpf10
                 where jc10type ='20'
                 AND  jc10dc='C'
                 AND jc10date::date='$TXDATE'::date
                -- AND etl_dt='$TXDATE'::date
                 ) A  
         group by A.jc10duebno)                                                                      AS  T9  
ON         T.ran0duebno=T9.jc10duebno
LEFT JOIN (select A.jc10duebno
                 ,sum(A.jc10amt) as jc10amt
           from (select jc10duebno
                       ,jc10amt 
                 from dw_sdata.ccs_006_jcpf10
                 where jc10type ='05'
                 AND  jc10dc='D'
                 AND month(jc10date::date)=month('$TXDATE'::date)                                   
                 AND jc10date<='$TXDATE'
                 union  all                    
                 select jc10duebno
                       ,jc10amt 
                 from dw_sdata.ccs_006_jcpf10
                 where jc10type ='20'
                 AND  jc10dc='C'
                 AND month(jc10date::date)=month('$TXDATE'::date)       
                 AND jc10date<='$TXDATE'
                 ) A  
           group by A.jc10duebno)                                                                AS T10
ON          T.ran0duebno=T10.jc10duebno
--LEFT JOIN (select A.jc10duebno
--                 ,sum(A.jc10amt) as jc10amt
--           from (select jc10duebno
--                       ,jc10amt 
--                 from dw_sdata.ccs_006_jcpf10
--                 where jc10type ='05'
--                 AND  jc10dc='D'                                                                                                                         
--                 AND etl_dt<='$TXDATE'::date
--                 union  all
--                 select jc10duebno,jc10amt from dw_sdata.ccs_006_jcpf10
--                 where jc10type ='20'
--                 AND  jc10dc='C'      
--                 AND etl_dt<='$TXDATE'::date
--                 ) A  
--           group by A.jc10duebno)                                                                      AS T11
--ON         T.ran0duebno=T11.jc10duebno
LEFT JOIN (SELECT rd90duebno,sum(rd90amtaor+rd90tamtar+rd90amtapr+rd90bamtar+rd90oamtar+rd90camtar)  AS AMT 
          FROM dw_sdata.ccs_006_rdpf90 
          WHERE rd90date ='$TXDATE'
          GROUP BY rd90duebno)                                                          AS T12  --rd90date='统计日期'
ON        T.ran0duebno=T12.rd90duebno
LEFT JOIN (SELECT rd90duebno, sum(rd90amtaor+rd90tamtar+rd90amtapr+rd90bamtar+rd90oamtar+rd90camtar) AS AMT 
           FROM dw_sdata.ccs_006_rdpf90
           WHERE   substr(rd90date,1,6)=substr('$TXDATE',1,6)        --varchar rd90date='当月'
           AND rd90date<='$TXDATE'  
           GROUP BY rd90duebno )          AS T13
ON        T.ran0duebno=T13.rd90duebno
--LEFT JOIN (SELECT rd90duebno, SUM(rd90amtaor+rd90tamtar+rd90amtapr+rd90bamtar+rd90oamtar+rd90camtar) AS AMT 
--          FROM dw_sdata.ccs_006_rdpf90
--          WHERE start_dt<='$TXDATE'::date
--          GROUP BY rd90duebno)                                                                       AS  T14
--ON        T.ran0duebno=T14.rd90duebno
/*
LEFT JOIN  dw_sdata.ccs_006_aapf10                                                                          AS T15
ON       T.ran0duebno=T15.aa10duebno
AND       T15.start_dt<='$TXDATE'::date
AND       T15.end_dt>'$TXDATE'::date
AND      T15.aa10type in ('03')*/
LEFT JOIN  (select * from (
select *,row_number()over(partition by AA10DUEBNO order by start_dt desc) Rn
 from 
dw_sdata.ccs_006_aapf10 
where start_dt<='$TXDATE'::date
AND   end_dt>'$TXDATE'::date
AND   aa10type ='03'
)t
where rn =1)  AS T15                                                                      
ON       T.ran0duebno=T15.aa10duebno
--LEFT JOIN dw_sdata.ccs_004_tb_con_extend_term_info                                                         AS  T16
--ON       T.ran0duebno=T16.orig_payout_num
--AND       T16.start_dt<='$TXDATE'::date
--AND       T16.end_dt>'$TXDATE'::date 
/*
LEFT JOIN dw_sdata.ccs_006_aapf10                                                                          AS T17
ON       T.ran0duebno=T17.aa10duebno 
AND       T17.start_dt<='$TXDATE'::date
AND       T17.end_dt>'$TXDATE'::date 
AND      T17.aa10type='14'
*/
LEFT JOIN  (select * from (
select *,row_number()over(partition by AA10DUEBNO order by start_dt desc) Rn
 from
dw_sdata.ccs_006_aapf10
where start_dt<='$TXDATE'::date
AND   end_dt>'$TXDATE'::date
AND   aa10type ='14'
)t
where rn =1)  AS T17
ON       T.ran0duebno=T17.aa10duebno
LEFT JOIN (select jc10duebno
                ,sum(case 
                          when  jc10dc='C' then jc10amt 
                          else -jc10amt 
                     end
                     ) as  jc10amt 
          from dw_sdata.ccs_006_jcpf10
          where jc10type='14'     
          AND jc10date::date='$TXDATE'::date 
         -- AND etl_dt='$TXDATE'::date  
          group by jc10duebno)                                                                           AS T18  
on T.ran0duebno=T18.jc10duebno
left join dw_sdata.ccs_006_rapf91                                                                        AS T19
on       T.ran0duebno=T19.ra91duebno
AND      T19.start_dt<='$TXDATE'::date
AND      T19.end_dt>'$TXDATE'::date 
/*left join dw_sdata.ccs_006_rapf92                                                                        AS  T20
on       T.ran0duebno=T20.ra92duebno
AND      T20.start_dt<='$TXDATE'::date
AND      T20.end_dt>'$TXDATE'::date
AND      T20.ra92dateb::date<='$TXDATE'::date
AND      T20.ra92datee::date>'$TXDATE'::date*/
left join 
(select ra92duebno,sum(ra92bjamt) as ra92bjamt
from 
dw_sdata.ccs_006_rapf92
where    start_dt<='$TXDATE'::date
AND      end_dt>'$TXDATE'::date
AND      ra92dateb::date<='$TXDATE'::date
AND      ra92datee::date>'$TXDATE'::date
group by ra92duebno
) AS  T20
on       T.ran0duebno=T20.ra92duebno                                                                -----modified by wyh at 20160930 

left join f_fdm.CD_RF_STD_CD_TRAN_REF T21
on       T.ran0cur=T21.SRC_CD
AND  T21.DATA_PLTF_SRC_TAB_NM = 'CCS_006_RAPFN0'
AND  T21.Data_Pltf_Src_Fld_Nm ='RAN0CUR' 
left join f_fdm.CD_RF_STD_CD_TRAN_REF T22
on       T.RAN0PAYTYP=T22.SRC_CD
AND      T22.DATA_PLTF_SRC_TAB_NM = 'CCS_006_RAPFN0'
AND      T22.Data_Pltf_Src_Fld_Nm ='RAN0PAYTYP'
left join f_fdm.CD_RF_STD_CD_TRAN_REF T23
on       T.RAN0STAT=T23.SRC_CD
AND      T23.DATA_PLTF_SRC_TAB_NM = 'CCS_006_RAPFN0'
AND      T23.Data_Pltf_Src_Fld_Nm ='RAN0STAT'
left join f_fdm.CD_RF_STD_CD_TRAN_REF T24
on       T2.IR_ADJUST_CYC=T24.SRC_CD
AND      T24.DATA_PLTF_SRC_TAB_NM = 'CCS_004_TB_CON_PAYOUT_INT_RATE'
AND      T24.Data_Pltf_Src_Fld_Nm ='IR_ADJUST_CYC_CNY'
left join f_fdm.CD_RF_STD_CD_TRAN_REF T25
on       T2.IR_ADJUST_CYC=T25.SRC_CD
AND      T25.DATA_PLTF_SRC_TAB_NM = 'CCS_004_TB_CON_PAYOUT_INT_RATE'
AND      T25.Data_Pltf_Src_Fld_Nm ='IR_ADJUST_CYC'



where T.start_dt<='$TXDATE'::date
AND   T.end_dt>'$TXDATE'::date  

;

/*数据处理区END*/

/*计算月积数、年积数、月日均余额、年日均余额*/

insert /* +direct */ into tt_f_loan_corp_dubil_info_yjs
(
Agmt_Id 
,Mth_Accm 
,Yr_Accm 
,Mth_Day_Avg_Bal 
,Yr_Day_Avg_Bal 
)
select t.Agmt_Id
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
from f_fdm.f_loan_corp_dubil_info t
left join f_fdm.f_loan_corp_dubil_info t1
on t.Agmt_Id=t1.Agmt_Id
and t1.etl_date='$TXDATE'::date-1
where t.etl_date='$TXDATE'::date
;
/*计算END*/

/*更新目标表月积数、年积数、月日均余额、年日均余额*/
update f_fdm.f_loan_corp_dubil_info t
set Mth_Accm=t1.Mth_Accm
   ,Yr_Accm=t1.Yr_Accm
   ,Mth_Day_Avg_Bal=t1.Mth_Day_Avg_Bal
   ,Yr_Day_Avg_Bal=t1.Yr_Day_Avg_Bal
from tt_f_loan_corp_dubil_info_yjs t1
where t.Agmt_Id=t1.Agmt_Id
and   t.etl_date='$TXDATE'::date
;
 
/*更新END*/
COMMIT;

