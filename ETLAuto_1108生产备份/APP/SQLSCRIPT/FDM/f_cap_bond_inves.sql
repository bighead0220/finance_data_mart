/*
Author             :魏银辉
Function           :资金债券投资信息表
Load method        :INSERT
Source table       :cos_000_mmdeals T1        
                   :cd_cd_table T2            
                   :cos_000_sectype T3        
                   :cos_000_cparty T4         
                   :ecf_002_t01_cust_info_T T5
Destination Table  :f_Cap_Bond_Inves 资金债券投资信息表
Frequency          :D
Modify history list:Created by 魏银辉
                   :Modify  by
                     modified by wyh 20160912 修改机构代码，交易对手客户号
                    modified by wyh 20160913 修改了字段"账面余额"，"市场价值"这2个字段的coalesce关系;
                    modified by zhangliang 20161009 修改临时表t6,变更223
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/
/*临时表创建区*/

DROP TABLE IF EXISTS f_Cap_Bond_Inves_TMP_T6 cascade;

create local temporary table if not exists f_Cap_Bond_Inves_TMP_T6              --本金科目、利息科目、计提利息科目、利息调整科目及对应的金额
on commit preserve rows as
select 
                deal_no
                --1430%001%\1510%001%\1525%001%\1505%001%\1540%001%\1515%001%\1436%001%\1106%001%\1125%\241001001
                ,max(case when map_code like '1430%001%' or map_code like '1510%001%' or map_code like '1525%001%' 
                               or map_code like '1505%001%' or map_code like '1540%001%' or map_code like '1515%001%' 
                               or map_code like '1436%001%' or map_code like '1106%001%' or map_code like '1125%' or map_code='241001001'
                          then map_code end)    as prin_acc  --本金科目
                ,sum(case when map_code like '1430%001%' or map_code like '1510%001%' or map_code like '1525%001%' 
                               or map_code like '1505%001%' or map_code like '1540%001%' or map_code like '1515%001%' 
                               or map_code like '1436%001%' or map_code like '1106%001%' or map_code like '1125%' or map_code='241001001'
                          then amount else 0 end)                    as prin_amount --购入成本
                --5305%\5315%\5320%\5310%\5322%\5316%\5306%\5205%\5105%\5660%\621201
                ,max(case when map_code like '5305%' or map_code like '5315%' or map_code like '5320%' 
                               or map_code like '5310%' or map_code like '5322%' or map_code like '5316%' 
                               or map_code like '5306%' or map_code like '5205%' or map_code like '5105%' or map_code like '5660%' or map_code='621201'
                          then map_code end)    as intr_acc   --利息科目：利息收入/利息支出
                ,sum(case when (map_code like '5305%' or map_code like '5315%' or map_code like '5320%' 
                               or map_code like '5310%' or map_code like '5322%' or map_code like '5316%' 
                               or map_code like '5306%' or map_code like '5205%' or map_code like '5105%' or map_code like '5660%' or map_code='621201') 
                               and gl_date='$TXDATE'::date
                          then amount else 0 end)                    as intr_amount_d --当日收付息           
                ,sum(case when (map_code like '5305%' or map_code like '5315%' or map_code like '5320%' 
                               or map_code like '5310%' or map_code like '5322%' or map_code like '5316%' 
                               or map_code like '5306%' or map_code like '5205%' or map_code like '5105%' or map_code like '5660%' or map_code='621201')
                               and to_char(gl_date,'yyyymm')=to_char('$TXDATE'::date,'yyyymm')
                          then amount else 0 end)                    as intr_amount_m --当月收付息           
                ,sum(case when map_code like '5305%' or map_code like '5315%' or map_code like '5320%' 
                               or map_code like '5310%' or map_code like '5322%' or map_code like '5316%' 
                               or map_code like '5306%' or map_code like '5205%' or map_code like '5105%' or map_code like '5660%' or map_code='621201'
                          then amount else 0 end)                    as intr_amount_t --累计收付息                --1325%\1335%\1340%\1505%003%\1330%\1341%\1342%\1515%003%\1336%\1326%\1315%\253501              
                ,max(case when map_code like '1325%' or map_code like '1335%' or map_code like '1340%' or map_code like '1505%003%' 
                               or map_code like '1330%' or map_code like '1341%' or map_code like '1342%' or map_code like '1515%003%' or map_code like '1336%' 
                               or map_code like '1326%' or map_code like '1315%' or map_code='253501'
                          then map_code end )   as accr_acc     --计提利息科目
                ,sum(case when (map_code like '1325%' or map_code like '1335%' or map_code like '1340%' or map_code like '1505%003%' 
                               or map_code like '1330%' or map_code like '1342%' or map_code like '1515%003%' or map_code like '1336%' 
                               or map_code like '1326%' or map_code like '1315%' or map_code='253501') 
                               and gl_date='$TXDATE'::date 
                          then amount else 0 end )                   as accr_amount_d --当日计提利息
                ,sum(case when (map_code like '1325%' or map_code like '1335%' or map_code like '1340%' or map_code like '1505%003%' 
                               or map_code like '1330%' or map_code like '1342%' or map_code like '1515%003%' or map_code like '1336%' 
                               or map_code like '1326%' or map_code like '1315%' or map_code='253501') 
                               and to_char(gl_date,'yyyymm')=to_char('$TXDATE'::date,'yyyymm')
                          then amount else 0 end )                   as accr_amount_m --当月计提利息
                ,sum(case when map_code like '1325%' or map_code like '1335%' or map_code like '1340%' or map_code like '1505%003%' 
                               or map_code like '1330%' or map_code like '1342%' or map_code like '1515%003%' or map_code like '1336%' 
                               or map_code like '1326%' or map_code like '1315%' or map_code='253501'
                          then amount else 0 end )                   as accr_amount_t --累计计提利息       
                --1510%002%\1525%002%\1505%002%\1515%002%\1106%002%
                 ,max(case when  map_code like '1510%002%' or map_code like '1525%002%' 
                                 or map_code like '1505%002%' or map_code like '1515%002%' or map_code like '1106%002%'
                           then map_code end )  as intr_dis_acc   --利息调整科目
                 ,sum(case when  (map_code like '1510%002%' or map_code like '1525%002%' 
                                 or map_code like '1505%002%' or map_code like '1515%002%' or map_code like '1106%002%')
                                 and gl_date='$TXDATE'::date
                           then amount else 0 end )                  as intr_dis_amount_d --当日利息调整金额
                 ,sum(case when  (map_code like '1510%002%' or map_code like '1525%002%' 
                                 or map_code like '1505%002%' or map_code like '1515%002%' or map_code like '1106%002%')
                                 and to_char(gl_date,'yyyymm')=to_char('$TXDATE'::date,'yyyymm')
                           then amount else 0 end )                  as intr_dis_amount_m --当月利息调整金额
                 ,sum(case when  (map_code like '1510%002%' or map_code like '1525%002%' 
                                 or map_code like '1505%002%' or map_code like '1515%002%' or map_code like '1106%002%')
                           then amount else 0 end )                  as intr_dis_amount_t --累计利息调整金额
                --公允价值变动科目:1430%002%\1525%004%\1436%002%\5710%
                 ,max(case when  map_code like '1430%002%' or map_code like '1525%004%' or map_code like '1436%002%' or map_code like '5710%'                               
                           then  map_code end )  as fair_val_chgs_acc   --公允价值变动科目
                 ,sum(case when  (map_code like '1430%002%' or map_code like '1525%004%' or map_code like '1436%002%' or map_code like '5710%' )
                                 and gl_date='$TXDATE'::date
                           then amount else 0 end )                  as fair_val_chgs_amount_d --当日公允价值变动金额
                 ,sum(case when  (map_code like '1430%002%' or map_code like '1525%004%' or map_code like '1436%002%' or map_code like '5710%' )
                                 and to_char(gl_date,'yyyymm')=to_char('$TXDATE'::date,'yyyymm')
                           then amount else 0 end )                  as fair_val_chgs_amount_m --当月公允价值变动金额
                 ,sum(case when  (map_code like '1430%002%' or map_code like '1525%004%' or map_code like '1436%002%' or map_code like '5710%' )
                           then amount else 0 end )                  as fair_val_chgs_amount_t --累计公允价值变动金额
                 --投资收益科目:5705%
                 ,max(case when  map_code like '5705%'                               
                           then  map_code end )  as inv_acc   --买卖价差收益科目
                 ,sum(case when  map_code like '5705%' and gl_date='$TXDATE'::date
                           then amount else 0 end )                  as inv_amount_d --当日买卖价差金额
                 ,sum(case when  map_code like '5705%' and to_char(gl_date,'yyyymm')=to_char('$TXDATE'::date,'yyyymm')
                           then amount else 0 end )                  as inv_amount_m --当月买卖价差金额
                 ,sum(case when  map_code like '5705%' 
                           then amount else 0 end )                  as inv_amount_t --累计买卖价差金额
                 --手续费科目:5325%
                 ,max(case when  map_code like '5325%'                             
                           then  map_code end )  as fee_acc   --手续费科目            
                 ,sum(case when  map_code like '5325%' and gl_date='$TXDATE'::date
                           then amount else 0 end )                  as fee_amount_d --当日手续费金额          
                 ,sum(case when  map_code like '5325%' and to_char(gl_date,'yyyymm')=to_char('$TXDATE'::date,'yyyymm')
                           then amount else 0 end )                  as fee_amount_m --当月手续费金额          
                 ,sum(case when  map_code like '5325%' 
                           then amount else 0 end )                  as fee_amount_t --累计手续费金�
      from      dw_sdata.cos_000_qta_gl_accounting --账务信息主表，存放交易会�
     where     ret_code='000000'  --会计处理平台处理成功
       and     gl_date<='$TXDATE'::date
    group by   deal_no
;
/*临时表创建区END*/
/*数据回退区*/
DELETE/* +direct */ from f_fdm.f_Cap_Bond_Inves
where etl_date='$TXDATE'::date;
/*数据回退区END*/
/*数据处理区*/
INSERT INTO f_fdm.f_Cap_Bond_Inves
(
        grp_typ                         --组别
        ,etl_date                       --数据日期
        ,agmt_id                        --协议编号
        ,org_num                        --机构号
        ,cur_cd                         --货币代码
        ,bond_cd                        --债券代码
        ,tx_tool_cls                    --交易工具分类
        ,tx_comb_cd                     --交易组合代码
        ,tx_cnt_pty_cust_num            --交易对手客户号
        ,prod_cd                        --产品代码
        ,bond_typ_cd                    --债券类型代码
        ,bond_issur                     --债券发行人
        ,Bond_Issu_Dt                   --债券发行日期
        ,st_int_dt                      --起息日
        ,due_dt                         --到期日
        ,int_base_cd                    --计息基础代码
        ,cmpd_int_calc_mode_cd          --复利计算方式代码
        ,int_pay_freq_cd                --付息频率代码
        ,int_rate_attr_cd               --利率属性代码
        ,orgnl_term                     --原始期限
        ,orgnl_term_corp_cd             --原始期限单位代码
        ,rprc_prd                       --重定价周期
        ,rprc_prd_corp_cd               --重定价周期单位代码
        ,last_rprc_day                  --上次重定价日
        ,next_rprc_day                  --下次重定价日
        ,curr_int_rate                  --当前利率
        ,bmk_int_rate                   --基准利率
        ,basis                          --基差
        ,prin_subj                      --本金科目
        ,buy_cost                       --购入成本
        ,book_bal                       --账面余额
        ,mkt_val                        --市场价值
        ,deval_prep_bal                 --减值准备余额
        ,int_subj                       --利息科目
        ,today_provs_int                --当日计提利息
        ,curmth_provs_int               --当月计提利息
        ,accm_provs_int                 --累计计提利息
        ,today_chrg_int                 --当日收息
        ,curmth_recvd_int               --当月已收息
        ,accm_recvd_int                 --累计已收息
        ,int_adj_subj                   --利息调整科目
        ,today_int_adj_amt              --当日利息调整金额
        ,curmth_int_adj_amt             --当月利息调整金额
        ,accm_int_adj_amt               --累计利息调整金额
        ,valtn_prft_loss_subj           --估值损益科目
        ,today_valtn_prft_loss_amt      --当日估值损益金额
        ,curmth_valtn_prft_loss_amt     --当月估值损益金额
        ,accm_valtn_prft_loss_amt       --累计估值损益金额
        ,biz_prc_diff_prft_subj         --买卖价差收益科目
        ,today_biz_prc_diff_amt         --当日买卖价差金额
        ,curmth_biz_prc_diff_amt        --当月买卖价差金额
        ,accm_biz_prc_diff_amt          --累计买卖价差金额
        ,comm_fee_subj                  --手续费科目
        ,today_comm_fee_amt             --当日手续费金额
        ,curmth_comm_fee_amt            --当月手续费金额
        ,accm_comm_fee_amt              --累计手续费金额
        ,mth_accm                       --月积数
        ,yr_accm                        --年积数
        ,mth_day_avg_bal                --月日均余额
        ,yr_day_avg_bal                 --年日均余额
        ,sys_src                        --系统来源
)
SELECT
        '1'                             AS grp_typ
        ,'$TXDATE'::DATE       AS data_dt
        ,T.DEAL_NO                      AS agmt_id
        ,coalesce(T_org_2.name,'')                     AS org_num
        ,coalesce(T_CD05.TGT_CD,'@'||T.ccy)                          AS cur_cd
        ,coalesce(T3.NAME,'')                        AS bond_cd
        ,(case
                when T6.prin_acc like '1430%001%' then '1' --交易性债券
                when T6.prin_acc like '1510%001%' then '2' --持有至到期债券
                when T6.prin_acc like '1525%001%' then '3' --可供出售债券
                when T6.prin_acc like '1505%001%' then '4' --应收款项类投资
                when T6.prin_acc like '1540%001%' then '5' --其他可供出售金融资产
                when T6.prin_acc like '1515%001%' then '6' --其他持有至到期投资-同业存单
                when T6.prin_acc like '1436%001%' then '7' --其他交易性金融资产-同业存单
                when T6.prin_acc like '1106%001%' then '8' --转贴现票据
                when T6.prin_acc like '1125%' then '9' --银团贷款
                else ''
         end
         )                                                                              AS tx_tool_cls
        ,T.entity                                                                       AS tx_comb_cd
        ,coalesce(T5.ECIF_CUST_NO,'@'||t.cparty)                                                                AS tx_cnt_pty_cust_num
        ,T.sectype                                                                      AS prod_cd
        ,coalesce(T3.owner,'')                                                                       AS bond_typ_cd
        ,coalesce(T4.NAME,'')                                                                        AS bond_issur
        ,coalesce(to_date(T3.issue_dt,'YYYYMMDD'),'$MINDATE')                                                AS Bond_Issu_Dt
        ,to_date(T.settle_dt,'YYYYMMDD')                                                AS st_int_dt
        ,to_date(T.cur_mat_dt,'YYYYMMDD')                                               AS due_dt
        ,coalesce(T_CD15.TGT_CD,'@'||T1.INT_DAYS)                                                                    AS int_base_cd
        ,'1'                                                                            AS cmpd_int_calc_mode_cd       --单利
        ,(case
                when T1.pay_frq<>'' then coalesce(T_CD17.TGT_CD,'@'||T1.pay_frq)
                else '2' --按季
        end
        )                                                                               AS int_pay_freq_cd
        ,(case
                when T1.fix_float='FIXED' THEN '1'
                WHEN T1.FIX_FLOAT='FLOATING' AND T1.review_frq <>'' THEN '3'  --按定期浮动利率
                else '4'  --按不定期浮动利率
        end
        )                                                                                AS int_rate_attr_cd
        ,to_date(T.mature_dt,'YYYYMMDD') - to_date(T.settle_dt,'YYYYMMDD')               AS orgnl_term
        ,'D'                                                                             AS orgnl_term_corp_cd
        ,(case
                when T1.FIX_FLOAT='FLOATING'
                    THEN (case --转换为重定价单位周期对应的数值，
                                when T1.review_frq in ('DAILY','MONTHLY','ANNUAL') then 1
                                when T1.review_frq='SEMI ANNUAL' then 6
                                when T1.review_frq='QUARTERLY' then 3
                                when T1.review_frq='WEEKLY' then 7
                                else 0
                    END
                    )
                ElSE 0
        end
        )       AS rprc_prd
        ,(case
                when T1.FIX_FLOAT='FLOATING'
                    THEN (case
                                when T1.review_frq in ('DAILY','WEEKLY') then 'D'
                                when T1.review_frq in ('MONTHLY','QUARTERLY','SEMI ANNUAL') then 'M'
                                when T1.review_frq='ANNUAL'  then 'Y'
                                else ''
                         end
                         )
                else ''
        end
        )       AS rprc_prd_corp_cd
        ,to_date(T1.review_dt,'YYYYMMDD')   AS last_rprc_day
        /*,case
                when T1.review_frq = 'ANNUAL'       then (CASE
                                                                WHEN ADD_MONTHS(to_date(T1.review_dt,'YYYYMMDD')+ceil(datediff(MM,to_date(T1.review_dt,'YYYYMMDD','$TXDATE'::DATE))/12))>'$TXDATE'::DATE
                                                                    THEN ADD_MONTH(to_date(T1.review_dt,'YYYYMMDD'),ceil(datediff(MM,to_date(T1.review_dt,'YYYYMMDD','$TXDATE'::DATE))/12))
                                                                ELSE ADD_MONTHS(to_date(T1.review_dt,'YYYYMMDD'),ceil(datediff(MM,to_date(T1.review_dt,'YYYYMMDD','$TXDATE'::DATE))/12)+1)
                                                         END
                                                         )
                when T1.review_frq = 'SEMI ANNUAL'  then (CASE
                                                                WHEN ADD_MONTHS(to_date(T1.review_dt,'YYYYMMDD')+ceil(datediff(MM,to_date(T1.review_dt,'YYYYMMDD','$TXDATE'::DATE))/6))>'$TXDATE'::DATE
                                                                    THEN ADD_MONTH(to_date(T1.review_dt,'YYYYMMDD'),ceil(datediff(MM,to_date(T1.review_dt,'YYYYMMDD','$TXDATE'::DATE))/6))
                                                                ELSE ADD_MONTHS(to_date(T1.review_dt,'YYYYMMDD'),ceil(datediff(MM,to_date(T1.review_dt,'YYYYMMDD','$TXDATE'::DATE))/6)+1)
                                                         END
                                                         )
                when T1.review_frq = 'QUARTERLY'    then (CASE
                                                                WHEN ADD_MONTHS(to_date(T1.review_dt,'YYYYMMDD')+ceil(datediff(MM,to_date(T1.review_dt,'YYYYMMDD','$TXDATE'::DATE))/3))>'$TXDATE'::DATE
                                                                    THEN ADD_MONTH(to_date(T1.review_dt,'YYYYMMDD'),ceil(datediff(MM,to_date(T1.review_dt,'YYYYMMDD','$TXDATE'::DATE))/3))
                                                                ELSE ADD_MONTHS(to_date(T1.review_dt,'YYYYMMDD'),ceil(datediff(MM,to_date(T1.review_dt,'YYYYMMDD','$TXDATE'::DATE))/3)+1)
                                                         END
                                                         )
                when T1.review_frq = 'MONTHLY'      then (CASE
                                                                WHEN ADD_MONTHS(to_date(T1.review_dt,'YYYYMMDD')+ceil(datediff(MM,to_date(T1.review_dt,'YYYYMMDD','$TXDATE'::DATE))/1))>'$TXDATE'::DATE
                                                                    THEN ADD_MONTH(to_date(T1.review_dt,'YYYYMMDD'),ceil(datediff(MM,to_date(T1.review_dt,'YYYYMMDD','$TXDATE'::DATE))/1))
                                                                ELSE ADD_MONTHS(to_date(T1.review_dt,'YYYYMMDD'),ceil(datediff(MM,to_date(T1.review_dt,'YYYYMMDD','$TXDATE'::DATE))/1)+1)
                                                         END
                                                         )
                when T1.review_frq = 'WEEKLY'       then (CASE
                                                                WHEN to_date(T1.review_dt,'YYYYMMDD')+ceil((to_date(T1.review_dt,'YYYYMMDD'-'$TXDATE'::DATE)/7))*7>'$TXDATE'::DATE
                                                                    THEN to_date(T1.review_dt,'YYYYMMDD')+ceil((to_date(T1.review_dt,'YYYYMMDD'-'$TXDATE'::DATE)/7))*7
                                                                ELSE to_date(T1.review_dt,'YYYYMMDD')+ceil((to_date(T1.review_dt,'YYYYMMDD'-'$TXDATE'::DATE)/7)+1)*7
                                                         END
                                                         )
                when T1.review_frq = 'DAILY'        then '$TXDATE'+1
                ELSE '$TXDATE'::DATE + 1
        END
        )*/
        ,(case
                when T1.fix_float='FIXED' and to_date(T.cur_mat_dt,'YYYYMMDD') > '$TXDATE'::DATE
                    then to_date(T.cur_mat_dt,'YYYYMMDD')
                when T3.fix_float='FLOATING' and T1.review_frq <> '' and to_date(T.cur_mat_dt,'YYYYMMDD') > '$TXDATE'::DATE
                    then (case
                                when T1.review_frq = 'ANNUAL'       then (CASE
                                                                                WHEN ADD_MONTHS(to_date(T1.review_dt,'YYYYMMDD'),(ceil(datediff(MM,to_date(T1.review_dt,'YYYYMMDD'),'$TXDATE'::DATE)/12))::INTEGER)>'$TXDATE'::DATE
                                                                                    THEN ADD_MONTHS(to_date(T1.review_dt,'YYYYMMDD'),(ceil(datediff(MM,to_date(T1.review_dt,'YYYYMMDD'),'$TXDATE'::DATE)/12))::INTEGER)
                                                                                ELSE ADD_MONTHS(to_date(T1.review_dt,'YYYYMMDD'),(ceil(datediff(MM,to_date(T1.review_dt,'YYYYMMDD'),'$TXDATE'::DATE)/12)+1)::INTEGER)
                                                                         END
                                                                         )
                                when T1.review_frq = 'SEMI ANNUAL'  then (CASE
                                                                                WHEN ADD_MONTHS(to_date(T1.review_dt,'YYYYMMDD'),(ceil(datediff(MM,to_date(T1.review_dt,'YYYYMMDD'),'$TXDATE'::DATE)/6))::INTEGER)>'$TXDATE'::DATE
                                                                                    THEN ADD_MONTHS(to_date(T1.review_dt,'YYYYMMDD'),(ceil(datediff(MM,to_date(T1.review_dt,'YYYYMMDD'),'$TXDATE'::DATE)/6))::INTEGER)
                                                                                ELSE ADD_MONTHS(to_date(T1.review_dt,'YYYYMMDD'),(ceil(datediff(MM,to_date(T1.review_dt,'YYYYMMDD'),'$TXDATE'::DATE)/6)+1)::INTEGER)
                                                                         END
                                                                         )
                                when T1.review_frq = 'QUARTERLY'    then (CASE
                                                                                WHEN ADD_MONTHS(to_date(T1.review_dt,'YYYYMMDD'),(ceil(datediff(MM,to_date(T1.review_dt,'YYYYMMDD'),'$TXDATE'::DATE)/3))::INTEGER)>'$TXDATE'::DATE
                                                                                    THEN ADD_MONTHS(to_date(T1.review_dt,'YYYYMMDD'),(ceil(datediff(MM,to_date(T1.review_dt,'YYYYMMDD'),'$TXDATE'::DATE)/3))::INTEGER)
                                                                                ELSE ADD_MONTHS(to_date(T1.review_dt,'YYYYMMDD'),(ceil(datediff(MM,to_date(T1.review_dt,'YYYYMMDD'),'$TXDATE'::DATE)/3)+1)::INTEGER)
                                                                         END
                                                                         )
                                when T1.review_frq = 'MONTHLY'      then (CASE
                                                                                WHEN ADD_MONTHS(to_date(T1.review_dt,'YYYYMMDD'),(ceil(datediff(MM,to_date(T1.review_dt,'YYYYMMDD'),'$TXDATE'::DATE)/1))::INTEGER)>'$TXDATE'::DATE
                                                                                    THEN ADD_MONTHS(to_date(T1.review_dt,'YYYYMMDD'),(ceil(datediff(MM,to_date(T1.review_dt,'YYYYMMDD'),'$TXDATE'::DATE)/1))::INTEGER)
                                                                                ELSE ADD_MONTHS(to_date(T1.review_dt,'YYYYMMDD'),(ceil(datediff(MM,to_date(T1.review_dt,'YYYYMMDD'),'$TXDATE'::DATE)/1)+1)::INTEGER)
                                                                         END
                                                                         )
                                when T1.review_frq = 'WEEKLY'       then (CASE
                                                                                WHEN to_date(T1.review_dt,'YYYYMMDD')+(ceil((to_date(T1.review_dt,'YYYYMMDD')-'$TXDATE'::DATE)/7)*7)::INTEGER > '$TXDATE'::DATE
                                                                                    THEN to_date(T1.review_dt,'YYYYMMDD')+(ceil((to_date(T1.review_dt,'YYYYMMDD')-'$TXDATE'::DATE)/7)*7)::INTEGER
                                                                                ELSE to_date(T1.review_dt,'YYYYMMDD')+((ceil((to_date(T1.review_dt,'YYYYMMDD')-'$TXDATE'::DATE)/7)+1)*7)::INTEGER
                                                                         END
                                                                         )
                                when T1.review_frq = 'DAILY'        then '$TXDATE'::DATE + 1
                                ELSE '$TXDATE'::DATE + 1
                        END
                        )
                else  '$TXDATE'::DATE + 1
        end
        )   AS next_rprc_day
        ,T1.int_rate                                                                    AS curr_int_rate
        ,0                                                                              AS bmk_int_rate
        ,0                                                                              AS basis
        ,coalesce(T6.prin_acc,'')                                                                    AS prin_subj
        ,coalesce(abs(T6.prin_amount),0)                                                            AS buy_cost
        ,coalesce(abs(coalesce(T6.prin_amount) + coalesce(T6.fair_val_chgs_amount_t) + coalesce(T6.intr_dis_amount_t)),0)         AS book_bal --所有字段取coalesce 20160913
        ,coalesce(abs(coalesce(T6.prin_amount) + coalesce(T6.fair_val_chgs_amount_t)),0)                                AS mkt_val
        ,0                                                                              AS deval_prep_bal
        ,coalesce(T6.accr_acc,'')                                                                    AS int_subj
        ,coalesce(abs(T6.accr_amount_d),0)                                                          AS today_provs_int
        ,coalesce(abs(T6.accr_amount_m),0)                                                          AS curmth_provs_int
        ,coalesce(abs(T6.accr_amount_t),0)                                                          AS accm_provs_int
        ,coalesce(abs(T6.intr_amount_d),0)                                                          AS today_chrg_int
        ,coalesce(abs(T6.intr_amount_m),0)                                                          AS curmth_recvd_int
        ,coalesce(abs(T6.accr_amount_t),0)                                                          AS accm_recvd_int
        ,coalesce(T6.intr_dis_acc,'')                                                                AS int_adj_subj
        ,coalesce(abs(T6.intr_dis_amount_d),0)                                                      AS today_int_adj_amt
        ,coalesce(abs(T6.intr_dis_amount_m),0)                                                      AS curmth_int_adj_amt
        ,coalesce(abs(T6.intr_dis_amount_t),0)                                                      AS accm_int_adj_amt
        ,coalesce(T6.fair_val_chgs_acc,'')                                                           AS valtn_prft_loss_subj
        ,coalesce(abs(T6.fair_val_chgs_amount_d),0)                                                 AS today_valtn_prft_loss_amt
        ,coalesce(abs(T6.fair_val_chgs_amount_m),0)                                                 AS curmth_valtn_prft_loss_amt
        ,coalesce(abs(T6.fair_val_chgs_amount_t),0)                                                 AS accm_valtn_prft_loss_amt
        ,coalesce(T6.inv_acc,'')                                                                     AS biz_prc_diff_prft_subj
        ,coalesce(abs(T6.inv_amount_d),0)                                                           AS today_biz_prc_diff_amt
        ,coalesce(abs(T6.inv_amount_m),0)                                                           AS curmth_biz_prc_diff_amt
        ,coalesce(abs(T6.inv_amount_t),0)                                                           AS accm_biz_prc_diff_amt
        ,coalesce(T6.fee_acc,'')                                                                     AS comm_fee_subj
        ,coalesce(abs(T6.fee_amount_d),0)                                                           AS today_comm_fee_amt
        ,coalesce(abs(T6.fee_amount_m),0)                                                           AS curmth_comm_fee_amt
        ,coalesce(abs(T6.fee_amount_t),0)                                                           AS accm_comm_fee_amt
        ,0.00                                                                           AS mth_accm
        ,0.00                                                                           AS yr_accm
        ,0.00                                                                           AS mth_day_avg_bal
        ,0.00                                                                           AS yr_day_avg_bal
        ,'COS'                                                                          AS sys_src
FROM    dw_sdata.cos_000_deals T --交易信息主表
INNER JOIN  dw_sdata.cos_000_mmdeals T1--货币市场交易表--交易信息主表
ON      T1.DEAL_NO=T.DEAL_NO
and     T1.start_dt <= '$TXDATE'::DATE
and     T1.end_dt > '$TXDATE'::DATE
INNER JOIN  f_fdm.cd_cd_table T2--代码表（财务数据集市基础层）
ON      T.sectype = T2.Cd
and     T2.Cd_Typ_Encd='FDM086' --资金债券投资产品代码
LEFT JOIN   dw_sdata.cos_000_sectype T3 --产品表
ON      T.sectype=T3.THEKEY
AND     T3.start_dt <= '$TXDATE'::DATE
and     T3.end_dt > '$TXDATE'::DATE
LEFT JOIN   dw_sdata.cos_000_cparty T4 --交易对手
ON      T3.acc_iss = T4.thekey
AND     T4.start_dt <= '$TXDATE'::DATE
and     T4.end_dt > '$TXDATE'::DATE
LEFT JOIN   dw_sdata.ecf_002_t01_cust_info_T T5 --同业客户基本信息表
ON      T.cparty = T5.trans_emt_no--(交易对手编号)
AND     T5.start_dt <= '$TXDATE'::DATE
AND     T5.end_dt > '$TXDATE'::DATE
LEFT JOIN   f_Cap_Bond_Inves_tmp_T6 T6
ON      T.DEAL_NO=T6.DEAL_NO
LEFT JOIN f_fdm.CD_RF_STD_CD_TRAN_REF T_CD05
ON      T.ccy = T_CD05.SRC_CD                       --源代码值相同
AND     T_CD05.DATA_PLTF_SRC_TAB_NM = upper('cos_000_deals') --数据平台源表主干名
AND     T_CD05.Data_Pltf_Src_Fld_Nm =upper('ccy')
LEFT JOIN f_fdm.CD_RF_STD_CD_TRAN_REF T_CD15
ON      T1.INT_DAYS = T_CD15.SRC_CD                       --源代码值相同
AND     T_CD15.DATA_PLTF_SRC_TAB_NM = upper('cos_000_mmdeals') --数据平台源表主干名
AND     T_CD15.Data_Pltf_Src_Fld_Nm =upper('INT_DAYS')
LEFT JOIN f_fdm.CD_RF_STD_CD_TRAN_REF T_CD17
ON      T1.pay_frq = T_CD17.SRC_CD                       --源代码值相同
AND     T_CD17.DATA_PLTF_SRC_TAB_NM =upper('cos_000_mmdeals') --数据平台源表主干名
AND     T_CD17.Data_Pltf_Src_Fld_Nm =upper('pay_frq')
LEFT JOIN f_fdm.CD_RF_STD_CD_TRAN_REF T_CD17_1
ON      '2' = T_CD17_1.SRC_CD                       --源代码值相同
AND     T_CD17_1.DATA_PLTF_SRC_TAB_NM = upper('cos_000_mmdeals') --数据平台源表主干名
AND     T_CD17_1.Data_Pltf_Src_Fld_Nm =upper('pay_frq')
left join dw_sdata.cos_000_bustruct T_org_1
on t.entity = T_org_1.thekey
and T_org_1.start_dt <= '$txdate'::date
and T_org_1.end_dt > '$txdate'::date
left join dw_sdata.cos_000_anacode T_org_2
on T_org_1.analyse04 = T_org_2.thekey 
and T_org_1.start_dt <= '$txdate'::date
and T_org_1.end_dt > '$txdate'::date                  --modified 20160912
where   T.start_dt <= '$TXDATE'::DATE
and     T.end_dt > '$TXDATE'::DATE  ----modified by zmx
;
/*月积数、年积数、月日均余额、年日均余额临时表创建区*/
create local temporary table if not exists f_Cap_Raw_TX_tmp_yjs
on commit preserve rows as
select
      t.agmt_id
      ,(case
            when '$TXDATE'= '$MONTHBGNDAY'
            then t.book_bal
            else t.book_bal+coalesce(t1.mth_accm,0)
            end
       )                                                                      as mth_accm  --月积数
      ,(case
            when  '$TXDATE' = '$YEARBGNDAY'
            then t.book_bal
            else t.book_bal+coalesce(t1.yr_accm,0)
            end
       )                                                                      as yr_accm   --年积数
      ,(case
            when '$TXDATE' = '$MONTHBGNDAY'
            then t.book_bal
            else t.book_bal+coalesce(t1.mth_accm,0)
            end
       )/('$TXDATE'::date-'$MONTHBGNDAY'::date+1)               as mth_day_avg_bal  --月日均余额
      ,(case
           when '$TXDATE' = '$YEARBGNDAY'
           then t.book_bal
           else t.book_bal+coalesce(t1.yr_accm,0)
           end
       )/('$TXDATE'::date-'$YEARBGNDAY'::date+1)                    as Yr_Day_Avg_Bal   --年日均余额
from  f_fdm.f_cap_bond_inves     t
left join f_fdm.f_cap_bond_inves t1
on         t.agmt_id= t1.agmt_id
and  t1.etl_date='$TXDATE'::date -1
where     t.etl_date='$TXDATE'::date
;
/*月积数、年积数、月日均余额、年日均余额临时表创建区END*/
/*更新月积数、年积数、月日均余额、年日均余额*/
update f_fdm.f_cap_bond_inves   t
set mth_accm=t1.mth_accm
,yr_accm=t1.yr_accm
,mth_day_avg_bal=t1.mth_day_avg_bal
,Yr_Day_Avg_Bal=t1.Yr_Day_Avg_Bal
from  f_Cap_Raw_TX_tmp_yjs    t1
where t.agmt_id= t1.agmt_id
and   t.etl_date='$TXDATE'::date
;
/*数据处理区END*/
COMMIT;
