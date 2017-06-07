/*
Author             :zhangwj
Function           :信用卡账户信息表
Load method        :
Source table       :dw_sdata.ccb_000_event,dw_sdata.ccb_000_acct,dw_sdata.ccb_000_card,dw_sdata.ecf_001_t01_cust_info,dw_sdata.ecf_004_t01_cust_info,f_fdm.f_fnc_exchg_rate,dw_sdata.ccb_000_stmt,dw_sdata.ccb_000_prmau,dw_sdata.ccb_000_accx,dw_sdata.ccb_000_stmx   
Destination Table  :f_fdm.f_acct_crdt_info
Frequency          :D
Modify history list:Created by zhangwj at 2016-4-21 10:22 v1.0
                    Changed by zhangwj at 2016-4-26 16:22 v1.1
                    Changed by zhangwj at 2016-5-25 10:12 v1.2   大数据贴源层表名修改，表为拉链表或流水表，与之保持一致
                    modified by liuxz 20160623 1.新增主键“货币代码”
                                               2.修改T2表个人基本信息表
                                               3.修改字段“计息基础代码”，“复利计算方式代码”
                                               4.添加第2组映射规则：外币贷记帐户    (变更记录86)
                    modified by liuxz 20160629 修改字段“信用额度”、“当前余额”、“溢缴款余额”映射规则 (变更记录88)
                                               添加字段“逾期状态”及映射规则   (变更记录90)
                    modified by liudongyan 20160704 添加月积数，年积数等映射规则
                    modified by liudongyan 20160704 修改字段当月已收息的取数规则（变更记录108）
                    modified by liuxz 20160714 ccb_000_event改为dw_sdata.ccb_000_event
                    modified by liudongyan 20160715删除第1组T5表的“AND  T.etl_dt=ETL加载日期 ” 和第2组T7表“ AND  T.etl_dt=ETL加载日期 ”（见变更记录119）
                    modified by zhangliang 20160824 在第1,2组新增：核销标志，核销日期，核销本金，核销手续费，核销滞纳金，核销表里利息，核销表外利息等字段以及映射规则，变更166,168
                    modified by zhangliang 20160830 修改组1关联表t2及关联条件，修改组2关联表t1及关联条件
                    modified by zhangliang 20160901 修改组1，组2关联表t3
                    modified by zhangliang 20160908 删除组2关联表t5,修改第二组字段“还款日”的映射规则
                    modified by zhangliang 20160908 新增字段：逾期日期， 转透支日期， 剩余逾期本金， 剩余逾期利息以及相应映射规则组一新增关联表t7,组二新增关联表t8;变更记录152
                    modified by zmx 20160922 修改分期付款额度 null为0，修改信用额度casewhen中写死的日期该为当前日期
                    modified by zhangliang20160927 修改关联表t7,组1，组2
                    modify by duhy 20161001 添加临时表 
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/

/*临时表创建区*/
create local temporary table tt_f_acct_crdt_info_temp_1                   --帐务交易流水表
on commit preserve rows as
SELECT  
 T.ACCTNBR ,SUM(CASE WHEN T.TRANS_TYPE IN ('5000','8010','8012','8136') THEN
 CASE 
    WHEN TRIM(T.BILL_AMTFLAG)='-' THEN ABS(T.BILL_AMT)*-1
 ELSE ABS(T.BILL_AMT)
 END
   ELSE 0
   END) AS LXSUM 
 FROM dw_sdata.CCB_000_EVENT T
 WHERE  SUBSTR(TO_CHAR(T.INP_DATE),1,6) = substr('$TXDATE',1,6)  --此处限制当月“数据日期”
 GROUP BY T.ACCTNBR
;
create local temporary table tt_f_acct_crdt_info_temp_3                   --帐务交易流水表
on commit preserve rows as
 select T.ACCTNBR ,sum(case 
                            when T.TRANS_TYPE in ('5000','8010','8012','8136')
                            then case 
                                      when trim(T.BILL_AMTFLAG)='-' then ABS(T.BILL_AMT)*-1 
                                 else ABS(T.BILL_AMT) 
                                 end
                              else 0
                              end
                               ) as LXSUM 
     from dw_sdata.CCB_000_EVENT T 
     where  SUBSTR(TO_CHAR(T.INP_DATE),1,6)=substr( '$TXDATE',1,6)  --此处限制当月“数据日期”
     group by T.ACCTNBR  
;
--月积数等临时表--
create local temporary table tt_f_acct_crdt_info_temp_yjs  --月积数，年积数，月日均余额，年日均余额临时表
on commit preserve rows as 
select * 
from f_fdm.f_acct_crdt_info
where 1=2;  

create local temp table tmp_t3
 on commit preserve rows as 
   select d.* 
     from (SELECT ECIF_CUST_NO,
                  CERT_NO,
                  PARTY_NAME,
                  IS_VIP_FLAG,
                  start_dt,
                  end_dt,
                  row_number() over(partition by CERT_NO order by IS_VIP_FLAG desc) as num2
             FROM (select * from (select ECIF_CUST_NO,
                                         CERT_NO,
                                         PARTY_NAME,
                                         IS_VIP_FLAG,
                                         start_dt,
                                         end_dt,
                                         row_number() over(partition by CERT_NO order by cert_due_date desc) as num
                                    from dw_sdata.ecf_001_t01_cust_info
                                   where UPDATED_TS = '99991231 00:00:00:000000'
                                     and start_dt<='$TXDATE'::date
                                     and '$TXDATE'::date<end_dt ) a
                               where a.num=1
                   union all
                  select * from (select ECIF_CUST_NO,
                                        CERT_NO,
                                        PARTY_NAME,
                                        IS_VIP_FLAG,
                                        start_dt,
                                        end_dt,
                                        row_number() over(partition by CERT_NO order by cert_due_date desc) as num
                                   from dw_sdata.ecf_004_t01_cust_info
                                  where UPDATED_TS = '99991231 00:00:00:000000'
                                    and start_dt<='$TXDATE'::date
                                    and '$TXDATE'::date<end_dt) b
                                where b.num=1
                   ) c
             )d 

       where d.num2=1
        AND d.CERT_NO||PARTY_NAME IN
    (select CUSTR_NBR||SUBSTR(ACC_NAME1,0,INSTR(ACC_NAME1,' ')) AS NEW_CUST_NAME  from   dw_sdata.ccb_000_acct  ) 
       order by CERT_NO 
       SEGMENTED BY hash(CERT_NO) ALL NODES KSAFE 1; 
;


/*临时表创建区END*/

/*数据回退区*/
delete /* +direct */ from f_fdm.f_acct_crdt_info
where etl_date = '$TXDATE'::date
;
/*数据回退区END*/

/*数据处理区*/
insert /* +direct */  into f_fdm.f_acct_crdt_info
     (grp_typ                                                                 --组别
     ,etl_date                                                                --数据日期
     ,agmt_id                                                                 --协议编号
     ,card_num                                                                --卡号
     ,cust_id                                                                 --客户编号
     ,org_num                                                                 --机构号
     ,cur_cd                                                                  --货币代码
     ,open_acct_org_num                                                       --开户机构号
     ,prod_cd                                                                 --产品代码
     ,stmt_day                                                                --账单日
     ,open_acct_dt                                                            --开户日
     ,crdt_lmt                                                                --信用额度
     ,repay_day                                                               --还款日
     ,repay_acct_num_1                                                        --还款账号1
     ,repay_acct_num_2                                                        --还款账号2
     ,repay_acct_num_3                                                        --还款账号3
     ,repay_acct_num_4                                                        --还款账号4
     ,pre_brw_cash_ratio                                                      --预借现金比率
     ,amtbl_pay_lmt                                                           --分期付款额度
     ,curr_int_rate                                                           --当前利率
     ,int_base_cd                                                             --计息基础代码
     ,cmpd_int_calc_mode_cd                                                   --复利计算方式代码
     ,acct_stat_cd                                                            --账户状态
     ,prin_subj_od                                                            --本金科目—透支
     ,prin_subj_dpst                                                          --本金科目—存款
     ,int_subj                                                                --利息科目
     ,curr_bal                                                                --当前余额
     ,Ovrd_Stat                                                               --逾期状态
     ,ovrd_dt	          -- 逾期日期    
     ,tranfm_od_dt	  -- 转透支日期    
     ,remn_ovrd_prin      -- 剩余逾期本金  
     ,remn_ovrd_int	  -- 剩余逾期利息
     ,spl_pay_bal                                                             --溢缴款余额
     ,wrtoff_ind                       --核销标志
     ,wrtoff_dt                        --核销日期
     ,wrtoff_prin                      --核销本金
     ,wrtoff_comm_fee                  --核销手续费
     ,wrtoff_late_chrg                 --核销滞纳金
     ,wrtoff_in_bal_int                --核销表内利息
     ,wrtoff_off_bal_int               --核销表外利息
     ,today_provs_int                                                         --当日计提利息
     ,curmth_provs_int                                                        --当月计提利息
     ,accm_provs_int                                                          --累计计提利息
     ,today_chrg_int                                                          --当日收息
     ,curmth_recvd_int                                                        --当月已收息
     ,accm_recvd_int                                                          --累计已收息
     ,int_adj_amt                                                             --月积数
     ,mth_accm                                                                --年积数
     ,yr_accm                                                                 --利息调整金额
     ,loan_deval_prep_bal                                                     --贷款减值准备余额
     ,loan_deval_prep_amt                                                     --贷款减值准备发生额
     ,mth_day_avg_bal                                                         --月日均余额
     ,yr_day_avg_bal                                                          --年日均余额
     ,sys_src                                                                 --系统来源
     )
 select
      1                                                                       as  grp_typ            --组别
      ,'$TXDATE'::date                                               as  etl_date           --数据日期
      ,coalesce(t1.xaccount,0)                                                as  agmt_id            --协议编号
      ,coalesce(trim(t2.master_nbr),'')                                       as  card_num           --映射中未给出       --卡号
      ,coalesce(t3.ecif_cust_no,'')                                           as  cust_id            --客户编号
      ,'1199523Q'                                                             as  org_num            --机构号
      ,coalesce(t1.curr_num,0 )                                               as  cur_cd             --货币代码
      ,'1199523Q'                                                             as  open_acct_org_num  --开户机构号
      ,coalesce(t1.prod_nbr,0)                                                as  prod_cd            --产品代码
      ,coalesce(t1.cycle_nbr,0)                                               as  stmt_day           --账单日
      ,coalesce(to_date(to_char(day_opened),'yyyymmdd'),'$MAXDATE'::date) as  open_acct_dt       --开户日 
      ,(case 
             when coalesce(T1.TEMP_LIMIT,0)<>0 then (case  
                                             when '$TXDATE'>=coalesce(T1.TLMT_BEG,0)::varchar::date and '$TXDATE'<coalesce(T1.TLMT_END,0)::varchar::date AND coalesce(T1.TEMP_LIMIT,0)>coalesce(T1.CRED_LIMIT,0) then coalesce(T1.TEMP_LIMIT,0)
                                        else coalesce(T1.CRED_LIMIT,0)
                                        end
                                       )
             else coalesce(T1.CRED_LIMIT,0)
        end  
       )                                                                      as  crdt_lmt           --信用额度  
    ,to_date(to_char(t4.dayspay),'yyyymmdd')                                  as  repay_day          --空文件，暂时无法验证       --还款日
      ,coalesce(t1.bankacct1,'')                                              as  repay_acct_num_1   --还款账号1
      ,coalesce(t1.bankacct2,'')                                              as  repay_acct_num_2   --还款账号2
      ,coalesce(t1.bankacct3,'')                                              as  repay_acct_num_3   --还款账号3
      ,coalesce(t1.bankacct4,'')                                              as  repay_acct_num_4   --还款账号4
      ,coalesce(t5.ca_lmtprct,0 )                                             as  pre_brw_cash_ratio --预借现金比率
      ,0                                                      as  amtbl_pay_lmt      --映射中未给出       --分期付款额度
      ,coalesce(t1.int_rate,0 )                                               as  curr_int_rate      --当前利率
      ,'6'                                                                    as  int_base_cd        --计息基础代码
      ,'2'                                                                    as  cmpd_int_calc_mode_cd --复利计算方式代码
      ,coalesce( t1.close_code ,'')                                           as  acct_stat_cd       --账户状态
      ,'11352000101R'                                                         as  prin_subj_od       --本金科目—透支
      ,'22752000101R'                                                         as  prin_subj_dpst     --本金科目—存款
      ,'51053100401R'                                                         as  int_subj           --利息科目
      ,t1.STM_BALFRE  --帐单消费余额
       +(CASE 
             WHEN t1.STMBALINTFLAG <> '-'     --帐单日记息余额符号
             THEN abs(t1.STM_BALINT) 
         ELSE 0 
         END
         )    --帐单日记息余额
       +(CASE 
             WHEN t1.STM_BMFLAG = '-' --分期付款已出帐单余额符号
             THEN (-1)*abs(t1.STM_BALMP)    --分期付款已出帐单余额
         ELSE abs(t1.STM_BALMP) 
         END
         )
       +t1.BAL_FREE    --消费余额（未出账单组成）
       +(CASE 
             WHEN t1.BAL_INTFLAG <> '-'   --日记息余额符号
             THEN abs(t1.BAL_INT) 
        ELSE 0 
        END
        )    --日记息余额（未出账单组成）
       +(CASE 
             WHEN t1.BAL_MPFLAG = '-'     --分期付款未出帐单余额符号
             THEN (-1)*abs(t1.BAL_MP)    --分期付款未出帐单余额
         ELSE abs(t1.BAL_MP) 
         END
         ) 
       +t1.MP_REM_PPL   --分期付款目前剩余本金
       +t1.BAL_NOINT    --不记息余额（未出账单组成）
       +t1.STM_NOINT   --帐单免息余额
       +t1.BAL_ORINT   --利息余额（未出账单组成）
       +t1.STM_BALORI   --帐单利息余额
       +t1.BAL_CMPINT  --复利余�    
                                                                                                   as  curr_od_bal       --当前余额 
      ,t1.ODUE_FLAG                                                                   as  Ovrd_Stat         --逾期状态
      ,case when t1.ODUE_FLAG=1 then  t7.INP_DAY::varchar::date   else '$MINDATE'::DATE end        as       ovrd_dt	        -- 逾期日期                  
      ,case when t1.ODUE_FLAG=1 then  t7.DRAFT_DAY::varchar::date else '$MINDATE'::DATE end        as       tranfm_od_dt	        -- 转透支日期                  
      ,case when t1.ODUE_FLAG=1 then  t7.REM_BAL                  else 0                end        as       remn_ovrd_prin	-- 剩余逾期本金                  
      ,case when t1.ODUE_FLAG=1 then  t7.REM_BALORI               else 0                end        as       remn_ovrd_int	        -- 剩余逾期利息  
      ,(case 
            when
                (case 
                     when t1.stmbalintflag = '-' then (-1)*abs(t1.stm_balint)
                 else abs(t1.stm_balint)
                 end
                   )    --前期取现
                  +
                (case 
                     when t1.bal_intflag = '-' then (-1)*abs(t1.bal_int)
                 else abs(t1.bal_int)
                 end
                   )<0     --本期取现
                then abs(case 
                              when t1.stmbalintflag = '-' then (-1)*abs(t1.stm_balint)
                          else abs(t1.stm_balint) 
                          end+
                          case 
                              when t1.bal_intflag = '-' then (-1)*abs(t1.bal_int)
                          else abs(t1.bal_int)
                          end
                           )
          else 0
          end
           )                                                                          as  spl_pay_bal        --溢缴款余额
      ,t1.wrof_flag                                                                    as   wrtoff_ind        --核销标志
      ,t1.wroff_chdy::varchar::date                                                                   as   wrtoff_dt         --核销日期
      ,case when t1.wrof_flag='1' then coalesce(t1.bal_free,0)                          
       +(case when t1.bal_intflag='-' then 0 else coalesce(t1.bal_int,0) end)
       +coalesce(t1.stm_balfre)
       +(case when t1.stmbalintflag='-' then 0 else coalesce(t1.stm_balint,0) end)
       +coalesce(t1.stm_balmp,0)
       +coalesce(t1.bal_mp,0)
       else   0
       end                                                                            as   wrtoff_prin       --核销金
      ,0                                                                              as   wrtoff_comm_fee   --核销手续费
      ,case when t1.wrof_flag='1' then coalesce(t1.penchg_acc,0)-coalesce(t1.pen_chrg,0) else 0 end    as  wrtoff_late_chrg    --核销滞纳金
      ,case when t1.wrof_flag='1' then coalesce(t1.stm_balori,0) else 0 end                           as  wrtoff_in_bal_int  --核销表里利息
      ,case when t1.wrof_flag='1' then coalesce(t1.bal_cmpint,0) else 0 end                           as  wrtoff_off_bal_int --核销表外利息
      ,0                                                                         as  today_provs_int    --当日计提利息
      ,0                                                                         as  curmth_provs_int   --当月计提利息
      ,0                                                                         as  accm_provs_int     --累计计提利息
      ,0                                                                         as  today_chrg_int     --当日收息
      ,coalesce(t6.LXSUM,0 )                                                     as  curmth_recvd_int   --当月已收息
      ,0                                                                         as  accm_recvd_int     --累计已收息
      ,0.00                                                                      as  int_adj_amt        --映射暂未确定  --月积数
      ,0.00                                                                      as  mth_accm           --映射暂未确定  --年积数
      ,0                                                                         as  yr_accm            --利息调整金额
      ,0                                                                      as  loan_deval_prep_bal --映射暂未确定 --贷款减值准备余额
      ,0                                                                      as  loan_deval_prep_amt --映射暂未确定 --贷款减值准备发生额
      ,0.00                                                                      as  mth_day_avg_bal    --映射暂未确定  --月日均余额
      ,0.00                                                                      as  yr_day_avg_bal     --映射暂未确定  --年日均余额
      ,'CCB'                                                                     as  sys_src            --系统来源
 from   dw_sdata.ccb_000_acct            t1                                     --人民币贷记账户
 left join  (select xaccount,master_nbr,start_dt,end_dt,row_number() over(partition by xaccount order by issue_reas desc) as num
 from dw_sdata.ccb_000_card
 where cardholder=1
 and trim(cancl_code)=''
 and start_dt<='$TXDATE'::date
 and end_dt>'$TXDATE'::date) T2                                    --卡片资料表
 on  t1.xaccount=t2.xaccount
and  t2.num=1
-- and       t2.start_dt<='$TXDATE'::date
-- and       t2.end_dt>'$TXDATE'::date
/* left join (select * 
            from dw_sdata.ecf_001_t01_cust_info 
            where start_dt<='$TXDATE'::date
            and   end_dt>'$TXDATE'::date 
            union all
            select * 
            from dw_sdata.ecf_004_t01_cust_info 
            where start_dt<='$TXDATE'::date
            and   end_dt>'$TXDATE'::date) t3 */
left join tmp_t3  t3                          --个人基本信息表
 on T1.CUSTR_NBR=T3.CERT_NO
 left join dw_sdata.ccb_000_stmt t4                                              --帐单记录表
 on        t1.xaccount=t4.xaccount
 and       t4.start_dt<='$TXDATE'::date
 and       t4.end_dt>'$TXDATE'::date
 left join dw_sdata.ccb_000_prmau         t5                                   --产品授权参数表
 on        t1.prod_nbr=t5.product
  and      t5.start_dt<='$TXDATE'::date
 and       t5.end_dt>'$TXDATE'::date
 left join tt_f_acct_crdt_info_temp_1          t6                                   --帐务交易流水表
 on        t1.xaccount=t6.acctnbr
 left join ( select account,curr_num,min(inp_day)as inp_day,max(draft_day) as draft_day,sum(rem_bal) as rem_bal,sum(rem_balori) as rem_balori      
             from   dw_sdata.CCB_000_ODUE
             where  odue_flag=1
             and start_dt<='$TXDATE'::date
             and end_dt>'$TXDATE'::date 
             group by 1,2)   t7
 on        T1.XACCOUNT=T7.ACCOUNT  
 and       t1.CURR_NUM=t7.CURR_NUM
 where     t1.start_dt<='$TXDATE'::date
 and       t1.end_dt>'$TXDATE'::date
 ;

 insert /* +direct */  into f_fdm.f_acct_crdt_info 
     (grp_typ                                                                 --组别
     ,etl_date                                                                --数据日期
     ,agmt_id                                                                 --协议编号
     ,card_num                                                                --卡号
     ,cust_id                                                                 --客户编号
     ,org_num                                                                 --机构号
     ,cur_cd                                                                  --货币代码
     ,open_acct_org_num                                                       --开户机构号
     ,prod_cd                                                                 --产品代码
     ,stmt_day                                                                --账单日
     ,open_acct_dt                                                            --开户日
     ,crdt_lmt                                                                --信用额度
     ,repay_day                                                               --还款日
     ,repay_acct_num_1                                                        --还款账号1
     ,repay_acct_num_2                                                        --还款账号2
     ,repay_acct_num_3                                                        --还款账号3
     ,repay_acct_num_4                                                        --还款账号4
     ,pre_brw_cash_ratio                                                      --预借现金比率
     ,amtbl_pay_lmt                                                           --分期付款额度
     ,curr_int_rate                                                           --当前利率
     ,int_base_cd                                                             --计息基础代码
     ,cmpd_int_calc_mode_cd                                                   --复利计算方式代码
     ,acct_stat_cd                                                            --账户状态
     ,prin_subj_od                                                            --本金科目—透支
     ,prin_subj_dpst                                                          --本金科目—存款
     ,int_subj                                                                --利息科目
     ,curr_bal                                                                --当前余额
     ,Ovrd_Stat                                                              --逾期状态
     ,ovrd_dt	          -- 逾期日期    
     ,tranfm_od_dt	  -- 转透支日期    
     ,remn_ovrd_prin	  -- 剩余逾期本金  
     ,remn_ovrd_int	  -- 剩余逾期利息
     ,spl_pay_bal                                                             --溢缴款余额
     ,wrtoff_ind                       --核销标志
     ,wrtoff_dt                        --核销日期
     ,wrtoff_prin                      --核销本金
     ,wrtoff_comm_fee                  --核销手续费
     ,wrtoff_late_chrg                 --核销滞纳金
     ,wrtoff_in_bal_int                --核销表内利息
     ,wrtoff_off_bal_int               --核销表外利息
     ,today_provs_int                                                         --当日计提利息
     ,curmth_provs_int                                                        --当月计提利息
     ,accm_provs_int                                                          --累计计提利息
     ,today_chrg_int                                                          --当日收息
     ,curmth_recvd_int                                                        --当月已收息
     ,accm_recvd_int                                                          --累计已收息
     ,int_adj_amt                                                             --月积数
     ,mth_accm                                                                --年积数
     ,yr_accm                                                                 --利息调整金额
     ,loan_deval_prep_bal                                                     --贷款减值准备余额
     ,loan_deval_prep_amt                                                     --贷款减值准备发生额
     ,mth_day_avg_bal                                                         --月日均余额
     ,yr_day_avg_bal                                                          --年日均余额
     ,sys_src                                                                 --系统来源
     )
 select
        2                                                             as grp_typ                 --组别                
       ,'$TXDATE'::date                                      as etl_date                --数据日期             
       ,T.XACCOUNT                                                    as agmt_id                 --协议编号             
     ,trim(T1.MASTER_NBR)                                             as card_num                --卡号              
       ,T3.ECIF_CUST_NO                                               as cust_id                 --客户编号             
       ,'1199523Q'                                                    as org_num                 --机构号              
       ,T.CURR_NUM                                                    as cur_cd                  --货币代码             
      ,'1199523Q'                                                     as open_acct_org_num       --开户机构号            
       ,T2.PROD_NBR                                                   as prod_cd                 --产品代码             
       ,T2.CYCLE_NBR                                                  as stmt_day                --账单日              
       ,to_date(to_char(T2.day_opened),'yyyymmdd')                    as open_acct_dt            --开户日              
       ,(case 
              when T2.TEMP_LIMIT<>0 then 
                                         (case  
                                               when '$TXDATE'::date>=T2.TLMT_BEG::varchar::date and '$TXDATE'::date<T2.TLMT_END::varchar::date and T2.TEMP_LIMIT>T2.CRED_LIMIT then T2.TEMP_LIMIT/t4.exchg_rate_val
                                          else T2.CRED_LIMIT/t4.exchg_rate_val
                                          end 
                                          )
         else T2.CRED_LIMIT
         end 
         )                                                             as crdt_lmt               --信用额度            
       ,'$MINDATE'::DATE                        as repay_day              --还款日              
       ,T2.BANKACCT1                                                   as repay_acct_num_1       --还款账号1            
       ,T2.BANKACCT2                                                   as repay_acct_num_2       --还款账号2            
       ,T2.BANKACCT3                                                   as repay_acct_num_3       --还款账号3            
       ,T2.BANKACCT4                                                   as repay_acct_num_4       --还款账号4            
       ,T6.CA_LMTPRCT                                                  as pre_brw_cash_ratio     --预借现金比率           
       ,0                                                           as amtbl_pay_lmt          --分期付款额度           
       ,T.INT_RATE                                                     as curr_int_rate          --当前利率             
       ,'6'                                                            as int_base_cd            --计息基础代码           
       ,'2'                                                            as cmpd_int_calc_mode_cd  --复利计算方式代码         
       ,T2.CLOSE_CODE                                                  as acct_stat_cd           --账户状态             
       ,'11352000101R'                                                 as prin_subj_od           --本金科目—透支          
       ,'22752000101R'                                                 as prin_subj_dpst         --本金科目—存款          
       ,'51053100401R'                                                 as int_subj               --利息科目             
       ,T.BAL_FREE
       +T.STM_BALFRE
       +(CASE 
             WHEN T.BALINTFLAG='+' THEN T.BAL_INT 
             ELSE 0 
         END
        )
       +(CASE 
             WHEN T.STMBALINTFLAG='+' THEN T.STM_BALINT 
             ELSE 0 
         END
        ) 
       +T.BAL_MP
       +T.STM_BALMP                                                     as curr_bal               --当前余额          
      ,T.ODUE_FLAG                                                     as Ovrd_Stat              --逾期状态
      ,case when t2.ODUE_FLAG=1  then   t8.INP_DAY::varchar::date    else '$MINDATE'::DATE end               as       ovrd_dt               -- 逾期日期                  
      ,case when t2.ODUE_FLAG=1  then   t8.DRAFT_DAY::varchar::date  else '$MINDATE'::DATE  end           as       tranfm_od_dt            -- 转透支日期                  
      ,case when t2.ODUE_FLAG=1  then   t8.REM_BAL                   else 0 end           as       remn_ovrd_prin     -- 剩余逾期本金                  
      ,case when t2.ODUE_FLAG=1  then   t8.REM_BALORI                else 0 end      as       remn_ovrd_int              -- 剩余逾期利息 
       ,(CASE 
              WHEN 
         (CASE 
               WHEN T.BALINTFLAG='-' THEN -T.BAL_INT 
               ELSE T.BAL_INT 
          END
         )
        +(CASE 
               WHEN T.STMBALINTFLAG='-' THEN -T.STM_BALINT 
               ELSE T.STM_BALINT 
          END
         ) <0
         THEN ABS(
                  (CASE 
                        WHEN T.BALINTFLAG='-' THEN -T.BAL_INT 
                        ELSE T.BAL_INT 
                   END
                 )
                  
        +(CASE 
               WHEN T.STMBALINTFLAG='-' THEN -T.STM_BALINT 
               ELSE T.STM_BALINT 
           END
          )
                  ) 
         ELSE 0 
         END
         )                                                             as spl_pay_bal            --溢缴款余额           
     ,coalesce(t2.wrof_flag::varchar,'')                 as    wrtoff_ind                       --核销标志
     ,t2.wroff_chdy::varchar::date                 as    wrtoff_dt                        --核销日期
     ,case when t2.wrof_flag='1'
           then coalesce(t.bal_free,0)
               +(case when t.balintflag='-' then 0 else coalesce(t.bal_int,0) end)
               +coalesce(t.stm_balfre,0)
               +(case when t.stmbalintflag='-' then 0 else coalesce(t.stm_balint,0) end)
               +coalesce(t.bal_mp,0)
               +coalesce(t.stm_balmp,0)
       else 0 end                 as    wrtoff_prin                      --核销本金
     ,0                 as    wrtoff_comm_fee                  --核销手续费
     ,case when t2.wrof_flag='1' then t.penchg_acc-t.pen_chrg else 0 end                 as    wrtoff_late_chrg                 --核销滞纳金
     ,case when t2.wrof_flag='1' then t.stm_balori else 0 end                  as    wrtoff_in_bat_int                --核销表内利息
     ,case when t2.wrof_flag='1' then t.bal_cmpint else 0 end                  as    wrtoff_off_bat_int               --核销表外利息
       ,0.00                                                           as today_provs_int        --当日计提利息           
       ,0.00                                                           as curmth_provs_int       --当月计提利息           
       ,0.00                                                           as accm_provs_int         --累计计提利息           
       ,0.00                                                           as today_chrg_int         --当日收息             
       ,coalesce(T7.LXSUM,0)                                           as curmth_recvd_int       --当月已收息            
       ,0.00                                                           as accm_recvd_int         --累计已收息            
       ,0.00                                                           as int_adj_amt            --月积数              
       ,0.00                                                           as mth_accm               --年积数              
       ,0.00                                                           as yr_accm                --利息调整金额           
       ,0                                                           as loan_deval_prep_bal    --贷款减值准备余额         
       ,0                                                           as loan_deval_prep_amt    --贷款减值准备发生额        
       ,0.00                                                           as mth_day_avg_bal        --月日均余额            
       ,0.00                                                           as yr_day_avg_bal         --年日均余额            
       ,'CCB'                                                          as sys_src                --系统来源       
from dw_sdata.ccb_000_accx T
left join (select xaccount,master_nbr,start_dt,end_dt,row_number() over(partition by xaccount order by issue_reas desc) as num
 from dw_sdata.ccb_000_card
 where cardholder=1
 and trim(cancl_code)=''
 and start_dt<='$TXDATE'::date
 and end_dt>'$TXDATE'::date) T1
on T.XACCOUNT=T1.XACCOUNT  
and t1.num=1
--and T1.start_dt<='$TXDATE'::date
--and T1.end_dt>'$TXDATE'::date
left join  dw_sdata.ccb_000_acct  T2
on T.XACCOUNT=T2.XACCOUNT 
AND T2.start_dt<='$TXDATE'::date
and T2.end_dt>'$TXDATE'::date
left join tmp_t3 t3
/*(select * from 
(SELECT ECIF_CUST_NO,CERT_NO,IS_VIP_FLAG,start_dt,end_dt,
row_number() over(partition by CERT_NO order by IS_VIP_FLAG desc) as num2 
FROM 
(select * from 
(select ECIF_CUST_NO,CERT_NO,IS_VIP_FLAG,start_dt,end_dt,
row_number() over(partition by CERT_NO order by cert_due_date desc) as num 
from dw_sdata.ecf_001_t01_cust_info
where UPDATED_TS = '99991231 00:00:00:000000'
and start_dt<='$TXDATE'::date
and '$TXDATE'::date<end_dt ) a
where a.num=1
union all
select * from (select ECIF_CUST_NO,CERT_NO,IS_VIP_FLAG,start_dt,end_dt,
row_number() over(partition by CERT_NO  order by cert_due_date desc) as num 
from dw_sdata.ecf_004_t01_cust_info 
where UPDATED_TS = '99991231 00:00:00:000000'
and start_dt<='$TXDATE'::date
and '$TXDATE'::date<end_dt) b
where b.num=1
 ) c)d
where d.num2=1
) t3
*/
on T2.CUSTR_NBR=T3.CERT_NO
left join f_fdm.f_fnc_Exchg_Rate T4
on T2.CURR_NUM=T4.orgnl_cur_cd
and  T.CURR_NUM=T4.convt_cur_cd
and '$TXDATE'=efft_day 
/*
left join dw_sdata.ccb_000_stmx T5  
on T.XACCOUNT=T5.XACCOUNT  
and T5.etl_dt='$TXDATE'::date
*/
left join dw_sdata.ccb_000_prmau T6
on T2.PROD_NBR=T6.PRODUCT 
and T6.start_dt<='$TXDATE'::date
and T6.end_dt>'$TXDATE'::date
left join tt_f_acct_crdt_info_temp_3   T7
on T.XACCOUNT=T7.ACCTNBR
left join (select account,curr_num,min(inp_day)as inp_day,max(draft_day) as draft_day,sum(rem_bal) as rem_bal,sum(rem_balori) as rem_balori
              from   dw_sdata.CCB_000_ODUE
              where  odue_flag=1
              and start_dt<='$TXDATE'::date
              and end_dt>'$TXDATE'::date
              group by 1,2)   t8
on   T.XACCOUNT=T8.ACCOUNT  
and  t.CURR_NUM=t8.CURR_NUM
where T.start_dt<='$TXDATE'::date
and   T.end_dt>'$TXDATE'::date
;
   /*月积数、年积数、月日均余额、年日均余额临时表创建区*/
 
insert into tt_f_acct_crdt_info_temp_yjs
(
       agmt_id
       ,Mth_Accm 
       ,Yr_Accm 
       ,Mth_Day_Avg_Bal 
       ,Yr_Day_Avg_Bal
)
select 
      t.agmt_id
      ,(case 
            when '$TXDATE'= '$MONTHBGNDAY' 
            then t.curr_bal
            else t.curr_bal+coalesce(t1.mth_accm,0)
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
from  f_fdm.f_acct_crdt_info     t
left join  f_fdm.f_acct_crdt_info t1
on        t.agmt_id=t1.agmt_id
and       t1.etl_date='$TXDATE'::date-1
where     t.etl_date='$TXDATE'::date
;
/*月积数、年积数、月日均余额、年日均余额临时表创建区END*/
/*更新月积数、年积数、月日均余额、年日均余额*/
update f_fdm.f_acct_crdt_info    t
set mth_accm=t1.mth_accm 
,yr_accm=t1.yr_accm
,mth_day_avg_bal=t1.mth_day_avg_bal
,Yr_Day_Avg_Bal=t1.Yr_Day_Avg_Bal
from  tt_f_acct_crdt_info_temp_yjs  t1
where t.agmt_id=t1.agmt_id
and   t.etl_date='$TXDATE'::date
;

/*数据处理区END*/
COMMIT;
