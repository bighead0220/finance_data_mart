/*
Author             :Liuxz
Function           :法人透支账户信息表
Load method        :
Source table       :dw_sdata.cbs_001_ammst_spec_corp,dw_sdata.cbs_001_fsrgt_acct_comp,dw_sdata.cbs_001_ammst_corp,dw_sdata.cbs_001_fsrgt_cust_limit,dw_sdata.cbs_001_fsrgt_acct_ovrd,dw_sdata.cbs_001_pmctl_irate_code,dw_sdata.acc_003_t_acc_assets_ledger,dw_sdata.acc_003_t_accdata_last_item_no,dw_sdata.cbs_001_fsrgt_return_int,dw_sdata.cbs_001_amdtl_prep_int
Destination Table  :f_fdm.f_acct_lpr_od
Frequency          :D
Modify history list:Created by zhangwj at 2016-4-19 11:28 v1.0
                    Changed by zhangwj at 2016-5-04 15:41 v1.1
                    Changed by zhangwj at 2016-5-23 10:12 v1.2   大数据贴源层表名修改，表为拉链表或流水表，与之保持一致
                    Changed by zhangwj at 2016-6-14 15:41 v1.3   新增月积数、年积数、月日均余额、年日均余额 
                              :Modify  by liuxz 20160714  删除T11，T12表 cbs_001_amdtl_prep_int “and   start_dt<=ETL加载日期<end_dt
                              modified by liuxz 20160715 添加 协议状态代码 代码转换
                               modified by Liuxz 20160819 增加coalesce
                              modified by wyh 20160925 修改当日计提利息,累计以收息,修改t2,t4关联条件,修改日期为MIN和MAX;
                              modified by wyh 20160926	a增�关联条件
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/

/*临时表创建区*/
create local temporary table IF NOT EXISTS tt_f_acct_lpr_od_yjs  
on commit preserve rows as
select *
from f_fdm.f_acct_lpr_od 
where 1=2;
/*临时表创建区END*/

/*数据回退区*/
delete /* +direct */ from f_fdm.f_acct_lpr_od
where etl_date = '$TXDATE'
;
/*数据回退区END*/

/*数据处理区*/
insert /* +direct */  into f_fdm.f_acct_lpr_od
      (grp_typ                                                                        --组别
      ,etl_date                                                                       --数据日期
      ,agmt_id                                                                        --协议编号
      ,cust_num                                                                       --客户号
      ,org_num                                                                        --机构号
      ,cur_cd                                                                         --货币代码
      ,prod_cd                                                                        --产品代码
      ,st_int_dt                                                                      --起息日
      ,due_dt                                                                         --到期日
      ,init_lmt                                                                       --初始额度
      ,agmt_stat_cd                                                                   --协议状态代码
      ,exec_int_rate                                                                  --执行利率
      ,bmk_int_rate                                                                   --基准利率
      ,basic_diff                                                                     --基差
      ,int_base_cd                                                                    --计息基础代码
      ,cmpd_int_calc_mode_cd                                                          --复利计算方式代码
      ,pre_chrg_int                                                                   --是否先收息
      ,int_rate_attr_cd                                                               --利率属性代码
      ,orgnl_term                                                                     --原始期限   
      ,orgnl_term_corp_cd                                                             --原始期限单位代码
      ,rprc_prd                                                                       --重定价周期
      ,rprc_prd_corp_cd                                                               --重定价周期单位代码
      ,last_rprc_day                                                                  --上次重定价日
      ,next_rprc_day                                                                  --下次重定价日
      ,prin_subj                                                                      --本金科目
      ,curr_bal                                                                       --当前余额
      ,int_subj                                                                       --利息科目
      ,today_provs_int                                                                --当日计提利息
      ,curmth_provs_int                                                               --当月计提利息
      ,accm_provs_int                                                                 --累计计提利息
      ,today_chrg_int                                                                 --当日收息
      ,curmth_recvd_int                                                               --当月已收息
      ,accm_recvd_int                                                                 --累计已收息
      ,int_adj_amt                                                                    --利息调整金额
      ,deval_prep_bal                                                                 --减值准备余额
      ,deval_prep_amt                                                                 --减值准备发生额
      ,mth_accm                                                                       --月积数
      ,yr_accm                                                                        --年积数
      ,mth_day_avg_bal                                                                --月日均余额
      ,yr_day_avg_bal                                                                 --年日均余额
      ,sys_src                                                                        --系统来源
      )
 select
      1                                                                               as  grp_typ         --组别
      ,'$TXDATE'::date                                                       as  etl_date        --数据日期
      ,coalesce(t.account ,'')                                                        as  agmt_id         --协议编号
      ,coalesce(t2.cust_id ,'')                                                       as  cust_num        --客户号
      ,coalesce(t.open_unit ,'')                                                      as  org_num         --机构号
      ,coalesce(t.cur_code ,'')                                                       as  cur_cd          --货币代码
      ,coalesce(null ,'')                                                             as  prod_cd         --产品代码     
      ,coalesce(to_date(t3.start_date,'yyyymmdd'),'$MINDATE'::date)               as  st_int_dt       --起息日
      ,coalesce(to_date(t3.end_date,'yyyymmdd'),'$MINDATE'::date)                 as  due_dt          --到期日
      ,coalesce(t4.ovrd_limit,0)                                                      as  init_lmt        --初始额度
      ,coalesce(t13.TGT_CD,'@'||t.acct_state)                                                     as  agmt_stat_cd    --协议状态代码
      ,coalesce(t3.ovrd_irate,0)                                                      as  exec_int_rate   --执行利率
      ,coalesce(t5.irate1,0)                                                          as  bmk_int_rate    --基准利率
      ,coalesce(t5.irate1,0)-coalesce(t3.ovrd_irate,0)                                as  basic_diff      --基差
      ,'6'                                                                            as  int_base_cd     --计息基础代码             --码值表未维护，后期需转换代码
      ,'1'                                                                            as  cmpd_int_calc_mode_cd  --复利计算方式代码    --码值表未维护，后期需转换代码
      ,'0'                                                                            as  pre_chrg_int    --是否先收息            --码值表未维护，后期需转换代码
      ,'4'                                                                            as  int_rate_attr_cd  --利率属性代码        --码值表未维护，后期需转换代码
      ,coalesce(to_date(t3.end_date,'yyyymmdd'),'$MINDATE'::date)-coalesce(to_date(t3.start_date,'yyyymmdd'),'$MINDATE'::date)    as  orgnl_term      --原始期限
      ,'D'                                                                            as  orgnl_term_corp_cd  --原始期限单位代码      --码值表未维护，后期需转换代码
      ,0                                                                              as  rprc_prd        --重定价周期
      ,coalesce(null ,'')                                                             as  rprc_prd_corp_cd  --重定价周期单位代码
      --modified 20160925
      ,'$MINDATE'::date                                                                           as  last_rprc_day   --上次重定价日
      ,'$MAXDATE'::date                                                                           as  next_rprc_day   --下次重定价日
      ,coalesce(t6.itm_no ,'')                                                        as  prin_subj       --本金科目
      ,coalesce(t6.bal,0)                                                             as  curr_bal        --当前余额
      ,coalesce(t7.itm_no ,'')                                                        as  int_subj        --利息科目
      ,coalesce(t10.cur_cope_int,0)                                                   as  today_provs_int --当日计提利息
      ,coalesce(t11.cur_cope_int,0)                                                   as  curmth_provs_int --当月计提利息
      ,coalesce(t12.cur_face_int,0)                                                   as  accm_provs_int  --累计计提利息
      ,coalesce(t8.ret_int ,0)                                                        as  today_chrg_int  --当日收息
      ,coalesce(t9.ret_int ,0)                                                        as  curmth_recvd_int --当月已收息
      ,0                                                                              as  accm_recvd_int   --累计已收息
      ,coalesce(null ,0)                                                              as  int_adj_amt      --利息调整金额
      ,coalesce(null ,0)                                                              as  deval_prep_bal   --减值准备余额
      ,coalesce(null ,0)                                                              as  deval_prep_amt   --减值准备发生额
      ,coalesce(null ,0)                                                              as  mth_accm         --月积数
      ,coalesce(null ,0)                                                              as  yr_accm          --年积数
      ,coalesce(null ,0)                                                              as  mth_day_avg_bal  --月日均余额
      ,coalesce(null ,0)                                                              as  yr_day_avg_bal   --年日均余额
      ,'CBS'                                                                          as  sys_src          --系统来源
 from dw_sdata.cbs_001_ammst_spec_corp      t                                         --特殊单位分户账
 left join dw_sdata.cbs_001_fsrgt_acct_comp t1                                        --现金管理账户对应核算账户对照表
 on        t.account =t1.comp_ovrd_acct
 and       t1.start_dt<='$TXDATE'::date
 and       t1.end_dt>'$TXDATE'::date
 left join dw_sdata.cbs_001_ammst_corp      t2                                       --单位分户账
 on        t1.account=t2.account
 and       t1.subacct = t2.subacct        --modified at 20160925
 and       t2.start_dt<='$TXDATE'::date
 and       t2.end_dt>'$TXDATE'::date
 left join dw_sdata.cbs_001_fsrgt_cust_limit  t3                                     --法人透支额度登记簿
 on        t2.cust_id =t3.cust_id
 and       t3.start_dt<='$TXDATE'::date
 and       t3.end_dt>'$TXDATE'::date
 left join dw_sdata.cbs_001_fsrgt_acct_ovrd        t4                                --法人透支账户信息主表
 on        t1.account=t4.account
 and       t1.subacct = t4.subacct       --modified at 20160925
 and       t4.ovrd_kind = '0'            --modified at 20160926
 and       t4.start_dt<='$TXDATE'::date
 and       t4.end_dt>'$TXDATE'::date
/* left join (
 select * from (
 select 
		ROW_NUMBER()OVER(PARTITION BY T.ACCOUNT ORDER BY decode(T.CANCEL_DATE,'','99991231',t.CANCEL_DATE) DESC) NUM
		,t.ACCOUNT
		,t.subacct
		,t.ovrd_limit
 from dw_sdata.cbs_001_fsrgt_acct_ovrd t 
 where t.start_dt<='$TXDATE'::date
 and       t.end_dt>'$TXDATE'::date
 )t01
 where NUM = 1
 )t4
 on t1.account=t4.account
 and       t1.subacct = t4.subacct       --modified at 20160925 20:00
*/ 
left join
         (select
                a.irate_code
                ,a.cur_code
                ,a.bgn_date
                ,a.irate1
         from   dw_sdata.cbs_001_pmctl_irate_code a                                  --利率代码控制表
         inner join
                  (select
                         irate_code
                         ,cur_code
                         ,max(bgn_date) as bgn_date
                  from   dw_sdata.cbs_001_pmctl_irate_code
                  where  start_dt<='$TXDATE'::date
                  and    end_dt>'$TXDATE'::date
                  group by irate_code,cur_code
                  ) b
         on       a.irate_code=b.irate_code
         and      a.cur_code=b.cur_code
         and      a.bgn_date =b.bgn_date
         where    a.start_dt<='$TXDATE'::date
         and      a.end_dt>'$TXDATE'::date
         )t5
 on      t3.ovrd_intcode=t5.irate_code                                               --透支利率代码
 and     t3.cur_code =t5.cur_code                                                    --币种代码
 left join dw_sdata.acc_003_t_acc_assets_ledger  t6                                  --资产类客户账户分户账
 on        t.account=t6.acc
 and       t6. sys_code='99200000000'
 and       t6.start_dt<='$TXDATE'::date
 and       t6.end_dt>'$TXDATE'::date
 left join dw_sdata.acc_003_t_accdata_last_item_no t7                                --科目转换对照表
 on        t6.itm_no=t7.amt_itm
 and       t7.first_itm='18'                                                         --负债类
 and       t7.start_dt<='$TXDATE'::date
 and       t7.end_dt>'$TXDATE'::date
 left join 
         (
         select
                inside_acct
                ,sum(ret_int) as ret_int
         from   dw_sdata.cbs_001_fsrgt_return_int
         where   ret_date = '$TXDATE'                     --modified by 20160925
         group by inside_acct
         ) t8                                      --归还隔夜透支户利息登记簿
 on        t.account=t8.inside_acct                                                  --隔夜透支账户
 left join
         (select
                inside_acct
                ,sum(ret_int) as ret_int
         from   dw_sdata.cbs_001_fsrgt_return_int
         where  substr(to_char(ret_date),1,6)=substr('$TXDATE',1,6)
         and    ret_date::date <= '$TXDATE'::date
         group by inside_acct
         ) t9
 on      t.account=t9.inside_acct                                                    --隔夜透支账户
/*
 left join
         (select
                inside_acct
                ,sum(ret_int) as ret_int
         from   dw_sdata.cbs_001_fsrgt_return_int
         where  etl_dt<='$TXDATE'::date
         group by inside_acct
         ) t10
 on      t.account=t10.inside_acct                                                  --隔夜透支账户
*/
 left join 
        (select
                account
                ,sum(cur_cope_int)  as cur_cope_int
         from   dw_sdata.cbs_001_amdtl_prep_int                                      --账户利息预提清单
         where draw_date::date = '$TXDATE'::date
         group by account
          )t10
on T.account =T10.account
 left join
        (select
                account
                ,sum(cur_cope_int)  as cur_cope_int
         from   dw_sdata.cbs_001_amdtl_prep_int                                      --账户利息预提清单
         where  substr(draw_date,1,6)=substr('$TXDATE',1,6)
          and draw_date::date <= '$TXDATE'::date
        -- and    start_dt<='$TXDATE'::date
        -- and    end_dt>'$TXDATE'::date
         group by account
         ) t11
ON  T.account =T11.account
 left join
         ( select * from
 (
 select distinct
        account
        ,cur_cope_int
        ,cur_face_int
        ,row_number()over(partition by account order by draw_date desc ,cur_face_int desc ) rn
   from dw_sdata.cbs_001_amdtl_prep_int
  where draw_date::date <='$TXDATE'::date
  )q   where rn = 1
         )T12
     ON  T.account =T12.account
LEFT JOIN  f_fdm.CD_RF_STD_CD_TRAN_REF T13 --需转换代码表
ON  T.ACCT_STATE=T13.SRC_CD                       --源代码值相同
AND  T13.DATA_PLTF_SRC_TAB_NM = 'CBS_001_AMMST_SPEC_CORP' --数据平台源表主干名
AND  T13.Data_Pltf_Src_Fld_Nm ='ACCT_STATE' 
where    t.start_dt<='$TXDATE'::date
and      t.end_dt>'$TXDATE'::date
and      t.acct_attr='01' --账户属性（法透账户）
 ;
 /*月积数、年积数、月日均余额、年日均余额临时表创建区*/
 
insert /* +direct */ into tt_f_acct_lpr_od_yjs
(
agmt_id 
,mth_accm 
,yr_accm 
,mth_day_avg_bal 
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
from  f_fdm.f_acct_lpr_od     t
left join f_fdm.f_acct_lpr_od t1
on        t.agmt_Id=t1.agmt_Id
and       t1.etl_date='$TXDATE'::date-1
where     t.etl_date='$TXDATE'::date
;
/*月积数、年积数、月日均余额、年日均余额临时表创建区END*/
/*更新月积数、年积数、月日均余额、年日均余额*/
update f_fdm.f_acct_lpr_od t
set mth_accm=t1.mth_accm 
,yr_accm=t1.yr_accm
,mth_day_avg_bal=t1.mth_day_avg_bal
,Yr_Day_Avg_Bal=t1.Yr_Day_Avg_Bal
from tt_f_acct_lpr_od_yjs t1
where  t.agmt_Id=t1.agmt_Id
and t.etl_date='$TXDATE'::date
;
/*数据处理区END*/

 COMMIT;
