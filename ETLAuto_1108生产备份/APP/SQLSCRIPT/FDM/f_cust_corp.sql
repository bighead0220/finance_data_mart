/*
Author             :Liuxz
Function           :公司客户基本信息表
Load method        :
Source table       :dw_sdata.ecf_002_t01_cust_info,dw_sdata.ecf_002_t06_dpmst_corp
Destination Table  :f_fdm.f_cust_corp
Frequency          :D
Modify history list:Created by zhangwj at 2016-05-05 17:49 v1.0
                    changed by zhangwj at 2016-05-30 14.51 v1.1  新增“证件类型代码”和“证件号码”
                   :Modify  by liuxz 20160617 客户类型代码 逻辑变更 （变更记录75）
                                              客户规模代码 逻辑变更 （变更记录75）
                                              注册资本货币代码 逻辑变更 （变更记录75）
                    modified by liuxz 20160701 注册资本货币代码 代码转换开发 （变更记录92）
                                               证件类型代码     代码转换开发
                    modified by liuxz 20160718 修改整体格式
                    modified by liuxz 20160719 将T1表限制条件更换至T表 t.updated_ts = '99991231 00:00:00:000000'
                    modified by zhangliang 20160826  修改关联表t1以及关联条件,筛选条件
                    modified by zhangliang 20160829 修改关联表修改关联t2表及条件
					          modified by wyh at 20161014  last_updated_ts 替换为 updated_ts
					          modified by wyh at 20161019  修改ROW_NUMBER逻辑:order by UPDATED_TS desc,last_updated_ts desc
-------------------------------------------逻辑说明---------------------------------------------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/

/*临时表创建区*/

/*临时表创建区END*/

/*数据回退区*/
delete /* +direct */ from f_fdm.f_cust_corp
where  etl_date = '$TXDATE'::date
;
/*数据回退区END*/

/*数据处理区*/
insert /* +direct */  into f_fdm.f_cust_corp
     (grp_typ                                           --组别
     ,etl_date                                          --数据日期
     ,cust_num                                          --客户号
     ,cust_nm                                           --客户名称
     ,cust_org_org_cd                                   --客户组织机构代码
     ,belg_org_num                                      --所属机构号
     ,open_acct_dt                                      --开户日期
     ,cust_typ_cd                                       --客户类型代码
     ,cust_inds_cd                                      --客户行业代码
     ,cust_size_cd                                      --客户规模代码
     ,rgst_cap_cur_cd                                   --注册资本货币代码
     ,rgst_cap                                          --注册资本
     ,ltst_rat_rslt                                     --最新评级结果
     ,cust_crdt_lvl_cd                                  --客户信用等级代码
     ,is_lst_corp_ind                                   --是否上市公司标志
     ,is_rltv_corp_ind                            --是否本行关联企业标志
     ,is_and_sign_bkcp_agmt_ind                   --是否与本行签订银企合作协议标志
     ,is_grp_cust_ind                                   --是否集团客户标志
     ,corp_grp_prnt_corp_cust_num                       --企业集团母公司客户号
     ,corp_grp_prnt_corp_nm                             --企业集团母公司名称
     ,is_vip                                            --是否VIP
     ,corp_own_typ_cd                                   --企业所有制类型代码
     ,cust_stat_cd                                      --客户状态代码
     ,corp_charc_cd                                     --企业性质代码
     ,cust_impt_lvl_cd                                  --客户重要性等级代码
     ,cust_mgr_id                                       --客户经理编号
     ,strgy_cust_ind                                    --战略客户标识
     ,pltf_ind                                          --平台标识
     ,prtnr_ind                                         --合作方标识
     ,ovrs_cust_rgst_num                                --境外客户注册号
     ,cert_typ_cd                                       --证件类型代码
     ,cert_num                                          --证件号码
     ,sys_src                                           --系统来源
     )
 select
       1                                                as  grp_typ                            --组别
       ,'$TXDATE'::date                        as  etl_date                           --数据日期
       ,coalesce(t.ecif_cust_no,'')                     as  cust_num                           --客户号
       ,coalesce(t.party_name,'')                       as  cust_nm                            --客户名称
       ,coalesce(t.org_code,'')                         as  cust_org_org_cd                    --客户组织机构代码
       ,coalesce(t.own_org,'')                          as  belg_org_num                       --所属机构号
       ,coalesce(to_date(t1.open_date,'yyyymmdd'),'$MAXDATE'::date)  as  open_acct_dt      --开户日期
       ,coalesce(substr(t.customer_type_cd,5),'')                 as  cust_typ_cd                        --客户类型代码
       ,coalesce(substr(t.industry_type,5),'')          as cust_inds_cd                        --客户行业代码
       ,coalesce(substr(t.org_siz,5),'')                          as  cust_size_cd                       --客户规模代码
       ,coalesce(t3.tgt_cd,'@'||substr(t.reg_cptl_curr,5))                    as  rgst_cap_cur_cd                    --注册资本货币代码
       ,coalesce(t.reg_cptl,0)                          as  rgst_cap                           --注册资本
       ,coalesce(t.new_eval_result,'')                  as  ltst_rat_rslt                      --最新评级结果
       ,substr(t.entp_crdg_level,-2)                  as  cust_crdt_lvl_cd                   --客户信用等级代码
       ,coalesce(t.list_flag,'')                        as  is_lst_corp_ind                    --是否上市公司标志
       ,coalesce(t.bank_rel_entrps_flag,'')             as  is_rltv_corp_ind             --是否本行关联企业标志
       ,coalesce(t.coper_agr_cust_flag,'')              as  is_and_sign_bkcp_agmt_ind    --是否与本行签订银企合作协议标志
       ,coalesce(t.groupcus_flag,'')                    as  is_grp_cust_ind                    --是否集团客户标志
       ,coalesce(t2.ecif_cust_no,'')                    as  corp_grp_prnt_corp_cust_num        --企业集团母公司客户号
       ,coalesce(t2.group_cust_name,'')                 as  corp_grp_prnt_corp_nm              --企业集团母公司名称
       ,coalesce(null,'')                               as  is_vip                             --是否VIP
       ,coalesce(t.ownership_type_code,'')              as  corp_own_typ_cd                    --企业所有制类型代码
       ,coalesce(t.cust_state,'')                       as  cust_stat_cd                       --客户状态代码
       ,coalesce(t.org_type,'')                         as  corp_charc_cd                      --企业性质代码
       ,coalesce(t.corp_imp_level,'')                   as  cust_impt_lvl_cd                   --客户重要性等级代码
       ,coalesce(t.rm_team_no,'')                       as  cust_mgr_id                        --客户经理编号
       ,coalesce(null,'')                               as  strgy_cust_ind                     --战略客户标识
       ,null                                            as  pltf_ind                           --平台标识
       ,null                                            as  prtnr_ind                          --合作方标识
       ,case
        when  t.cert_type = 'c90513'
        then  t.cert_no
        else  ''
        end                                             as  ovrs_cust_rgst_num                 --境外客户注册号
       ,coalesce(t4.tgt_cd,'@'||substr(t.cert_type,5))                        as  cert_typ_cd                        --证件类型代码
       ,coalesce(t.cert_no,'')                          as  cert_num                           --证件号码
       ,'ECF'                                           as  sys_src                            --系统来源
 --from dw_sdata.ecf_002_t01_cust_info t                  --公司客户基本信息
from (select *,row_number() over(partition by ECIF_CUST_NO  order by updated_ts desc ,last_updated_ts desc ) as num 
        from dw_sdata.ecf_002_t01_cust_info 
       where --UPDATED_TS = '99991231 00:00:00:000000'
         --and                                                   --modified by wyh at 20161019
         START_DT<='$TXDATE'::date
         AND '$TXDATE'::DATE<end_dt ) t
 /*left join
         (select
               party_id
               ,min(open_date) as open_date
         from  dw_sdata.ecf_002_t06_dpmst_corp          --单位分户账
         where   start_dt<='$TXDATE'::date
         and   end_dt>'$TXDATE'::date
         group by party_id
         ) t1
 on      t.party_id =t1.party_id
*/
left join (select cust_id,min(open_date) as open_date 
             from  (select cust_id,open_date  
                      from dw_sdata.cbs_001_ammst_corp a  
                     where a.START_DT<='$TXDATE'::date
                      AND  '$TXDATE'::DATE<a.end_dt
                   union all 
                    select cust_id,open_date 
                      from dw_sdata.cbs_001_ammst_secu b
                     where b.START_DT<='$TXDATE'::date
                       AND '$TXDATE'::date<b.end_dt) c
          group by cust_id) t1
ON t.ECIF_CUST_NO=t1.cust_id
/*
 left join  dw_sdata.ecf_002_t01_cust_info t2
 on         trim(t.group_cust_name) =trim(t2.party_name)
 and        t2.start_dt<='$TXDATE'::date
 and        t2.end_dt>'$TXDATE'::date
*/
left join (select ecif_cust_no
             ,group_cust_name
             , party_name
             ,start_dt
             ,end_dt
             ,row_number() over(partition by party_name order by updated_ts desc ,last_updated_ts desc) as num  
             from dw_sdata.ecf_002_t01_cust_info 
           where start_dt<='$TXDATE'::DATE
              and '$TXDATE'::DATE<end_dt
              --and  updated_ts = '99991231 00:00:00:000000'                       --modified by wyh at 20161019
              )  t2
on trim(T.GROUP_CUST_NAME) =trim(T2.PARTY_NAME) 
and t2.num=1
 left join  f_fdm.CD_RF_STD_CD_TRAN_REF t3
 on         substr(t.reg_cptl_curr,5)=t3.SRC_CD
 AND        T3.DATA_PLTF_SRC_TAB_NM = 'ECF_002_T01_CUST_INFO'
 AND        T3.Data_Pltf_Src_Fld_Nm ='REG_CPTL_CURR'
 left join  f_fdm.CD_RF_STD_CD_TRAN_REF t4
 on         substr(t.cert_type,5)=t4.SRC_CD
 AND        T4.DATA_PLTF_SRC_TAB_NM = 'ECF_002_T01_CUST_INFO'
 AND        T4.Data_Pltf_Src_Fld_Nm ='CERT_TYPE'
 where      t.num=1
--t.updated_ts = '99991231 00:00:00:000000'
-- and        t.start_dt<='$TXDATE'::date
-- and        t.end_dt>'$TXDATE'::date
 ;
/*数据处理区END*/

 COMMIT;
