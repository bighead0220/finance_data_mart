/*
Author             :XMH
Function           :柜员信息表
Load method        :INSERT
Source table       :dw_sdata.tgs_000_t_teller_info
Destination Table  :f_org_tellr
Frequency          :D
Modify history list:Created by徐铭浩2016年4月25日12:48:55
                   :Modify  by liudongyan 20160914  修改柜员号和所属机构的映射规则 变更记录205 
                    modified by wyh at 20160929 增�t7拉链
-------------------------------------------逻辑说明----------
业务逻辑说明
-------------------------------------------逻辑说明END------------------------------------------
*/
/*临时表*/
/*临时表end*/
/*数据回退区*/
Delete /* +direct */  from  f_fdm.f_org_tellr
where etl_date='$TXDATE'::date
;
/*数据回退区END*/
/*数据处理区*/
insert  /* +direct */ into f_fdm.f_org_tellr
(
      grp_typ                        --组别
      ,etl_date                      --数据日期
      ,tellr_num                     --柜员号
      ,tellr_nm                      --柜员姓名
      ,belg_org                      --所属机构
      ,belg_dept                     --所属部门
      ,Post_Sav_Bank_Ind             --邮银标志
      ,id_card_num                   --身份证号
      ,birth_dt                      --出生日期
      ,gender                        --性别
      ,edu_degr                      --学历
      ,tellr_post_cd                 --柜员岗位代码
      ,tellr_stat_cd                 --柜员状态代码
      ,tellr_typ_cd                  --柜员类型代码
      ,tel                           --电话
      ,family_addr                   --家庭住址
      ,gradt_acad                    --毕业院校
      ,majr                          --专业
      ,prtc_workday                  --参加工作日期
      ,in_line_workday               --入行工作日期
      ,contr_start_dt                --合同起始日期
      ,contr_termn_dt                --合同终止日期
      ,contr_term_typ_cd             --合同期限类型代码
      ,rgst_dt                       --注册日期
      ,rec_setup_tm                  --记录创建时间
      ,rec_modi_tm                   --记录修改时间
      ,cret                          --创建者
      ,final_mdfr                    --最后修改者
      ,sys_src                       --系统来源
)
select    
        '1'                               as    grp_typ
        ,'$TXDATE':: date        as    etl_date
        ,coalesce(T.code_,'')               as    tellr_num      
        ,coalesce(name_,'')               as    tellr_nm            
        ,coalesce(T7.code_,'')            as    belg_org            
        ,coalesce(dept_,'')               as    belg_dept           
        ,NVL(T1.TGT_CD,'@'||T.FLAG_)      as    Post_Sav_Bank_Ind            
        ,coalesce(id_card_,'')            as    id_card_num         
        ,coalesce(to_date(birthday_,'yyyymmdd'),to_date('','yyyymmdd')) as    birth_dt            
        ,NVL(T2.TGT_CD,'@'||T.sex_)       as    gender              
        ,NVL(T3.TGT_CD,'@'||T.diploma_)   as    edu_degr            
        ,coalesce(job_id_,'')             as    tellr_post_cd       
        ,NVL(T4.TGT_CD,'@'||T.status_)    as    tellr_stat_cd       
        ,NVL(T5.TGT_CD,'@'||T.type_)      as    tellr_typ_cd   
        ,coalesce(phone_,'')              as    tel                 
        ,coalesce(address_ ,'')           as    family_addr         
        ,coalesce(school_,'')             as    gradt_acad          
        ,coalesce(major_,'')              as    majr                
        ,case when work_date_=null then  '$MINDATE'::date else work_date_::date end  as   prtc_workday       
        ,case when join_date_=null then '$MINDATE'::date else join_date_::date end  as   in_line_workday     
        ,case when contract_start_date_=null then '$MINDATE'::date else contract_start_date_::date end  as    contr_start_dt      
        ,case when contract_end_date_=null then '$MINDATE'::date else contract_end_date_::date end as  contr_termn_dt      
        ,NVL(T6.TGT_CD,'@'||T.contract_type_)                                      as    contr_term_typ_cd  
        ,coalesce(to_date(register_date_,'yyyymmdd'),'$MINDATE'::date)       as    rgst_dt            
        ,coalesce(to_date(create_time_,'yyyymmdd'),'$MINDATE'::date)         as    rec_setup_tm        
        ,coalesce(to_date(update_time_,'yyyymmdd'),'$MINDATE'::date)         as    rec_modi_tm         
        ,coalesce(create_user_,'')                                                 as    cret                
        ,coalesce(update_user_,'')                                                 as    final_mdfr      
        ,'TGS'                                                                     as    sys_src   
from dw_sdata.tgs_000_t_teller_info T
left join dw_sdata.tgs_000_ADM_AGENCY T7
on  T.org_code_=T7.ID_
AND T7.START_DT<='$TXDATE'::date
AND T7.END_DT>'$TXDATE'::date    ----------modified by wyh 增�拉链
left join   f_fdm.CD_RF_STD_CD_TRAN_REF T1 --需转换代码表
  on T.FLAG_=T1.SRC_CD 
 and T1.DATA_PLTF_SRC_TAB_NM = 'TGS_000_T_TELLER_INFO' --数据平台源表主干名
 and T1.Data_Pltf_Src_Fld_Nm ='FLAG_'
left join f_fdm.CD_RF_STD_CD_TRAN_REF T2
  on T.SEX_=T2.SRC_CD 
 and T2.DATA_PLTF_SRC_TAB_NM = 'TGS_000_T_TELLER_INFO' --数据平台源表主干名
 and T2.Data_Pltf_Src_Fld_Nm ='SEX_'
left join f_fdm.CD_RF_STD_CD_TRAN_REF T3
  on T.DIPLOMA_=T3.SRC_CD 
 and T3.DATA_PLTF_SRC_TAB_NM = 'TGS_000_T_TELLER_INFO' --数据平台源表主干名
 and T3.Data_Pltf_Src_Fld_Nm ='DIPLOMA_'
left join f_fdm.CD_RF_STD_CD_TRAN_REF T4
  on T.STATUS_=T4.SRC_CD 
 and T4.DATA_PLTF_SRC_TAB_NM = 'TGS_000_T_TELLER_INFO' --数据平台源表主干名
 and T4.Data_Pltf_Src_Fld_Nm ='STATUS_'
left join f_fdm.CD_RF_STD_CD_TRAN_REF T5
  on T.TYPE_=T5.SRC_CD 
 and T5.DATA_PLTF_SRC_TAB_NM = 'TGS_000_T_TELLER_INFO' --数据平台源表主干名
 and T5.Data_Pltf_Src_Fld_Nm ='TYPE_'
left join f_fdm.CD_RF_STD_CD_TRAN_REF T6
  on T.CONTRACT_TYPE_=T6.SRC_CD 
 and T6.DATA_PLTF_SRC_TAB_NM = 'TGS_000_T_TELLER_INFO' --数据平台源表主干名
 and T6.Data_Pltf_Src_Fld_Nm ='CONTRACT_TYPE_'
where T.START_DT<='$TXDATE'::date
and '$TXDATE'::date<T.end_dt ;
 /*数据回退区END*/                   
commit;
                                                          
