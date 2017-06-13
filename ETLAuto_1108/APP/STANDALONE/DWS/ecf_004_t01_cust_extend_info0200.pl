#!/usr/bin/perl

use strict;     # Declare using Perl strict syntax
use DBI;        # If you are using other package, declare here

# ------------ Variable Section ------------
my ${AUTO_HOME} = $ENV{"AUTO_HOME"};

my ${WML_DB} = $ENV{"AUTO_WML_DB"};
if ( !defined(${WML_DB}) ) {
    ${WML_DB} = "WML";
}
my ${WTL_DB} = $ENV{"AUTO_WTL_DB"};
if ( !defined(${WTL_DB}) ) {
    ${WTL_DB} = "WTL";
}
my ${WMLVIEW_DB} = $ENV{"AUTO_WMLVIEW_DB"};
if ( !defined(${WMLVIEW_DB}) ) {
    ${WMLVIEW_DB} = "WMLVIEW";
}
my ${WTLVIEW_DB} = $ENV{"AUTO_WTLVIEW_DB"};
if ( !defined(${WTLVIEW_DB}) ) {
    ${WTLVIEW_DB} = "WTLVIEW";
}

my ${NULL_DATE} = "1900-01-02";
my ${MIN_DATE} = "1900-01-01";
my ${MAX_DATE} = "2100-12-31";

my ${LOGON_FILE} = "${AUTO_HOME}/etc/VERTICA_LOGON";
my ${LOGON_STR};
my ${CONTROL_FILE};
my ${TX_DATE};
my ${TX_DATE_YYYYMMDD};
my ${TX_MON_DAY_MMDD};

# ------------ VSQL function ------------
sub run_vsql_command
{
  #my $rc = open(VSQL, "${LOGON_STR}");
  my $rc = open(VSQL, "|vsql -h 22.224.65.171 -p 5433 -d CPCIMDB_TEST -U dwtrans -w dwtrans2016");

  unless ($rc) {
      print "Could not invoke VSQL command
";
      return -1;
  }

# ------ Below are VSQL scripts ----------
  print VSQL <<ENDOFINPUT;

\\set ON_ERROR_STOP on

--Step0:
DELETE FROM dw_sdata.ECF_004_T01_CUST_EXTEND_INFO WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.ECF_004_T01_CUST_EXTEND_INFO SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_169 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.ECF_004_T01_CUST_EXTEND_INFO WHERE 1=0;

--Step2:
INSERT  INTO T_169 (
  PARTY_ID,
  EDU_STATE,
  GRAD_SCHOOL,
  ENG_SCHOOL,
  STUDYINST,
  STUDYSPEC,
  IDVU_SCL_INSURS_NO,
  POSTON,
  TECH_TITLE,
  PRFSSN_LEVEL,
  AREA_CODE,
  NO_OF_DPND,
  YEAR_SALARY,
  FAM_SALARY,
  FAM_ASSETS,
  ECON_RESUR,
  BEST_CALL_TIME,
  REG_TIME,
  RISK_CHG_DATE,
  INDUSTRY_TYPE,
  ECONOMIC_NATURE_TYPE,
  DEPARTMENT,
  EMPLOYER,
  SPOUSE_NAME,
  LANGUAGE,
  CALLED,
  SHORT_NAME,
  FI_EMP_FLAG,
  SALES_ID,
  MANAGER_CODE,
  POINT,
  WILL_VIP_FLAG,
  RSD_BEGIN_TIME,
  IN_COUNTRY_FLAG,
  COUNTRY,
  LICENCE,
  IDVU_TX_NO,
  WORK_BEGIN_DATE,
  CORP_SIZE,
  RELIG_CDE,
  SPOUSE_CERT_ID,
  SPOUSE_CERT_TYPE,
  HEALTH_STATE,
  CHECK_FLAG,
  CBU_FLAG,
  FILLER1,
  FILLER2,
  FILLER3,
  FILLER4,
  LAST_UPDATED_TE,
  LAST_UPDATED_ORG,
  CREATED_TS,
  UPDATED_TS,
  INIT_SYSTEM_ID,
  INIT_CREATED_TS,
  LAST_SYSTEM_ID,
  LAST_UPDATED_TS,
  LIMIT_FLAG,
  LIMIT_REASON,
  LIMIT_FILE_NAME,
  start_dt,
  end_dt)
SELECT
  N.PARTY_ID,
  N.EDU_STATE,
  N.GRAD_SCHOOL,
  N.ENG_SCHOOL,
  N.STUDYINST,
  N.STUDYSPEC,
  N.IDVU_SCL_INSURS_NO,
  N.POSTON,
  N.TECH_TITLE,
  N.PRFSSN_LEVEL,
  N.AREA_CODE,
  N.NO_OF_DPND,
  N.YEAR_SALARY,
  N.FAM_SALARY,
  N.FAM_ASSETS,
  N.ECON_RESUR,
  N.BEST_CALL_TIME,
  N.REG_TIME,
  N.RISK_CHG_DATE,
  N.INDUSTRY_TYPE,
  N.ECONOMIC_NATURE_TYPE,
  N.DEPARTMENT,
  N.EMPLOYER,
  N.SPOUSE_NAME,
  N.LANGUAGE,
  N.CALLED,
  N.SHORT_NAME,
  N.FI_EMP_FLAG,
  N.SALES_ID,
  N.MANAGER_CODE,
  N.POINT,
  N.WILL_VIP_FLAG,
  N.RSD_BEGIN_TIME,
  N.IN_COUNTRY_FLAG,
  N.COUNTRY,
  N.LICENCE,
  N.IDVU_TX_NO,
  N.WORK_BEGIN_DATE,
  N.CORP_SIZE,
  N.RELIG_CDE,
  N.SPOUSE_CERT_ID,
  N.SPOUSE_CERT_TYPE,
  N.HEALTH_STATE,
  N.CHECK_FLAG,
  N.CBU_FLAG,
  N.FILLER1,
  N.FILLER2,
  N.FILLER3,
  N.FILLER4,
  N.LAST_UPDATED_TE,
  N.LAST_UPDATED_ORG,
  N.CREATED_TS,
  N.UPDATED_TS,
  N.INIT_SYSTEM_ID,
  N.INIT_CREATED_TS,
  N.LAST_SYSTEM_ID,
  N.LAST_UPDATED_TS,
  N.LIMIT_FLAG,
  N.LIMIT_REASON,
  N.LIMIT_FILE_NAME,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(PARTY_ID, '' ) AS PARTY_ID ,
  COALESCE(EDU_STATE, '' ) AS EDU_STATE ,
  COALESCE(GRAD_SCHOOL, '' ) AS GRAD_SCHOOL ,
  COALESCE(ENG_SCHOOL, '' ) AS ENG_SCHOOL ,
  COALESCE(STUDYINST, '' ) AS STUDYINST ,
  COALESCE(STUDYSPEC, '' ) AS STUDYSPEC ,
  COALESCE(IDVU_SCL_INSURS_NO, '' ) AS IDVU_SCL_INSURS_NO ,
  COALESCE(POSTON, '' ) AS POSTON ,
  COALESCE(TECH_TITLE, '' ) AS TECH_TITLE ,
  COALESCE(PRFSSN_LEVEL, '' ) AS PRFSSN_LEVEL ,
  COALESCE(AREA_CODE, '' ) AS AREA_CODE ,
  COALESCE(NO_OF_DPND, '' ) AS NO_OF_DPND ,
  COALESCE(YEAR_SALARY, 0 ) AS YEAR_SALARY ,
  COALESCE(FAM_SALARY, 0 ) AS FAM_SALARY ,
  COALESCE(FAM_ASSETS, 0 ) AS FAM_ASSETS ,
  COALESCE(ECON_RESUR, '' ) AS ECON_RESUR ,
  COALESCE(BEST_CALL_TIME, '' ) AS BEST_CALL_TIME ,
  COALESCE(REG_TIME, '' ) AS REG_TIME ,
  COALESCE(RISK_CHG_DATE, '' ) AS RISK_CHG_DATE ,
  COALESCE(INDUSTRY_TYPE, '' ) AS INDUSTRY_TYPE ,
  COALESCE(ECONOMIC_NATURE_TYPE, '' ) AS ECONOMIC_NATURE_TYPE ,
  COALESCE(DEPARTMENT, '' ) AS DEPARTMENT ,
  COALESCE(EMPLOYER, '' ) AS EMPLOYER ,
  COALESCE(SPOUSE_NAME, '' ) AS SPOUSE_NAME ,
  COALESCE(LANGUAGE, '' ) AS LANGUAGE ,
  COALESCE(CALLED, '' ) AS CALLED ,
  COALESCE(SHORT_NAME, '' ) AS SHORT_NAME ,
  COALESCE(FI_EMP_FLAG, '' ) AS FI_EMP_FLAG ,
  COALESCE(SALES_ID, '' ) AS SALES_ID ,
  COALESCE(MANAGER_CODE, '' ) AS MANAGER_CODE ,
  COALESCE(POINT, 0 ) AS POINT ,
  COALESCE(WILL_VIP_FLAG, '' ) AS WILL_VIP_FLAG ,
  COALESCE(RSD_BEGIN_TIME, '' ) AS RSD_BEGIN_TIME ,
  COALESCE(IN_COUNTRY_FLAG, '' ) AS IN_COUNTRY_FLAG ,
  COALESCE(COUNTRY, '' ) AS COUNTRY ,
  COALESCE(LICENCE, '' ) AS LICENCE ,
  COALESCE(IDVU_TX_NO, '' ) AS IDVU_TX_NO ,
  COALESCE(WORK_BEGIN_DATE, '' ) AS WORK_BEGIN_DATE ,
  COALESCE(CORP_SIZE, '' ) AS CORP_SIZE ,
  COALESCE(RELIG_CDE, '' ) AS RELIG_CDE ,
  COALESCE(SPOUSE_CERT_ID, '' ) AS SPOUSE_CERT_ID ,
  COALESCE(SPOUSE_CERT_TYPE, '' ) AS SPOUSE_CERT_TYPE ,
  COALESCE(HEALTH_STATE, '' ) AS HEALTH_STATE ,
  COALESCE(CHECK_FLAG, '' ) AS CHECK_FLAG ,
  COALESCE(CBU_FLAG, '' ) AS CBU_FLAG ,
  COALESCE(FILLER1, '' ) AS FILLER1 ,
  COALESCE(FILLER2, '' ) AS FILLER2 ,
  COALESCE(FILLER3, 0 ) AS FILLER3 ,
  COALESCE(FILLER4, 0 ) AS FILLER4 ,
  COALESCE(LAST_UPDATED_TE, '' ) AS LAST_UPDATED_TE ,
  COALESCE(LAST_UPDATED_ORG, '' ) AS LAST_UPDATED_ORG ,
  COALESCE(CREATED_TS, '4999-12-31 00:00:00' ) AS CREATED_TS ,
  COALESCE(UPDATED_TS, '4999-12-31 00:00:00' ) AS UPDATED_TS ,
  COALESCE(INIT_SYSTEM_ID, '' ) AS INIT_SYSTEM_ID ,
  COALESCE(INIT_CREATED_TS, '4999-12-31 00:00:00' ) AS INIT_CREATED_TS ,
  COALESCE(LAST_SYSTEM_ID, '' ) AS LAST_SYSTEM_ID ,
  COALESCE(LAST_UPDATED_TS, '4999-12-31 00:00:00' ) AS LAST_UPDATED_TS , 
  COALESCE(LIMIT_FLAG,'') AS LIMIT_FLAG,
  COALESCE(LIMIT_REASON,'') AS LIMIT_REASON,
  COALESCE(LIMIT_FILE_NAME,'') AS LIMIT_FILE_NAME
 FROM  dw_tdata.ECF_004_T01_CUST_EXTEND_INFO_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  PARTY_ID ,
  EDU_STATE ,
  GRAD_SCHOOL ,
  ENG_SCHOOL ,
  STUDYINST ,
  STUDYSPEC ,
  IDVU_SCL_INSURS_NO ,
  POSTON ,
  TECH_TITLE ,
  PRFSSN_LEVEL ,
  AREA_CODE ,
  NO_OF_DPND ,
  YEAR_SALARY ,
  FAM_SALARY ,
  FAM_ASSETS ,
  ECON_RESUR ,
  BEST_CALL_TIME ,
  REG_TIME ,
  RISK_CHG_DATE ,
  INDUSTRY_TYPE ,
  ECONOMIC_NATURE_TYPE ,
  DEPARTMENT ,
  EMPLOYER ,
  SPOUSE_NAME ,
  LANGUAGE ,
  CALLED ,
  SHORT_NAME ,
  FI_EMP_FLAG ,
  SALES_ID ,
  MANAGER_CODE ,
  POINT ,
  WILL_VIP_FLAG ,
  RSD_BEGIN_TIME ,
  IN_COUNTRY_FLAG ,
  COUNTRY ,
  LICENCE ,
  IDVU_TX_NO ,
  WORK_BEGIN_DATE ,
  CORP_SIZE ,
  RELIG_CDE ,
  SPOUSE_CERT_ID ,
  SPOUSE_CERT_TYPE ,
  HEALTH_STATE ,
  CHECK_FLAG ,
  CBU_FLAG ,
  FILLER1 ,
  FILLER2 ,
  FILLER3 ,
  FILLER4 ,
  LAST_UPDATED_TE ,
  LAST_UPDATED_ORG ,
  CREATED_TS ,
  UPDATED_TS ,
  INIT_SYSTEM_ID ,
  INIT_CREATED_TS ,
  LAST_SYSTEM_ID ,
  LAST_UPDATED_TS ,
  LIMIT_FLAG,
  LIMIT_REASON,
  LIMIT_FILE_NAME
 FROM dw_sdata.ECF_004_T01_CUST_EXTEND_INFO 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.PARTY_ID = T.PARTY_ID
WHERE
(T.PARTY_ID IS NULL)
 OR N.EDU_STATE<>T.EDU_STATE
 OR N.GRAD_SCHOOL<>T.GRAD_SCHOOL
 OR N.ENG_SCHOOL<>T.ENG_SCHOOL
 OR N.STUDYINST<>T.STUDYINST
 OR N.STUDYSPEC<>T.STUDYSPEC
 OR N.IDVU_SCL_INSURS_NO<>T.IDVU_SCL_INSURS_NO
 OR N.POSTON<>T.POSTON
 OR N.TECH_TITLE<>T.TECH_TITLE
 OR N.PRFSSN_LEVEL<>T.PRFSSN_LEVEL
 OR N.AREA_CODE<>T.AREA_CODE
 OR N.NO_OF_DPND<>T.NO_OF_DPND
 OR N.YEAR_SALARY<>T.YEAR_SALARY
 OR N.FAM_SALARY<>T.FAM_SALARY
 OR N.FAM_ASSETS<>T.FAM_ASSETS
 OR N.ECON_RESUR<>T.ECON_RESUR
 OR N.BEST_CALL_TIME<>T.BEST_CALL_TIME
 OR N.REG_TIME<>T.REG_TIME
 OR N.RISK_CHG_DATE<>T.RISK_CHG_DATE
 OR N.INDUSTRY_TYPE<>T.INDUSTRY_TYPE
 OR N.ECONOMIC_NATURE_TYPE<>T.ECONOMIC_NATURE_TYPE
 OR N.DEPARTMENT<>T.DEPARTMENT
 OR N.EMPLOYER<>T.EMPLOYER
 OR N.SPOUSE_NAME<>T.SPOUSE_NAME
 OR N.LANGUAGE<>T.LANGUAGE
 OR N.CALLED<>T.CALLED
 OR N.SHORT_NAME<>T.SHORT_NAME
 OR N.FI_EMP_FLAG<>T.FI_EMP_FLAG
 OR N.SALES_ID<>T.SALES_ID
 OR N.MANAGER_CODE<>T.MANAGER_CODE
 OR N.POINT<>T.POINT
 OR N.WILL_VIP_FLAG<>T.WILL_VIP_FLAG
 OR N.RSD_BEGIN_TIME<>T.RSD_BEGIN_TIME
 OR N.IN_COUNTRY_FLAG<>T.IN_COUNTRY_FLAG
 OR N.COUNTRY<>T.COUNTRY
 OR N.LICENCE<>T.LICENCE
 OR N.IDVU_TX_NO<>T.IDVU_TX_NO
 OR N.WORK_BEGIN_DATE<>T.WORK_BEGIN_DATE
 OR N.CORP_SIZE<>T.CORP_SIZE
 OR N.RELIG_CDE<>T.RELIG_CDE
 OR N.SPOUSE_CERT_ID<>T.SPOUSE_CERT_ID
 OR N.SPOUSE_CERT_TYPE<>T.SPOUSE_CERT_TYPE
 OR N.HEALTH_STATE<>T.HEALTH_STATE
 OR N.CHECK_FLAG<>T.CHECK_FLAG
 OR N.CBU_FLAG<>T.CBU_FLAG
 OR N.FILLER1<>T.FILLER1
 OR N.FILLER2<>T.FILLER2
 OR N.FILLER3<>T.FILLER3
 OR N.FILLER4<>T.FILLER4
 OR N.LAST_UPDATED_TE<>T.LAST_UPDATED_TE
 OR N.LAST_UPDATED_ORG<>T.LAST_UPDATED_ORG
 OR N.CREATED_TS<>T.CREATED_TS
 OR N.UPDATED_TS<>T.UPDATED_TS
 OR N.INIT_SYSTEM_ID<>T.INIT_SYSTEM_ID
 OR N.INIT_CREATED_TS<>T.INIT_CREATED_TS
 OR N.LAST_SYSTEM_ID<>T.LAST_SYSTEM_ID
 OR N.LAST_UPDATED_TS<>T.LAST_UPDATED_TS
 OR N.LIMIT_FLAG<>T.LIMIT_FLAG
 OR N.LIMIT_REASON<>T.LIMIT_REASON
 OR N.LIMIT_FILE_NAME<>T.LIMIT_FILE_NAME
;

--Step3:
UPDATE dw_sdata.ECF_004_T01_CUST_EXTEND_INFO P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_169
WHERE P.End_Dt=DATE('2100-12-31')
AND P.PARTY_ID=T_169.PARTY_ID
;

--Step4:
INSERT  INTO dw_sdata.ECF_004_T01_CUST_EXTEND_INFO SELECT * FROM T_169;

COMMIT;

ENDOFINPUT

  close(VSQL);

  my $RET_CODE = $? >> 8;

  if ( $RET_CODE == 0 ) {
      return 0;
  }
  else {
      return 1;
  }
}

# ------------ main function ------------
sub main
{
   my $ret;
   open(LOGONFILE_H, "${LOGON_FILE}");
   ${LOGON_STR} = <LOGONFILE_H>;
   close(LOGONFILE_H);
   
   # Get the decoded logon string
   my($user,$passwd) = split(',',${LOGON_STR}); 
   #my $decodepasswd = `${AUTO_HOME}/bin/IceCode.exe -d "$passwd" "$user"`;                     
   #${LOGON_STR} = "|vsql -h 192.168.2.44 -p 5433 -d CPCIMDB_TEST -U ".$user." -w ".$decodepasswd;

   # Call vsql command to load data
   $ret = run_vsql_command();

   print "run_vsql_command() = $ret";
   return $ret;
}

# ------------ program section ------------
if ( $#ARGV < 0 ) {
   print "Usage: [perl ������ Control_File] (Control_File format: dir.jobnameYYYYMMDD or sysname_jobname_YYYYMMDD.dir) 
";
   print "
";
   exit(1);
}

# Get the first argument
${CONTROL_FILE} = $ARGV[0];

if (${CONTROL_FILE} =~/[0-9]{8}($|\.)/) {
   ${TX_DATE_YYYYMMDD} = substr($&,0,8);
}
else{
   print "Usage: [perl ������ Control_File] (Control_File format: dir.jobnameYYYYMMDD or sysname_jobname_YYYYMMDD.dir) 
";
   print "
";
   exit(1);
}

${TX_MON_DAY_MMDD} = substr(${TX_DATE_YYYYMMDD}, length(${TX_DATE_YYYYMMDD})-4,4);
${TX_DATE} = substr(${TX_DATE_YYYYMMDD}, 0, 4)."-".substr(${TX_DATE_YYYYMMDD}, 4, 2)."-".substr(${TX_DATE_YYYYMMDD}, 6, 2);
open(STDERR, ">&STDOUT");

my $ret = main();

exit($ret);