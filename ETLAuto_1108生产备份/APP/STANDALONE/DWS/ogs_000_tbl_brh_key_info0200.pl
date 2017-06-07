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
DELETE FROM dw_sdata.OGS_000_TBL_BRH_KEY_INFO WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.OGS_000_TBL_BRH_KEY_INFO SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_309 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.OGS_000_TBL_BRH_KEY_INFO WHERE 1=0;

--Step2:
INSERT  INTO T_309 (
  BRH_CODE,
  PBC_BRH_CODE,
  BRH_TYPE,
  BRH_SUB_TYPE,
  UP_BRH_CODE,
  BRH_NAME,
  BRH_SHT_NAME,
  BRH_ENG_NAME,
  BRH_ADDR,
  BRH_LVL,
  BRH_STEP,
  BRH_ABLI,
  BRH_ATTR,
  BRH_STAT,
  BLG_FLAG,
  RGLM_CODE,
  ACCT_CODE,
  AREA_ATTR,
  BRH_SYS,
  INTNAL_FINC_BRH_CODE,
  PBC_PAY_CODE,
  BRH_PROVINCE,
  BRH_POST_CODE,
  PROV_RGLM_CODE,
  CITY_RGLM_CODE,
  LAST_UPD_TM,
  LAST_UPD_OPR,
  LAST_UPD_TXN,
  RESV1,
  start_dt,
  end_dt)
SELECT
  N.BRH_CODE,
  N.PBC_BRH_CODE,
  N.BRH_TYPE,
  N.BRH_SUB_TYPE,
  N.UP_BRH_CODE,
  N.BRH_NAME,
  N.BRH_SHT_NAME,
  N.BRH_ENG_NAME,
  N.BRH_ADDR,
  N.BRH_LVL,
  N.BRH_STEP,
  N.BRH_ABLI,
  N.BRH_ATTR,
  N.BRH_STAT,
  N.BLG_FLAG,
  N.RGLM_CODE,
  N.ACCT_CODE,
  N.AREA_ATTR,
  N.BRH_SYS,
  N.INTNAL_FINC_BRH_CODE,
  N.PBC_PAY_CODE,
  N.BRH_PROVINCE,
  N.BRH_POST_CODE,
  N.PROV_RGLM_CODE,
  N.CITY_RGLM_CODE,
  N.LAST_UPD_TM,
  N.LAST_UPD_OPR,
  N.LAST_UPD_TXN,
  N.RESV1,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(BRH_CODE, '' ) AS BRH_CODE ,
  COALESCE(PBC_BRH_CODE, '' ) AS PBC_BRH_CODE ,
  COALESCE(BRH_TYPE, '' ) AS BRH_TYPE ,
  COALESCE(BRH_SUB_TYPE, '' ) AS BRH_SUB_TYPE ,
  COALESCE(UP_BRH_CODE, '' ) AS UP_BRH_CODE ,
  COALESCE(BRH_NAME, '' ) AS BRH_NAME ,
  COALESCE(BRH_SHT_NAME, '' ) AS BRH_SHT_NAME ,
  COALESCE(BRH_ENG_NAME, '' ) AS BRH_ENG_NAME ,
  COALESCE(BRH_ADDR, '' ) AS BRH_ADDR ,
  COALESCE(BRH_LVL, '' ) AS BRH_LVL ,
  COALESCE(BRH_STEP, '' ) AS BRH_STEP ,
  COALESCE(BRH_ABLI, '' ) AS BRH_ABLI ,
  COALESCE(BRH_ATTR, '' ) AS BRH_ATTR ,
  COALESCE(BRH_STAT, '' ) AS BRH_STAT ,
  COALESCE(BLG_FLAG, '' ) AS BLG_FLAG ,
  COALESCE(RGLM_CODE, '' ) AS RGLM_CODE ,
  COALESCE(ACCT_CODE, '' ) AS ACCT_CODE ,
  COALESCE(AREA_ATTR, '' ) AS AREA_ATTR ,
  COALESCE(BRH_SYS, '' ) AS BRH_SYS ,
  COALESCE(INTNAL_FINC_BRH_CODE, '' ) AS INTNAL_FINC_BRH_CODE ,
  COALESCE(PBC_PAY_CODE, '' ) AS PBC_PAY_CODE ,
  COALESCE(BRH_PROVINCE, '' ) AS BRH_PROVINCE ,
  COALESCE(BRH_POST_CODE, '' ) AS BRH_POST_CODE ,
  COALESCE(PROV_RGLM_CODE, '' ) AS PROV_RGLM_CODE ,
  COALESCE(CITY_RGLM_CODE, '' ) AS CITY_RGLM_CODE ,
  COALESCE(LAST_UPD_TM, '' ) AS LAST_UPD_TM ,
  COALESCE(LAST_UPD_OPR, '' ) AS LAST_UPD_OPR ,
  COALESCE(LAST_UPD_TXN, '' ) AS LAST_UPD_TXN ,
  COALESCE(RESV1, '' ) AS RESV1 
 FROM  dw_tdata.OGS_000_TBL_BRH_KEY_INFO_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  BRH_CODE ,
  PBC_BRH_CODE ,
  BRH_TYPE ,
  BRH_SUB_TYPE ,
  UP_BRH_CODE ,
  BRH_NAME ,
  BRH_SHT_NAME ,
  BRH_ENG_NAME ,
  BRH_ADDR ,
  BRH_LVL ,
  BRH_STEP ,
  BRH_ABLI ,
  BRH_ATTR ,
  BRH_STAT ,
  BLG_FLAG ,
  RGLM_CODE ,
  ACCT_CODE ,
  AREA_ATTR ,
  BRH_SYS ,
  INTNAL_FINC_BRH_CODE ,
  PBC_PAY_CODE ,
  BRH_PROVINCE ,
  BRH_POST_CODE ,
  PROV_RGLM_CODE ,
  CITY_RGLM_CODE ,
  LAST_UPD_TM ,
  LAST_UPD_OPR ,
  LAST_UPD_TXN ,
  RESV1 
 FROM dw_sdata.OGS_000_TBL_BRH_KEY_INFO 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.BRH_CODE = T.BRH_CODE
WHERE
(T.BRH_CODE IS NULL)
 OR N.PBC_BRH_CODE<>T.PBC_BRH_CODE
 OR N.BRH_TYPE<>T.BRH_TYPE
 OR N.BRH_SUB_TYPE<>T.BRH_SUB_TYPE
 OR N.UP_BRH_CODE<>T.UP_BRH_CODE
 OR N.BRH_NAME<>T.BRH_NAME
 OR N.BRH_SHT_NAME<>T.BRH_SHT_NAME
 OR N.BRH_ENG_NAME<>T.BRH_ENG_NAME
 OR N.BRH_ADDR<>T.BRH_ADDR
 OR N.BRH_LVL<>T.BRH_LVL
 OR N.BRH_STEP<>T.BRH_STEP
 OR N.BRH_ABLI<>T.BRH_ABLI
 OR N.BRH_ATTR<>T.BRH_ATTR
 OR N.BRH_STAT<>T.BRH_STAT
 OR N.BLG_FLAG<>T.BLG_FLAG
 OR N.RGLM_CODE<>T.RGLM_CODE
 OR N.ACCT_CODE<>T.ACCT_CODE
 OR N.AREA_ATTR<>T.AREA_ATTR
 OR N.BRH_SYS<>T.BRH_SYS
 OR N.INTNAL_FINC_BRH_CODE<>T.INTNAL_FINC_BRH_CODE
 OR N.PBC_PAY_CODE<>T.PBC_PAY_CODE
 OR N.BRH_PROVINCE<>T.BRH_PROVINCE
 OR N.BRH_POST_CODE<>T.BRH_POST_CODE
 OR N.PROV_RGLM_CODE<>T.PROV_RGLM_CODE
 OR N.CITY_RGLM_CODE<>T.CITY_RGLM_CODE
 OR N.LAST_UPD_TM<>T.LAST_UPD_TM
 OR N.LAST_UPD_OPR<>T.LAST_UPD_OPR
 OR N.LAST_UPD_TXN<>T.LAST_UPD_TXN
 OR N.RESV1<>T.RESV1
;

--Step3:
UPDATE dw_sdata.OGS_000_TBL_BRH_KEY_INFO P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_309
WHERE P.End_Dt=DATE('2100-12-31')
AND P.BRH_CODE=T_309.BRH_CODE
;

--Step4:
INSERT  INTO dw_sdata.OGS_000_TBL_BRH_KEY_INFO SELECT * FROM T_309;

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
   print "Usage: [perl 程序名 Control_File] (Control_File format: dir.jobnameYYYYMMDD or sysname_jobname_YYYYMMDD.dir) 
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
   print "Usage: [perl 程序名 Control_File] (Control_File format: dir.jobnameYYYYMMDD or sysname_jobname_YYYYMMDD.dir) 
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
