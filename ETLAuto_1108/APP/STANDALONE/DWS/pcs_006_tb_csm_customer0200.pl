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
DELETE FROM dw_sdata.PCS_006_TB_CSM_CUSTOMER WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.PCS_006_TB_CSM_CUSTOMER SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_331 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.PCS_006_TB_CSM_CUSTOMER WHERE 1=0;

--Step2:
INSERT  INTO T_331 (
  CUS_ID,
  CUS_NO,
  CUS_KIND,
  CUS_NAME,
  CUS_ENGNAME,
  CUS_PROPERTY,
  CUS_CLASS,
  CERTIFICATE_TYPE,
  CERTIFICATE_CODE,
  CERTIFICATE_VALID_DEADLINE,
  CREDITRATING,
  REGISTER_USER_ID,
  DEPOSIT_BALANCE,
  CREATE_TIME,
  UPDATE_TIME,
  CORP_CODE,
  DELFLAG,
  TRUNC_NO,
  CREDIT_NUM,
  REGIST_POSITION_CD,
  ORG_ID_MAINTAIN,
  start_dt,
  end_dt)
SELECT
  N.CUS_ID,
  N.CUS_NO,
  N.CUS_KIND,
  N.CUS_NAME,
  N.CUS_ENGNAME,
  N.CUS_PROPERTY,
  N.CUS_CLASS,
  N.CERTIFICATE_TYPE,
  N.CERTIFICATE_CODE,
  N.CERTIFICATE_VALID_DEADLINE,
  N.CREDITRATING,
  N.REGISTER_USER_ID,
  N.DEPOSIT_BALANCE,
  N.CREATE_TIME,
  N.UPDATE_TIME,
  N.CORP_CODE,
  N.DELFLAG,
  N.TRUNC_NO,
  N.CREDIT_NUM,
  N.REGIST_POSITION_CD,
  N.ORG_ID_MAINTAIN,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(CUS_ID, '' ) AS CUS_ID ,
  COALESCE(CUS_NO, '' ) AS CUS_NO ,
  COALESCE(CUS_KIND, '' ) AS CUS_KIND ,
  COALESCE(CUS_NAME, '' ) AS CUS_NAME ,
  COALESCE(CUS_ENGNAME, '' ) AS CUS_ENGNAME ,
  COALESCE(CUS_PROPERTY, '' ) AS CUS_PROPERTY ,
  COALESCE(CUS_CLASS, '' ) AS CUS_CLASS ,
  COALESCE(CERTIFICATE_TYPE, '' ) AS CERTIFICATE_TYPE ,
  COALESCE(CERTIFICATE_CODE, '' ) AS CERTIFICATE_CODE ,
  COALESCE(CERTIFICATE_VALID_DEADLINE,DATE('4999-12-31') ) AS CERTIFICATE_VALID_DEADLINE ,
  COALESCE(CREDITRATING, '' ) AS CREDITRATING ,
  COALESCE(REGISTER_USER_ID, '' ) AS REGISTER_USER_ID ,
  COALESCE(DEPOSIT_BALANCE, 0 ) AS DEPOSIT_BALANCE ,
  COALESCE(CREATE_TIME,'4999-12-31 00:00:00' ) AS CREATE_TIME ,
  COALESCE(UPDATE_TIME,'4999-12-31 00:00:00' ) AS UPDATE_TIME ,
  COALESCE(CORP_CODE, '' ) AS CORP_CODE ,
  COALESCE(DELFLAG, '' ) AS DELFLAG ,
  COALESCE(TRUNC_NO, 0 ) AS TRUNC_NO ,
  COALESCE(CREDIT_NUM, '' ) AS CREDIT_NUM ,
  COALESCE(REGIST_POSITION_CD, '' ) AS REGIST_POSITION_CD ,
  COALESCE(ORG_ID_MAINTAIN, '' ) AS ORG_ID_MAINTAIN 
 FROM  dw_tdata.PCS_006_TB_CSM_CUSTOMER_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  CUS_ID ,
  CUS_NO ,
  CUS_KIND ,
  CUS_NAME ,
  CUS_ENGNAME ,
  CUS_PROPERTY ,
  CUS_CLASS ,
  CERTIFICATE_TYPE ,
  CERTIFICATE_CODE ,
  CERTIFICATE_VALID_DEADLINE ,
  CREDITRATING ,
  REGISTER_USER_ID ,
  DEPOSIT_BALANCE ,
  CREATE_TIME ,
  UPDATE_TIME ,
  CORP_CODE ,
  DELFLAG ,
  TRUNC_NO ,
  CREDIT_NUM ,
  REGIST_POSITION_CD ,
  ORG_ID_MAINTAIN 
 FROM dw_sdata.PCS_006_TB_CSM_CUSTOMER 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.CUS_ID = T.CUS_ID
WHERE
(T.CUS_ID IS NULL)
 OR N.CUS_NO<>T.CUS_NO
 OR N.CUS_KIND<>T.CUS_KIND
 OR N.CUS_NAME<>T.CUS_NAME
 OR N.CUS_ENGNAME<>T.CUS_ENGNAME
 OR N.CUS_PROPERTY<>T.CUS_PROPERTY
 OR N.CUS_CLASS<>T.CUS_CLASS
 OR N.CERTIFICATE_TYPE<>T.CERTIFICATE_TYPE
 OR N.CERTIFICATE_CODE<>T.CERTIFICATE_CODE
 OR N.CERTIFICATE_VALID_DEADLINE<>T.CERTIFICATE_VALID_DEADLINE
 OR N.CREDITRATING<>T.CREDITRATING
 OR N.REGISTER_USER_ID<>T.REGISTER_USER_ID
 OR N.DEPOSIT_BALANCE<>T.DEPOSIT_BALANCE
 OR N.CREATE_TIME<>T.CREATE_TIME
 OR N.UPDATE_TIME<>T.UPDATE_TIME
 OR N.CORP_CODE<>T.CORP_CODE
 OR N.DELFLAG<>T.DELFLAG
 OR N.TRUNC_NO<>T.TRUNC_NO
 OR N.CREDIT_NUM<>T.CREDIT_NUM
 OR N.REGIST_POSITION_CD<>T.REGIST_POSITION_CD
 OR N.ORG_ID_MAINTAIN<>T.ORG_ID_MAINTAIN
;

--Step3:
UPDATE dw_sdata.PCS_006_TB_CSM_CUSTOMER P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_331
WHERE P.End_Dt=DATE('2100-12-31')
AND P.CUS_ID=T_331.CUS_ID
;

--Step4:
INSERT  INTO dw_sdata.PCS_006_TB_CSM_CUSTOMER SELECT * FROM T_331;

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
