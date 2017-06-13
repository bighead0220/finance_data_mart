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
DELETE FROM dw_sdata.PCS_006_TB_ABS_LOAN_INFO WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.PCS_006_TB_ABS_LOAN_INFO SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_327 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.PCS_006_TB_ABS_LOAN_INFO WHERE 1=0;

--Step2:
INSERT  INTO T_327 (
  DUEBILL_NO,
  DUEBILL_ID,
  LOAN_ID,
  LOAN_CONTRACT_NO,
  IS_ABS,
  CURR_STATUS,
  LOAN_SECU_NO,
  CUR_ORG_ID,
  PROVINCE_NUM,
  PACK_TRANS_DATE,
  PACKET_DATE,
  SETT_TRANS_DATE,
  SETT_DATE,
  LOAN_BACK_NUM,
  BACK_TRANS_DATE,
  BACK_DATE,
  SECU_NUM,
  SECU_NAME,
  IN_RCV_ACC,
  IN_RCV_ACC_DEP,
  IN_RCV_ACC_NAME,
  IN_PAY_ACC,
  IN_PAY_ACC_DEP,
  IN_PAY_ACC_NAME,
  BAL_IN_ACC,
  BAL_IN_ACC_DEP,
  BAL_IN_ACC_NAME,
  ITR_IN_ACC,
  ITR_IN_ACC_DEP,
  ITR_IN_ACC_NAME,
  BACK_TYPE,
  IS_QUALIFIED,
  FAIL_REASON,
  DELFLAG,
  CREATE_TIME,
  UPDATE_TIME,
  TRUNC_NO,
  FRE_ICM_IN_ACC,
  FRE_ICM_IN_ACC_DEP,
  FRE_ICM_IN_ACC_NAME,
  RMK_ICM_IN_ACC,
  RMK_ICM_IN_ACC_DEP,
  RMK_ICM_IN_ACC_NAME,
  start_dt,
  end_dt)
SELECT
  N.DUEBILL_NO,
  N.DUEBILL_ID,
  N.LOAN_ID,
  N.LOAN_CONTRACT_NO,
  N.IS_ABS,
  N.CURR_STATUS,
  N.LOAN_SECU_NO,
  N.CUR_ORG_ID,
  N.PROVINCE_NUM,
  N.PACK_TRANS_DATE,
  N.PACKET_DATE,
  N.SETT_TRANS_DATE,
  N.SETT_DATE,
  N.LOAN_BACK_NUM,
  N.BACK_TRANS_DATE,
  N.BACK_DATE,
  N.SECU_NUM,
  N.SECU_NAME,
  N.IN_RCV_ACC,
  N.IN_RCV_ACC_DEP,
  N.IN_RCV_ACC_NAME,
  N.IN_PAY_ACC,
  N.IN_PAY_ACC_DEP,
  N.IN_PAY_ACC_NAME,
  N.BAL_IN_ACC,
  N.BAL_IN_ACC_DEP,
  N.BAL_IN_ACC_NAME,
  N.ITR_IN_ACC,
  N.ITR_IN_ACC_DEP,
  N.ITR_IN_ACC_NAME,
  N.BACK_TYPE,
  N.IS_QUALIFIED,
  N.FAIL_REASON,
  N.DELFLAG,
  N.CREATE_TIME,
  N.UPDATE_TIME,
  N.TRUNC_NO,
  N.FRE_ICM_IN_ACC,
  N.FRE_ICM_IN_ACC_DEP,
  N.FRE_ICM_IN_ACC_NAME,
  N.RMK_ICM_IN_ACC,
  N.RMK_ICM_IN_ACC_DEP,
  N.RMK_ICM_IN_ACC_NAME,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(DUEBILL_NO, '' ) AS DUEBILL_NO ,
  COALESCE(DUEBILL_ID, '' ) AS DUEBILL_ID ,
  COALESCE(LOAN_ID, '' ) AS LOAN_ID ,
  COALESCE(LOAN_CONTRACT_NO, '' ) AS LOAN_CONTRACT_NO ,
  COALESCE(IS_ABS, '' ) AS IS_ABS ,
  COALESCE(CURR_STATUS, '' ) AS CURR_STATUS ,
  COALESCE(LOAN_SECU_NO, '' ) AS LOAN_SECU_NO ,
  COALESCE(CUR_ORG_ID, '' ) AS CUR_ORG_ID ,
  COALESCE(PROVINCE_NUM, '' ) AS PROVINCE_NUM ,
  COALESCE(PACK_TRANS_DATE,DATE('4999-12-31') ) AS PACK_TRANS_DATE ,
  COALESCE(PACKET_DATE,DATE('4999-12-31') ) AS PACKET_DATE ,
  COALESCE(SETT_TRANS_DATE,DATE('4999-12-31') ) AS SETT_TRANS_DATE ,
  COALESCE(SETT_DATE,DATE('4999-12-31') ) AS SETT_DATE ,
  COALESCE(LOAN_BACK_NUM, '' ) AS LOAN_BACK_NUM ,
  COALESCE(BACK_TRANS_DATE,DATE('4999-12-31') ) AS BACK_TRANS_DATE ,
  COALESCE(BACK_DATE,DATE('4999-12-31') ) AS BACK_DATE ,
  COALESCE(SECU_NUM, '' ) AS SECU_NUM ,
  COALESCE(SECU_NAME, '' ) AS SECU_NAME ,
  COALESCE(IN_RCV_ACC, '' ) AS IN_RCV_ACC ,
  COALESCE(IN_RCV_ACC_DEP, '' ) AS IN_RCV_ACC_DEP ,
  COALESCE(IN_RCV_ACC_NAME, '' ) AS IN_RCV_ACC_NAME ,
  COALESCE(IN_PAY_ACC, '' ) AS IN_PAY_ACC ,
  COALESCE(IN_PAY_ACC_DEP, '' ) AS IN_PAY_ACC_DEP ,
  COALESCE(IN_PAY_ACC_NAME, '' ) AS IN_PAY_ACC_NAME ,
  COALESCE(BAL_IN_ACC, '' ) AS BAL_IN_ACC ,
  COALESCE(BAL_IN_ACC_DEP, '' ) AS BAL_IN_ACC_DEP ,
  COALESCE(BAL_IN_ACC_NAME, '' ) AS BAL_IN_ACC_NAME ,
  COALESCE(ITR_IN_ACC, '' ) AS ITR_IN_ACC ,
  COALESCE(ITR_IN_ACC_DEP, '' ) AS ITR_IN_ACC_DEP ,
  COALESCE(ITR_IN_ACC_NAME, '' ) AS ITR_IN_ACC_NAME ,
  COALESCE(BACK_TYPE, '' ) AS BACK_TYPE ,
  COALESCE(IS_QUALIFIED, '' ) AS IS_QUALIFIED ,
  COALESCE(FAIL_REASON, '' ) AS FAIL_REASON ,
  COALESCE(DELFLAG, '' ) AS DELFLAG ,
  COALESCE(CREATE_TIME,'4999-12-31 00:00:00' ) AS CREATE_TIME ,
  COALESCE(UPDATE_TIME,'4999-12-31 00:00:00' ) AS UPDATE_TIME ,
  COALESCE(TRUNC_NO, 0 ) AS TRUNC_NO ,
  COALESCE(FRE_ICM_IN_ACC, '' ) AS FRE_ICM_IN_ACC ,
  COALESCE(FRE_ICM_IN_ACC_DEP, '' ) AS FRE_ICM_IN_ACC_DEP ,
  COALESCE(FRE_ICM_IN_ACC_NAME, '' ) AS FRE_ICM_IN_ACC_NAME ,
  COALESCE(RMK_ICM_IN_ACC, '' ) AS RMK_ICM_IN_ACC ,
  COALESCE(RMK_ICM_IN_ACC_DEP, '' ) AS RMK_ICM_IN_ACC_DEP ,
  COALESCE(RMK_ICM_IN_ACC_NAME, '' ) AS RMK_ICM_IN_ACC_NAME 
 FROM  dw_tdata.PCS_006_TB_ABS_LOAN_INFO_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  DUEBILL_NO ,
  DUEBILL_ID ,
  LOAN_ID ,
  LOAN_CONTRACT_NO ,
  IS_ABS ,
  CURR_STATUS ,
  LOAN_SECU_NO ,
  CUR_ORG_ID ,
  PROVINCE_NUM ,
  PACK_TRANS_DATE ,
  PACKET_DATE ,
  SETT_TRANS_DATE ,
  SETT_DATE ,
  LOAN_BACK_NUM ,
  BACK_TRANS_DATE ,
  BACK_DATE ,
  SECU_NUM ,
  SECU_NAME ,
  IN_RCV_ACC ,
  IN_RCV_ACC_DEP ,
  IN_RCV_ACC_NAME ,
  IN_PAY_ACC ,
  IN_PAY_ACC_DEP ,
  IN_PAY_ACC_NAME ,
  BAL_IN_ACC ,
  BAL_IN_ACC_DEP ,
  BAL_IN_ACC_NAME ,
  ITR_IN_ACC ,
  ITR_IN_ACC_DEP ,
  ITR_IN_ACC_NAME ,
  BACK_TYPE ,
  IS_QUALIFIED ,
  FAIL_REASON ,
  DELFLAG ,
  CREATE_TIME ,
  UPDATE_TIME ,
  TRUNC_NO ,
  FRE_ICM_IN_ACC ,
  FRE_ICM_IN_ACC_DEP ,
  FRE_ICM_IN_ACC_NAME ,
  RMK_ICM_IN_ACC ,
  RMK_ICM_IN_ACC_DEP ,
  RMK_ICM_IN_ACC_NAME 
 FROM dw_sdata.PCS_006_TB_ABS_LOAN_INFO 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.LOAN_ID = T.LOAN_ID
WHERE
(T.LOAN_ID IS NULL)
 OR N.DUEBILL_NO<>T.DUEBILL_NO
 OR N.DUEBILL_ID<>T.DUEBILL_ID
 OR N.LOAN_CONTRACT_NO<>T.LOAN_CONTRACT_NO
 OR N.IS_ABS<>T.IS_ABS
 OR N.CURR_STATUS<>T.CURR_STATUS
 OR N.LOAN_SECU_NO<>T.LOAN_SECU_NO
 OR N.CUR_ORG_ID<>T.CUR_ORG_ID
 OR N.PROVINCE_NUM<>T.PROVINCE_NUM
 OR N.PACK_TRANS_DATE<>T.PACK_TRANS_DATE
 OR N.PACKET_DATE<>T.PACKET_DATE
 OR N.SETT_TRANS_DATE<>T.SETT_TRANS_DATE
 OR N.SETT_DATE<>T.SETT_DATE
 OR N.LOAN_BACK_NUM<>T.LOAN_BACK_NUM
 OR N.BACK_TRANS_DATE<>T.BACK_TRANS_DATE
 OR N.BACK_DATE<>T.BACK_DATE
 OR N.SECU_NUM<>T.SECU_NUM
 OR N.SECU_NAME<>T.SECU_NAME
 OR N.IN_RCV_ACC<>T.IN_RCV_ACC
 OR N.IN_RCV_ACC_DEP<>T.IN_RCV_ACC_DEP
 OR N.IN_RCV_ACC_NAME<>T.IN_RCV_ACC_NAME
 OR N.IN_PAY_ACC<>T.IN_PAY_ACC
 OR N.IN_PAY_ACC_DEP<>T.IN_PAY_ACC_DEP
 OR N.IN_PAY_ACC_NAME<>T.IN_PAY_ACC_NAME
 OR N.BAL_IN_ACC<>T.BAL_IN_ACC
 OR N.BAL_IN_ACC_DEP<>T.BAL_IN_ACC_DEP
 OR N.BAL_IN_ACC_NAME<>T.BAL_IN_ACC_NAME
 OR N.ITR_IN_ACC<>T.ITR_IN_ACC
 OR N.ITR_IN_ACC_DEP<>T.ITR_IN_ACC_DEP
 OR N.ITR_IN_ACC_NAME<>T.ITR_IN_ACC_NAME
 OR N.BACK_TYPE<>T.BACK_TYPE
 OR N.IS_QUALIFIED<>T.IS_QUALIFIED
 OR N.FAIL_REASON<>T.FAIL_REASON
 OR N.DELFLAG<>T.DELFLAG
 OR N.CREATE_TIME<>T.CREATE_TIME
 OR N.UPDATE_TIME<>T.UPDATE_TIME
 OR N.TRUNC_NO<>T.TRUNC_NO
 OR N.FRE_ICM_IN_ACC<>T.FRE_ICM_IN_ACC
 OR N.FRE_ICM_IN_ACC_DEP<>T.FRE_ICM_IN_ACC_DEP
 OR N.FRE_ICM_IN_ACC_NAME<>T.FRE_ICM_IN_ACC_NAME
 OR N.RMK_ICM_IN_ACC<>T.RMK_ICM_IN_ACC
 OR N.RMK_ICM_IN_ACC_DEP<>T.RMK_ICM_IN_ACC_DEP
 OR N.RMK_ICM_IN_ACC_NAME<>T.RMK_ICM_IN_ACC_NAME
;

--Step3:
UPDATE dw_sdata.PCS_006_TB_ABS_LOAN_INFO P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_327
WHERE P.End_Dt=DATE('2100-12-31')
AND P.LOAN_ID=T_327.LOAN_ID
;

--Step4:
INSERT  INTO dw_sdata.PCS_006_TB_ABS_LOAN_INFO SELECT * FROM T_327;

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