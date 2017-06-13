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
DELETE FROM dw_sdata.BBS_001_PLG_SAVE_CONTRACT WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.BBS_001_PLG_SAVE_CONTRACT SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_52 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.BBS_001_PLG_SAVE_CONTRACT WHERE 1=0;

--Step2:
INSERT  INTO T_52 (
  ID,
  APPNO,
  BUSINESS_NO,
  BRH_ID,
  CUST_ACCNO,
  CUST_ID,
  BAIL_ACCOUNT,
  BAIL_ACCOUNT_NAME,
  STATUS,
  PLEDGE_TYPE,
  CREDIT_START_DATE,
  CREDIT_END_DATE,
  SAVE_DATE,
  CREATE_OPR_ID,
  CREATE_TIME,
  LAST_UPD_OPR_ID,
  LAST_UPD_TIME,
  BUSINESS_TYPE,
  GROUP_FLAG,
  REMOVE_DRAFT_FLAG,
  BILL_DRAFT_FLAG,
  BILL_AMOUNT,
  BILL_OUT_AMOUNT,
  CREDIT_CHECK_STATUS,
  DRAFT_TYPE,
  PLG_BUSINESS_TYPE,
  DRAFT_ATTR,
  PRETREATMENT_DATE,
  start_dt,
  end_dt)
SELECT
  N.ID,
  N.APPNO,
  N.BUSINESS_NO,
  N.BRH_ID,
  N.CUST_ACCNO,
  N.CUST_ID,
  N.BAIL_ACCOUNT,
  N.BAIL_ACCOUNT_NAME,
  N.STATUS,
  N.PLEDGE_TYPE,
  N.CREDIT_START_DATE,
  N.CREDIT_END_DATE,
  N.SAVE_DATE,
  N.CREATE_OPR_ID,
  N.CREATE_TIME,
  N.LAST_UPD_OPR_ID,
  N.LAST_UPD_TIME,
  N.BUSINESS_TYPE,
  N.GROUP_FLAG,
  N.REMOVE_DRAFT_FLAG,
  N.BILL_DRAFT_FLAG,
  N.BILL_AMOUNT,
  N.BILL_OUT_AMOUNT,
  N.CREDIT_CHECK_STATUS,
  N.DRAFT_TYPE,
  N.PLG_BUSINESS_TYPE,
  N.DRAFT_ATTR,
  N.PRETREATMENT_DATE,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(ID, 0 ) AS ID ,
  COALESCE(APPNO, '' ) AS APPNO ,
  COALESCE(BUSINESS_NO, '' ) AS BUSINESS_NO ,
  COALESCE(BRH_ID, 0 ) AS BRH_ID ,
  COALESCE(CUST_ACCNO, '' ) AS CUST_ACCNO ,
  COALESCE(CUST_ID, 0 ) AS CUST_ID ,
  COALESCE(BAIL_ACCOUNT, '' ) AS BAIL_ACCOUNT ,
  COALESCE(BAIL_ACCOUNT_NAME, '' ) AS BAIL_ACCOUNT_NAME ,
  COALESCE(STATUS, '' ) AS STATUS ,
  COALESCE(PLEDGE_TYPE, '' ) AS PLEDGE_TYPE ,
  COALESCE(CREDIT_START_DATE, '' ) AS CREDIT_START_DATE ,
  COALESCE(CREDIT_END_DATE, '' ) AS CREDIT_END_DATE ,
  COALESCE(SAVE_DATE, '' ) AS SAVE_DATE ,
  COALESCE(CREATE_OPR_ID, 0 ) AS CREATE_OPR_ID ,
  COALESCE(CREATE_TIME, '' ) AS CREATE_TIME ,
  COALESCE(LAST_UPD_OPR_ID, 0 ) AS LAST_UPD_OPR_ID ,
  COALESCE(LAST_UPD_TIME, '' ) AS LAST_UPD_TIME ,
  COALESCE(BUSINESS_TYPE, '' ) AS BUSINESS_TYPE ,
  COALESCE(GROUP_FLAG, '' ) AS GROUP_FLAG ,
  COALESCE(REMOVE_DRAFT_FLAG, '' ) AS REMOVE_DRAFT_FLAG ,
  COALESCE(BILL_DRAFT_FLAG, '' ) AS BILL_DRAFT_FLAG ,
  COALESCE(BILL_AMOUNT, 0 ) AS BILL_AMOUNT ,
  COALESCE(BILL_OUT_AMOUNT, 0 ) AS BILL_OUT_AMOUNT ,
  COALESCE(CREDIT_CHECK_STATUS, '' ) AS CREDIT_CHECK_STATUS ,
  COALESCE(DRAFT_TYPE, '' ) AS DRAFT_TYPE ,
  COALESCE(PLG_BUSINESS_TYPE, '' ) AS PLG_BUSINESS_TYPE ,
  COALESCE(DRAFT_ATTR, '' ) AS DRAFT_ATTR ,
  COALESCE(PRETREATMENT_DATE, '' ) AS PRETREATMENT_DATE 
 FROM  dw_tdata.BBS_001_PLG_SAVE_CONTRACT_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  ID ,
  APPNO ,
  BUSINESS_NO ,
  BRH_ID ,
  CUST_ACCNO ,
  CUST_ID ,
  BAIL_ACCOUNT ,
  BAIL_ACCOUNT_NAME ,
  STATUS ,
  PLEDGE_TYPE ,
  CREDIT_START_DATE ,
  CREDIT_END_DATE ,
  SAVE_DATE ,
  CREATE_OPR_ID ,
  CREATE_TIME ,
  LAST_UPD_OPR_ID ,
  LAST_UPD_TIME ,
  BUSINESS_TYPE ,
  GROUP_FLAG ,
  REMOVE_DRAFT_FLAG ,
  BILL_DRAFT_FLAG ,
  BILL_AMOUNT ,
  BILL_OUT_AMOUNT ,
  CREDIT_CHECK_STATUS ,
  DRAFT_TYPE ,
  PLG_BUSINESS_TYPE ,
  DRAFT_ATTR ,
  PRETREATMENT_DATE 
 FROM dw_sdata.BBS_001_PLG_SAVE_CONTRACT 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.ID = T.ID
WHERE
(T.ID IS NULL)
 OR N.APPNO<>T.APPNO
 OR N.BUSINESS_NO<>T.BUSINESS_NO
 OR N.BRH_ID<>T.BRH_ID
 OR N.CUST_ACCNO<>T.CUST_ACCNO
 OR N.CUST_ID<>T.CUST_ID
 OR N.BAIL_ACCOUNT<>T.BAIL_ACCOUNT
 OR N.BAIL_ACCOUNT_NAME<>T.BAIL_ACCOUNT_NAME
 OR N.STATUS<>T.STATUS
 OR N.PLEDGE_TYPE<>T.PLEDGE_TYPE
 OR N.CREDIT_START_DATE<>T.CREDIT_START_DATE
 OR N.CREDIT_END_DATE<>T.CREDIT_END_DATE
 OR N.SAVE_DATE<>T.SAVE_DATE
 OR N.CREATE_OPR_ID<>T.CREATE_OPR_ID
 OR N.CREATE_TIME<>T.CREATE_TIME
 OR N.LAST_UPD_OPR_ID<>T.LAST_UPD_OPR_ID
 OR N.LAST_UPD_TIME<>T.LAST_UPD_TIME
 OR N.BUSINESS_TYPE<>T.BUSINESS_TYPE
 OR N.GROUP_FLAG<>T.GROUP_FLAG
 OR N.REMOVE_DRAFT_FLAG<>T.REMOVE_DRAFT_FLAG
 OR N.BILL_DRAFT_FLAG<>T.BILL_DRAFT_FLAG
 OR N.BILL_AMOUNT<>T.BILL_AMOUNT
 OR N.BILL_OUT_AMOUNT<>T.BILL_OUT_AMOUNT
 OR N.CREDIT_CHECK_STATUS<>T.CREDIT_CHECK_STATUS
 OR N.DRAFT_TYPE<>T.DRAFT_TYPE
 OR N.PLG_BUSINESS_TYPE<>T.PLG_BUSINESS_TYPE
 OR N.DRAFT_ATTR<>T.DRAFT_ATTR
 OR N.PRETREATMENT_DATE<>T.PRETREATMENT_DATE
;

--Step3:
UPDATE dw_sdata.BBS_001_PLG_SAVE_CONTRACT P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_52
WHERE P.End_Dt=DATE('2100-12-31')
AND P.ID=T_52.ID
;

--Step4:
INSERT  INTO dw_sdata.BBS_001_PLG_SAVE_CONTRACT SELECT * FROM T_52;

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
