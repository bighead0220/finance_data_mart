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
DELETE FROM dw_sdata.BBS_001_PLG_REMOVE_CONTRACT WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.BBS_001_PLG_REMOVE_CONTRACT SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_51 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.BBS_001_PLG_REMOVE_CONTRACT WHERE 1=0;

--Step2:
INSERT  INTO T_51 (
  ID,
  CUST_ACCNO,
  APPNO,
  CUST_ID,
  BUSINESS_NO,
  BRH_ID,
  REMOVE_DATE,
  STATUS,
  CREATE_OPR_ID,
  CREATE_TIME,
  LAST_UPD_OPR_ID,
  LAST_UPD_TIME,
  RISK_CHECK_STATUS,
  LOGIC_CHECK_STATUS,
  REMOVE_DRAFT_FLAG,
  GROUP_FLAG,
  BILL_DRAFT_FLAG,
  BACK_ACCOUNT,
  BACK_NAME,
  BILL_AMOUNT,
  BILL_OUT_AMOUNT,
  DRAFT_TYPE,
  DRAFT_ATTR,
  ENT_BANK_MSGID,
  start_dt,
  end_dt)
SELECT
  N.ID,
  N.CUST_ACCNO,
  N.APPNO,
  N.CUST_ID,
  N.BUSINESS_NO,
  N.BRH_ID,
  N.REMOVE_DATE,
  N.STATUS,
  N.CREATE_OPR_ID,
  N.CREATE_TIME,
  N.LAST_UPD_OPR_ID,
  N.LAST_UPD_TIME,
  N.RISK_CHECK_STATUS,
  N.LOGIC_CHECK_STATUS,
  N.REMOVE_DRAFT_FLAG,
  N.GROUP_FLAG,
  N.BILL_DRAFT_FLAG,
  N.BACK_ACCOUNT,
  N.BACK_NAME,
  N.BILL_AMOUNT,
  N.BILL_OUT_AMOUNT,
  N.DRAFT_TYPE,
  N.DRAFT_ATTR,
  N.ENT_BANK_MSGID,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(ID, 0 ) AS ID ,
  COALESCE(CUST_ACCNO, '' ) AS CUST_ACCNO ,
  COALESCE(APPNO, '' ) AS APPNO ,
  COALESCE(CUST_ID, 0 ) AS CUST_ID ,
  COALESCE(BUSINESS_NO, '' ) AS BUSINESS_NO ,
  COALESCE(BRH_ID, 0 ) AS BRH_ID ,
  COALESCE(REMOVE_DATE, '' ) AS REMOVE_DATE ,
  COALESCE(STATUS, '' ) AS STATUS ,
  COALESCE(CREATE_OPR_ID, 0 ) AS CREATE_OPR_ID ,
  COALESCE(CREATE_TIME, '' ) AS CREATE_TIME ,
  COALESCE(LAST_UPD_OPR_ID, 0 ) AS LAST_UPD_OPR_ID ,
  COALESCE(LAST_UPD_TIME, '' ) AS LAST_UPD_TIME ,
  COALESCE(RISK_CHECK_STATUS, '' ) AS RISK_CHECK_STATUS ,
  COALESCE(LOGIC_CHECK_STATUS, '' ) AS LOGIC_CHECK_STATUS ,
  COALESCE(REMOVE_DRAFT_FLAG, '' ) AS REMOVE_DRAFT_FLAG ,
  COALESCE(GROUP_FLAG, '' ) AS GROUP_FLAG ,
  COALESCE(BILL_DRAFT_FLAG, '' ) AS BILL_DRAFT_FLAG ,
  COALESCE(BACK_ACCOUNT, '' ) AS BACK_ACCOUNT ,
  COALESCE(BACK_NAME, '' ) AS BACK_NAME ,
  COALESCE(BILL_AMOUNT, 0 ) AS BILL_AMOUNT ,
  COALESCE(BILL_OUT_AMOUNT, 0 ) AS BILL_OUT_AMOUNT ,
  COALESCE(DRAFT_TYPE, '' ) AS DRAFT_TYPE ,
  COALESCE(DRAFT_ATTR, '' ) AS DRAFT_ATTR ,
  COALESCE(ENT_BANK_MSGID, 0 ) AS ENT_BANK_MSGID 
 FROM  dw_tdata.BBS_001_PLG_REMOVE_CONTRACT_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  ID ,
  CUST_ACCNO ,
  APPNO ,
  CUST_ID ,
  BUSINESS_NO ,
  BRH_ID ,
  REMOVE_DATE ,
  STATUS ,
  CREATE_OPR_ID ,
  CREATE_TIME ,
  LAST_UPD_OPR_ID ,
  LAST_UPD_TIME ,
  RISK_CHECK_STATUS ,
  LOGIC_CHECK_STATUS ,
  REMOVE_DRAFT_FLAG ,
  GROUP_FLAG ,
  BILL_DRAFT_FLAG ,
  BACK_ACCOUNT ,
  BACK_NAME ,
  BILL_AMOUNT ,
  BILL_OUT_AMOUNT ,
  DRAFT_TYPE ,
  DRAFT_ATTR ,
  ENT_BANK_MSGID 
 FROM dw_sdata.BBS_001_PLG_REMOVE_CONTRACT 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.ID = T.ID
WHERE
(T.ID IS NULL)
 OR N.CUST_ACCNO<>T.CUST_ACCNO
 OR N.APPNO<>T.APPNO
 OR N.CUST_ID<>T.CUST_ID
 OR N.BUSINESS_NO<>T.BUSINESS_NO
 OR N.BRH_ID<>T.BRH_ID
 OR N.REMOVE_DATE<>T.REMOVE_DATE
 OR N.STATUS<>T.STATUS
 OR N.CREATE_OPR_ID<>T.CREATE_OPR_ID
 OR N.CREATE_TIME<>T.CREATE_TIME
 OR N.LAST_UPD_OPR_ID<>T.LAST_UPD_OPR_ID
 OR N.LAST_UPD_TIME<>T.LAST_UPD_TIME
 OR N.RISK_CHECK_STATUS<>T.RISK_CHECK_STATUS
 OR N.LOGIC_CHECK_STATUS<>T.LOGIC_CHECK_STATUS
 OR N.REMOVE_DRAFT_FLAG<>T.REMOVE_DRAFT_FLAG
 OR N.GROUP_FLAG<>T.GROUP_FLAG
 OR N.BILL_DRAFT_FLAG<>T.BILL_DRAFT_FLAG
 OR N.BACK_ACCOUNT<>T.BACK_ACCOUNT
 OR N.BACK_NAME<>T.BACK_NAME
 OR N.BILL_AMOUNT<>T.BILL_AMOUNT
 OR N.BILL_OUT_AMOUNT<>T.BILL_OUT_AMOUNT
 OR N.DRAFT_TYPE<>T.DRAFT_TYPE
 OR N.DRAFT_ATTR<>T.DRAFT_ATTR
 OR N.ENT_BANK_MSGID<>T.ENT_BANK_MSGID
;

--Step3:
UPDATE dw_sdata.BBS_001_PLG_REMOVE_CONTRACT P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_51
WHERE P.End_Dt=DATE('2100-12-31')
AND P.ID=T_51.ID
;

--Step4:
INSERT  INTO dw_sdata.BBS_001_PLG_REMOVE_CONTRACT SELECT * FROM T_51;

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
