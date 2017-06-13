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
DELETE FROM dw_sdata.BBS_001_SEND_COLLECTION_BATCH WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.BBS_001_SEND_COLLECTION_BATCH SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_55 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.BBS_001_SEND_COLLECTION_BATCH WHERE 1=0;

--Step2:
INSERT  INTO T_55 (
  ID,
  BRANCH_ID,
  DRAFT_ATTR,
  DRAFT_TYPE,
  COLLECTION_DATE,
  UBANK_ID,
  EMS_NO,
  STATUS,
  STTLM_MK,
  ACCOUNT_STATUS,
  AUDIT_STATUS,
  APPNO,
  OPERATOR_ID,
  TXN_DATE,
  LAST_UPD_OPER_ID,
  LAST_UPD_TIME,
  SRC_TYPE,
  ACCEPTOR_BANK_NAME,
  BACK_AMOUNT_ACCNO,
  BACK_AMOUNT_NAME,
  start_dt,
  end_dt)
SELECT
  N.ID,
  N.BRANCH_ID,
  N.DRAFT_ATTR,
  N.DRAFT_TYPE,
  N.COLLECTION_DATE,
  N.UBANK_ID,
  N.EMS_NO,
  N.STATUS,
  N.STTLM_MK,
  N.ACCOUNT_STATUS,
  N.AUDIT_STATUS,
  N.APPNO,
  N.OPERATOR_ID,
  N.TXN_DATE,
  N.LAST_UPD_OPER_ID,
  N.LAST_UPD_TIME,
  N.SRC_TYPE,
  N.ACCEPTOR_BANK_NAME,
  N.BACK_AMOUNT_ACCNO,
  N.BACK_AMOUNT_NAME,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(ID, 0 ) AS ID ,
  COALESCE(BRANCH_ID, 0 ) AS BRANCH_ID ,
  COALESCE(DRAFT_ATTR, '' ) AS DRAFT_ATTR ,
  COALESCE(DRAFT_TYPE, '' ) AS DRAFT_TYPE ,
  COALESCE(COLLECTION_DATE, '' ) AS COLLECTION_DATE ,
  COALESCE(UBANK_ID, 0 ) AS UBANK_ID ,
  COALESCE(EMS_NO, '' ) AS EMS_NO ,
  COALESCE(STATUS, '' ) AS STATUS ,
  COALESCE(STTLM_MK, '' ) AS STTLM_MK ,
  COALESCE(ACCOUNT_STATUS, '' ) AS ACCOUNT_STATUS ,
  COALESCE(AUDIT_STATUS, '' ) AS AUDIT_STATUS ,
  COALESCE(APPNO, '' ) AS APPNO ,
  COALESCE(OPERATOR_ID, 0 ) AS OPERATOR_ID ,
  COALESCE(TXN_DATE, '' ) AS TXN_DATE ,
  COALESCE(LAST_UPD_OPER_ID, 0 ) AS LAST_UPD_OPER_ID ,
  COALESCE(LAST_UPD_TIME, '' ) AS LAST_UPD_TIME ,
  COALESCE(SRC_TYPE, '' ) AS SRC_TYPE ,
  COALESCE(ACCEPTOR_BANK_NAME, '' ) AS ACCEPTOR_BANK_NAME ,
  COALESCE(BACK_AMOUNT_ACCNO, '' ) AS BACK_AMOUNT_ACCNO ,
  COALESCE(BACK_AMOUNT_NAME, '' ) AS BACK_AMOUNT_NAME 
 FROM  dw_tdata.BBS_001_SEND_COLLECTION_BATCH_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  ID ,
  BRANCH_ID ,
  DRAFT_ATTR ,
  DRAFT_TYPE ,
  COLLECTION_DATE ,
  UBANK_ID ,
  EMS_NO ,
  STATUS ,
  STTLM_MK ,
  ACCOUNT_STATUS ,
  AUDIT_STATUS ,
  APPNO ,
  OPERATOR_ID ,
  TXN_DATE ,
  LAST_UPD_OPER_ID ,
  LAST_UPD_TIME ,
  SRC_TYPE ,
  ACCEPTOR_BANK_NAME ,
  BACK_AMOUNT_ACCNO ,
  BACK_AMOUNT_NAME 
 FROM dw_sdata.BBS_001_SEND_COLLECTION_BATCH 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.ID = T.ID
WHERE
(T.ID IS NULL)
 OR N.BRANCH_ID<>T.BRANCH_ID
 OR N.DRAFT_ATTR<>T.DRAFT_ATTR
 OR N.DRAFT_TYPE<>T.DRAFT_TYPE
 OR N.COLLECTION_DATE<>T.COLLECTION_DATE
 OR N.UBANK_ID<>T.UBANK_ID
 OR N.EMS_NO<>T.EMS_NO
 OR N.STATUS<>T.STATUS
 OR N.STTLM_MK<>T.STTLM_MK
 OR N.ACCOUNT_STATUS<>T.ACCOUNT_STATUS
 OR N.AUDIT_STATUS<>T.AUDIT_STATUS
 OR N.APPNO<>T.APPNO
 OR N.OPERATOR_ID<>T.OPERATOR_ID
 OR N.TXN_DATE<>T.TXN_DATE
 OR N.LAST_UPD_OPER_ID<>T.LAST_UPD_OPER_ID
 OR N.LAST_UPD_TIME<>T.LAST_UPD_TIME
 OR N.SRC_TYPE<>T.SRC_TYPE
 OR N.ACCEPTOR_BANK_NAME<>T.ACCEPTOR_BANK_NAME
 OR N.BACK_AMOUNT_ACCNO<>T.BACK_AMOUNT_ACCNO
 OR N.BACK_AMOUNT_NAME<>T.BACK_AMOUNT_NAME
;

--Step3:
UPDATE dw_sdata.BBS_001_SEND_COLLECTION_BATCH P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_55
WHERE P.End_Dt=DATE('2100-12-31')
AND P.ID=T_55.ID
;

--Step4:
INSERT  INTO dw_sdata.BBS_001_SEND_COLLECTION_BATCH SELECT * FROM T_55;

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
