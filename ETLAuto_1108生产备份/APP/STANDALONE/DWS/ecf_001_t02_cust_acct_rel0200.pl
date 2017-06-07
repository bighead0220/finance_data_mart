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
DELETE FROM dw_sdata.ECF_001_T02_CUST_ACCT_REL WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.ECF_001_T02_CUST_ACCT_REL SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_164 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.ECF_001_T02_CUST_ACCT_REL WHERE 1=0;

--Step2:
INSERT  INTO T_164 (
  SEQ_ID,
  PARTY_ID,
  SIGN_ID,
  MAIN_ACC_NO,
  SUB_ACC_NO,
  CURR_TYPE,
  RELATED_TYPE,
  OPEN_ORG,
  OP_TLR,
  OP_DATE,
  CLS_INST,
  CLS_TLR,
  CLS_DATE,
  LAST_UPDATED_TE,
  LAST_UPDATED_ORG,
  CREATED_TS,
  UPDATED_TS,
  INIT_SYSTEM_ID,
  INIT_CREATED_TS,
  LAST_SYSTEM_ID,
  LAST_UPDATED_TS,
  start_dt,
  end_dt)
SELECT
  N.SEQ_ID,
  N.PARTY_ID,
  N.SIGN_ID,
  N.MAIN_ACC_NO,
  N.SUB_ACC_NO,
  N.CURR_TYPE,
  N.RELATED_TYPE,
  N.OPEN_ORG,
  N.OP_TLR,
  N.OP_DATE,
  N.CLS_INST,
  N.CLS_TLR,
  N.CLS_DATE,
  N.LAST_UPDATED_TE,
  N.LAST_UPDATED_ORG,
  N.CREATED_TS,
  N.UPDATED_TS,
  N.INIT_SYSTEM_ID,
  N.INIT_CREATED_TS,
  N.LAST_SYSTEM_ID,
  N.LAST_UPDATED_TS,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(SEQ_ID, '' ) AS SEQ_ID ,
  COALESCE(PARTY_ID, '' ) AS PARTY_ID ,
  COALESCE(SIGN_ID, '' ) AS SIGN_ID ,
  COALESCE(MAIN_ACC_NO, '' ) AS MAIN_ACC_NO ,
  COALESCE(SUB_ACC_NO, '' ) AS SUB_ACC_NO ,
  COALESCE(CURR_TYPE, '' ) AS CURR_TYPE ,
  COALESCE(RELATED_TYPE, '' ) AS RELATED_TYPE ,
  COALESCE(OPEN_ORG, '' ) AS OPEN_ORG ,
  COALESCE(OP_TLR, '' ) AS OP_TLR ,
  COALESCE(OP_DATE, '' ) AS OP_DATE ,
  COALESCE(CLS_INST, '' ) AS CLS_INST ,
  COALESCE(CLS_TLR, '' ) AS CLS_TLR ,
  COALESCE(CLS_DATE, '' ) AS CLS_DATE ,
  COALESCE(LAST_UPDATED_TE, '' ) AS LAST_UPDATED_TE ,
  COALESCE(LAST_UPDATED_ORG, '' ) AS LAST_UPDATED_ORG ,
  COALESCE(CREATED_TS, '' ) AS CREATED_TS ,
  COALESCE(UPDATED_TS, '' ) AS UPDATED_TS ,
  COALESCE(INIT_SYSTEM_ID, '' ) AS INIT_SYSTEM_ID ,
  COALESCE(INIT_CREATED_TS, '' ) AS INIT_CREATED_TS ,
  COALESCE(LAST_SYSTEM_ID, '' ) AS LAST_SYSTEM_ID ,
  COALESCE(LAST_UPDATED_TS, '' ) AS LAST_UPDATED_TS 
 FROM  dw_tdata.ECF_001_T02_CUST_ACCT_REL_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  SEQ_ID ,
  PARTY_ID ,
  SIGN_ID ,
  MAIN_ACC_NO ,
  SUB_ACC_NO ,
  CURR_TYPE ,
  RELATED_TYPE ,
  OPEN_ORG ,
  OP_TLR ,
  OP_DATE ,
  CLS_INST ,
  CLS_TLR ,
  CLS_DATE ,
  LAST_UPDATED_TE ,
  LAST_UPDATED_ORG ,
  CREATED_TS ,
  UPDATED_TS ,
  INIT_SYSTEM_ID ,
  INIT_CREATED_TS ,
  LAST_SYSTEM_ID ,
  LAST_UPDATED_TS 
 FROM dw_sdata.ECF_001_T02_CUST_ACCT_REL 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.MAIN_ACC_NO = T.MAIN_ACC_NO AND N.SUB_ACC_NO = T.SUB_ACC_NO AND N.UPDATED_TS = T.UPDATED_TS
WHERE
(T.MAIN_ACC_NO IS NULL AND T.SUB_ACC_NO IS NULL AND T.UPDATED_TS IS NULL)
 OR N.SEQ_ID<>T.SEQ_ID
 OR N.PARTY_ID<>T.PARTY_ID
 OR N.SIGN_ID<>T.SIGN_ID
 OR N.CURR_TYPE<>T.CURR_TYPE
 OR N.RELATED_TYPE<>T.RELATED_TYPE
 OR N.OPEN_ORG<>T.OPEN_ORG
 OR N.OP_TLR<>T.OP_TLR
 OR N.OP_DATE<>T.OP_DATE
 OR N.CLS_INST<>T.CLS_INST
 OR N.CLS_TLR<>T.CLS_TLR
 OR N.CLS_DATE<>T.CLS_DATE
 OR N.LAST_UPDATED_TE<>T.LAST_UPDATED_TE
 OR N.LAST_UPDATED_ORG<>T.LAST_UPDATED_ORG
 OR N.CREATED_TS<>T.CREATED_TS
 OR N.INIT_SYSTEM_ID<>T.INIT_SYSTEM_ID
 OR N.INIT_CREATED_TS<>T.INIT_CREATED_TS
 OR N.LAST_SYSTEM_ID<>T.LAST_SYSTEM_ID
 OR N.LAST_UPDATED_TS<>T.LAST_UPDATED_TS
;

--Step3:
UPDATE dw_sdata.ECF_001_T02_CUST_ACCT_REL P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_164
WHERE P.End_Dt=DATE('2100-12-31')
AND P.MAIN_ACC_NO=T_164.MAIN_ACC_NO
AND P.SUB_ACC_NO=T_164.SUB_ACC_NO
AND P.UPDATED_TS=T_164.UPDATED_TS
;

--Step4:
INSERT  INTO dw_sdata.ECF_001_T02_CUST_ACCT_REL SELECT * FROM T_164;

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
