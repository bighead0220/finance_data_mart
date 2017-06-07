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
DELETE FROM dw_sdata.ACC_004_T_ACCDATA_OUTACC_DTL WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.ACC_004_T_ACCDATA_OUTACC_DTL SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_27 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.ACC_004_T_ACCDATA_OUTACC_DTL WHERE 1=0;

--Step2:
INSERT  INTO T_27 (
  ACC_DATE,
  SYSTEM_ID,
  CLT_SEQNO,
  ACC_DATA_ID,
  ENTRY_SERNO,
  SERNO,
  DR_CR_FLAG,
  CERT_FLAG,
  CERT_TYPE,
  CERT_BGN_NO,
  CERT_END_NO,
  NUM,
  start_dt,
  end_dt)
SELECT
  N.ACC_DATE,
  N.SYSTEM_ID,
  N.CLT_SEQNO,
  N.ACC_DATA_ID,
  N.ENTRY_SERNO,
  N.SERNO,
  N.DR_CR_FLAG,
  N.CERT_FLAG,
  N.CERT_TYPE,
  N.CERT_BGN_NO,
  N.CERT_END_NO,
  N.NUM,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(ACC_DATE, '' ) AS ACC_DATE ,
  COALESCE(SYSTEM_ID, '' ) AS SYSTEM_ID ,
  COALESCE(CLT_SEQNO, '' ) AS CLT_SEQNO ,
  COALESCE(ACC_DATA_ID, '' ) AS ACC_DATA_ID ,
  COALESCE(ENTRY_SERNO, 0 ) AS ENTRY_SERNO ,
  COALESCE(SERNO, 0 ) AS SERNO ,
  COALESCE(DR_CR_FLAG, 0 ) AS DR_CR_FLAG ,
  COALESCE(CERT_FLAG, 0 ) AS CERT_FLAG ,
  COALESCE(CERT_TYPE, '' ) AS CERT_TYPE ,
  COALESCE(CERT_BGN_NO, '' ) AS CERT_BGN_NO ,
  COALESCE(CERT_END_NO, '' ) AS CERT_END_NO ,
  COALESCE(NUM, 0 ) AS NUM 
 FROM  dw_tdata.ACC_004_T_ACCDATA_OUTACC_DTL_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  ACC_DATE ,
  SYSTEM_ID ,
  CLT_SEQNO ,
  ACC_DATA_ID ,
  ENTRY_SERNO ,
  SERNO ,
  DR_CR_FLAG ,
  CERT_FLAG ,
  CERT_TYPE ,
  CERT_BGN_NO ,
  CERT_END_NO ,
  NUM 
 FROM dw_sdata.ACC_004_T_ACCDATA_OUTACC_DTL 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.ACC_DATE = T.ACC_DATE AND N.SYSTEM_ID = T.SYSTEM_ID AND N.CLT_SEQNO = T.CLT_SEQNO AND N.ENTRY_SERNO = T.ENTRY_SERNO AND N.SERNO = T.SERNO
WHERE
(T.ACC_DATE IS NULL AND T.SYSTEM_ID IS NULL AND T.CLT_SEQNO IS NULL AND T.ENTRY_SERNO IS NULL AND T.SERNO IS NULL)
 OR N.ACC_DATA_ID<>T.ACC_DATA_ID
 OR N.DR_CR_FLAG<>T.DR_CR_FLAG
 OR N.CERT_FLAG<>T.CERT_FLAG
 OR N.CERT_TYPE<>T.CERT_TYPE
 OR N.CERT_BGN_NO<>T.CERT_BGN_NO
 OR N.CERT_END_NO<>T.CERT_END_NO
 OR N.NUM<>T.NUM
;

--Step3:
UPDATE dw_sdata.ACC_004_T_ACCDATA_OUTACC_DTL P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_27
WHERE P.End_Dt=DATE('2100-12-31')
AND P.ACC_DATE=T_27.ACC_DATE
AND P.SYSTEM_ID=T_27.SYSTEM_ID
AND P.CLT_SEQNO=T_27.CLT_SEQNO
AND P.ENTRY_SERNO=T_27.ENTRY_SERNO
AND P.SERNO=T_27.SERNO
;

--Step4:
INSERT  INTO dw_sdata.ACC_004_T_ACCDATA_OUTACC_DTL SELECT * FROM T_27;

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
