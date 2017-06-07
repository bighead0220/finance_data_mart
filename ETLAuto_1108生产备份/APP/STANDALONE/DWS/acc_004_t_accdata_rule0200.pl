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
DELETE FROM dw_sdata.ACC_004_T_ACCDATA_RULE WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.ACC_004_T_ACCDATA_RULE SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_28 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.ACC_004_T_ACCDATA_RULE WHERE 1=0;

--Step2:
INSERT  INTO T_28 (
  ENTRY_CODE,
  ENTRY_NAME,
  ENTRY_SERNO,
  SERNO,
  CURR_FLAG,
  ENTRY_ABBR,
  ACC_FLAG,
  DR_ITM_NO,
  DR_INST_TYPE,
  DR_AMT_TYPE,
  DR_COM_AMT_TYPE,
  DR_COLL_INST_TYPE,
  DR_FLAGS,
  CR_ITM_NO,
  CR_INST_TYPE,
  CR_AMT_TYPE,
  CR_COM_AMT_TYPE,
  CR_COLL_INST_TYPE,
  CR_FLAGS,
  CERT_STAT,
  SUMM_FLAG,
  BGN_DATE,
  END_DATE,
  start_dt,
  end_dt)
SELECT
  N.ENTRY_CODE,
  N.ENTRY_NAME,
  N.ENTRY_SERNO,
  N.SERNO,
  N.CURR_FLAG,
  N.ENTRY_ABBR,
  N.ACC_FLAG,
  N.DR_ITM_NO,
  N.DR_INST_TYPE,
  N.DR_AMT_TYPE,
  N.DR_COM_AMT_TYPE,
  N.DR_COLL_INST_TYPE,
  N.DR_FLAGS,
  N.CR_ITM_NO,
  N.CR_INST_TYPE,
  N.CR_AMT_TYPE,
  N.CR_COM_AMT_TYPE,
  N.CR_COLL_INST_TYPE,
  N.CR_FLAGS,
  N.CERT_STAT,
  N.SUMM_FLAG,
  N.BGN_DATE,
  N.END_DATE,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(ENTRY_CODE, '' ) AS ENTRY_CODE ,
  COALESCE(ENTRY_NAME, '' ) AS ENTRY_NAME ,
  COALESCE(ENTRY_SERNO, 0 ) AS ENTRY_SERNO ,
  COALESCE(SERNO, 0 ) AS SERNO ,
  COALESCE(CURR_FLAG, 0 ) AS CURR_FLAG ,
  COALESCE(ENTRY_ABBR, '' ) AS ENTRY_ABBR ,
  COALESCE(ACC_FLAG, 0 ) AS ACC_FLAG ,
  COALESCE(DR_ITM_NO, '' ) AS DR_ITM_NO ,
  COALESCE(DR_INST_TYPE, '' ) AS DR_INST_TYPE ,
  COALESCE(DR_AMT_TYPE, '' ) AS DR_AMT_TYPE ,
  COALESCE(DR_COM_AMT_TYPE, '' ) AS DR_COM_AMT_TYPE ,
  COALESCE(DR_COLL_INST_TYPE, '' ) AS DR_COLL_INST_TYPE ,
  COALESCE(DR_FLAGS, '' ) AS DR_FLAGS ,
  COALESCE(CR_ITM_NO, '' ) AS CR_ITM_NO ,
  COALESCE(CR_INST_TYPE, '' ) AS CR_INST_TYPE ,
  COALESCE(CR_AMT_TYPE, '' ) AS CR_AMT_TYPE ,
  COALESCE(CR_COM_AMT_TYPE, '' ) AS CR_COM_AMT_TYPE ,
  COALESCE(CR_COLL_INST_TYPE, '' ) AS CR_COLL_INST_TYPE ,
  COALESCE(CR_FLAGS, '' ) AS CR_FLAGS ,
  COALESCE(CERT_STAT, '' ) AS CERT_STAT ,
  COALESCE(SUMM_FLAG, 0 ) AS SUMM_FLAG ,
  COALESCE(BGN_DATE, '' ) AS BGN_DATE ,
  COALESCE(END_DATE, '' ) AS END_DATE 
 FROM  dw_tdata.ACC_004_T_ACCDATA_RULE_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  ENTRY_CODE ,
  ENTRY_NAME ,
  ENTRY_SERNO ,
  SERNO ,
  CURR_FLAG ,
  ENTRY_ABBR ,
  ACC_FLAG ,
  DR_ITM_NO ,
  DR_INST_TYPE ,
  DR_AMT_TYPE ,
  DR_COM_AMT_TYPE ,
  DR_COLL_INST_TYPE ,
  DR_FLAGS ,
  CR_ITM_NO ,
  CR_INST_TYPE ,
  CR_AMT_TYPE ,
  CR_COM_AMT_TYPE ,
  CR_COLL_INST_TYPE ,
  CR_FLAGS ,
  CERT_STAT ,
  SUMM_FLAG ,
  BGN_DATE ,
  END_DATE 
 FROM dw_sdata.ACC_004_T_ACCDATA_RULE 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.ENTRY_CODE = T.ENTRY_CODE AND N.ENTRY_SERNO = T.ENTRY_SERNO AND N.SERNO = T.SERNO
WHERE
(T.ENTRY_CODE IS NULL AND T.ENTRY_SERNO IS NULL AND T.SERNO IS NULL)
 OR N.ENTRY_NAME<>T.ENTRY_NAME
 OR N.CURR_FLAG<>T.CURR_FLAG
 OR N.ENTRY_ABBR<>T.ENTRY_ABBR
 OR N.ACC_FLAG<>T.ACC_FLAG
 OR N.DR_ITM_NO<>T.DR_ITM_NO
 OR N.DR_INST_TYPE<>T.DR_INST_TYPE
 OR N.DR_AMT_TYPE<>T.DR_AMT_TYPE
 OR N.DR_COM_AMT_TYPE<>T.DR_COM_AMT_TYPE
 OR N.DR_COLL_INST_TYPE<>T.DR_COLL_INST_TYPE
 OR N.DR_FLAGS<>T.DR_FLAGS
 OR N.CR_ITM_NO<>T.CR_ITM_NO
 OR N.CR_INST_TYPE<>T.CR_INST_TYPE
 OR N.CR_AMT_TYPE<>T.CR_AMT_TYPE
 OR N.CR_COM_AMT_TYPE<>T.CR_COM_AMT_TYPE
 OR N.CR_COLL_INST_TYPE<>T.CR_COLL_INST_TYPE
 OR N.CR_FLAGS<>T.CR_FLAGS
 OR N.CERT_STAT<>T.CERT_STAT
 OR N.SUMM_FLAG<>T.SUMM_FLAG
 OR N.BGN_DATE<>T.BGN_DATE
 OR N.END_DATE<>T.END_DATE
;

--Step3:
UPDATE dw_sdata.ACC_004_T_ACCDATA_RULE P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_28
WHERE P.End_Dt=DATE('2100-12-31')
AND P.ENTRY_CODE=T_28.ENTRY_CODE
AND P.ENTRY_SERNO=T_28.ENTRY_SERNO
AND P.SERNO=T_28.SERNO
;

--Step4:
INSERT  INTO dw_sdata.ACC_004_T_ACCDATA_RULE SELECT * FROM T_28;

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
