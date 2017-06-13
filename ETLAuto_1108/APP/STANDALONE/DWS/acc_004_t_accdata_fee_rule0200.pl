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
DELETE FROM dw_sdata.ACC_004_T_ACCDATA_FEE_RULE WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.ACC_004_T_ACCDATA_FEE_RULE SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_25 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.ACC_004_T_ACCDATA_FEE_RULE WHERE 1=0;

--Step2:
INSERT  INTO T_25 (
  FEE_CODE,
  FEE_NAME,
  SERNO,
  INCOME_PAY_FLAG,
  PRE_CALC_FLAG,
  COLLECT_FLAG,
  COLLECT_TYPE,
  ACC_FLAG,
  CHNL_NO,
  DR_ITM_NO,
  CR_ITM_NO,
  PREF_FLAG,
  BGN_DATE,
  END_DATE,
  start_dt,
  end_dt)
SELECT
  N.FEE_CODE,
  N.FEE_NAME,
  N.SERNO,
  N.INCOME_PAY_FLAG,
  N.PRE_CALC_FLAG,
  N.COLLECT_FLAG,
  N.COLLECT_TYPE,
  N.ACC_FLAG,
  N.CHNL_NO,
  N.DR_ITM_NO,
  N.CR_ITM_NO,
  N.PREF_FLAG,
  N.BGN_DATE,
  N.END_DATE,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(FEE_CODE, '' ) AS FEE_CODE ,
  COALESCE(FEE_NAME, '' ) AS FEE_NAME ,
  COALESCE(SERNO, 0 ) AS SERNO ,
  COALESCE(INCOME_PAY_FLAG, 0 ) AS INCOME_PAY_FLAG ,
  COALESCE(PRE_CALC_FLAG, 0 ) AS PRE_CALC_FLAG ,
  COALESCE(COLLECT_FLAG, 0 ) AS COLLECT_FLAG ,
  COALESCE(COLLECT_TYPE, '' ) AS COLLECT_TYPE ,
  COALESCE(ACC_FLAG, 0 ) AS ACC_FLAG ,
  COALESCE(CHNL_NO, '' ) AS CHNL_NO ,
  COALESCE(DR_ITM_NO, '' ) AS DR_ITM_NO ,
  COALESCE(CR_ITM_NO, '' ) AS CR_ITM_NO ,
  COALESCE(PREF_FLAG, 0 ) AS PREF_FLAG ,
  COALESCE(BGN_DATE, '' ) AS BGN_DATE ,
  COALESCE(END_DATE, '' ) AS END_DATE 
 FROM  dw_tdata.ACC_004_T_ACCDATA_FEE_RULE_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  FEE_CODE ,
  FEE_NAME ,
  SERNO ,
  INCOME_PAY_FLAG ,
  PRE_CALC_FLAG ,
  COLLECT_FLAG ,
  COLLECT_TYPE ,
  ACC_FLAG ,
  CHNL_NO ,
  DR_ITM_NO ,
  CR_ITM_NO ,
  PREF_FLAG ,
  BGN_DATE ,
  END_DATE 
 FROM dw_sdata.ACC_004_T_ACCDATA_FEE_RULE 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.FEE_CODE = T.FEE_CODE AND N.PRE_CALC_FLAG = T.PRE_CALC_FLAG AND N.COLLECT_FLAG = T.COLLECT_FLAG AND N.COLLECT_TYPE = T.COLLECT_TYPE AND N.CHNL_NO = T.CHNL_NO
WHERE
(T.FEE_CODE IS NULL AND T.PRE_CALC_FLAG IS NULL AND T.COLLECT_FLAG IS NULL AND T.COLLECT_TYPE IS NULL AND T.CHNL_NO IS NULL)
 OR N.FEE_NAME<>T.FEE_NAME
 OR N.SERNO<>T.SERNO
 OR N.INCOME_PAY_FLAG<>T.INCOME_PAY_FLAG
 OR N.ACC_FLAG<>T.ACC_FLAG
 OR N.DR_ITM_NO<>T.DR_ITM_NO
 OR N.CR_ITM_NO<>T.CR_ITM_NO
 OR N.PREF_FLAG<>T.PREF_FLAG
 OR N.BGN_DATE<>T.BGN_DATE
 OR N.END_DATE<>T.END_DATE
;

--Step3:
UPDATE dw_sdata.ACC_004_T_ACCDATA_FEE_RULE P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_25
WHERE P.End_Dt=DATE('2100-12-31')
AND P.FEE_CODE=T_25.FEE_CODE
AND P.PRE_CALC_FLAG=T_25.PRE_CALC_FLAG
AND P.COLLECT_FLAG=T_25.COLLECT_FLAG
AND P.COLLECT_TYPE=T_25.COLLECT_TYPE
AND P.CHNL_NO=T_25.CHNL_NO
;

--Step4:
INSERT  INTO dw_sdata.ACC_004_T_ACCDATA_FEE_RULE SELECT * FROM T_25;

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
