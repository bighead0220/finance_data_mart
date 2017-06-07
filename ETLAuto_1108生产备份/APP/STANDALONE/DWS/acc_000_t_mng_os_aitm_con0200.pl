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
DELETE FROM dw_sdata.ACC_000_T_MNG_OS_AITM_CON WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.ACC_000_T_MNG_OS_AITM_CON SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_5 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.ACC_000_T_MNG_OS_AITM_CON WHERE 1=0;

--Step2:
INSERT  INTO T_5 (
  ITM_NO,
  FLAG,
  CE_FLAG,
  ACC_TYPE,
  PRODUCT_TYPE1,
  DEADLINE1,
  CSTM_TRADE1,
  AMT_USE,
  OUT_FLAG1,
  CAP_STAT,
  PRODUCT_TYPE2,
  DEADLINE2,
  DEINT_FLAG,
  MORTGAGE_FLAG,
  STAGE_FLAG,
  CSTM_TRADE2,
  AMT_TX,
  OUT_FLAG2,
  COST_STAT,
  start_dt,
  end_dt)
SELECT
  N.ITM_NO,
  N.FLAG,
  N.CE_FLAG,
  N.ACC_TYPE,
  N.PRODUCT_TYPE1,
  N.DEADLINE1,
  N.CSTM_TRADE1,
  N.AMT_USE,
  N.OUT_FLAG1,
  N.CAP_STAT,
  N.PRODUCT_TYPE2,
  N.DEADLINE2,
  N.DEINT_FLAG,
  N.MORTGAGE_FLAG,
  N.STAGE_FLAG,
  N.CSTM_TRADE2,
  N.AMT_TX,
  N.OUT_FLAG2,
  N.COST_STAT,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(ITM_NO, '' ) AS ITM_NO ,
  COALESCE(FLAG, '' ) AS FLAG ,
  COALESCE(CE_FLAG, '' ) AS CE_FLAG ,
  COALESCE(ACC_TYPE, '' ) AS ACC_TYPE ,
  COALESCE(PRODUCT_TYPE1, '' ) AS PRODUCT_TYPE1 ,
  COALESCE(DEADLINE1, '' ) AS DEADLINE1 ,
  COALESCE(CSTM_TRADE1, '' ) AS CSTM_TRADE1 ,
  COALESCE(AMT_USE, '' ) AS AMT_USE ,
  COALESCE(OUT_FLAG1, '' ) AS OUT_FLAG1 ,
  COALESCE(CAP_STAT, '' ) AS CAP_STAT ,
  COALESCE(PRODUCT_TYPE2, '' ) AS PRODUCT_TYPE2 ,
  COALESCE(DEADLINE2, '' ) AS DEADLINE2 ,
  COALESCE(DEINT_FLAG, '' ) AS DEINT_FLAG ,
  COALESCE(MORTGAGE_FLAG, '' ) AS MORTGAGE_FLAG ,
  COALESCE(STAGE_FLAG, '' ) AS STAGE_FLAG ,
  COALESCE(CSTM_TRADE2, '' ) AS CSTM_TRADE2 ,
  COALESCE(AMT_TX, '' ) AS AMT_TX ,
  COALESCE(OUT_FLAG2, '' ) AS OUT_FLAG2 ,
  COALESCE(COST_STAT, '' ) AS COST_STAT 
 FROM  dw_tdata.ACC_000_T_MNG_OS_AITM_CON_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  ITM_NO ,
  FLAG ,
  CE_FLAG ,
  ACC_TYPE ,
  PRODUCT_TYPE1 ,
  DEADLINE1 ,
  CSTM_TRADE1 ,
  AMT_USE ,
  OUT_FLAG1 ,
  CAP_STAT ,
  PRODUCT_TYPE2 ,
  DEADLINE2 ,
  DEINT_FLAG ,
  MORTGAGE_FLAG ,
  STAGE_FLAG ,
  CSTM_TRADE2 ,
  AMT_TX ,
  OUT_FLAG2 ,
  COST_STAT 
 FROM dw_sdata.ACC_000_T_MNG_OS_AITM_CON 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.ITM_NO = T.ITM_NO
WHERE
(T.ITM_NO IS NULL)
 OR N.FLAG<>T.FLAG
 OR N.CE_FLAG<>T.CE_FLAG
 OR N.ACC_TYPE<>T.ACC_TYPE
 OR N.PRODUCT_TYPE1<>T.PRODUCT_TYPE1
 OR N.DEADLINE1<>T.DEADLINE1
 OR N.CSTM_TRADE1<>T.CSTM_TRADE1
 OR N.AMT_USE<>T.AMT_USE
 OR N.OUT_FLAG1<>T.OUT_FLAG1
 OR N.CAP_STAT<>T.CAP_STAT
 OR N.PRODUCT_TYPE2<>T.PRODUCT_TYPE2
 OR N.DEADLINE2<>T.DEADLINE2
 OR N.DEINT_FLAG<>T.DEINT_FLAG
 OR N.MORTGAGE_FLAG<>T.MORTGAGE_FLAG
 OR N.STAGE_FLAG<>T.STAGE_FLAG
 OR N.CSTM_TRADE2<>T.CSTM_TRADE2
 OR N.AMT_TX<>T.AMT_TX
 OR N.OUT_FLAG2<>T.OUT_FLAG2
 OR N.COST_STAT<>T.COST_STAT
;

--Step3:
UPDATE dw_sdata.ACC_000_T_MNG_OS_AITM_CON P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_5
WHERE P.End_Dt=DATE('2100-12-31')
AND P.ITM_NO=T_5.ITM_NO
;

--Step4:
INSERT  INTO dw_sdata.ACC_000_T_MNG_OS_AITM_CON SELECT * FROM T_5;

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
