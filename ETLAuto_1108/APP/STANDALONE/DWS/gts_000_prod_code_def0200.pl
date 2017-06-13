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
DELETE FROM dw_sdata.GTS_000_PROD_CODE_DEF WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.GTS_000_PROD_CODE_DEF SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_209 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.GTS_000_PROD_CODE_DEF WHERE 1=0;

--Step2:
INSERT  INTO T_209 (
  PROD_CODE,
  PROD_NAME,
  BOURSE_ID,
  CURRENCY_ID,
  MARKET_ID,
  VARIETY_TYPE,
  VARIETY_ID,
  TICK,
  T0_SIGN,
  BAIL_SIGN,
  BAIL_RATE,
  LIMIT_SIGN,
  UPPER_LIMIT_VALUE,
  LOWER_LIMIT_VALUE,
  ACTIVE_FLAG,
  INST_STAT,
  REF_PRICE,
  RECV_RATE,
  BUSI_MODE,
  MAX_HAND,
  MIN_HAND,
  EXCH_UNIT,
  MEASURE_UNIT,
  ENTR_WAY_STR,
  FARE_SIGN,
  FARE_VALUE,
  FARE_MODEL_ID,
  DUE_DATE,
  DELI_DAYS,
  start_dt,
  end_dt)
SELECT
  N.PROD_CODE,
  N.PROD_NAME,
  N.BOURSE_ID,
  N.CURRENCY_ID,
  N.MARKET_ID,
  N.VARIETY_TYPE,
  N.VARIETY_ID,
  N.TICK,
  N.T0_SIGN,
  N.BAIL_SIGN,
  N.BAIL_RATE,
  N.LIMIT_SIGN,
  N.UPPER_LIMIT_VALUE,
  N.LOWER_LIMIT_VALUE,
  N.ACTIVE_FLAG,
  N.INST_STAT,
  N.REF_PRICE,
  N.RECV_RATE,
  N.BUSI_MODE,
  N.MAX_HAND,
  N.MIN_HAND,
  N.EXCH_UNIT,
  N.MEASURE_UNIT,
  N.ENTR_WAY_STR,
  N.FARE_SIGN,
  N.FARE_VALUE,
  N.FARE_MODEL_ID,
  N.DUE_DATE,
  N.DELI_DAYS,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(PROD_CODE, '' ) AS PROD_CODE ,
  COALESCE(PROD_NAME, '' ) AS PROD_NAME ,
  COALESCE(BOURSE_ID, '' ) AS BOURSE_ID ,
  COALESCE(CURRENCY_ID, '' ) AS CURRENCY_ID ,
  COALESCE(MARKET_ID, '' ) AS MARKET_ID ,
  COALESCE(VARIETY_TYPE, '' ) AS VARIETY_TYPE ,
  COALESCE(VARIETY_ID, '' ) AS VARIETY_ID ,
  COALESCE(TICK, 0 ) AS TICK ,
  COALESCE(T0_SIGN, '' ) AS T0_SIGN ,
  COALESCE(BAIL_SIGN, '' ) AS BAIL_SIGN ,
  COALESCE(BAIL_RATE, 0 ) AS BAIL_RATE ,
  COALESCE(LIMIT_SIGN, '' ) AS LIMIT_SIGN ,
  COALESCE(UPPER_LIMIT_VALUE, 0 ) AS UPPER_LIMIT_VALUE ,
  COALESCE(LOWER_LIMIT_VALUE, 0 ) AS LOWER_LIMIT_VALUE ,
  COALESCE(ACTIVE_FLAG, '' ) AS ACTIVE_FLAG ,
  COALESCE(INST_STAT, '' ) AS INST_STAT ,
  COALESCE(REF_PRICE, 0 ) AS REF_PRICE ,
  COALESCE(RECV_RATE, 0 ) AS RECV_RATE ,
  COALESCE(BUSI_MODE, '' ) AS BUSI_MODE ,
  COALESCE(MAX_HAND, 0 ) AS MAX_HAND ,
  COALESCE(MIN_HAND, 0 ) AS MIN_HAND ,
  COALESCE(EXCH_UNIT, '' ) AS EXCH_UNIT ,
  COALESCE(MEASURE_UNIT, 0 ) AS MEASURE_UNIT ,
  COALESCE(ENTR_WAY_STR, '' ) AS ENTR_WAY_STR ,
  COALESCE(FARE_SIGN, '' ) AS FARE_SIGN ,
  COALESCE(FARE_VALUE, 0 ) AS FARE_VALUE ,
  COALESCE(FARE_MODEL_ID, '' ) AS FARE_MODEL_ID ,
  COALESCE(DUE_DATE, '' ) AS DUE_DATE ,
  COALESCE(DELI_DAYS, 0 ) AS DELI_DAYS 
 FROM  dw_tdata.GTS_000_PROD_CODE_DEF_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  PROD_CODE ,
  PROD_NAME ,
  BOURSE_ID ,
  CURRENCY_ID ,
  MARKET_ID ,
  VARIETY_TYPE ,
  VARIETY_ID ,
  TICK ,
  T0_SIGN ,
  BAIL_SIGN ,
  BAIL_RATE ,
  LIMIT_SIGN ,
  UPPER_LIMIT_VALUE ,
  LOWER_LIMIT_VALUE ,
  ACTIVE_FLAG ,
  INST_STAT ,
  REF_PRICE ,
  RECV_RATE ,
  BUSI_MODE ,
  MAX_HAND ,
  MIN_HAND ,
  EXCH_UNIT ,
  MEASURE_UNIT ,
  ENTR_WAY_STR ,
  FARE_SIGN ,
  FARE_VALUE ,
  FARE_MODEL_ID ,
  DUE_DATE ,
  DELI_DAYS 
 FROM dw_sdata.GTS_000_PROD_CODE_DEF 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.PROD_CODE = T.PROD_CODE
WHERE
(T.PROD_CODE IS NULL)
 OR N.PROD_NAME<>T.PROD_NAME
 OR N.BOURSE_ID<>T.BOURSE_ID
 OR N.CURRENCY_ID<>T.CURRENCY_ID
 OR N.MARKET_ID<>T.MARKET_ID
 OR N.VARIETY_TYPE<>T.VARIETY_TYPE
 OR N.VARIETY_ID<>T.VARIETY_ID
 OR N.TICK<>T.TICK
 OR N.T0_SIGN<>T.T0_SIGN
 OR N.BAIL_SIGN<>T.BAIL_SIGN
 OR N.BAIL_RATE<>T.BAIL_RATE
 OR N.LIMIT_SIGN<>T.LIMIT_SIGN
 OR N.UPPER_LIMIT_VALUE<>T.UPPER_LIMIT_VALUE
 OR N.LOWER_LIMIT_VALUE<>T.LOWER_LIMIT_VALUE
 OR N.ACTIVE_FLAG<>T.ACTIVE_FLAG
 OR N.INST_STAT<>T.INST_STAT
 OR N.REF_PRICE<>T.REF_PRICE
 OR N.RECV_RATE<>T.RECV_RATE
 OR N.BUSI_MODE<>T.BUSI_MODE
 OR N.MAX_HAND<>T.MAX_HAND
 OR N.MIN_HAND<>T.MIN_HAND
 OR N.EXCH_UNIT<>T.EXCH_UNIT
 OR N.MEASURE_UNIT<>T.MEASURE_UNIT
 OR N.ENTR_WAY_STR<>T.ENTR_WAY_STR
 OR N.FARE_SIGN<>T.FARE_SIGN
 OR N.FARE_VALUE<>T.FARE_VALUE
 OR N.FARE_MODEL_ID<>T.FARE_MODEL_ID
 OR N.DUE_DATE<>T.DUE_DATE
 OR N.DELI_DAYS<>T.DELI_DAYS
;

--Step3:
UPDATE dw_sdata.GTS_000_PROD_CODE_DEF P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_209
WHERE P.End_Dt=DATE('2100-12-31')
AND P.PROD_CODE=T_209.PROD_CODE
;

--Step4:
INSERT  INTO dw_sdata.GTS_000_PROD_CODE_DEF SELECT * FROM T_209;

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
