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
DELETE FROM dw_sdata.COS_000_EVENTS WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.COS_000_EVENTS SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_153 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.COS_000_EVENTS WHERE 1=0;

--Step2:
INSERT  INTO T_153 (
  DEAL_NO,
  DEALTYPE,
  EVENT_NO,
  FLOW_NO,
  INPUT_DT,
  EFFECT_DT,
  ACTION_DT,
  CCY,
  RATE,
  MARGIN,
  COMMENTS,
  AMOUNT,
  CLOSEAMT,
  FLAG,
  STATUS,
  ADVPRINTED,
  ADVTIME,
  CONFO_NO,
  CNFSTATUS,
  EXCH_GROUP,
  CUSTOMISED,
  NOTIONAL,
  CASHFLOW,
  CPARTY,
  CMPT_NO,
  EVENTGROUP,
  ACTN_NO,
  BASE_AMOUNT,
  DEALT_FACTOR,
  BASE_DATE,
  DEAL_DATETIME,
  start_dt,
  end_dt)
SELECT
  N.DEAL_NO,
  N.DEALTYPE,
  N.EVENT_NO,
  N.FLOW_NO,
  N.INPUT_DT,
  N.EFFECT_DT,
  N.ACTION_DT,
  N.CCY,
  N.RATE,
  N.MARGIN,
  N.COMMENTS,
  N.AMOUNT,
  N.CLOSEAMT,
  N.FLAG,
  N.STATUS,
  N.ADVPRINTED,
  N.ADVTIME,
  N.CONFO_NO,
  N.CNFSTATUS,
  N.EXCH_GROUP,
  N.CUSTOMISED,
  N.NOTIONAL,
  N.CASHFLOW,
  N.CPARTY,
  N.CMPT_NO,
  N.EVENTGROUP,
  N.ACTN_NO,
  N.BASE_AMOUNT,
  N.DEALT_FACTOR,
  N.BASE_DATE,
  N.DEAL_DATETIME,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(DEAL_NO, 0 ) AS DEAL_NO ,
  COALESCE(DEALTYPE, '' ) AS DEALTYPE ,
  COALESCE(EVENT_NO, 0 ) AS EVENT_NO ,
  COALESCE(FLOW_NO, 0 ) AS FLOW_NO ,
  COALESCE(INPUT_DT, '' ) AS INPUT_DT ,
  COALESCE(EFFECT_DT, '' ) AS EFFECT_DT ,
  COALESCE(ACTION_DT, '' ) AS ACTION_DT ,
  COALESCE(CCY, '' ) AS CCY ,
  COALESCE(RATE, 0 ) AS RATE ,
  COALESCE(MARGIN, 0 ) AS MARGIN ,
  COALESCE(COMMENTS, '' ) AS COMMENTS ,
  COALESCE(AMOUNT, 0 ) AS AMOUNT ,
  COALESCE(CLOSEAMT, 0 ) AS CLOSEAMT ,
  COALESCE(FLAG, '' ) AS FLAG ,
  COALESCE(STATUS, '' ) AS STATUS ,
  COALESCE(ADVPRINTED, '' ) AS ADVPRINTED ,
  COALESCE(ADVTIME, '' ) AS ADVTIME ,
  COALESCE(CONFO_NO, 0 ) AS CONFO_NO ,
  COALESCE(CNFSTATUS, '' ) AS CNFSTATUS ,
  COALESCE(EXCH_GROUP, '' ) AS EXCH_GROUP ,
  COALESCE(CUSTOMISED, '' ) AS CUSTOMISED ,
  COALESCE(NOTIONAL, '' ) AS NOTIONAL ,
  COALESCE(CASHFLOW, '' ) AS CASHFLOW ,
  COALESCE(CPARTY, '' ) AS CPARTY ,
  COALESCE(CMPT_NO, 0 ) AS CMPT_NO ,
  COALESCE(EVENTGROUP, 0 ) AS EVENTGROUP ,
  COALESCE(ACTN_NO, 0 ) AS ACTN_NO ,
  COALESCE(BASE_AMOUNT, 0 ) AS BASE_AMOUNT ,
  COALESCE(DEALT_FACTOR, 0 ) AS DEALT_FACTOR ,
  COALESCE(BASE_DATE, '' ) AS BASE_DATE ,
  COALESCE(DEAL_DATETIME, '' ) AS DEAL_DATETIME 
 FROM  dw_tdata.COS_000_EVENTS_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  DEAL_NO ,
  DEALTYPE ,
  EVENT_NO ,
  FLOW_NO ,
  INPUT_DT ,
  EFFECT_DT ,
  ACTION_DT ,
  CCY ,
  RATE ,
  MARGIN ,
  COMMENTS ,
  AMOUNT ,
  CLOSEAMT ,
  FLAG ,
  STATUS ,
  ADVPRINTED ,
  ADVTIME ,
  CONFO_NO ,
  CNFSTATUS ,
  EXCH_GROUP ,
  CUSTOMISED ,
  NOTIONAL ,
  CASHFLOW ,
  CPARTY ,
  CMPT_NO ,
  EVENTGROUP ,
  ACTN_NO ,
  BASE_AMOUNT ,
  DEALT_FACTOR ,
  BASE_DATE ,
  DEAL_DATETIME 
 FROM dw_sdata.COS_000_EVENTS 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.EVENT_NO = T.EVENT_NO
WHERE
(T.EVENT_NO IS NULL)
 OR N.DEAL_NO<>T.DEAL_NO
 OR N.DEALTYPE<>T.DEALTYPE
 OR N.FLOW_NO<>T.FLOW_NO
 OR N.INPUT_DT<>T.INPUT_DT
 OR N.EFFECT_DT<>T.EFFECT_DT
 OR N.ACTION_DT<>T.ACTION_DT
 OR N.CCY<>T.CCY
 OR N.RATE<>T.RATE
 OR N.MARGIN<>T.MARGIN
 OR N.COMMENTS<>T.COMMENTS
 OR N.AMOUNT<>T.AMOUNT
 OR N.CLOSEAMT<>T.CLOSEAMT
 OR N.FLAG<>T.FLAG
 OR N.STATUS<>T.STATUS
 OR N.ADVPRINTED<>T.ADVPRINTED
 OR N.ADVTIME<>T.ADVTIME
 OR N.CONFO_NO<>T.CONFO_NO
 OR N.CNFSTATUS<>T.CNFSTATUS
 OR N.EXCH_GROUP<>T.EXCH_GROUP
 OR N.CUSTOMISED<>T.CUSTOMISED
 OR N.NOTIONAL<>T.NOTIONAL
 OR N.CASHFLOW<>T.CASHFLOW
 OR N.CPARTY<>T.CPARTY
 OR N.CMPT_NO<>T.CMPT_NO
 OR N.EVENTGROUP<>T.EVENTGROUP
 OR N.ACTN_NO<>T.ACTN_NO
 OR N.BASE_AMOUNT<>T.BASE_AMOUNT
 OR N.DEALT_FACTOR<>T.DEALT_FACTOR
 OR N.BASE_DATE<>T.BASE_DATE
 OR N.DEAL_DATETIME<>T.DEAL_DATETIME
;

--Step3:
UPDATE dw_sdata.COS_000_EVENTS P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_153
WHERE P.End_Dt=DATE('2100-12-31')
AND P.EVENT_NO=T_153.EVENT_NO
;

--Step4:
INSERT  INTO dw_sdata.COS_000_EVENTS SELECT * FROM T_153;

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
