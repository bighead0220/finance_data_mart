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
DELETE FROM dw_sdata.IBS_001_T_CE_PRC WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.IBS_001_T_CE_PRC SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_215 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.IBS_001_T_CE_PRC WHERE 1=0;

--Step2:
INSERT  INTO T_215 (
  BUY_CURR_TYPE,
  SELL_CURR_TYPE,
  EFFECT_DATE,
  EFFECT_TIME,
  FC_RATION,
  MID_PRC,
  FE_BUY_PRC,
  FC_BUY_PRC,
  FE_SELL_PRC,
  FC_SELL_PRC,
  INN_BUY_PRC,
  INN_SELL_PRC,
  INTTAX_PRC,
  LST_MOD_TLR,
  LST_MOD_DATE,
  LST_MOD_TIME,
  start_dt,
  end_dt)
SELECT
  N.BUY_CURR_TYPE,
  N.SELL_CURR_TYPE,
  N.EFFECT_DATE,
  N.EFFECT_TIME,
  N.FC_RATION,
  N.MID_PRC,
  N.FE_BUY_PRC,
  N.FC_BUY_PRC,
  N.FE_SELL_PRC,
  N.FC_SELL_PRC,
  N.INN_BUY_PRC,
  N.INN_SELL_PRC,
  N.INTTAX_PRC,
  N.LST_MOD_TLR,
  N.LST_MOD_DATE,
  N.LST_MOD_TIME,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(BUY_CURR_TYPE, '' ) AS BUY_CURR_TYPE ,
  COALESCE(SELL_CURR_TYPE, '' ) AS SELL_CURR_TYPE ,
  COALESCE(EFFECT_DATE, 0 ) AS EFFECT_DATE ,
  COALESCE(EFFECT_TIME, '' ) AS EFFECT_TIME ,
  COALESCE(FC_RATION, 0 ) AS FC_RATION ,
  COALESCE(MID_PRC, 0 ) AS MID_PRC ,
  COALESCE(FE_BUY_PRC, 0 ) AS FE_BUY_PRC ,
  COALESCE(FC_BUY_PRC, 0 ) AS FC_BUY_PRC ,
  COALESCE(FE_SELL_PRC, 0 ) AS FE_SELL_PRC ,
  COALESCE(FC_SELL_PRC, 0 ) AS FC_SELL_PRC ,
  COALESCE(INN_BUY_PRC, 0 ) AS INN_BUY_PRC ,
  COALESCE(INN_SELL_PRC, 0 ) AS INN_SELL_PRC ,
  COALESCE(INTTAX_PRC, 0 ) AS INTTAX_PRC ,
  COALESCE(LST_MOD_TLR, '' ) AS LST_MOD_TLR ,
  COALESCE(LST_MOD_DATE, 0 ) AS LST_MOD_DATE ,
  COALESCE(LST_MOD_TIME, '' ) AS LST_MOD_TIME 
 FROM  dw_tdata.IBS_001_T_CE_PRC_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  BUY_CURR_TYPE ,
  SELL_CURR_TYPE ,
  EFFECT_DATE ,
  EFFECT_TIME ,
  FC_RATION ,
  MID_PRC ,
  FE_BUY_PRC ,
  FC_BUY_PRC ,
  FE_SELL_PRC ,
  FC_SELL_PRC ,
  INN_BUY_PRC ,
  INN_SELL_PRC ,
  INTTAX_PRC ,
  LST_MOD_TLR ,
  LST_MOD_DATE ,
  LST_MOD_TIME 
 FROM dw_sdata.IBS_001_T_CE_PRC 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.BUY_CURR_TYPE = T.BUY_CURR_TYPE AND N.SELL_CURR_TYPE = T.SELL_CURR_TYPE AND N.EFFECT_DATE = T.EFFECT_DATE AND N.EFFECT_TIME = T.EFFECT_TIME
WHERE
(T.BUY_CURR_TYPE IS NULL AND T.SELL_CURR_TYPE IS NULL AND T.EFFECT_DATE IS NULL AND T.EFFECT_TIME IS NULL)
 OR N.FC_RATION<>T.FC_RATION
 OR N.MID_PRC<>T.MID_PRC
 OR N.FE_BUY_PRC<>T.FE_BUY_PRC
 OR N.FC_BUY_PRC<>T.FC_BUY_PRC
 OR N.FE_SELL_PRC<>T.FE_SELL_PRC
 OR N.FC_SELL_PRC<>T.FC_SELL_PRC
 OR N.INN_BUY_PRC<>T.INN_BUY_PRC
 OR N.INN_SELL_PRC<>T.INN_SELL_PRC
 OR N.INTTAX_PRC<>T.INTTAX_PRC
 OR N.LST_MOD_TLR<>T.LST_MOD_TLR
 OR N.LST_MOD_DATE<>T.LST_MOD_DATE
 OR N.LST_MOD_TIME<>T.LST_MOD_TIME
;

--Step3:
UPDATE dw_sdata.IBS_001_T_CE_PRC P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_215
WHERE P.End_Dt=DATE('2100-12-31')
AND P.BUY_CURR_TYPE=T_215.BUY_CURR_TYPE
AND P.SELL_CURR_TYPE=T_215.SELL_CURR_TYPE
AND P.EFFECT_DATE=T_215.EFFECT_DATE
AND P.EFFECT_TIME=T_215.EFFECT_TIME
;

--Step4:
INSERT  INTO dw_sdata.IBS_001_T_CE_PRC SELECT * FROM T_215;

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
