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
DELETE FROM dw_sdata.ACC_002_T_ACC_INST_DVLP WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.ACC_002_T_ACC_INST_DVLP SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_10 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.ACC_002_T_ACC_INST_DVLP WHERE 1=0;

--Step2:
INSERT  INTO T_10 (
  INST_NO,
  ITM_NO,
  ACC_SET,
  CURR_TYPE,
  ITM_LV,
  OBS_FLAG,
  SUM_DATE,
  DR_BAL_BGN_MON,
  CR_BAL_BGN_MON,
  DR_AMT_MON,
  CR_AMT_MON,
  DR_NUM_MON,
  CR_NUM_MON,
  DR_CRT_BAL,
  CR_CRT_BAL,
  RCV_BAL_BGN_MON,
  SND_BAL_BGN_MON,
  RCV_NUM_BGN_MON,
  SND_NUM_BGN_MON,
  RCV_AMT_MON,
  SND_AMT_MON,
  RCV_NUM_MON,
  SND_NUM_MON,
  RCV_CRT_BAL,
  SND_CRT_BAL,
  RCV_CRT_NUM,
  SND_CRT_NUM,
  FLAG,
  start_dt,
  end_dt)
SELECT
  N.INST_NO,
  N.ITM_NO,
  N.ACC_SET,
  N.CURR_TYPE,
  N.ITM_LV,
  N.OBS_FLAG,
  N.SUM_DATE,
  N.DR_BAL_BGN_MON,
  N.CR_BAL_BGN_MON,
  N.DR_AMT_MON,
  N.CR_AMT_MON,
  N.DR_NUM_MON,
  N.CR_NUM_MON,
  N.DR_CRT_BAL,
  N.CR_CRT_BAL,
  N.RCV_BAL_BGN_MON,
  N.SND_BAL_BGN_MON,
  N.RCV_NUM_BGN_MON,
  N.SND_NUM_BGN_MON,
  N.RCV_AMT_MON,
  N.SND_AMT_MON,
  N.RCV_NUM_MON,
  N.SND_NUM_MON,
  N.RCV_CRT_BAL,
  N.SND_CRT_BAL,
  N.RCV_CRT_NUM,
  N.SND_CRT_NUM,
  N.FLAG,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(INST_NO, '' ) AS INST_NO ,
  COALESCE(ITM_NO, '' ) AS ITM_NO ,
  COALESCE(ACC_SET, '' ) AS ACC_SET ,
  COALESCE(CURR_TYPE, '' ) AS CURR_TYPE ,
  COALESCE(ITM_LV, '' ) AS ITM_LV ,
  COALESCE(OBS_FLAG, '' ) AS OBS_FLAG ,
  COALESCE(SUM_DATE, '' ) AS SUM_DATE ,
  COALESCE(DR_BAL_BGN_MON, 0 ) AS DR_BAL_BGN_MON ,
  COALESCE(CR_BAL_BGN_MON, 0 ) AS CR_BAL_BGN_MON ,
  COALESCE(DR_AMT_MON, 0 ) AS DR_AMT_MON ,
  COALESCE(CR_AMT_MON, 0 ) AS CR_AMT_MON ,
  COALESCE(DR_NUM_MON, 0 ) AS DR_NUM_MON ,
  COALESCE(CR_NUM_MON, 0 ) AS CR_NUM_MON ,
  COALESCE(DR_CRT_BAL, 0 ) AS DR_CRT_BAL ,
  COALESCE(CR_CRT_BAL, 0 ) AS CR_CRT_BAL ,
  COALESCE(RCV_BAL_BGN_MON, 0 ) AS RCV_BAL_BGN_MON ,
  COALESCE(SND_BAL_BGN_MON, 0 ) AS SND_BAL_BGN_MON ,
  COALESCE(RCV_NUM_BGN_MON, 0 ) AS RCV_NUM_BGN_MON ,
  COALESCE(SND_NUM_BGN_MON, 0 ) AS SND_NUM_BGN_MON ,
  COALESCE(RCV_AMT_MON, 0 ) AS RCV_AMT_MON ,
  COALESCE(SND_AMT_MON, 0 ) AS SND_AMT_MON ,
  COALESCE(RCV_NUM_MON, 0 ) AS RCV_NUM_MON ,
  COALESCE(SND_NUM_MON, 0 ) AS SND_NUM_MON ,
  COALESCE(RCV_CRT_BAL, 0 ) AS RCV_CRT_BAL ,
  COALESCE(SND_CRT_BAL, 0 ) AS SND_CRT_BAL ,
  COALESCE(RCV_CRT_NUM, 0 ) AS RCV_CRT_NUM ,
  COALESCE(SND_CRT_NUM, 0 ) AS SND_CRT_NUM ,
  COALESCE(FLAG, '' ) AS FLAG 
 FROM  dw_tdata.ACC_002_T_ACC_INST_DVLP_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  INST_NO ,
  ITM_NO ,
  ACC_SET ,
  CURR_TYPE ,
  ITM_LV ,
  OBS_FLAG ,
  SUM_DATE ,
  DR_BAL_BGN_MON ,
  CR_BAL_BGN_MON ,
  DR_AMT_MON ,
  CR_AMT_MON ,
  DR_NUM_MON ,
  CR_NUM_MON ,
  DR_CRT_BAL ,
  CR_CRT_BAL ,
  RCV_BAL_BGN_MON ,
  SND_BAL_BGN_MON ,
  RCV_NUM_BGN_MON ,
  SND_NUM_BGN_MON ,
  RCV_AMT_MON ,
  SND_AMT_MON ,
  RCV_NUM_MON ,
  SND_NUM_MON ,
  RCV_CRT_BAL ,
  SND_CRT_BAL ,
  RCV_CRT_NUM ,
  SND_CRT_NUM ,
  FLAG 
 FROM dw_sdata.ACC_002_T_ACC_INST_DVLP 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.INST_NO = T.INST_NO AND N.ITM_NO = T.ITM_NO AND N.ACC_SET = T.ACC_SET
WHERE
(T.INST_NO IS NULL AND T.ITM_NO IS NULL AND T.ACC_SET IS NULL)
 OR N.CURR_TYPE<>T.CURR_TYPE
 OR N.ITM_LV<>T.ITM_LV
 OR N.OBS_FLAG<>T.OBS_FLAG
 OR N.SUM_DATE<>T.SUM_DATE
 OR N.DR_BAL_BGN_MON<>T.DR_BAL_BGN_MON
 OR N.CR_BAL_BGN_MON<>T.CR_BAL_BGN_MON
 OR N.DR_AMT_MON<>T.DR_AMT_MON
 OR N.CR_AMT_MON<>T.CR_AMT_MON
 OR N.DR_NUM_MON<>T.DR_NUM_MON
 OR N.CR_NUM_MON<>T.CR_NUM_MON
 OR N.DR_CRT_BAL<>T.DR_CRT_BAL
 OR N.CR_CRT_BAL<>T.CR_CRT_BAL
 OR N.RCV_BAL_BGN_MON<>T.RCV_BAL_BGN_MON
 OR N.SND_BAL_BGN_MON<>T.SND_BAL_BGN_MON
 OR N.RCV_NUM_BGN_MON<>T.RCV_NUM_BGN_MON
 OR N.SND_NUM_BGN_MON<>T.SND_NUM_BGN_MON
 OR N.RCV_AMT_MON<>T.RCV_AMT_MON
 OR N.SND_AMT_MON<>T.SND_AMT_MON
 OR N.RCV_NUM_MON<>T.RCV_NUM_MON
 OR N.SND_NUM_MON<>T.SND_NUM_MON
 OR N.RCV_CRT_BAL<>T.RCV_CRT_BAL
 OR N.SND_CRT_BAL<>T.SND_CRT_BAL
 OR N.RCV_CRT_NUM<>T.RCV_CRT_NUM
 OR N.SND_CRT_NUM<>T.SND_CRT_NUM
 OR N.FLAG<>T.FLAG
;

--Step3:
UPDATE dw_sdata.ACC_002_T_ACC_INST_DVLP P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_10
WHERE P.End_Dt=DATE('2100-12-31')
AND P.INST_NO=T_10.INST_NO
AND P.ITM_NO=T_10.ITM_NO
AND P.ACC_SET=T_10.ACC_SET
;

--Step4:
INSERT  INTO dw_sdata.ACC_002_T_ACC_INST_DVLP SELECT * FROM T_10;

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
