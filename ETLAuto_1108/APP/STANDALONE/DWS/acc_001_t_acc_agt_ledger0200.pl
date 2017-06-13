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
DELETE FROM dw_sdata.ACC_001_T_ACC_AGT_LEDGER WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.ACC_001_T_ACC_AGT_LEDGER SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_8 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.ACC_001_T_ACC_AGT_LEDGER WHERE 1=0;

--Step2:
INSERT  INTO T_8 (
  ACC,
  SYS_CODE,
  CSTM_NAME,
  CURR_TYPE,
  ACC_INDEX,
  CE_FLAG,
  ACC_TYPE,
  CSTM_TRADE,
  PRODUCT_TYPE,
  DEADLINE,
  STAT,
  BAL,
  LST_BAL,
  LST_T_DATE,
  LST_TRAN_DATE,
  OP_INST,
  ACC_INST,
  OP_DATE,
  CLS_DATE,
  ITM_NO,
  ACC_OWNNER,
  PROFITE_RATIO,
  ACC_SET,
  OUT_FLAG,
  INT_FLAG,
  COST_RATIO,
  OVDF_FLAG,
  AMT_USE,
  CAP_STAT,
  EXPAND_1,
  EXPAND_2,
  EXPAND_3,
  EXPAND_4,
  EXPAND_5,
  start_dt,
  end_dt)
SELECT
  N.ACC,
  N.SYS_CODE,
  N.CSTM_NAME,
  N.CURR_TYPE,
  N.ACC_INDEX,
  N.CE_FLAG,
  N.ACC_TYPE,
  N.CSTM_TRADE,
  N.PRODUCT_TYPE,
  N.DEADLINE,
  N.STAT,
  N.BAL,
  N.LST_BAL,
  N.LST_T_DATE,
  N.LST_TRAN_DATE,
  N.OP_INST,
  N.ACC_INST,
  N.OP_DATE,
  N.CLS_DATE,
  N.ITM_NO,
  N.ACC_OWNNER,
  N.PROFITE_RATIO,
  N.ACC_SET,
  N.OUT_FLAG,
  N.INT_FLAG,
  N.COST_RATIO,
  N.OVDF_FLAG,
  N.AMT_USE,
  N.CAP_STAT,
  N.EXPAND_1,
  N.EXPAND_2,
  N.EXPAND_3,
  N.EXPAND_4,
  N.EXPAND_5,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(ACC, '' ) AS ACC ,
  COALESCE(SYS_CODE, '' ) AS SYS_CODE ,
  COALESCE(CSTM_NAME, '' ) AS CSTM_NAME ,
  COALESCE(CURR_TYPE, '' ) AS CURR_TYPE ,
  COALESCE(ACC_INDEX, '' ) AS ACC_INDEX ,
  COALESCE(CE_FLAG, '' ) AS CE_FLAG ,
  COALESCE(ACC_TYPE, '' ) AS ACC_TYPE ,
  COALESCE(CSTM_TRADE, '' ) AS CSTM_TRADE ,
  COALESCE(PRODUCT_TYPE, '' ) AS PRODUCT_TYPE ,
  COALESCE(DEADLINE, '' ) AS DEADLINE ,
  COALESCE(STAT, '' ) AS STAT ,
  COALESCE(BAL, 0 ) AS BAL ,
  COALESCE(LST_BAL, 0 ) AS LST_BAL ,
  COALESCE(LST_T_DATE, '' ) AS LST_T_DATE ,
  COALESCE(LST_TRAN_DATE, '' ) AS LST_TRAN_DATE ,
  COALESCE(OP_INST, '' ) AS OP_INST ,
  COALESCE(ACC_INST, '' ) AS ACC_INST ,
  COALESCE(OP_DATE, '' ) AS OP_DATE ,
  COALESCE(CLS_DATE, '' ) AS CLS_DATE ,
  COALESCE(ITM_NO, '' ) AS ITM_NO ,
  COALESCE(ACC_OWNNER, '' ) AS ACC_OWNNER ,
  COALESCE(PROFITE_RATIO, 0 ) AS PROFITE_RATIO ,
  COALESCE(ACC_SET, '' ) AS ACC_SET ,
  COALESCE(OUT_FLAG, '' ) AS OUT_FLAG ,
  COALESCE(INT_FLAG, '' ) AS INT_FLAG ,
  COALESCE(COST_RATIO, 0 ) AS COST_RATIO ,
  COALESCE(OVDF_FLAG, '' ) AS OVDF_FLAG ,
  COALESCE(AMT_USE, '' ) AS AMT_USE ,
  COALESCE(CAP_STAT, '' ) AS CAP_STAT ,
  COALESCE(EXPAND_1, '' ) AS EXPAND_1 ,
  COALESCE(EXPAND_2, '' ) AS EXPAND_2 ,
  COALESCE(EXPAND_3, '' ) AS EXPAND_3 ,
  COALESCE(EXPAND_4, '' ) AS EXPAND_4 ,
  COALESCE(EXPAND_5, 0 ) AS EXPAND_5 
 FROM  dw_tdata.ACC_001_T_ACC_AGT_LEDGER_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  ACC ,
  SYS_CODE ,
  CSTM_NAME ,
  CURR_TYPE ,
  ACC_INDEX ,
  CE_FLAG ,
  ACC_TYPE ,
  CSTM_TRADE ,
  PRODUCT_TYPE ,
  DEADLINE ,
  STAT ,
  BAL ,
  LST_BAL ,
  LST_T_DATE ,
  LST_TRAN_DATE ,
  OP_INST ,
  ACC_INST ,
  OP_DATE ,
  CLS_DATE ,
  ITM_NO ,
  ACC_OWNNER ,
  PROFITE_RATIO ,
  ACC_SET ,
  OUT_FLAG ,
  INT_FLAG ,
  COST_RATIO ,
  OVDF_FLAG ,
  AMT_USE ,
  CAP_STAT ,
  EXPAND_1 ,
  EXPAND_2 ,
  EXPAND_3 ,
  EXPAND_4 ,
  EXPAND_5 
 FROM dw_sdata.ACC_001_T_ACC_AGT_LEDGER 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.ACC = T.ACC AND N.SYS_CODE = T.SYS_CODE
WHERE
(T.ACC IS NULL AND T.SYS_CODE IS NULL)
 OR N.CSTM_NAME<>T.CSTM_NAME
 OR N.CURR_TYPE<>T.CURR_TYPE
 OR N.ACC_INDEX<>T.ACC_INDEX
 OR N.CE_FLAG<>T.CE_FLAG
 OR N.ACC_TYPE<>T.ACC_TYPE
 OR N.CSTM_TRADE<>T.CSTM_TRADE
 OR N.PRODUCT_TYPE<>T.PRODUCT_TYPE
 OR N.DEADLINE<>T.DEADLINE
 OR N.STAT<>T.STAT
 OR N.BAL<>T.BAL
 OR N.LST_BAL<>T.LST_BAL
 OR N.LST_T_DATE<>T.LST_T_DATE
 OR N.LST_TRAN_DATE<>T.LST_TRAN_DATE
 OR N.OP_INST<>T.OP_INST
 OR N.ACC_INST<>T.ACC_INST
 OR N.OP_DATE<>T.OP_DATE
 OR N.CLS_DATE<>T.CLS_DATE
 OR N.ITM_NO<>T.ITM_NO
 OR N.ACC_OWNNER<>T.ACC_OWNNER
 OR N.PROFITE_RATIO<>T.PROFITE_RATIO
 OR N.ACC_SET<>T.ACC_SET
 OR N.OUT_FLAG<>T.OUT_FLAG
 OR N.INT_FLAG<>T.INT_FLAG
 OR N.COST_RATIO<>T.COST_RATIO
 OR N.OVDF_FLAG<>T.OVDF_FLAG
 OR N.AMT_USE<>T.AMT_USE
 OR N.CAP_STAT<>T.CAP_STAT
 OR N.EXPAND_1<>T.EXPAND_1
 OR N.EXPAND_2<>T.EXPAND_2
 OR N.EXPAND_3<>T.EXPAND_3
 OR N.EXPAND_4<>T.EXPAND_4
 OR N.EXPAND_5<>T.EXPAND_5
;

--Step3:
UPDATE dw_sdata.ACC_001_T_ACC_AGT_LEDGER P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_8
WHERE P.End_Dt=DATE('2100-12-31')
AND P.ACC=T_8.ACC
AND P.SYS_CODE=T_8.SYS_CODE
;

--Step4:
INSERT  INTO dw_sdata.ACC_001_T_ACC_AGT_LEDGER SELECT * FROM T_8;

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
