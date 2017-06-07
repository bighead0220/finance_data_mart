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
DELETE FROM dw_sdata.ACC_003_T_ACC_INN_OPCL WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.ACC_003_T_ACC_INN_OPCL SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_22 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.ACC_003_T_ACC_INN_OPCL WHERE 1=0;

--Step2:
INSERT  INTO T_22 (
  INN_ACC,
  ACC_NAME,
  TRAN_DATE,
  ACC_DATA_ID,
  OP_CLS_FLAG,
  TRAN_INST,
  ACC_SET,
  CURR_TYPE,
  OP_DATE,
  DUE_DATE,
  CLS_DATE,
  TLR_NO,
  OP_FLAG,
  OP_BATCH_NO,
  OP_APP_INST,
  OP_APP_TLR_NO,
  OP_APP_NO,
  ACC_INST,
  CLS_INST,
  CLS_BATCH_NO,
  CLS_APP_INST,
  CLS_APP_TLR_NO,
  CLS_APP_NO,
  start_dt,
  end_dt)
SELECT
  N.INN_ACC,
  N.ACC_NAME,
  N.TRAN_DATE,
  N.ACC_DATA_ID,
  N.OP_CLS_FLAG,
  N.TRAN_INST,
  N.ACC_SET,
  N.CURR_TYPE,
  N.OP_DATE,
  N.DUE_DATE,
  N.CLS_DATE,
  N.TLR_NO,
  N.OP_FLAG,
  N.OP_BATCH_NO,
  N.OP_APP_INST,
  N.OP_APP_TLR_NO,
  N.OP_APP_NO,
  N.ACC_INST,
  N.CLS_INST,
  N.CLS_BATCH_NO,
  N.CLS_APP_INST,
  N.CLS_APP_TLR_NO,
  N.CLS_APP_NO,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(INN_ACC, '' ) AS INN_ACC ,
  COALESCE(ACC_NAME, '' ) AS ACC_NAME ,
  COALESCE(TRAN_DATE, '' ) AS TRAN_DATE ,
  COALESCE(ACC_DATA_ID, '' ) AS ACC_DATA_ID ,
  COALESCE(OP_CLS_FLAG, '' ) AS OP_CLS_FLAG ,
  COALESCE(TRAN_INST, '' ) AS TRAN_INST ,
  COALESCE(ACC_SET, '' ) AS ACC_SET ,
  COALESCE(CURR_TYPE, '' ) AS CURR_TYPE ,
  COALESCE(OP_DATE, '' ) AS OP_DATE ,
  COALESCE(DUE_DATE, '' ) AS DUE_DATE ,
  COALESCE(CLS_DATE, '' ) AS CLS_DATE ,
  COALESCE(TLR_NO, '' ) AS TLR_NO ,
  COALESCE(OP_FLAG, '' ) AS OP_FLAG ,
  COALESCE(OP_BATCH_NO, '' ) AS OP_BATCH_NO ,
  COALESCE(OP_APP_INST, '' ) AS OP_APP_INST ,
  COALESCE(OP_APP_TLR_NO, '' ) AS OP_APP_TLR_NO ,
  COALESCE(OP_APP_NO, '' ) AS OP_APP_NO ,
  COALESCE(ACC_INST, '' ) AS ACC_INST ,
  COALESCE(CLS_INST, '' ) AS CLS_INST ,
  COALESCE(CLS_BATCH_NO, '' ) AS CLS_BATCH_NO ,
  COALESCE(CLS_APP_INST, '' ) AS CLS_APP_INST ,
  COALESCE(CLS_APP_TLR_NO, '' ) AS CLS_APP_TLR_NO ,
  COALESCE(CLS_APP_NO, '' ) AS CLS_APP_NO 
 FROM  dw_tdata.ACC_003_T_ACC_INN_OPCL_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  INN_ACC ,
  ACC_NAME ,
  TRAN_DATE ,
  ACC_DATA_ID ,
  OP_CLS_FLAG ,
  TRAN_INST ,
  ACC_SET ,
  CURR_TYPE ,
  OP_DATE ,
  DUE_DATE ,
  CLS_DATE ,
  TLR_NO ,
  OP_FLAG ,
  OP_BATCH_NO ,
  OP_APP_INST ,
  OP_APP_TLR_NO ,
  OP_APP_NO ,
  ACC_INST ,
  CLS_INST ,
  CLS_BATCH_NO ,
  CLS_APP_INST ,
  CLS_APP_TLR_NO ,
  CLS_APP_NO 
 FROM dw_sdata.ACC_003_T_ACC_INN_OPCL 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.INN_ACC = T.INN_ACC
WHERE
(T.INN_ACC IS NULL)
 OR N.ACC_NAME<>T.ACC_NAME
 OR N.TRAN_DATE<>T.TRAN_DATE
 OR N.ACC_DATA_ID<>T.ACC_DATA_ID
 OR N.OP_CLS_FLAG<>T.OP_CLS_FLAG
 OR N.TRAN_INST<>T.TRAN_INST
 OR N.ACC_SET<>T.ACC_SET
 OR N.CURR_TYPE<>T.CURR_TYPE
 OR N.OP_DATE<>T.OP_DATE
 OR N.DUE_DATE<>T.DUE_DATE
 OR N.CLS_DATE<>T.CLS_DATE
 OR N.TLR_NO<>T.TLR_NO
 OR N.OP_FLAG<>T.OP_FLAG
 OR N.OP_BATCH_NO<>T.OP_BATCH_NO
 OR N.OP_APP_INST<>T.OP_APP_INST
 OR N.OP_APP_TLR_NO<>T.OP_APP_TLR_NO
 OR N.OP_APP_NO<>T.OP_APP_NO
 OR N.ACC_INST<>T.ACC_INST
 OR N.CLS_INST<>T.CLS_INST
 OR N.CLS_BATCH_NO<>T.CLS_BATCH_NO
 OR N.CLS_APP_INST<>T.CLS_APP_INST
 OR N.CLS_APP_TLR_NO<>T.CLS_APP_TLR_NO
 OR N.CLS_APP_NO<>T.CLS_APP_NO
;

--Step3:
UPDATE dw_sdata.ACC_003_T_ACC_INN_OPCL P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_22
WHERE P.End_Dt=DATE('2100-12-31')
AND P.INN_ACC=T_22.INN_ACC
;

--Step4:
INSERT  INTO dw_sdata.ACC_003_T_ACC_INN_OPCL SELECT * FROM T_22;

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
