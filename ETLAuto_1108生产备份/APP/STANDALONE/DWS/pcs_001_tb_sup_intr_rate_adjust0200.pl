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
DELETE FROM dw_sdata.PCS_001_TB_SUP_INTR_RATE_ADJUST WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.PCS_001_TB_SUP_INTR_RATE_ADJUST SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_314 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.PCS_001_TB_SUP_INTR_RATE_ADJUST WHERE 1=0;

--Step2:
INSERT  INTO T_314 (
  PRV_COD,
  RCV_DATE,
  OPN_DEP,
  TAL_DEP,
  DUE_NUM,
  TRAN_FROM,
  NOR_ITR_RATE,
  DEL_ITR_RATE,
  ITR_DATE,
  FIN_FLG,
  FIN_DATE,
  RMK1,
  RMK2,
  start_dt,
  end_dt)
SELECT
  N.PRV_COD,
  N.RCV_DATE,
  N.OPN_DEP,
  N.TAL_DEP,
  N.DUE_NUM,
  N.TRAN_FROM,
  N.NOR_ITR_RATE,
  N.DEL_ITR_RATE,
  N.ITR_DATE,
  N.FIN_FLG,
  N.FIN_DATE,
  N.RMK1,
  N.RMK2,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(PRV_COD, '' ) AS PRV_COD ,
  COALESCE(RCV_DATE, '' ) AS RCV_DATE ,
  COALESCE(OPN_DEP, '' ) AS OPN_DEP ,
  COALESCE(TAL_DEP, '' ) AS TAL_DEP ,
  COALESCE(DUE_NUM, '' ) AS DUE_NUM ,
  COALESCE(TRAN_FROM, '' ) AS TRAN_FROM ,
  COALESCE(NOR_ITR_RATE, 0 ) AS NOR_ITR_RATE ,
  COALESCE(DEL_ITR_RATE, 0 ) AS DEL_ITR_RATE ,
  COALESCE(ITR_DATE, '' ) AS ITR_DATE ,
  COALESCE(FIN_FLG, '' ) AS FIN_FLG ,
  COALESCE(FIN_DATE, '' ) AS FIN_DATE ,
  COALESCE(RMK1, '' ) AS RMK1 ,
  COALESCE(RMK2, '' ) AS RMK2 
 FROM  dw_tdata.PCS_001_TB_SUP_INTR_RATE_ADJUST_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  PRV_COD ,
  RCV_DATE ,
  OPN_DEP ,
  TAL_DEP ,
  DUE_NUM ,
  TRAN_FROM ,
  NOR_ITR_RATE ,
  DEL_ITR_RATE ,
  ITR_DATE ,
  FIN_FLG ,
  FIN_DATE ,
  RMK1 ,
  RMK2 
 FROM dw_sdata.PCS_001_TB_SUP_INTR_RATE_ADJUST 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.DUE_NUM = T.DUE_NUM AND N.ITR_DATE = T.ITR_DATE
WHERE
(T.DUE_NUM IS NULL AND T.ITR_DATE IS NULL)
 OR N.PRV_COD<>T.PRV_COD
 OR N.RCV_DATE<>T.RCV_DATE
 OR N.OPN_DEP<>T.OPN_DEP
 OR N.TAL_DEP<>T.TAL_DEP
 OR N.TRAN_FROM<>T.TRAN_FROM
 OR N.NOR_ITR_RATE<>T.NOR_ITR_RATE
 OR N.DEL_ITR_RATE<>T.DEL_ITR_RATE
 OR N.FIN_FLG<>T.FIN_FLG
 OR N.FIN_DATE<>T.FIN_DATE
 OR N.RMK1<>T.RMK1
 OR N.RMK2<>T.RMK2
;

--Step3:
UPDATE dw_sdata.PCS_001_TB_SUP_INTR_RATE_ADJUST P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_314
WHERE P.End_Dt=DATE('2100-12-31')
AND P.DUE_NUM=T_314.DUE_NUM
AND P.ITR_DATE=T_314.ITR_DATE
;

--Step4:
INSERT  INTO dw_sdata.PCS_001_TB_SUP_INTR_RATE_ADJUST SELECT * FROM T_314;

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
