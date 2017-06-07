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
DELETE FROM dw_sdata.PCS_005_TB_SUP_PRIN_PLAN_A WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.PCS_005_TB_SUP_PRIN_PLAN_A SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_321 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.PCS_005_TB_SUP_PRIN_PLAN_A WHERE 1=0;

--Step2:
INSERT  INTO T_321 (
  PRV_COD,
  OPN_DEP,
  TAL_DEP,
  DUE_NUM,
  TRAN_FROM,
  CURR_PERI,
  BEG_DATE,
  END_DATE,
  RCV_PRN,
  PAD_UP_PRN,
  PRN_BAL,
  PRN_OTD_ITR,
  NOR_DLY_ITR_BAL,
  PRN_PROC_FLG,
  NOR_ITR_RATE,
  DLY_ITR_RATE,
  RCV_DATE,
  HLD_FLG,
  RMK1,
  RMK2,
  RMK3,
  start_dt,
  end_dt)
SELECT
  N.PRV_COD,
  N.OPN_DEP,
  N.TAL_DEP,
  N.DUE_NUM,
  N.TRAN_FROM,
  N.CURR_PERI,
  N.BEG_DATE,
  N.END_DATE,
  N.RCV_PRN,
  N.PAD_UP_PRN,
  N.PRN_BAL,
  N.PRN_OTD_ITR,
  N.NOR_DLY_ITR_BAL,
  N.PRN_PROC_FLG,
  N.NOR_ITR_RATE,
  N.DLY_ITR_RATE,
  N.RCV_DATE,
  N.HLD_FLG,
  N.RMK1,
  N.RMK2,
  N.RMK3,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(PRV_COD, '' ) AS PRV_COD ,
  COALESCE(OPN_DEP, '' ) AS OPN_DEP ,
  COALESCE(TAL_DEP, '' ) AS TAL_DEP ,
  COALESCE(DUE_NUM, '' ) AS DUE_NUM ,
  COALESCE(TRAN_FROM, '' ) AS TRAN_FROM ,
  COALESCE(CURR_PERI, 0 ) AS CURR_PERI ,
  COALESCE(BEG_DATE, '' ) AS BEG_DATE ,
  COALESCE(END_DATE, '' ) AS END_DATE ,
  COALESCE(RCV_PRN, 0 ) AS RCV_PRN ,
  COALESCE(PAD_UP_PRN, 0 ) AS PAD_UP_PRN ,
  COALESCE(PRN_BAL, 0 ) AS PRN_BAL ,
  COALESCE(PRN_OTD_ITR, 0 ) AS PRN_OTD_ITR ,
  COALESCE(NOR_DLY_ITR_BAL, 0 ) AS NOR_DLY_ITR_BAL ,
  COALESCE(PRN_PROC_FLG, '' ) AS PRN_PROC_FLG ,
  COALESCE(NOR_ITR_RATE, 0 ) AS NOR_ITR_RATE ,
  COALESCE(DLY_ITR_RATE, 0 ) AS DLY_ITR_RATE ,
  COALESCE(RCV_DATE, '' ) AS RCV_DATE ,
  COALESCE(HLD_FLG, '' ) AS HLD_FLG ,
  COALESCE(RMK1, '' ) AS RMK1 ,
  COALESCE(RMK2, '' ) AS RMK2 ,
  COALESCE(RMK3, '' ) AS RMK3 
 FROM  dw_tdata.PCS_005_TB_SUP_PRIN_PLAN_A_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  PRV_COD ,
  OPN_DEP ,
  TAL_DEP ,
  DUE_NUM ,
  TRAN_FROM ,
  CURR_PERI ,
  BEG_DATE ,
  END_DATE ,
  RCV_PRN ,
  PAD_UP_PRN ,
  PRN_BAL ,
  PRN_OTD_ITR ,
  NOR_DLY_ITR_BAL ,
  PRN_PROC_FLG ,
  NOR_ITR_RATE ,
  DLY_ITR_RATE ,
  RCV_DATE ,
  HLD_FLG ,
  RMK1 ,
  RMK2 ,
  RMK3 
 FROM dw_sdata.PCS_005_TB_SUP_PRIN_PLAN_A 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.DUE_NUM = T.DUE_NUM AND N.CURR_PERI = T.CURR_PERI
WHERE
(T.DUE_NUM IS NULL AND T.CURR_PERI IS NULL)
 OR N.PRV_COD<>T.PRV_COD
 OR N.OPN_DEP<>T.OPN_DEP
 OR N.TAL_DEP<>T.TAL_DEP
 OR N.TRAN_FROM<>T.TRAN_FROM
 OR N.BEG_DATE<>T.BEG_DATE
 OR N.END_DATE<>T.END_DATE
 OR N.RCV_PRN<>T.RCV_PRN
 OR N.PAD_UP_PRN<>T.PAD_UP_PRN
 OR N.PRN_BAL<>T.PRN_BAL
 OR N.PRN_OTD_ITR<>T.PRN_OTD_ITR
 OR N.NOR_DLY_ITR_BAL<>T.NOR_DLY_ITR_BAL
 OR N.PRN_PROC_FLG<>T.PRN_PROC_FLG
 OR N.NOR_ITR_RATE<>T.NOR_ITR_RATE
 OR N.DLY_ITR_RATE<>T.DLY_ITR_RATE
 OR N.RCV_DATE<>T.RCV_DATE
 OR N.HLD_FLG<>T.HLD_FLG
 OR N.RMK1<>T.RMK1
 OR N.RMK2<>T.RMK2
 OR N.RMK3<>T.RMK3
;

--Step3:
UPDATE dw_sdata.PCS_005_TB_SUP_PRIN_PLAN_A P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_321
WHERE P.End_Dt=DATE('2100-12-31')
AND P.DUE_NUM=T_321.DUE_NUM
AND P.CURR_PERI=T_321.CURR_PERI
;

--Step4:
INSERT  INTO dw_sdata.PCS_005_TB_SUP_PRIN_PLAN_A SELECT * FROM T_321;

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
