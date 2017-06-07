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
DELETE FROM dw_sdata.PCS_004_TB_SUP_WRITE_OFF_INFO WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.PCS_004_TB_SUP_WRITE_OFF_INFO SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_273 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.PCS_004_TB_SUP_WRITE_OFF_INFO WHERE 1=0;

--Step2:
INSERT  INTO T_273 (
  RCV_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PRV_COD ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OPN_DEP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TAL_DEP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  DUE_NUM ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PRM_CLS ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AST_CLS ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TRAN_FROM ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RECR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OFT_PRIN ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OFT_NOR_ITR_IN ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OFT_DFT_ITR_IN ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OFT_PNS_ITR_IN ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OFT_NOR_ITR_OUT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OFT_DFT_ITR_OUT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OFT_PNS_ITR_OUT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RMK1 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RMK2 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RMK3 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RMK1_BAL ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RMK2_BAL ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RMK3_BAL ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  STAN ,            
  start_dt,
  end_dt)
SELECT
   N.RCV_DATE ,
  N.PRV_COD ,
  N.OPN_DEP ,
  N.TAL_DEP ,
  N.DUE_NUM ,
  N.PRM_CLS ,
  N.AST_CLS ,
  N.TRAN_FROM ,
  N.AMT ,
  N.RECR ,
  N.OFT_PRIN ,
  N.OFT_NOR_ITR_IN ,
  N.OFT_DFT_ITR_IN ,
  N.OFT_PNS_ITR_IN ,
  N.OFT_NOR_ITR_OUT ,
  N.OFT_DFT_ITR_OUT ,
  N.OFT_PNS_ITR_OUT ,
  N.RMK1 ,
  N.RMK2 ,
  N.RMK3 ,
  N.RMK1_BAL ,
  N.RMK2_BAL ,
  N.RMK3_BAL ,
  N.STAN ,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
COALESCE(RCV_DATE, '' ) AS RCV_DATE ,
  COALESCE(PRV_COD, '' ) AS PRV_COD ,
  COALESCE(OPN_DEP, '' ) AS OPN_DEP ,
  COALESCE(TAL_DEP, '' ) AS TAL_DEP ,
  COALESCE(DUE_NUM, '' ) AS DUE_NUM ,
  COALESCE(PRM_CLS, '' ) AS PRM_CLS ,
  COALESCE(AST_CLS, '' ) AS AST_CLS ,
  COALESCE(TRAN_FROM, '' ) AS TRAN_FROM ,
  COALESCE(AMT, 0 ) AS AMT ,
  COALESCE(RECR, '' ) AS RECR ,
  COALESCE(OFT_PRIN, 0 ) AS OFT_PRIN ,
  COALESCE(OFT_NOR_ITR_IN, 0 ) AS OFT_NOR_ITR_IN ,
  COALESCE(OFT_DFT_ITR_IN, 0 ) AS OFT_DFT_ITR_IN ,
  COALESCE(OFT_PNS_ITR_IN, 0 ) AS OFT_PNS_ITR_IN ,
  COALESCE(OFT_NOR_ITR_OUT, 0 ) AS OFT_NOR_ITR_OUT ,
  COALESCE(OFT_DFT_ITR_OUT, 0 ) AS OFT_DFT_ITR_OUT ,
  COALESCE(OFT_PNS_ITR_OUT, 0 ) AS OFT_PNS_ITR_OUT ,
  COALESCE(RMK1, '' ) AS RMK1 ,
  COALESCE(RMK2, '' ) AS RMK2 ,
  COALESCE(RMK3, '' ) AS RMK3 ,
  COALESCE(RMK1_BAL, 0 ) AS RMK1_BAL ,
  COALESCE(RMK2_BAL, 0 ) AS RMK2_BAL ,
  COALESCE(RMK3_BAL, 0 ) AS RMK3_BAL ,
  COALESCE(STAN, 0 ) AS STAN 
 FROM  dw_tdata.PCS_004_TB_SUP_WRITE_OFF_INFO_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
     RCV_DATE ,
  PRV_COD ,
  OPN_DEP ,
  TAL_DEP ,
  DUE_NUM ,
  PRM_CLS ,
  AST_CLS ,
  TRAN_FROM ,
  AMT ,
  RECR ,
  OFT_PRIN ,
  OFT_NOR_ITR_IN ,
  OFT_DFT_ITR_IN ,
  OFT_PNS_ITR_IN ,
  OFT_NOR_ITR_OUT ,
  OFT_DFT_ITR_OUT ,
  OFT_PNS_ITR_OUT ,
  RMK1 ,
  RMK2 ,
  RMK3 ,
  RMK1_BAL ,
  RMK2_BAL ,
  RMK3_BAL ,
  STAN 
 FROM dw_sdata.PCS_004_TB_SUP_WRITE_OFF_INFO 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.DUE_NUM  = T.DUE_NUM  
WHERE
(T.DUE_NUM IS NULL )
  OR N.RCV_DATE<>T.RCV_DATE
OR N.PRV_COD<>T.PRV_COD
OR N.OPN_DEP<>T.OPN_DEP
OR N.TAL_DEP<>T.TAL_DEP
OR N.PRM_CLS<>T.PRM_CLS
OR N.AST_CLS<>T.AST_CLS
OR N.TRAN_FROM<>T.TRAN_FROM
OR N.AMT<>T.AMT
OR N.RECR<>T.RECR
OR N.OFT_PRIN<>T.OFT_PRIN
OR N.OFT_NOR_ITR_IN<>T.OFT_NOR_ITR_IN
OR N.OFT_DFT_ITR_IN<>T.OFT_DFT_ITR_IN
OR N.OFT_PNS_ITR_IN<>T.OFT_PNS_ITR_IN
OR N.OFT_NOR_ITR_OUT<>T.OFT_NOR_ITR_OUT
OR N.OFT_DFT_ITR_OUT<>T.OFT_DFT_ITR_OUT
OR N.OFT_PNS_ITR_OUT<>T.OFT_PNS_ITR_OUT
OR N.RMK1<>T.RMK1
OR N.RMK2<>T.RMK2
OR N.RMK3<>T.RMK3
OR N.RMK1_BAL<>T.RMK1_BAL
OR N.RMK2_BAL<>T.RMK2_BAL
OR N.RMK3_BAL<>T.RMK3_BAL
OR N.STAN<>T.STAN
;

--Step3:
UPDATE dw_sdata.PCS_004_TB_SUP_WRITE_OFF_INFO P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_273
WHERE P.End_Dt=DATE('2100-12-31')
AND P.DUE_NUM=T_273.DUE_NUM
;

--Step4:
INSERT  INTO dw_sdata.PCS_004_TB_SUP_WRITE_OFF_INFO SELECT * FROM T_273;

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
