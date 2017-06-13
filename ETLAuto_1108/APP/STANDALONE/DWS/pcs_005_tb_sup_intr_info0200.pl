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

DELETE FROM dw_sdata.PCS_005_TB_SUP_INTR_INFO WHERE etl_dt=DATE('${TX_DATE_YYYYMMDD}');                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
INSERT  INTO dw_sdata.PCS_005_TB_SUP_INTR_INFO(                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
  PRV_COD ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RCV_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OPN_DEP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TAL_DEP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  STAN ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  DUE_NUM ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BRW_NAME ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CURR_PERI ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PRM_CLS ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AST_CLS ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TRAN_FROM ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BUS_COD ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PRM_PAY_TYP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AST_PAY_TYP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CURR_COD ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BEG_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  END_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PAY_PRM_ACCT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  NOR_DFT_FLG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  NOR_ITR_RATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  DEL_ITR_RATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  NOR_ITR_IN ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  DFT_ITR_IN ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PNS_ITR_IN ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  NOR_ITR_OUT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  DFT_ITR_OUT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PNS_ITR_OUT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OFT_ACR_ITR_BAL ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RMK1 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RMK2 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RMK3 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RMK4 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RMK_1 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RMK1_BAL ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  etl_dt                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
SELECT                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
  COALESCE(PRV_COD, '' ) AS PRV_COD ,
  COALESCE(RCV_DATE, '' ) AS RCV_DATE ,
  COALESCE(OPN_DEP, '' ) AS OPN_DEP ,
  COALESCE(TAL_DEP, '' ) AS TAL_DEP ,
  COALESCE(STAN, 0 ) AS STAN ,
  COALESCE(DUE_NUM, '' ) AS DUE_NUM ,
  COALESCE(BRW_NAME, '' ) AS BRW_NAME ,
  COALESCE(CURR_PERI, 0 ) AS CURR_PERI ,
  COALESCE(PRM_CLS, '' ) AS PRM_CLS ,
  COALESCE(AST_CLS, '' ) AS AST_CLS ,
  COALESCE(TRAN_FROM, '' ) AS TRAN_FROM ,
  COALESCE(BUS_COD, '' ) AS BUS_COD ,
  COALESCE(PRM_PAY_TYP, '' ) AS PRM_PAY_TYP ,
  COALESCE(AST_PAY_TYP, '' ) AS AST_PAY_TYP ,
  COALESCE(CURR_COD, '' ) AS CURR_COD ,
  COALESCE(BEG_DATE, '' ) AS BEG_DATE ,
  COALESCE(END_DATE, '' ) AS END_DATE ,
  COALESCE(PAY_PRM_ACCT, '' ) AS PAY_PRM_ACCT ,
  COALESCE(NOR_DFT_FLG, '' ) AS NOR_DFT_FLG ,
  COALESCE(NOR_ITR_RATE, 0 ) AS NOR_ITR_RATE ,
  COALESCE(DEL_ITR_RATE, 0 ) AS DEL_ITR_RATE ,
  COALESCE(NOR_ITR_IN, 0 ) AS NOR_ITR_IN ,
  COALESCE(DFT_ITR_IN, 0 ) AS DFT_ITR_IN ,
  COALESCE(PNS_ITR_IN, 0 ) AS PNS_ITR_IN ,
  COALESCE(NOR_ITR_OUT, 0 ) AS NOR_ITR_OUT ,
  COALESCE(DFT_ITR_OUT, 0 ) AS DFT_ITR_OUT ,
  COALESCE(PNS_ITR_OUT, 0 ) AS PNS_ITR_OUT ,
  COALESCE(OFT_ACR_ITR_BAL, 0 ) AS OFT_ACR_ITR_BAL ,
  COALESCE(RMK1, 0 ) AS RMK1 ,
  COALESCE(RMK2, 0 ) AS RMK2 ,
  COALESCE(RMK3, '' ) AS RMK3 ,
  COALESCE(RMK4, '' ) AS RMK4 ,
  COALESCE(RMK_1, '' ) AS RMK_1 ,
  COALESCE(RMK1_BAL, 0 ) AS RMK1_BAL ,
  DATE('${TX_DATE_YYYYMMDD}')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
FROM  dw_tdata.PCS_005_TB_SUP_INTR_INFO_${TX_DATE_YYYYMMDD}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
;                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
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
   print "Usage: [perl ������ Control_File] (Control_File format: dir.jobnameYYYYMMDD or sysname_jobname_YYYYMMDD.dir) 
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
   print "Usage: [perl ������ Control_File] (Control_File format: dir.jobnameYYYYMMDD or sysname_jobname_YYYYMMDD.dir) 
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