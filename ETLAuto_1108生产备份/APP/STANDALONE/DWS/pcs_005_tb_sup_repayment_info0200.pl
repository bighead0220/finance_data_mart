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

DELETE FROM dw_sdata.PCS_005_TB_SUP_REPAYMENT_INFO WHERE etl_dt=DATE('${TX_DATE_YYYYMMDD}');                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
INSERT  INTO dw_sdata.PCS_005_TB_SUP_REPAYMENT_INFO(                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
  RCV_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PRV_COD ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OPN_DEP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TAL_DEP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BUS_COD ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  DUE_NUM ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BRW_NAME ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PERIOD ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PRM_CLS ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AST_CLS ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PRM_PAY_TYP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AST_PAY_TYP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TRAN_FROM ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BEG_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  END_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PAY_TYP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PAY_CHL ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PAY_PRM_ACCT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PAY_PRIM_ACCT_TYP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PAY_PRIM_ACCT_FLG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CURR_COD ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PAD_UP_PRN ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PAD_UP_PENT_PRN ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PAD_UP_NOR_ITR_IN ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PAD_UP_DFT_ITR_IN ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PAD_UP_PNS_ITR_IN ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PAD_UP_NOR_ITR_OUT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PAD_UP_DFT_ITR_OUT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PAD_UP_PNS_ITR_OUT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PAD_UP_PENT_ICM ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PAD_UP_OFT_PRN ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PAD_UP_OFT_ITR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OFT_ACR_ITR_BAL ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RMK1 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RMK2 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RMK3 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OPR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  STAN ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  etl_dt                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
SELECT                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
  COALESCE(RCV_DATE, '' ) AS RCV_DATE ,
  COALESCE(PRV_COD, '' ) AS PRV_COD ,
  COALESCE(OPN_DEP, '' ) AS OPN_DEP ,
  COALESCE(TAL_DEP, '' ) AS TAL_DEP ,
  COALESCE(BUS_COD, '' ) AS BUS_COD ,
  COALESCE(DUE_NUM, '' ) AS DUE_NUM ,
  COALESCE(BRW_NAME, '' ) AS BRW_NAME ,
  COALESCE(PERIOD, 0 ) AS PERIOD ,
  COALESCE(PRM_CLS, '' ) AS PRM_CLS ,
  COALESCE(AST_CLS, '' ) AS AST_CLS ,
  COALESCE(PRM_PAY_TYP, '' ) AS PRM_PAY_TYP ,
  COALESCE(AST_PAY_TYP, '' ) AS AST_PAY_TYP ,
  COALESCE(TRAN_FROM, '' ) AS TRAN_FROM ,
  COALESCE(BEG_DATE, '' ) AS BEG_DATE ,
  COALESCE(END_DATE, '' ) AS END_DATE ,
  COALESCE(PAY_TYP, '' ) AS PAY_TYP ,
  COALESCE(PAY_CHL, '' ) AS PAY_CHL ,
  COALESCE(PAY_PRM_ACCT, '' ) AS PAY_PRM_ACCT ,
  COALESCE(PAY_PRIM_ACCT_TYP, '' ) AS PAY_PRIM_ACCT_TYP ,
  COALESCE(PAY_PRIM_ACCT_FLG, '' ) AS PAY_PRIM_ACCT_FLG ,
  COALESCE(CURR_COD, '' ) AS CURR_COD ,
  COALESCE(PAD_UP_PRN, 0 ) AS PAD_UP_PRN ,
  COALESCE(PAD_UP_PENT_PRN, 0 ) AS PAD_UP_PENT_PRN ,
  COALESCE(PAD_UP_NOR_ITR_IN, 0 ) AS PAD_UP_NOR_ITR_IN ,
  COALESCE(PAD_UP_DFT_ITR_IN, 0 ) AS PAD_UP_DFT_ITR_IN ,
  COALESCE(PAD_UP_PNS_ITR_IN, 0 ) AS PAD_UP_PNS_ITR_IN ,
  COALESCE(PAD_UP_NOR_ITR_OUT, 0 ) AS PAD_UP_NOR_ITR_OUT ,
  COALESCE(PAD_UP_DFT_ITR_OUT, 0 ) AS PAD_UP_DFT_ITR_OUT ,
  COALESCE(PAD_UP_PNS_ITR_OUT, 0 ) AS PAD_UP_PNS_ITR_OUT ,
  COALESCE(PAD_UP_PENT_ICM, 0 ) AS PAD_UP_PENT_ICM ,
  COALESCE(PAD_UP_OFT_PRN, 0 ) AS PAD_UP_OFT_PRN ,
  COALESCE(PAD_UP_OFT_ITR, 0 ) AS PAD_UP_OFT_ITR ,
  COALESCE(OFT_ACR_ITR_BAL, 0 ) AS OFT_ACR_ITR_BAL ,
  COALESCE(RMK1, 0 ) AS RMK1 ,
  COALESCE(RMK2, 0 ) AS RMK2 ,
  COALESCE(RMK3, '' ) AS RMK3 ,
  COALESCE(OPR, '' ) AS OPR ,
  COALESCE(STAN, 0 ) AS STAN ,
  DATE('${TX_DATE_YYYYMMDD}')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
FROM  dw_tdata.PCS_005_TB_SUP_REPAYMENT_INFO_${TX_DATE_YYYYMMDD}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
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
