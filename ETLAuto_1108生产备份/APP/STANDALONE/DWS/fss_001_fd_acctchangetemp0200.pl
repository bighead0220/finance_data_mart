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

DELETE FROM dw_sdata.FSS_001_FD_ACCTCHANGETEMP WHERE etl_dt=DATE('${TX_DATE_YYYYMMDD}');                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
INSERT  INTO dw_sdata.FSS_001_FD_ACCTCHANGETEMP(                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
  CENSERIALNO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CLISERIALNO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CLRDATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TXDATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BGNINTDATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TIMESTAMP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SYSCODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TRADEORGANCODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OPERCODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TXNAMECODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TXCHARA ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TXSTATECODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  DAC ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TXINSTROLENUM ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TXINSTROLE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TXUNITCODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TXACCOUNT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TXINSTROLE1 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TXUNITCODE1 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TXACCOUNT1 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BILLGROUPQTY ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BILLTYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BILLNO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TXGROUPQTY ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TXMONEYTYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TXAMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CURRTYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  INTTXACCOUNT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  INTMATCHACCOUNT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TXMONEYTYPE1 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TXAMT1 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CURRTYPE1 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  INTTXACCOUNT1 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  INTMATCHACCOUNT1 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CASHUSEMODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FEEGROUPQTY ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FEETXACCOUNT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FEETXAMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FEETYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FEECURRTYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  HANDFEEMODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PREFFEEUNIT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PREFFEE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FEEPROVFLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FEECOLLUNIT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SYSSERIALNO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BGNTXDATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OUTSYSCODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  INSYSCODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  MERCHANTMARK ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TERMIMARK ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SHOPTYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  EXPANDINFO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  INSIDELEDGER ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ORITRADESERIALINFO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SENDFLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BATCH_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TXACCOUNTSYSCODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TXACCOUNTSYSCODE1 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BUFAFLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  NEWCLISERIALNO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  etl_dt                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
SELECT                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
  COALESCE(CENSERIALNO, '' ) AS CENSERIALNO ,
  COALESCE(CLISERIALNO, '' ) AS CLISERIALNO ,
  COALESCE(CLRDATE, '' ) AS CLRDATE ,
  COALESCE(TXDATE, '' ) AS TXDATE ,
  COALESCE(BGNINTDATE, '' ) AS BGNINTDATE ,
  COALESCE(TIMESTAMP, '' ) AS TIMESTAMP ,
  COALESCE(SYSCODE, '' ) AS SYSCODE ,
  COALESCE(TRADEORGANCODE, '' ) AS TRADEORGANCODE ,
  COALESCE(OPERCODE, '' ) AS OPERCODE ,
  COALESCE(TXNAMECODE, '' ) AS TXNAMECODE ,
  COALESCE(TXCHARA, '' ) AS TXCHARA ,
  COALESCE(TXSTATECODE, '' ) AS TXSTATECODE ,
  COALESCE(DAC, '' ) AS DAC ,
  COALESCE(TXINSTROLENUM, '' ) AS TXINSTROLENUM ,
  COALESCE(TXINSTROLE, '' ) AS TXINSTROLE ,
  COALESCE(TXUNITCODE, '' ) AS TXUNITCODE ,
  COALESCE(TXACCOUNT, '' ) AS TXACCOUNT ,
  COALESCE(TXINSTROLE1, '' ) AS TXINSTROLE1 ,
  COALESCE(TXUNITCODE1, '' ) AS TXUNITCODE1 ,
  COALESCE(TXACCOUNT1, '' ) AS TXACCOUNT1 ,
  COALESCE(BILLGROUPQTY, '' ) AS BILLGROUPQTY ,
  COALESCE(BILLTYPE, '' ) AS BILLTYPE ,
  COALESCE(BILLNO, '' ) AS BILLNO ,
  COALESCE(TXGROUPQTY, '' ) AS TXGROUPQTY ,
  COALESCE(TXMONEYTYPE, '' ) AS TXMONEYTYPE ,
  COALESCE(TXAMT, 0 ) AS TXAMT ,
  COALESCE(CURRTYPE, '' ) AS CURRTYPE ,
  COALESCE(INTTXACCOUNT, '' ) AS INTTXACCOUNT ,
  COALESCE(INTMATCHACCOUNT, '' ) AS INTMATCHACCOUNT ,
  COALESCE(TXMONEYTYPE1, '' ) AS TXMONEYTYPE1 ,
  COALESCE(TXAMT1, 0 ) AS TXAMT1 ,
  COALESCE(CURRTYPE1, '' ) AS CURRTYPE1 ,
  COALESCE(INTTXACCOUNT1, '' ) AS INTTXACCOUNT1 ,
  COALESCE(INTMATCHACCOUNT1, '' ) AS INTMATCHACCOUNT1 ,
  COALESCE(CASHUSEMODE, '' ) AS CASHUSEMODE ,
  COALESCE(FEEGROUPQTY, '' ) AS FEEGROUPQTY ,
  COALESCE(FEETXACCOUNT, '' ) AS FEETXACCOUNT ,
  COALESCE(FEETXAMT, 0 ) AS FEETXAMT ,
  COALESCE(FEETYPE, '' ) AS FEETYPE ,
  COALESCE(FEECURRTYPE, '' ) AS FEECURRTYPE ,
  COALESCE(HANDFEEMODE, '' ) AS HANDFEEMODE ,
  COALESCE(PREFFEEUNIT, '' ) AS PREFFEEUNIT ,
  COALESCE(PREFFEE, 0 ) AS PREFFEE ,
  COALESCE(FEEPROVFLAG, '' ) AS FEEPROVFLAG ,
  COALESCE(FEECOLLUNIT, '' ) AS FEECOLLUNIT ,
  COALESCE(SYSSERIALNO, '' ) AS SYSSERIALNO ,
  COALESCE(BGNTXDATE, '' ) AS BGNTXDATE ,
  COALESCE(OUTSYSCODE, '' ) AS OUTSYSCODE ,
  COALESCE(INSYSCODE, '' ) AS INSYSCODE ,
  COALESCE(MERCHANTMARK, '' ) AS MERCHANTMARK ,
  COALESCE(TERMIMARK, '' ) AS TERMIMARK ,
  COALESCE(SHOPTYPE, '' ) AS SHOPTYPE ,
  COALESCE(EXPANDINFO, '' ) AS EXPANDINFO ,
  COALESCE(INSIDELEDGER, '' ) AS INSIDELEDGER ,
  COALESCE(ORITRADESERIALINFO, '' ) AS ORITRADESERIALINFO ,
  COALESCE(SENDFLAG, '' ) AS SENDFLAG ,
  COALESCE(BATCH_NO, '' ) AS BATCH_NO ,
  COALESCE(TXACCOUNTSYSCODE, '' ) AS TXACCOUNTSYSCODE ,
  COALESCE(TXACCOUNTSYSCODE1, '' ) AS TXACCOUNTSYSCODE1 ,
  COALESCE(BUFAFLAG, '' ) AS BUFAFLAG ,
  COALESCE(NEWCLISERIALNO, '' ) AS NEWCLISERIALNO ,
  DATE('${TX_DATE_YYYYMMDD}')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
FROM  dw_tdata.FSS_001_FD_ACCTCHANGETEMP_${TX_DATE_YYYYMMDD}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
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
