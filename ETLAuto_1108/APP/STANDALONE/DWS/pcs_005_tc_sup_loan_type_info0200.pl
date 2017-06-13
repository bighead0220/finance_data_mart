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
DELETE FROM dw_sdata.PCS_005_TC_SUP_LOAN_TYPE_INFO WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.PCS_005_TC_SUP_LOAN_TYPE_INFO SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_325 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.PCS_005_TC_SUP_LOAN_TYPE_INFO WHERE 1=0;

--Step2:
INSERT  INTO T_325 (
  BUS_COD,
  CURR_COD,
  BUS_COD_NAME,
  ACC_TYPE,
  ACC_NAME,
  ACC_PROPT,
  BALN_DIRT,
  OVERDRAFT_FLG,
  FIN_STAG_SUB,
  start_dt,
  end_dt)
SELECT
  N.BUS_COD,
  N.CURR_COD,
  N.BUS_COD_NAME,
  N.ACC_TYPE,
  N.ACC_NAME,
  N.ACC_PROPT,
  N.BALN_DIRT,
  N.OVERDRAFT_FLG,
  N.FIN_STAG_SUB,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(BUS_COD, '' ) AS BUS_COD ,
  COALESCE(CURR_COD, '' ) AS CURR_COD ,
  COALESCE(BUS_COD_NAME, '' ) AS BUS_COD_NAME ,
  COALESCE(ACC_TYPE, '' ) AS ACC_TYPE ,
  COALESCE(ACC_NAME, '' ) AS ACC_NAME ,
  COALESCE(ACC_PROPT, '' ) AS ACC_PROPT ,
  COALESCE(BALN_DIRT, '' ) AS BALN_DIRT ,
  COALESCE(OVERDRAFT_FLG, '' ) AS OVERDRAFT_FLG ,
  COALESCE(FIN_STAG_SUB, '' ) AS FIN_STAG_SUB 
 FROM  dw_tdata.PCS_005_TC_SUP_LOAN_TYPE_INFO_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  BUS_COD ,
  CURR_COD ,
  BUS_COD_NAME ,
  ACC_TYPE ,
  ACC_NAME ,
  ACC_PROPT ,
  BALN_DIRT ,
  OVERDRAFT_FLG ,
  FIN_STAG_SUB 
 FROM dw_sdata.PCS_005_TC_SUP_LOAN_TYPE_INFO 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.BUS_COD = T.BUS_COD AND N.ACC_TYPE = T.ACC_TYPE
WHERE
(T.BUS_COD IS NULL AND T.ACC_TYPE IS NULL)
 OR N.CURR_COD<>T.CURR_COD
 OR N.BUS_COD_NAME<>T.BUS_COD_NAME
 OR N.ACC_NAME<>T.ACC_NAME
 OR N.ACC_PROPT<>T.ACC_PROPT
 OR N.BALN_DIRT<>T.BALN_DIRT
 OR N.OVERDRAFT_FLG<>T.OVERDRAFT_FLG
 OR N.FIN_STAG_SUB<>T.FIN_STAG_SUB
;

--Step3:
UPDATE dw_sdata.PCS_005_TC_SUP_LOAN_TYPE_INFO P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_325
WHERE P.End_Dt=DATE('2100-12-31')
AND P.BUS_COD=T_325.BUS_COD
AND P.ACC_TYPE=T_325.ACC_TYPE
;

--Step4:
INSERT  INTO dw_sdata.PCS_005_TC_SUP_LOAN_TYPE_INFO SELECT * FROM T_325;

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
