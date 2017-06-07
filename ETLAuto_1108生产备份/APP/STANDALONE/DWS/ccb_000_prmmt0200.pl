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
DELETE FROM dw_sdata.CCB_000_PRMMT WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.CCB_000_PRMMT SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_101 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.CCB_000_PRMMT WHERE 1=0;

--Step2:
INSERT  INTO T_101 (
  MER_TYPE,
  AMCCG_TYPE,
  CATTYPE_MC,
  CATTYPE_VI,
  CONTR_TYPE,
  CUSTPAYSER,
  DAILY_SUBS,
  FLOOR_LIM,
  FLOOR_LIM2,
  HI_RSK_CSH,
  INDUAL_AMT,
  INDUL_MSG,
  INDUL_RESP,
  M_P_IND,
  MCC_ANALY,
  MCC_CNTRL,
  MCC_DESC,
  MCCAT_TYPE,
  QUASI_CASH,
  RECUR_TX,
  REP_BULL,
  TE_IND,
  TRANS_TYPE,
  TXN_PERMIT,
  start_dt,
  end_dt)
SELECT
  N.MER_TYPE,
  N.AMCCG_TYPE,
  N.CATTYPE_MC,
  N.CATTYPE_VI,
  N.CONTR_TYPE,
  N.CUSTPAYSER,
  N.DAILY_SUBS,
  N.FLOOR_LIM,
  N.FLOOR_LIM2,
  N.HI_RSK_CSH,
  N.INDUAL_AMT,
  N.INDUL_MSG,
  N.INDUL_RESP,
  N.M_P_IND,
  N.MCC_ANALY,
  N.MCC_CNTRL,
  N.MCC_DESC,
  N.MCCAT_TYPE,
  N.QUASI_CASH,
  N.RECUR_TX,
  N.REP_BULL,
  N.TE_IND,
  N.TRANS_TYPE,
  N.TXN_PERMIT,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(MER_TYPE, 0 ) AS MER_TYPE ,
  COALESCE(AMCCG_TYPE, '' ) AS AMCCG_TYPE ,
  COALESCE(CATTYPE_MC, '' ) AS CATTYPE_MC ,
  COALESCE(CATTYPE_VI, '' ) AS CATTYPE_VI ,
  COALESCE(CONTR_TYPE, '' ) AS CONTR_TYPE ,
  COALESCE(CUSTPAYSER, '' ) AS CUSTPAYSER ,
  COALESCE(DAILY_SUBS, 0 ) AS DAILY_SUBS ,
  COALESCE(FLOOR_LIM, 0 ) AS FLOOR_LIM ,
  COALESCE(FLOOR_LIM2, 0 ) AS FLOOR_LIM2 ,
  COALESCE(HI_RSK_CSH, '' ) AS HI_RSK_CSH ,
  COALESCE(INDUAL_AMT, 0 ) AS INDUAL_AMT ,
  COALESCE(INDUL_MSG, 0 ) AS INDUL_MSG ,
  COALESCE(INDUL_RESP, 0 ) AS INDUL_RESP ,
  COALESCE(M_P_IND, '' ) AS M_P_IND ,
  COALESCE(MCC_ANALY, '' ) AS MCC_ANALY ,
  COALESCE(MCC_CNTRL, '' ) AS MCC_CNTRL ,
  COALESCE(MCC_DESC, '' ) AS MCC_DESC ,
  COALESCE(MCCAT_TYPE, '' ) AS MCCAT_TYPE ,
  COALESCE(QUASI_CASH, '' ) AS QUASI_CASH ,
  COALESCE(RECUR_TX, '' ) AS RECUR_TX ,
  COALESCE(REP_BULL, '' ) AS REP_BULL ,
  COALESCE(TE_IND, '' ) AS TE_IND ,
  COALESCE(TRANS_TYPE, 0 ) AS TRANS_TYPE ,
  COALESCE(TXN_PERMIT, 0 ) AS TXN_PERMIT 
 FROM  dw_tdata.CCB_000_PRMMT_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  MER_TYPE ,
  AMCCG_TYPE ,
  CATTYPE_MC ,
  CATTYPE_VI ,
  CONTR_TYPE ,
  CUSTPAYSER ,
  DAILY_SUBS ,
  FLOOR_LIM ,
  FLOOR_LIM2 ,
  HI_RSK_CSH ,
  INDUAL_AMT ,
  INDUL_MSG ,
  INDUL_RESP ,
  M_P_IND ,
  MCC_ANALY ,
  MCC_CNTRL ,
  MCC_DESC ,
  MCCAT_TYPE ,
  QUASI_CASH ,
  RECUR_TX ,
  REP_BULL ,
  TE_IND ,
  TRANS_TYPE ,
  TXN_PERMIT 
 FROM dw_sdata.CCB_000_PRMMT 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.MER_TYPE = T.MER_TYPE
WHERE
(T.MER_TYPE IS NULL)
 OR N.AMCCG_TYPE<>T.AMCCG_TYPE
 OR N.CATTYPE_MC<>T.CATTYPE_MC
 OR N.CATTYPE_VI<>T.CATTYPE_VI
 OR N.CONTR_TYPE<>T.CONTR_TYPE
 OR N.CUSTPAYSER<>T.CUSTPAYSER
 OR N.DAILY_SUBS<>T.DAILY_SUBS
 OR N.FLOOR_LIM<>T.FLOOR_LIM
 OR N.FLOOR_LIM2<>T.FLOOR_LIM2
 OR N.HI_RSK_CSH<>T.HI_RSK_CSH
 OR N.INDUAL_AMT<>T.INDUAL_AMT
 OR N.INDUL_MSG<>T.INDUL_MSG
 OR N.INDUL_RESP<>T.INDUL_RESP
 OR N.M_P_IND<>T.M_P_IND
 OR N.MCC_ANALY<>T.MCC_ANALY
 OR N.MCC_CNTRL<>T.MCC_CNTRL
 OR N.MCC_DESC<>T.MCC_DESC
 OR N.MCCAT_TYPE<>T.MCCAT_TYPE
 OR N.QUASI_CASH<>T.QUASI_CASH
 OR N.RECUR_TX<>T.RECUR_TX
 OR N.REP_BULL<>T.REP_BULL
 OR N.TE_IND<>T.TE_IND
 OR N.TRANS_TYPE<>T.TRANS_TYPE
 OR N.TXN_PERMIT<>T.TXN_PERMIT
;

--Step3:
UPDATE dw_sdata.CCB_000_PRMMT P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_101
WHERE P.End_Dt=DATE('2100-12-31')
AND P.MER_TYPE=T_101.MER_TYPE
;

--Step4:
INSERT  INTO dw_sdata.CCB_000_PRMMT SELECT * FROM T_101;

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
