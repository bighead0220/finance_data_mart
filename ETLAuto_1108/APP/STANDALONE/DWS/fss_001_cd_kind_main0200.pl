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
DELETE FROM dw_sdata.FSS_001_CD_KIND_MAIN WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.FSS_001_CD_KIND_MAIN SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_181 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.FSS_001_CD_KIND_MAIN WHERE 1=0;

--Step2:
INSERT  INTO T_181 (
  KINDCODE,
  KINDABNAME,
  KINDNAME,
  KINDTYPE,
  YEAR,
  LIMITEDPERIOD,
  PUBLISHPERIOD,
  MINLIMITED,
  YNPARTY,
  PUBLISHBDATE,
  PUBLISHEDATE,
  CASHBDATE,
  CASHEDATE,
  ZAISHOUFLAG,
  RESELLBDATE,
  RESELLEDATE,
  INPERIOD,
  UNITMAX,
  PERSONMAX,
  UNITSMAX,
  PERSONSMAX,
  JXDFLAG,
  RATEAKIND,
  RATEEKIND,
  MAXLIMITEDJE,
  YYDATE,
  YYENDDATE,
  YYBEGINDATE,
  GROUPYGRATE,
  GROUPYGNEAP,
  GROUPYGTIPTOP,
  GROUPRGNEAP,
  GROUPRGTIPTOP,
  FEERATE,
  start_dt,
  end_dt)
SELECT
  N.KINDCODE,
  N.KINDABNAME,
  N.KINDNAME,
  N.KINDTYPE,
  N.YEAR,
  N.LIMITEDPERIOD,
  N.PUBLISHPERIOD,
  N.MINLIMITED,
  N.YNPARTY,
  N.PUBLISHBDATE,
  N.PUBLISHEDATE,
  N.CASHBDATE,
  N.CASHEDATE,
  N.ZAISHOUFLAG,
  N.RESELLBDATE,
  N.RESELLEDATE,
  N.INPERIOD,
  N.UNITMAX,
  N.PERSONMAX,
  N.UNITSMAX,
  N.PERSONSMAX,
  N.JXDFLAG,
  N.RATEAKIND,
  N.RATEEKIND,
  N.MAXLIMITEDJE,
  N.YYDATE,
  N.YYENDDATE,
  N.YYBEGINDATE,
  N.GROUPYGRATE,
  N.GROUPYGNEAP,
  N.GROUPYGTIPTOP,
  N.GROUPRGNEAP,
  N.GROUPRGTIPTOP,
  N.FEERATE,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(KINDCODE, '' ) AS KINDCODE ,
  COALESCE(KINDABNAME, '' ) AS KINDABNAME ,
  COALESCE(KINDNAME, '' ) AS KINDNAME ,
  COALESCE(KINDTYPE, '' ) AS KINDTYPE ,
  COALESCE(YEAR, '' ) AS YEAR ,
  COALESCE(LIMITEDPERIOD, 0 ) AS LIMITEDPERIOD ,
  COALESCE(PUBLISHPERIOD, 0 ) AS PUBLISHPERIOD ,
  COALESCE(MINLIMITED, 0 ) AS MINLIMITED ,
  COALESCE(YNPARTY, '' ) AS YNPARTY ,
  COALESCE(PUBLISHBDATE, '' ) AS PUBLISHBDATE ,
  COALESCE(PUBLISHEDATE, '' ) AS PUBLISHEDATE ,
  COALESCE(CASHBDATE, '' ) AS CASHBDATE ,
  COALESCE(CASHEDATE, '' ) AS CASHEDATE ,
  COALESCE(ZAISHOUFLAG, '' ) AS ZAISHOUFLAG ,
  COALESCE(RESELLBDATE, '' ) AS RESELLBDATE ,
  COALESCE(RESELLEDATE, '' ) AS RESELLEDATE ,
  COALESCE(INPERIOD, '' ) AS INPERIOD ,
  COALESCE(UNITMAX, 0 ) AS UNITMAX ,
  COALESCE(PERSONMAX, 0 ) AS PERSONMAX ,
  COALESCE(UNITSMAX, 0 ) AS UNITSMAX ,
  COALESCE(PERSONSMAX, 0 ) AS PERSONSMAX ,
  COALESCE(JXDFLAG, '' ) AS JXDFLAG ,
  COALESCE(RATEAKIND, '' ) AS RATEAKIND ,
  COALESCE(RATEEKIND, '' ) AS RATEEKIND ,
  COALESCE(MAXLIMITEDJE, 0 ) AS MAXLIMITEDJE ,
  COALESCE(YYDATE, 0 ) AS YYDATE ,
  COALESCE(YYENDDATE, '' ) AS YYENDDATE ,
  COALESCE(YYBEGINDATE, '' ) AS YYBEGINDATE ,
  COALESCE(GROUPYGRATE, 0 ) AS GROUPYGRATE ,
  COALESCE(GROUPYGNEAP, 0 ) AS GROUPYGNEAP ,
  COALESCE(GROUPYGTIPTOP, 0 ) AS GROUPYGTIPTOP ,
  COALESCE(GROUPRGNEAP, 0 ) AS GROUPRGNEAP ,
  COALESCE(GROUPRGTIPTOP, 0 ) AS GROUPRGTIPTOP ,
  COALESCE(FEERATE, 0 ) AS FEERATE 
 FROM  dw_tdata.FSS_001_CD_KIND_MAIN_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  KINDCODE ,
  KINDABNAME ,
  KINDNAME ,
  KINDTYPE ,
  YEAR ,
  LIMITEDPERIOD ,
  PUBLISHPERIOD ,
  MINLIMITED ,
  YNPARTY ,
  PUBLISHBDATE ,
  PUBLISHEDATE ,
  CASHBDATE ,
  CASHEDATE ,
  ZAISHOUFLAG ,
  RESELLBDATE ,
  RESELLEDATE ,
  INPERIOD ,
  UNITMAX ,
  PERSONMAX ,
  UNITSMAX ,
  PERSONSMAX ,
  JXDFLAG ,
  RATEAKIND ,
  RATEEKIND ,
  MAXLIMITEDJE ,
  YYDATE ,
  YYENDDATE ,
  YYBEGINDATE ,
  GROUPYGRATE ,
  GROUPYGNEAP ,
  GROUPYGTIPTOP ,
  GROUPRGNEAP ,
  GROUPRGTIPTOP ,
  FEERATE 
 FROM dw_sdata.FSS_001_CD_KIND_MAIN 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.KINDCODE = T.KINDCODE
WHERE
(T.KINDCODE IS NULL)
 OR N.KINDABNAME<>T.KINDABNAME
 OR N.KINDNAME<>T.KINDNAME
 OR N.KINDTYPE<>T.KINDTYPE
 OR N.YEAR<>T.YEAR
 OR N.LIMITEDPERIOD<>T.LIMITEDPERIOD
 OR N.PUBLISHPERIOD<>T.PUBLISHPERIOD
 OR N.MINLIMITED<>T.MINLIMITED
 OR N.YNPARTY<>T.YNPARTY
 OR N.PUBLISHBDATE<>T.PUBLISHBDATE
 OR N.PUBLISHEDATE<>T.PUBLISHEDATE
 OR N.CASHBDATE<>T.CASHBDATE
 OR N.CASHEDATE<>T.CASHEDATE
 OR N.ZAISHOUFLAG<>T.ZAISHOUFLAG
 OR N.RESELLBDATE<>T.RESELLBDATE
 OR N.RESELLEDATE<>T.RESELLEDATE
 OR N.INPERIOD<>T.INPERIOD
 OR N.UNITMAX<>T.UNITMAX
 OR N.PERSONMAX<>T.PERSONMAX
 OR N.UNITSMAX<>T.UNITSMAX
 OR N.PERSONSMAX<>T.PERSONSMAX
 OR N.JXDFLAG<>T.JXDFLAG
 OR N.RATEAKIND<>T.RATEAKIND
 OR N.RATEEKIND<>T.RATEEKIND
 OR N.MAXLIMITEDJE<>T.MAXLIMITEDJE
 OR N.YYDATE<>T.YYDATE
 OR N.YYENDDATE<>T.YYENDDATE
 OR N.YYBEGINDATE<>T.YYBEGINDATE
 OR N.GROUPYGRATE<>T.GROUPYGRATE
 OR N.GROUPYGNEAP<>T.GROUPYGNEAP
 OR N.GROUPYGTIPTOP<>T.GROUPYGTIPTOP
 OR N.GROUPRGNEAP<>T.GROUPRGNEAP
 OR N.GROUPRGTIPTOP<>T.GROUPRGTIPTOP
 OR N.FEERATE<>T.FEERATE
;

--Step3:
UPDATE dw_sdata.FSS_001_CD_KIND_MAIN P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_181
WHERE P.End_Dt=DATE('2100-12-31')
AND P.KINDCODE=T_181.KINDCODE
;

--Step4:
INSERT  INTO dw_sdata.FSS_001_CD_KIND_MAIN SELECT * FROM T_181;

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