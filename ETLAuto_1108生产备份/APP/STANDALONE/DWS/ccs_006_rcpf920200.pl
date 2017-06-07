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
DELETE FROM dw_sdata.CCS_006_RCPF92 WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.CCS_006_RCPF92 SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_143 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.CCS_006_RCPF92 WHERE 1=0;

--Step2:
INSERT  INTO T_143 (
  RC92PRE,
  RC92DPNOK,
  RC92DPNOA,
  RC92DUEBNO,
  RC92STDATE,
  RC92EDDATE,
  RC92LV,
  RC92FLAG,
  RC92STOP,
  RC92CUR,
  RC92AMT,
  RC92BAMT1,
  RC92BAMT2,
  RC92BZ,
  start_dt,
  end_dt)
SELECT
  N.RC92PRE,
  N.RC92DPNOK,
  N.RC92DPNOA,
  N.RC92DUEBNO,
  N.RC92STDATE,
  N.RC92EDDATE,
  N.RC92LV,
  N.RC92FLAG,
  N.RC92STOP,
  N.RC92CUR,
  N.RC92AMT,
  N.RC92BAMT1,
  N.RC92BAMT2,
  N.RC92BZ,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(RC92PRE, '' ) AS RC92PRE ,
  COALESCE(RC92DPNOK, '' ) AS RC92DPNOK ,
  COALESCE(RC92DPNOA, '' ) AS RC92DPNOA ,
  COALESCE(RC92DUEBNO, '' ) AS RC92DUEBNO ,
  COALESCE(RC92STDATE, '' ) AS RC92STDATE ,
  COALESCE(RC92EDDATE, '' ) AS RC92EDDATE ,
  COALESCE(RC92LV, 0 ) AS RC92LV ,
  COALESCE(RC92FLAG, '' ) AS RC92FLAG ,
  COALESCE(RC92STOP, '' ) AS RC92STOP ,
  COALESCE(RC92CUR, '' ) AS RC92CUR ,
  COALESCE(RC92AMT, 0 ) AS RC92AMT ,
  COALESCE(RC92BAMT1, 0 ) AS RC92BAMT1 ,
  COALESCE(RC92BAMT2, 0 ) AS RC92BAMT2 ,
  COALESCE(RC92BZ, '' ) AS RC92BZ 
 FROM  dw_tdata.CCS_006_RCPF92_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  RC92PRE ,
  RC92DPNOK ,
  RC92DPNOA ,
  RC92DUEBNO ,
  RC92STDATE ,
  RC92EDDATE ,
  RC92LV ,
  RC92FLAG ,
  RC92STOP ,
  RC92CUR ,
  RC92AMT ,
  RC92BAMT1 ,
  RC92BAMT2 ,
  RC92BZ 
 FROM dw_sdata.CCS_006_RCPF92 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.RC92DUEBNO = T.RC92DUEBNO AND N.RC92STDATE = T.RC92STDATE
WHERE
(T.RC92DUEBNO IS NULL AND T.RC92STDATE IS NULL)
 OR N.RC92PRE<>T.RC92PRE
 OR N.RC92DPNOK<>T.RC92DPNOK
 OR N.RC92DPNOA<>T.RC92DPNOA
 OR N.RC92EDDATE<>T.RC92EDDATE
 OR N.RC92LV<>T.RC92LV
 OR N.RC92FLAG<>T.RC92FLAG
 OR N.RC92STOP<>T.RC92STOP
 OR N.RC92CUR<>T.RC92CUR
 OR N.RC92AMT<>T.RC92AMT
 OR N.RC92BAMT1<>T.RC92BAMT1
 OR N.RC92BAMT2<>T.RC92BAMT2
 OR N.RC92BZ<>T.RC92BZ
;

--Step3:
UPDATE dw_sdata.CCS_006_RCPF92 P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_143
WHERE P.End_Dt=DATE('2100-12-31')
AND P.RC92DUEBNO=T_143.RC92DUEBNO
AND P.RC92STDATE=T_143.RC92STDATE
;

--Step4:
INSERT  INTO dw_sdata.CCS_006_RCPF92 SELECT * FROM T_143;

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
