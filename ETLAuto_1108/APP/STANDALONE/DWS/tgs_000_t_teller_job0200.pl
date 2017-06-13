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
DELETE FROM dw_sdata.TGS_000_T_TELLER_JOB WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.TGS_000_T_TELLER_JOB SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_344 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.TGS_000_T_TELLER_JOB WHERE 1=0;

--Step2:
INSERT  INTO T_344 (
  ID_,
  NAME_,
  CODE_,
  MEMO_,
  LEVEL_,
  STATUS_,
  TYPE_,
  JOB_TYPE_,
  start_dt,
  end_dt)
SELECT
  N.ID_,
  N.NAME_,
  N.CODE_,
  N.MEMO_,
  N.LEVEL_,
  N.STATUS_,
  N.TYPE_,
  N.JOB_TYPE_,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(ID_, '' ) AS ID_ ,
  COALESCE(NAME_, '' ) AS NAME_ ,
  COALESCE(CODE_, '' ) AS CODE_ ,
  COALESCE(MEMO_, '' ) AS MEMO_ ,
  COALESCE(LEVEL_, '' ) AS LEVEL_ ,
  COALESCE(STATUS_, '' ) AS STATUS_ ,
  COALESCE(TYPE_, '' ) AS TYPE_ ,
  COALESCE(JOB_TYPE_, '' ) AS JOB_TYPE_ 
 FROM  dw_tdata.TGS_000_T_TELLER_JOB_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  ID_ ,
  NAME_ ,
  CODE_ ,
  MEMO_ ,
  LEVEL_ ,
  STATUS_ ,
  TYPE_ ,
  JOB_TYPE_ 
 FROM dw_sdata.TGS_000_T_TELLER_JOB 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.ID_ = T.ID_
WHERE
(T.ID_ IS NULL)
 OR N.NAME_<>T.NAME_
 OR N.CODE_<>T.CODE_
 OR N.MEMO_<>T.MEMO_
 OR N.LEVEL_<>T.LEVEL_
 OR N.STATUS_<>T.STATUS_
 OR N.TYPE_<>T.TYPE_
 OR N.JOB_TYPE_<>T.JOB_TYPE_
;

--Step3:
UPDATE dw_sdata.TGS_000_T_TELLER_JOB P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_344
WHERE P.End_Dt=DATE('2100-12-31')
AND P.ID_=T_344.ID_
;

--Step4:
INSERT  INTO dw_sdata.TGS_000_T_TELLER_JOB SELECT * FROM T_344;

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
