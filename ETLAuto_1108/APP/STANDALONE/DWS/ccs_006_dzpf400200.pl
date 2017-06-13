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
DELETE FROM dw_sdata.CCS_006_DZPF40 WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.CCS_006_DZPF40 SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_130 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.CCS_006_DZPF40 WHERE 1=0;

--Step2:
INSERT  INTO T_130 (
  DZ40LNCATG,
  DZ40CUR,
  DZ40CATGNM,
  DZ40PRODC,
  DZ40PRODNM,
  DZ40DATE,
  start_dt,
  end_dt)
SELECT
  N.DZ40LNCATG,
  N.DZ40CUR,
  N.DZ40CATGNM,
  N.DZ40PRODC,
  N.DZ40PRODNM,
  N.DZ40DATE,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(DZ40LNCATG, '' ) AS DZ40LNCATG ,
  COALESCE(DZ40CUR, '' ) AS DZ40CUR ,
  COALESCE(DZ40CATGNM, '' ) AS DZ40CATGNM ,
  COALESCE(DZ40PRODC, '' ) AS DZ40PRODC ,
  COALESCE(DZ40PRODNM, '' ) AS DZ40PRODNM ,
  COALESCE(DZ40DATE, '' ) AS DZ40DATE 
 FROM  dw_tdata.CCS_006_DZPF40_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  DZ40LNCATG ,
  DZ40CUR ,
  DZ40CATGNM ,
  DZ40PRODC ,
  DZ40PRODNM ,
  DZ40DATE 
 FROM dw_sdata.CCS_006_DZPF40 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.DZ40LNCATG = T.DZ40LNCATG AND N.DZ40CUR = T.DZ40CUR
WHERE
(T.DZ40LNCATG IS NULL AND T.DZ40CUR IS NULL)
 OR N.DZ40CATGNM<>T.DZ40CATGNM
 OR N.DZ40PRODC<>T.DZ40PRODC
 OR N.DZ40PRODNM<>T.DZ40PRODNM
 OR N.DZ40DATE<>T.DZ40DATE
;

--Step3:
UPDATE dw_sdata.CCS_006_DZPF40 P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_130
WHERE P.End_Dt=DATE('2100-12-31')
AND P.DZ40LNCATG=T_130.DZ40LNCATG
AND P.DZ40CUR=T_130.DZ40CUR
;

--Step4:
INSERT  INTO dw_sdata.CCS_006_DZPF40 SELECT * FROM T_130;

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
