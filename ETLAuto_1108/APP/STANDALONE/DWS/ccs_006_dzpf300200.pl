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
DELETE FROM dw_sdata.CCS_006_DZPF30 WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.CCS_006_DZPF30 SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_129 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.CCS_006_DZPF30 WHERE 1=0;

--Step2:
INSERT  INTO T_129 (
  DZ30TYPE,
  DZ30TYPNM,
  DZ30PRDTYP,
  DZ30PRDNM,
  DZ30DATE,
  start_dt,
  end_dt)
SELECT
  N.DZ30TYPE,
  N.DZ30TYPNM,
  N.DZ30PRDTYP,
  N.DZ30PRDNM,
  N.DZ30DATE,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(DZ30TYPE, '' ) AS DZ30TYPE ,
  COALESCE(DZ30TYPNM, '' ) AS DZ30TYPNM ,
  COALESCE(DZ30PRDTYP, '' ) AS DZ30PRDTYP ,
  COALESCE(DZ30PRDNM, '' ) AS DZ30PRDNM ,
  COALESCE(DZ30DATE, '' ) AS DZ30DATE 
 FROM  dw_tdata.CCS_006_DZPF30_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  DZ30TYPE ,
  DZ30TYPNM ,
  DZ30PRDTYP ,
  DZ30PRDNM ,
  DZ30DATE 
 FROM dw_sdata.CCS_006_DZPF30 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.DZ30TYPE = T.DZ30TYPE
WHERE
(T.DZ30TYPE IS NULL)
 OR N.DZ30TYPNM<>T.DZ30TYPNM
 OR N.DZ30PRDTYP<>T.DZ30PRDTYP
 OR N.DZ30PRDNM<>T.DZ30PRDNM
 OR N.DZ30DATE<>T.DZ30DATE
;

--Step3:
UPDATE dw_sdata.CCS_006_DZPF30 P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_129
WHERE P.End_Dt=DATE('2100-12-31')
AND P.DZ30TYPE=T_129.DZ30TYPE
;

--Step4:
INSERT  INTO dw_sdata.CCS_006_DZPF30 SELECT * FROM T_129;

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
