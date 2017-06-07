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
DELETE FROM dw_sdata.ISS_001_GN_LCREJECTRELEASEINFO WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.ISS_001_GN_LCREJECTRELEASEINFO SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_348 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.ISS_001_GN_LCREJECTRELEASEINFO WHERE 1=0;

--Step2:
INSERT  INTO T_348 (
  TXNSERIALNO,
  BPNO,
  LCNO,
  ABNO,
  REJECTDATE,
  REJECTAMT,
  SENDBANKNO,
  SENDBANKSWIFTCODE,
  SENDBANKNAMEADDR,
  RECVBANKNO,
  RECVBANKSWIFTCODE,
  RECVBANKNAMEADDR,
  BPBANKNAMEADDR,
  BPBANKNO,
  REJECTREASON,
  BILLTYPE,
  BILLCOUNT,
  PARTPAYAMT,
  MEMO,
  start_dt,
  end_dt)
SELECT
  N.TXNSERIALNO,
  N.BPNO,
  N.LCNO,
  N.ABNO,
  N.REJECTDATE,
  N.REJECTAMT,
  N.SENDBANKNO,
  N.SENDBANKSWIFTCODE,
  N.SENDBANKNAMEADDR,
  N.RECVBANKNO,
  N.RECVBANKSWIFTCODE,
  N.RECVBANKNAMEADDR,
  N.BPBANKNAMEADDR,
  N.BPBANKNO,
  N.REJECTREASON,
  N.BILLTYPE,
  N.BILLCOUNT,
  N.PARTPAYAMT,
  N.MEMO,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(TXNSERIALNO, '' ) AS TXNSERIALNO ,
  COALESCE(BPNO, '' ) AS BPNO ,
  COALESCE(LCNO, '' ) AS LCNO ,
  COALESCE(ABNO, '' ) AS ABNO ,
  COALESCE(REJECTDATE,DATE('4999-12-31') ) AS REJECTDATE ,
  COALESCE(REJECTAMT, 0 ) AS REJECTAMT ,
  COALESCE(SENDBANKNO, '' ) AS SENDBANKNO ,
  COALESCE(SENDBANKSWIFTCODE, '' ) AS SENDBANKSWIFTCODE ,
  COALESCE(SENDBANKNAMEADDR, '' ) AS SENDBANKNAMEADDR ,
  COALESCE(RECVBANKNO, '' ) AS RECVBANKNO ,
  COALESCE(RECVBANKSWIFTCODE, '' ) AS RECVBANKSWIFTCODE ,
  COALESCE(RECVBANKNAMEADDR, '' ) AS RECVBANKNAMEADDR ,
  COALESCE(BPBANKNAMEADDR, '' ) AS BPBANKNAMEADDR ,
  COALESCE(BPBANKNO, '' ) AS BPBANKNO ,
  COALESCE(REJECTREASON, '' ) AS REJECTREASON ,
  COALESCE(BILLTYPE, '' ) AS BILLTYPE ,
  COALESCE(BILLCOUNT, 0 ) AS BILLCOUNT ,
  COALESCE(PARTPAYAMT, 0 ) AS PARTPAYAMT ,
  COALESCE(MEMO, '' ) AS MEMO 
 FROM  dw_tdata.ISS_001_GN_LCREJECTRELEASEINFO_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  TXNSERIALNO ,
  BPNO ,
  LCNO ,
  ABNO ,
  REJECTDATE ,
  REJECTAMT ,
  SENDBANKNO ,
  SENDBANKSWIFTCODE ,
  SENDBANKNAMEADDR ,
  RECVBANKNO ,
  RECVBANKSWIFTCODE ,
  RECVBANKNAMEADDR ,
  BPBANKNAMEADDR ,
  BPBANKNO ,
  REJECTREASON ,
  BILLTYPE ,
  BILLCOUNT ,
  PARTPAYAMT ,
  MEMO 
 FROM dw_sdata.ISS_001_GN_LCREJECTRELEASEINFO 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.TXNSERIALNO = T.TXNSERIALNO
WHERE
(T.TXNSERIALNO IS NULL)
 OR N.BPNO<>T.BPNO
 OR N.LCNO<>T.LCNO
 OR N.ABNO<>T.ABNO
 OR N.REJECTDATE<>T.REJECTDATE
 OR N.REJECTAMT<>T.REJECTAMT
 OR N.SENDBANKNO<>T.SENDBANKNO
 OR N.SENDBANKSWIFTCODE<>T.SENDBANKSWIFTCODE
 OR N.SENDBANKNAMEADDR<>T.SENDBANKNAMEADDR
 OR N.RECVBANKNO<>T.RECVBANKNO
 OR N.RECVBANKSWIFTCODE<>T.RECVBANKSWIFTCODE
 OR N.RECVBANKNAMEADDR<>T.RECVBANKNAMEADDR
 OR N.BPBANKNAMEADDR<>T.BPBANKNAMEADDR
 OR N.BPBANKNO<>T.BPBANKNO
 OR N.REJECTREASON<>T.REJECTREASON
 OR N.BILLTYPE<>T.BILLTYPE
 OR N.BILLCOUNT<>T.BILLCOUNT
 OR N.PARTPAYAMT<>T.PARTPAYAMT
 OR N.MEMO<>T.MEMO
;

--Step3:
UPDATE dw_sdata.ISS_001_GN_LCREJECTRELEASEINFO P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_348
WHERE P.End_Dt=DATE('2100-12-31')
AND P.TXNSERIALNO=T_348.TXNSERIALNO
;

--Step4:
INSERT  INTO dw_sdata.ISS_001_GN_LCREJECTRELEASEINFO SELECT * FROM T_348;

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
