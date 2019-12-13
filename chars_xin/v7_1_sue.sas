/* ********************************************* */
/* ********************************************* */
/* Calculate HXZ Replicating Anormalies          */
/* SUE                                           */
/* ********************************************* */
/* ********************************************* */

/* ********************************************* */
/*       load compq                              */
/* ********************************************* */
data comp1;
set comp.fundq;
where indfmt='INDL' and datafmt='STD' and consol='C' and popsrc='D'
  and datadate>="01JAN1950"d;
keep gvkey datadate fyearq fqtr epspxq ajexq;
run;

proc export data = comp1(where=(year(datadate)=2018))
outfile='/scratch/cityuhk/xinhe/eqchars/sue_comp1.csv' dbms=csv replace; run;

/* ********************************************* */
/*       prepare merging with crsp               */
/* ********************************************* */
/* Add Historical PERMNO identifier */
proc sql;
  create table comp2
  as select a.*, b.lpermno as permno, b.linkprim
  from comp1 as a, crsp.ccmxpf_linktable as b
  where a.gvkey = b.gvkey and
  b.LINKTYPE in ("LU","LC") and
 (b.LINKDT <= a.datadate) and (a.datadate <= b.LINKENDDT or missing(b.LINKENDDT));
quit;

proc export data = comp2(where=(year(datadate)=2018))
outfile='/scratch/cityuhk/xinhe/eqchars/sue_comp2.csv' dbms=csv replace; run;

/* ********************************************* */
/*       the time series of epspxq/ajexq         */
/* ********************************************* */
data comp3;
set comp2;
eps = epspxq/ajexq;
run;

proc sort data=comp3 nodupkey; by permno datadate;run;

proc export data = comp3(where=(year(datadate)>=2018))
outfile='/scratch/cityuhk/xinhe/eqchars/sue_comp3.csv' dbms=csv replace; run;

/* merge lag1 to lag8, then calc sd */

data comp4;
set comp3;

if missing(eps) then delete;

by permno datadate;
retain count;
if first.permno then count=1;
else count = count+1;

e1 = lag1(eps);
e2 = lag2(eps);
e3 = lag3(eps);
e4 = lag4(eps);
e5 = lag5(eps);
e6 = lag6(eps);
e7 = lag7(eps);
e8 = lag8(eps);

if count<=6 then sdsue=.;
if count=7 then do;
meansue = (e8+e7+e6+e5+e4+e3);
sdsue   = (( (e8-meansue)**2+(e7-meansue)**2+(e6-meansue)**2
            +(e5-meansue)**2+(e4-meansue)**2+(e3-meansue)**2
           )/5
          )**0.5
;
end;
if count=8 then do;
meansue = (e8+e7+e6+e5+e4+e3+e2);
sdsue   = (( (e8-meansue)**2+(e7-meansue)**2+(e6-meansue)**2
            +(e5-meansue)**2+(e4-meansue)**2+(e3-meansue)**2
            +(e2-meansue)**2
           )/6
          )**0.5
;
end;
if count>=9 then do;
meansue = (e8+e7+e6+e5+e4+e3+e2+e1);
sdsue   = (( (e8-meansue)**2+(e7-meansue)**2+(e6-meansue)**2
            +(e5-meansue)**2+(e4-meansue)**2+(e3-meansue)**2
            +(e2-meansue)**2+(e1-meansue)**2
           )/7
          )**0.5
;
end;
hxz_sue = (eps-lag4(eps))/sdsue;
run;

proc export data = comp4(where=(year(datadate)>=2010))
outfile='/scratch/cityuhk/xinhe/eqchars/sue_comp4.csv' dbms=csv replace; run;

/* ********************************************* */
/*       populate the quarterly sue to monthly   */
/* ********************************************* */
proc sql;
  create
    table sue as
  select
    a.*, b.date format=date9.
  from
    comp4 a left join (select distinct date from crsp.msf) b
  on
    a.datadate<=b.date and
    intnx('month',a.datadate,12,'E')>=b.date
  order by
    a.permno, b.date, a.datadate desc
  ;
quit;

proc export data = sue(where=(year(date)>=2017))
outfile='/scratch/cityuhk/xinhe/eqchars/v7_1_sue_dup.csv' dbms=csv replace; run;

proc sort data=sue nodupkey; by permno date; run;
/* ********************************************* */
/*  save sue                                     */
/* ********************************************* */

libname chars '/scratch/cityuhk/xinhe/eqchars';
data chars.v7_1_sue; set sue; run;

proc export data = sue(where=(year(date)>=2017))
outfile='/scratch/cityuhk/xinhe/eqchars/v7_1_sue.csv' dbms=csv replace; run;
