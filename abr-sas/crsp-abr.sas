/* ********************************************* */
/*      crsp abnormal return                     */
/* ********************************************* */

%let msfvars = permco permno prc ret vol shrout cfacpr cfacshr;
%let msevars = ncusip exchcd shrcd siccd ;

%crspmerge(s=d,start=01jan2010,end=31dec2018,
	sfvars=&msfvars,sevars=&msevars,
  filters=exchcd in (1,2,3) and shrcd in (10,11));
proc sql; create table mydaily
	as select a.*, b.dlret,
	  sum(1,ret)*sum(1,dlret)-1 as retadj "Return adjusted for delisting",
	  abs(a.prc)*a.shrout as MEq 'Market Value of Equity'
	 from Crsp_d a left join crsp.dsedelist(where=(missing(dlret)=0)) b
	 on a.permno=b.permno and a.date=b.DLSTDT
	 order by a.date, a.permno, MEq;
quit;

/* sprtrn */
data crspsp500d;
set crsp.dsi;
keep date sprtrn;
run;

proc sort data=crspsp500d nodupkey; by date;run;
proc export data = crspsp500d(where=(year(date)=2018))
outfile='/scratch/cityuhk/xinhe/eqchars/abr_sprtrn.csv' dbms=csv replace; run;

/* abnormal return */
proc sql;
create table mydaily1 as
select a.date, a.permno, a.ret, a.retadj, b.sprtrn, a.retadj-b.sprtrn as abrd
from mydaily a left join crspsp500d b
on a.date=b.date
order by a.permno, a.date;
quit;

proc export data = mydaily1(where=(year(date)=2018))
outfile='/scratch/cityuhk/xinhe/eqchars/abr_mydaily.csv' dbms=csv replace; run;
