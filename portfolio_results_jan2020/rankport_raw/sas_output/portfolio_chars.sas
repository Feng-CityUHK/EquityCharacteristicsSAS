
/* ********************************************************************************* */
/* MACRO 3            */
%MACRO FINRATIO_ind (begdate=, enddate=, nind=, avr=, input=, indratios=, vars=);
/*Impose filter to obtain unique gvkey-datadate records*/
%let compcond=indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C';
%let indclass=ffi&nind._desc;
/*List of Ratios to be calculated*/

%let allvars=&vars;

data ratios;
set &INPUT;
/*set time frame*/
where "&begdate"d<=public_date<="&enddate"d;
run;

/* proc export data=ratios outfile="firm_ratios_indclass_&indclass._.csv" */
/* dbms=csv replace; */

proc sort data = ratios; by public_date &indclass; run;
/*Computing Industry-level average financial ratios in a given month*/
proc means data=ratios noprint;
  where not missing(&indclass);
    by public_date; class &indclass;
     var &allvars;
     weight me;                       /* value-weight */
    output out=indratios &avr=/autoname;
run;
proc sort data=indratios; by public_date &indclass;run;

data &indratios; set indratios;
where &indclass ne '' and  &indclass ne '.';                                    /* how can I delete indclass null*/
drop _type_;
format public_date MMDDYYD10.;
run;

proc sql; drop table ratios, indratios;
quit;

%mend FINRATIO_ind;

/* ********************************************************************************* */
/* MACRO 4            */
%MACRO FINRATIO_ew_all (begdate=, enddate=, avr=, input=, indratios=, vars=);
/*Impose filter to obtain unique gvkey-datadate records*/
%let compcond=indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C';

/*List of Ratios to be calculated*/

%let allvars=&vars;

data ratios;
set &INPUT;
/*set time frame*/
where "&begdate"d<=public_date<="&enddate"d;
run;

/* proc export data=ratios outfile="firm_ratios_indclass_&indclass._.csv" */
/* dbms=csv replace; */

proc sort data = ratios; by public_date permno; run;
/*Computing Industry-level average financial ratios in a given month*/
proc means data=ratios noprint;
  where not missing(permno);
    by public_date;
     var &allvars;
    output out=indratios &avr=/autoname;
run;
proc sort data=indratios; by public_date;run;

data &indratios; set indratios;
drop _type_;
format public_date MMDDYYD10.;
run;

proc sql; drop table ratios, indratios;
quit;

%mend FINRATIO_ew_all;


/* ********************************************************************************* */
/* MACRO 5            */
%MACRO FINRATIO_ind_label (begdate=, enddate=, label=, avr=, weight=,input=, vars=);
/*Impose filter to obtain unique gvkey-datadate records*/
/* %let compcond=indfmt='INDL' and datafmt='STD' and popsrc='D' and consol='C'; */
/* %let indclass=ffi&nind._desc; */
/*List of Ratios to be calculated*/

%let allvars=&vars;
%let indclass = &label;

data ratios;
set &INPUT;
/*set time frame*/
where "&begdate"d<=public_date<="&enddate"d;
run;

/* proc export data=ratios outfile="firm_ratios_indclass_&indclass._.csv" */
/* dbms=csv replace; */

/* average zt */
proc sort data = ratios; by public_date &indclass; run;
/*Computing Industry-level average financial ratios in a given month*/
proc means data=ratios noprint;
  where not missing(&indclass);
    by public_date; class &indclass;
     var &allvars;
     weight &weight;                       /* value-weight */
    output out=indratios &avr=/autoname;
run;
proc sort data=indratios; by public_date &indclass;run;

data indratios; set indratios;
where not missing(&indclass);
drop _type_;
format public_date MMDDYYD10.;
run;

proc export data=indratios outfile="indratios_&indclass..csv"
dbms=csv replace;

/* merge next month portfolio returns */
proc sql;
create table rets as
select
  a.*, b.ret
from
  ratios a left join crsp.msf b
on
  a.permno=b.permno and
  intnx('month',a.public_date,0,'e')=intnx('month',b.date,-1,'e')
order by
  a.public_date, a.&indclass, a.permno
;
quit;

/* portfolio returns */
proc sort data = rets; by public_date &indclass; run;
/*Computing Industry-level average financial ratios in a given month*/
proc means data=rets noprint;
  where not missing(&indclass);
    by public_date; class &indclass;
     var ret;
     weight &weight;                       /* value-weight */
    output out=vwret (drop= _type_ _freq_ ) mean=vwret n=n_firms;
run;
proc sort data=vwret; by public_date &indclass; run;

proc transpose data=vwret(keep=public_date &indclass vwret)
 out=vwret2 (drop=_name_ _label_);
 by public_date ;
 ID &indclass;
 Var vwret;
run;

proc export data=vwret2 outfile="vwret2_&indclass..csv"
dbms=csv replace;

proc transpose data=vwret(keep=public_date &indclass n_firms)
               out=vwret3 (drop=_name_ _label_) prefix=n_;
by public_date ;
ID &indclass;
Var n_firms;
run;

proc export data=vwret3 outfile="vwret3_&indclass..csv"
dbms=csv replace;

/* clean house */
proc sql; drop table ratios, indratios, rets, indrets;
quit;

%mend FINRATIO_ind_label;
