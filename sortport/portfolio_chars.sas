
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
