libname chars '/scratch/cityuhk/xinhe_mandy/eqchars';

%let vars_rank =
  mom12m hxz_abr hxz_sue hxz_re
  bm ep cfp sp
  agr ni acc
  op roe
  seas1a adm rdm
  me svar beta mom1m
;

/* ********************************************* */
/*  Load data                    */
/* ********************************************* */

data da;
set chars.firmchars_v7_1_final;
keep permno public_date &vars_rank;
run;

data chars.v7_1_rank;
set da;
run;

/* ********************************************* */
/*  Loop function                   */
/* ********************************************* */

%macro loop(vars_rank);
%local i next_name;
%do i=1 %to %sysfunc(countw(&vars_rank));
   %let next_name = %scan(&vars_rank, &i);

   /* ********************************************* */
   /*  Get ranks                      */
   /* ********************************************* */

   proc rank data=da out=darank ties=mean descending;
      by public_date;
      var &next_name;
      ranks rk_&next_name;
   run;

   /* ********************************************* */
   /*  get no. of obs. in each date-var                      */
   /* ********************************************* */
   proc sql;
   create table
   	darank1 as
   select
   	a.*, count(a.rk_&next_name) as n_&next_name
   from
   	darank a group by public_date
   order by
   	public_date, permno
   ;
   quit;

   /* ********************************************* */
   /*  standardize to -1 1                      */
   /* ********************************************* */

   data darank2;
   set darank1;
   rank_&next_name = (rk_&next_name-1)/(n_&next_name-1)*2-1;
   if missing(rank_&next_name) then rank_&next_name=0;
   run;

   /* ********************************************* */
   /*  merge to da                      */
   /* ********************************************* */
   proc sql;
   create table da as
   select a.*, b.rank_&next_name
   from da a left join darank2 b on
   a.permno=b.permno and a.public_date=b.public_date
   order by public_date, permno;
   quit;

   data chars.v7_1_rank;
   set da;
   run;

%end;
%mend;

%loop(&vars_rank);

/* ********************************************* */
/*  Final results                     */
/* ********************************************* */

data da;
set chars.v7_1_rank;
run;

proc export data=da
outfile="/scratch/cityuhk/xinhe_mandy/eqchars/rank_final.csv" dbms=csv replace; run;

proc export data=da(where=(year(public_date)=2018))
outfile="/scratch/cityuhk/xinhe_mandy/eqchars/rank_final2018.csv" dbms=csv replace; run;
