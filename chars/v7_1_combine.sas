libname chars '/scratch/cityuhk/xinhe/eqchars';

/* accounting info */
data temp7; set chars.temp7_monthupdate; run;

data temp7;
set temp7;
za_bm=za_bm_n/mcap_crsp;
za_ep=za_ep_n/mcap_crsp;
za_cfp=za_cfp_n/mcap_crsp;
za_sp=za_sp_n/mcap_crsp;
za_adm=za_adm_n/mcap_crsp;
za_rdm=za_rdm_n/mcap_crsp;
z_bm=z_bm_n/mcap_crsp;
z_ep=z_ep_n/mcap_crsp;
z_cfp=z_cfp_n/mcap_crsp;
z_sp=z_sp_n/mcap_crsp;
run;

/* rvar ff3 */
data out_ds_ff3; set chars.v7_1_rvar_ff3; run;

/* rvar capm */
data out_ds_capm; set chars.v7_1_rvar_capm; run;

/* rvar mean */
data out_ds_mean; set chars.v7_1_rvar_mean; run;

/* beta */
data out_ds_beta; set chars.v7_1_beta; run;

/* sue */
data out_ds_sue; set chars.v7_1_sue; run;

/* abr */
data out_ds_abr; set chars.v7_1_abr; run;

/* re */
data out_ds_re; set chars.v7_1_re; run;

*==============================================================================================================

						merge all parts

==============================================================================================================;

proc sql;
create table temp7 as
select a.*, b.rvar as z_rvar_ff3 from
temp7 a left join work.out_ds_ff3 b
on
a.permno = b.permno
and year(a.date) = year(b.date)
and month(a.date) = month(b.date)
and day(a.date)>= day(b.date)-3
and day(a.date)<= day(b.date)
order by a.permno, a.date;
quit;

proc sql;
create table temp7 as
select a.*, b.rvar as z_rvar_capm from
temp7 a left join work.out_ds_capm b
on
a.permno = b.permno
and year(a.date) = year(b.date)
and month(a.date) = month(b.date)
and day(a.date)>= day(b.date)-3
and day(a.date)<= day(b.date)
order by a.permno, a.date;
quit;

proc sql;
create table temp7 as
select a.*, b.svar as z_rvar_mean from
temp7 a left join work.out_ds_mean b
on
a.permno = b.permno
and year(a.date) = year(b.date)
and month(a.date) = month(b.date)
and day(a.date)>= day(b.date)-3
and day(a.date)<= day(b.date)
order by a.permno, a.date;
quit;


proc sql;
create table temp7 as
select a.*, b.beta as z_beta from
temp7 a left join work.out_ds_beta b
on
a.permno = b.permno
and year(a.date) = year(b.date)
and month(a.date) = month(b.date)
and day(a.date)>= day(b.date)-3
and day(a.date)<= day(b.date)
order by a.permno, a.date;
quit;

proc sql;
create table temp7 as
select a.*, b.hxz_sue as z_hxz_sue from
temp7 a left join work.out_ds_sue b
on
a.permno = b.permno
and intnx('month',a.date,0,'End') = intnx('month',b.date,0,'End')
order by a.permno, a.date;
quit;

proc sql;
create table temp7 as
select a.*, b.abr as z_hxz_abr from
temp7 a left join work.out_ds_abr b
on
a.permno = b.permno
and intnx('month',a.date,0,'End') = intnx('month',b.date,0,'End')
order by a.permno, a.date;
quit;

proc sql;
create table temp7 as
select a.*, b.hxz_re as z_hxz_re from
temp7 a left join work.out_ds_re b
on
a.permno = b.permno
and intnx('month',a.date,0,'End') = intnx('month',b.date,0,'End')
order by a.permno, a.date;
quit;

proc sort data=temp7 nodupkey;
	where  year(date)>=1950;
	by permno date;
run;

data chars.temp7_rvars; set temp7; run;
proc export data = temp7(where=(year(date)=2018))
outfile='/scratch/cityuhk/xinhe/eqchars/temp7_combine.csv' dbms=csv replace; run;
