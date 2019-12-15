%include 'portfolio_chars.sas';

%let uni_begdt = 01JAN1976;
%let uni_enddt = 31DEC2018;

libname chars '/scratch/cityuhk/xinhe_mandy/eqchars';

%let vars =
mom12m hxz_abr hxz_sue hxz_re
bm ep cfp sp
agr ni acc
op roe
seas1a adm rdm
me svar beta mom1m
;

%let rank_vars =
rank_mom12m rank_hxz_abr rank_hxz_sue rank_hxz_re
rank_bm rank_ep rank_cfp rank_sp
rank_agr rank_ni rank_acc
rank_op rank_roe
rank_seas1a rank_adm rank_rdm
rank_me rank_svar rank_beta rank_mom1m
;

/* ********************************************* */
/*  Final results                     */
/* ********************************************* */

data da;
set chars.v7_1_rank_label;
keep
    public_date   permno   gvkey   sic  cusip
    me
    &rank_vars
    FFI49 FFI49_desc
    DGTW_PORT
    port_me port_beta port_svar;
run;

data da;
set da;
rename
me = weight_port
rank_mom12m = mom12m
rank_hxz_abr = hxz_abr
rank_hxz_sue = hxz_sue
rank_hxz_re = hxz_re
rank_bm = bm
rank_ep = ep
rank_cfp = cfp
rank_sp = sp
rank_agr = agr
rank_ni = ni
rank_acc = acc
rank_op = op
rank_roe = roe
rank_seas1a = seas1a
rank_adm = adm
rank_rdm = rdm
rank_me = me
rank_svar = svar
rank_beta = beta
rank_mom1m = mom1m;
run;

data eqchars;
set da;
keep
    public_date   permno   gvkey   sic  cusip
    weight_port
    &vars
    FFI49 FFI49_desc
    DGTW_PORT
    port_me port_beta port_svar;
run;

/* ********************************************************************************* */


%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=port_me, AVR=mean, weight=weight_port, Input=eqchars, vars=&vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=port_svar, AVR=mean, weight=weight_port, Input=eqchars, vars=&vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=port_beta, AVR=mean, weight=weight_port, Input=eqchars, vars=&vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=FFI49_desc, AVR=mean, weight=weight_port, Input=eqchars, vars=&vars);
%FINRATIO_ind_label  (BEGDATE=&uni_begdt, ENDDATE=&uni_enddt, label=DGTW_PORT, AVR=mean, weight=weight_port, Input=eqchars, vars=&vars);
