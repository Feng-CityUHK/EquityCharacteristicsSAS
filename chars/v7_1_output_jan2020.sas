libname chars '/scratch/cityuhk/xinhe/eqchars';

data temp7; set chars.temp7_rvars; run;

data temp;
	set temp7;
	/* where not missing(mve) and not missing(mom1m) and not missing(bm); */
	/* this filter ?*/
	where not missing(bm);
	if missing(eamonth) then eamonth=0;
	if missing(IPO) then IPO=0;
	run;


  %let vars=beta betasq z_beta ep dy sue z_sue chfeps bm fgr5yr lev currat pchcurrat quick pchquick
  baspread mom12m depr
  pchdepr mom1m mom6m mom36m sgr chempia SP acc turn
  pchsale_pchinvt pchsale_pchrect pchcapx_ia pchgm_pchsale pchsale_pchxsga
  nincr indmom ps mve_ia cfp_ia bm_ia meanrec dolvol std_dolvol std_turn
  sfe  nanalyst disp chinv idiovol
  obklg grltnoa cinvest tb cfp roavol lgr egr ill age ms pricedelay
  rd_sale rd_mve retvol herf grCAPX zerotrade chmom roic
  aeavol chnanalyst agr chcsho chpmia chatoia grGW
  ear  rsup z_rsup stdcf tang  spi  hire chadv cashpr roaq
  invest absacc stdacc chtx maxret pctacc  cash gma roe roeq
  salecash salerec saleinv pchsaleinv cashdebt realestate  secured credrat
  	z_ac z_bm z_cfp /* v3 Xin He add variables */
  	z_inv z_ni z_op
  	mom60m z_dy z_rvar_ff3	z_rvar_capm	z_rvar_mean	 																							        /* v4 add */
    z_ep
  	za_ac
  	za_inv
  	za_bm
  	za_cfp
  	za_ep
  	za_ni
  	za_op
  	z_mom1m
  	z_mom12m
  	z_mom60m
  	z_mom36m
  	z_mom6m
  	z_moms12m
		z_seas1a
  	za_rsup
  	za_sue
  	za_dy
  			za_cash
  		 za_chcsho
  		 za_rd
  		 za_cashdebt
  		 za_pctacc
  		 za_gma
  		 za_lev
  		 za_rd_mve
			 za_rdm
			 za_adm
  		 za_sgr
  		 za_sp
  			za_invest
  			za_rd_sale
  			za_ps
  			za_lgr
  			za_roa
  			za_depr
  			za_egr
  			za_grltnoa
  			za_chpm
  			za_chato
  			za_chtx
  			za_ala
  			za_alm
  			za_noa
  			za_rna
  			za_pm
  			za_ato

  					z_cash
  					z_chcsho
  					z_rd
  					z_cashdebt
  					z_pctacc
  					z_gma
  					z_lev
  					z_rd_mve
						z_rdm
  					z_sgr
  					z_sp
  						maxret
  						z_invest
  						z_rd_sale
  						z_ps
  						z_lgr
  						z_roa
  						z_depr
  						z_egr
  						z_grltnoa
  						z_chpm
  						z_chato
  						z_chtx
  						z_ala
  						z_alm
  						z_noa
  						z_rna
  						z_pm
  						z_ato
			z_hxz_re
			z_hxz_abr
			z_hxz_sue
  	operprof
  	;
  *this is for those bounded below at zero but may have large positive outliers;
  %let hitrim=betasq dy  lev baspread  depr  SP turn  dolvol std_dolvol std_turn
  			disp idiovol obklg roavol ill age rd_sale rd_mve retvol zerotrade  stdcf tang absacc stdacc
  			cash  salecash salerec saleinv pchsaleinv cashdebt realestate
  			z_rvar_ff3 z_dy	z_rvar_capm	z_rvar_mean	z_rdm za_rdm za_adm																						        /* v4 add */
  			secured
  			;
  *this is for those that may have large positive or negative outliers;
  %let hilotrim=beta z_beta ep fgr5yr mom12m mom1m mom6m mom36m indmom sue z_sue agr maxret chfeps bm currat pchcurrat quick pchquick pchdepr sgr chempia acc
  				pchsale_pchinvt pchsale_pchrect pchcapx_ia pchgm_pchsale pchsale_pchxsga mve_ia cfp_ia bm_ia
  				sfe chinv grltnoa cinvest tb cfp lgr egr pricedelay grCAPX chmom roic aeavol
  				chcsho chpmia chatoia grGW ear  rsup z_rsup spi  hire chadv cashpr roaq roe roeq invest  chtx pctacc gma
  				mom60m z_ac	z_bm z_cfp z_ep  /* v4 add */
  			 z_inv z_ni z_op z_hxz_re z_hxz_abr z_hxz_sue
  				za_ac
  				za_inv
  				za_bm
  				za_cfp
  				za_ep
  				za_ni
  				za_op
  				z_mom1m
  				z_mom12m
  				z_mom60m
  				z_mom36m
  				z_mom6m
  				z_moms12m
					z_seas1a
  				za_rsup
  				za_sue
  				za_dy
  					za_invest
  					za_rd_sale
  						za_cash
  					 za_chcsho
  					 za_rd
  					 za_cashdebt
  					 za_pctacc
  					 za_gma
  					 za_lev
  					 za_rd_mve
						 za_rdm
						 za_adm
  					 za_sgr
  					 za_sp
  					 za_ps
  					 za_lgr
  					 za_roa
  					 za_depr
  					 za_egr
  					 za_grltnoa
  					 za_chpm
  					 za_chato
  					 za_chtx
  					 za_ala
  					 za_alm
  					 za_noa
  					 za_rna
  					 za_pm
  					 za_ato
  								z_cash
  								z_chcsho
  								z_rd
  								z_cashdebt
  								z_pctacc
  								z_gma
  								z_lev
  								z_rd_mve
  								z_sgr
  								z_sp
  									z_invest
  									z_rd_sale
  									z_ps
  									z_lgr
  									z_roa
  									z_depr
  									z_egr
  									z_grltnoa
  									z_chpm
  									z_chato
  									z_chtx
  									z_ala
  									z_alm
  									z_noa
  									z_rna
  									z_pm
  									z_ato
  				operprof
  				;
  *Some of these are not continuous, they are dummy variables so they are excluded
  	from the outlier issue:
  	 rd  eamonth IPO  divi divo securedind convind	ltg credrat_dwn woGW sin retcons_pos retcons_neg;
  *----winsorize only positive variables-----;
  proc sort data=temp;
  	 by date;
  run;
  proc means data=temp noprint;
  	by date;
  	var &hitrim;
  	output out=stats p99=/autoname;
  run;
  proc sql;
  	create table temp2
  	as select *
  	from temp a left join stats b
  	on a.date=b.date;
  	quit;
  data temp2;
  	set temp2;
  	array base {*} &hitrim;
  	array high {*} betasq_p99--secured_p99;
  	do i=1 to dim(base);
  		if base(i) ne . and base(i)>(high(i)) then base(i)=(high(i));
  		if high(i)=. then base(i)=.;
  	end;
  	drop _type_ _freq_ betasq_p99--secured_p99;
  	run;
  *winsorize top and bottom of continuous variables;
  proc sort data=temp2;
  	 by date;
  run;
  proc means data=temp2 noprint;
  	by date;
  	var &hilotrim;
  	output out=stats p1= p99=/autoname;
  run;
  proc sql;
  	create table temp2
  	as select *
  	from temp2 a left join stats b
  	on a.date=b.date;
  	quit;
  data temp2;
  	set temp2;
  	array base {*} &hilotrim;
  	array low {*} beta_p1--operprof_p1;
  	array high {*} beta_p99--operprof_p99;
  	do i=1 to dim(base);
  		if base(i) ne . and base(i)<(low(i)) then base(i)=(low(i));
  		if base(i) ne . and base(i)>(high(i)) then base(i)=(high(i));
  		if low(i)=. then base(i)=.;
  	end;
  	drop _type_ _freq_ beta_p1--operprof_p1 beta_p99--operprof_p99;
  	run;
  proc sort data=temp2;
  	 by date;
  run;
  /*
  proc download data=temp2 out=rpsdata_RFS;
  	run;
  */
  data rpsdata_RFS;set temp2;run;
  /* endrsubmit;

  /* add FF industry */
  data rpsdata_RFS; set rpsdata_RFS;
  if sic=0 then sic=.;
  if missing(sic)=0 then %FFI5(sic);
  if missing(sic)=0 then %FFI10(sic);
  if missing(sic)=0 then %FFI12(sic);
  if missing(sic)=0 then %FFI17(sic);
  if missing(sic)=0 then %FFI30(sic);
  if missing(sic)=0 then %FFI38(sic);
  if missing(sic)=0 then %FFI48(sic);
  if missing(sic)=0 then %FFI49(sic);

  *ffi&nind._desc=upcase(ffi&nind._desc);
  ffi5_desc=upcase(ffi5_desc);
  ffi10_desc=upcase(ffi10_desc);
  ffi12_desc=upcase(ffi12_desc);
  ffi17_desc=upcase(ffi17_desc);
  ffi30_desc=upcase(ffi30_desc);
  ffi38_desc=upcase(ffi38_desc);
  ffi48_desc=upcase(ffi48_desc);
  ffi49_desc=upcase(ffi49_desc);

  run;

  *==============================================================================

  I finally download and save the data here,

  	if you are using this program, you need to save to a different location


  ==============================================================================;
  /*    Save data   */


  data chars.firmchars_v7_1_raw;
  	set RPSdata_RFS;
  	run;

  	proc export data=RPSdata_RFS
  	outfile="/scratch/cityuhk/xinhe/eqchars/raw.csv" dbms=csv replace; run;

  	*==============================================================================;
  	/*    format data   */
  	/* variables by sources */
  	%let info = date permno gvkey cnum ret
  	sic FFI5_desc	FFI5	FFI10_desc	FFI10	FFI12_desc	FFI12	FFI17_desc	FFI17
  	FFI30_desc	FFI30	FFI38_desc	FFI38	FFI48_desc	FFI48	FFI49_desc	FFI49
  	;
  	%let vara =
  	za_ac
  	za_inv
  	za_bm
  	za_cfp
  	za_ep
  	za_ni
  	za_op
  	za_rsup
  	za_sue
  	za_dy
  	za_invest
  	za_cash
   za_chcsho
   za_rd
   za_cashdebt
   za_pctacc
   za_gma
   za_lev
   za_rd_mve
	 za_rdm
	 za_adm
   za_sgr
   za_sp
  	za_ps
  	za_lgr
  	realestate
  	za_rd_sale
  		secured
  	za_roa
		roe
  	za_depr
  	za_egr
  	za_grltnoa
  	za_chpm
  	za_chato
  	za_chtx
  	za_ala
  	za_alm
  	za_noa
  	za_rna
  	za_pm
  	za_ato

  				 ;  /* 19 accounting vars + dy + cinvest */

  	%let varq = z_ac z_inv z_bm z_cfp z_ni z_op z_ep  z_rsup  z_sue
  	 z_cash
  	 z_chcsho
  	 z_rd
  	 z_cashdebt
  	 z_pctacc
  	 z_gma
  	 z_lev
  	 z_rd_mve
		 z_rdm
  	 z_sgr
  	 z_sp
  	 z_dy
  				cinvest
  		z_invest
  		z_rd_sale
  		z_ps
  		z_lgr
  		z_roa
			roeq
  		z_depr
  		z_egr
  		z_grltnoa
  		z_chapm
  		z_chato
  		z_chtx
  		z_ala
  		z_alm
  		z_noa
  		z_rna
  		z_pm
  		z_ato
				z_hxz_re
				z_hxz_abr
				z_hxz_sue
  		nincr
  	;   /* 19 accounting + dy */
  	%let varm =
  	disp
  	z_rvar_ff3
  	z_rvar_capm
    z_rvar_mean
  	z_beta
  	z_mom1m
  	z_mom12m
  	z_mom60m
  	z_mom36m
  	z_mom6m
  	z_moms12m
		z_seas1a
  	baspread
  	mcap_crsp
  	retvol
  	ill
  	pricedelay
  	dolvol
  	std_dolvol
  	turn
  	hire
  	maxret
  	zerotrade
  	std_turn
  		bm_ia
  		chatoia
  		chpmia
  		mve_ia
  		herf
  		indmom
  	;

  	/* final output name */
  	%let var  = ill me bm cfp dy ep lev sgr sp acc agr ni gma op mom12m mom1m mom60m
  	rsup sue bas beta rvar_ff3 rvar_capm svar hire rd_mve
  	cash pricedelay chcsho disp	indmom mve_ia rd turn herf dolvol std_dolvol
  	cashdebt pctacc cinvest
  	maxret invest zerotrade realestate std_turn rd_sale bm_ia chatoia chpmia secured ps lgr roa
  	tb depr egr grltnoa chato chpm chtx nincr mom6m mom36m moms12m ala alm noa rna pm ato
		rdm adm hxz_abr hxz_sue hxz_re seas1a roe
  	;

  	/* if quarterly varq is missing, fill in annual vara */
  	/* if z_dy is missing, fill in annual dy */
  	data check_sample; set RPSdata_RFS;
  	keep &info &vara &varq &varm;
  	run;

  	proc export data=check_sample(where=("01JAN2018"d<=date<="31JAN2018"d))
  	outfile="/scratch/cityuhk/xinhe/eqchars/raw2018.csv" dbms=csv replace; run;

    data check_sample; set check_sample;
  	/* acc */
		f_acc = z_ac;

  	/* agr */
  	f_agr = z_inv;

  	/* book to market */
		f_bm = z_bm;

  	/* cfp */

		f_cfp = z_cfp;
  	/* ni */
		f_ni = z_ni;

  	/* earnings to price */

		f_ep = z_ep;
  	/* operating profit*/

		f_op = z_op;
  	/* dividend yield */

		f_dy = z_dy;
  	/* sue */

		f_sue = z_sue;
  	/* rsup */

		f_rsup = z_rsup;
  	/*gma*/

		f_gma = z_gma;
  	/*lev*/

		f_lev = z_lev;
  	/*rd_mve*/

		f_rd_mve = z_rd_mve;
		/*rdm*/

		f_rdm = z_rdm;
		/* adm */

		f_adm = za_adm; /* no quarterly variable available*/
  	/*sgr*/

		f_sgr = z_sgr;
  	/*sp*/

		f_sp = z_sp;
  	/*cash*/

		f_cash = z_cash;
  	/*chcsho*/

		f_chcsho = z_chcsho;
  	/*rd*/

		f_rd = z_rd;
  	/*cashdebt*/

		f_cashdebt = z_cashdebt;
  	/*pctacc*/

		f_pctacc = z_pctacc;
  	/*invest*/

		f_invest = z_invest;
  	/*rd_sale*/

		f_rd_sale = z_rd_sale;
  	/*ps*/

		f_ps = z_ps;
  	/*lgr*/

		f_lgr = z_lgr;
  	/*roa*/

		f_roa = z_roa;
		/* roe */

		f_roe = roeq;
  	/*depr*/

		f_depr = z_depr;
  	/*egr*/

		f_egr = z_egr;
  	/*grltnoa*/

		f_grltnoa = z_grltnoa;
  	/**/
  	/*chpm*/

		f_chpm = z_chpm;
  	/*chato*/

		f_chato = z_chato;
  	/*chtx*/

		f_chtx = z_chtx;
  	/*ala*/

		f_ala = z_ala;
  	/*alm*/

		f_alm = z_alm;
  	/*noa*/

		f_noa = z_noa;
  	/*rna*/

		f_rna = z_rna;
  	/*pm*/

		f_pm = z_pm;
  	/*ato*/

		f_ato = z_ato;

  	run;





  	data check_sample;set check_sample;
  	drop z_ac za_AC
  			 z_inv za_inv
  			 z_bm za_BM
  			 z_cfp za_cfp
  			 z_ep za_EP
  			 z_ni za_ni
  			 z_op za_OP
  			 z_dy za_dy
  			 z_sue za_sue
  			 z_rsup za_rsup
  			 z_cash za_cash
  			 z_chcsho za_chcsho
  			 z_rd za_rd
  			 z_cashdebt za_cashdebt
  			 z_pctacc za_pctacc
  			 z_gma za_gma
  			 z_lev za_lev
  			 z_rd_mve za_rd_mve
  			 z_sgr za_sgr
  			 z_sp za_sp
  			 z_invest za_invest
  			 z_rd_sale za_rd_sale
  			 z_ps za_ps
  			 z_lgr za_lgr
  			 z_roa za_roa
  			 z_depr za_depr
  			 z_egr za_egr
  			 z_grltnoa za_grltnoa
  			 z_chpm za_chpm
  			 z_chato za_chato
  			 z_chtx za_chtx
  			 z_ala za_ala
  			 z_alm za_alm
  			 z_noa za_noa
  			 z_rna za_rna
  			 z_pm za_pm
  			 z_ato za_ato
  			 ;
  	run;

  	data check_sample;set check_sample;
  	acc = f_acc;
  	agr = f_agr;
  	bm = f_bm;
  	cfp = f_cfp;
  	ep = f_ep;
  	op = f_op;
  	ni = f_ni;
  	dy = f_dy;
  	sue = f_sue;
  	rsup = f_rsup;
  	cash = f_cash;
  	chcsho = f_chcsho;
  	rd = f_rd;
  	cashdebt = f_cashdebt;
  	pctacc = f_pctacc;
  	gma = f_gma;
  	lev = f_lev;
  	rd_mve = f_rd_mve;
		rdm = f_rdm;
		adm = f_adm;
  	sgr = f_sgr;
  	sp = f_sp;
  	invest = f_invest;
  	rd_sale = f_rd_sale;
  	ps = f_ps;
  	lgr = f_lgr;
  	roa = f_roa;
		roe = f_roe;
  	depr = f_depr;
  	egr = f_egr;
  	grltnoa = f_grltnoa;
    chpm = f_chpm;
    chato = f_chato;
    chtx = f_chtx;
  	ala = f_ala;
  	alm=f_alm;
  	noa=f_noa;
  	rna=f_rna;
  	pm=f_pm;
  	ato=f_ato;

  	/* rename other variables */
  	me = mcap_crsp;
  	bas = baspread;
  	rvar_ff3 = z_rvar_ff3;
  	rvar_capm = z_rvar_capm;
  	svar = z_rvar_mean;
  	beta = z_beta;
  	mom1m = z_mom1m;
  	mom12m = z_mom12m;
  	mom60m = z_mom60m;
  	mom36m = z_mom36m;
  	mom6m = z_mom6m;
  	moms12m = z_moms12m;
		seas1a = z_seas1a;
		hxz_re = z_hxz_re;
		hxz_abr = z_hxz_abr;
		hxz_sue = z_hxz_sue;
  	run;

  	data check_sample;set check_sample;
  	keep &info &var;
  	run;

  	data check_sample; set check_sample;
  	cusip = cnum;
  	public_date = intnx('month',date,0,'e');
  	format public_date yymmdd10.;
  	run;

  	data check_sample;set check_sample;
  	drop date cnum;
  	run;

  	data chars.firmchars_v7_1_final;
  		set check_sample;
  	run;

  	proc export data=check_sample
  	outfile="/scratch/cityuhk/xinhe/eqchars/final.csv" dbms=csv replace; run;

  	*==============================================================================;
  	/*    check data   */

  	/* latest data */
  	data check;
  	set check_sample;
  	where "01JAN2018"d<=public_date<="31JAN2018"d;
  	run;

  	proc sort data=check nodupkey; by permno public_date;run;

  	proc export data=check
  	outfile="/scratch/cityuhk/xinhe/eqchars/final2018.csv" dbms=csv replace; run;
