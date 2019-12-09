# EquityCharacteristicsSAS
Calculate US equity (portfolio) characteristics.

The main file is in SAS. 

To use this preoject, you need access to WRDS (CRSP, COMPUSTAT, IBES). As for compusting system, I use WRDS cloud server.

### Details

##### chars folder

This folder is to calculate equity characteristics in individual level.

The most important part is **accounting**, which is modified from Green Han Zhang 2017 RFS code. Then, **rvar_mean** is for stock total variance, **rvar_capm** is residual variance based on CAPM, **rvar_ff3** is residual variance based on Fama French three-factor model, **beta** is for CAPM beta, **abr sue re** is from Hou Xue Zhang's Replicating Anormalies.

In **combine**, I combine the tables. In **output**, I do winsorization. In **rank**, I give a standardized version of the variables in uniform distribution.

You may run the sas files in the following order:

1. **parallel**: accounting, rvar_mean, rvar_capm, rvar_ff3, beta, abr, sue, re
2. combine
3. output
4. rank

##### sortport

This folder is to calculate equity characteristics in portfolio level.

We provide 3 kinds of portfolios:

1. Fama French Industry Classification
2. Sorted portfolio (2x3 1x10 5x5)
3. DGTW benchmark portfolio

We assign the portfolio labels to each equity in each month, then calculate the portfolio characteristics as the value-weighted (equal-weight) mean (median) of the underlying equities.

### Reference

**Dissecting Anomalies with a Five-Factor Model** by [Fama and French 2015 RFS](https://doi.org/10.1093/rfs/hhv043)

**The Characteristics that Provide Independent Information about Average U.S. Monthly Stock Returns** by [Green Hand Zhang 2017 RFS](https://doi.org/10.1093/rfs/hhx019)

**Replicating Anormalies** by [Hou Xue Zhang 2018 RFS](https://doi.org/10.1093/rfs/hhy131)



### Related

A python version is [here](https://feng-cityuhk.github.io/EquityCharacteristics/)

