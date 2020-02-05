library(abind)
library(timeDate)
library(xts)

####### Parameter ####
date_start = "1976-01-01"
date_end   = "2018-11-30"

######## Data ########
## zit
load("raw_20zt_port.RData") # 16 zt of 6 times 9 portfolios
Zt0 = chars_port_me
cat("\nDimension of Zt is: ", dim(Zt0), "\n")

Zt = Zt0[(date_start<=dimnames(Zt0)[[1]])&(dimnames(Zt0)[[1]]<=date_end),,]
cat("\nDimension of Zt is: ", dim(Zt), "\n")

dimnames(Zt)[[3]]
Zt = Zt[,,c(
  "mom12m_Mean","hxz_abr_Mean","hxz_sue_Mean","hxz_re_Mean",
  "bm_Mean","ep_Mean","cfp_Mean","sp_Mean",
  "agr_Mean","ni_Mean","acc_Mean","op_Mean",     
  "roe_Mean","seas1a_Mean","adm_Mean","rdm_Mean",    
  "me_Mean","svar_Mean","beta_Mean","mom1m_Mean"
)]


### Rit
Rt = ret_port_me
Rt = Rt[(date_start<=dimnames(Zt0)[[1]])&(dimnames(Zt0)[[1]]<=date_end),]

# Xt
XTS = read.csv("xt_1963_2018_v1.csv", head = TRUE)
XTS$date = transform(XTS$dateff, time=as.Date(as.character(XTS$dateff), "%Y%m%d"))$time
# ltRevDaily$X = transform(ltRevDaily$X, time=as.Date(as.character(ltRevDaily$X), "%Y%m%d"))$time
XTS = XTS[(date_start<=XTS$date)&(XTS$date<=date_end),]
Xt = XTS[,c("mktrf","tbl","ntis","infl","svar","dfy")]
# risk free rate
rf = XTS[,c("rf")]

# dimensions
N = dim(Rt)[2] # number of assets
K = dim(Xt)[2] # number of X
P = dim(Zt)[3] # number of Z
T = dim(Rt)[1] # time length

# Excess return
# Rit - rf_{t-1}

Rt = Rt - rf

# # lag Xt, Zt
# Rt = Rt[2:T, ]
# Xt = Xt[1:(T-1), ]
# Zt = Zt[1:(T-1), ,]

# rm(list=setdiff(ls(), c("Rt","Xt","Zt")))

########### OLS ################
predictor_name_list = dimnames(Zt)[[3]]
tstat <- matrix(NA,N,P)
r2 <- matrix(NA,N,P)

for (predictor_index in c(1:P)){
  
  cat('the predictor is ',predictor_name_list[predictor_index],'\n')
  
  for (asset_index in c(1:N)){
    
    y = Rt[,asset_index] 
    x = Zt[,asset_index,predictor_index]
    
    ols <-  lm(y~x)
    tstat[asset_index,predictor_index] <- summary(ols)$coefficients[2,3]
    r2[asset_index,predictor_index] <- summary(ols)$r.squared*100
    
  }
}

t_list = colMeans(tstat)
r2_list = colMeans(r2)

########### Report ################
pdf(file=paste("report_10me_raw",date_start,date_end,".pdf", sep=""))
par(mfrow=c(2,2))

hist(t_list, main='Histogram of t-stat', xlim = c(-3,3), breaks=10)

hist(r2_list, main='Histogram of R2(%)', xlim = c(0,5))

barplot(t_list, main='Barplot of t-stat')

barplot(r2_list, main='Barplot of R2(%)')
dev.off()
