rm(list=ls(all=T))
library(plyr)
library(rJava)
library(readr)
library(tseries)
library(xlsx)
library(RODBC)
library(stats)
library(MASS)
library(DMwR)
library(corrplot)
library(lubridate)
setwd("C:/Users/robapp/Desktop/allocazioni2019")
############################CONNESSIONI
conn <- odbcConnect("DB_MISURE_64", uid="MISURE", pwd="fj84299jfi",believeNRows=FALSE,rows_at_time=1024)
conn1 <- odbcConnect("DB_MISURE_64", uid="MISURE_GAS", pwd="hYui42oPvf",believeNRows=FALSE,rows_at_time=1024)
sqlQuery(conn, "ALTER SESSION SET NLS_DATE_FOrMAT = 'YYYY-MM-DD'")

#PARAMETRI
citta=c("Rimini","Popi1","Popi2","porto_sge","Fermo","porto_slg","Recanati","Riccione","Osimo", "Morciano")
a=c(0.01,0.05,0.1)
areaclim=c(600,593,590,485,590,598,424,560,569)
#mesi=matrix(c(9,10,11,              10,11,12,              11,12,1,              12,1,2,              1,2,3,              2,3,4,              3,4,5),7,3,byrow=T)
mesi=c(10,11,12,1,2,3,4)

#vol=sqlQuery(conn1,"
#select PDR, GROUP1, ANNO, MESE from( 
#             select pdr, group1, anno,mese , dense_rank() 
#           over (partition by group1, pdr order by  anno desc,mese desc) as rank 
#             from gas ) where rank=1
#            ", as.is=T,rows_at_time=1024)
vol=read.csv("C:/Users/robapp/Desktop/allocazioni2019/clienti/Merge/remi_cons_perprevisione.csv", sep=";", header=T)
vol$AREA_CLIMATICA_ID[vol$REMI=='34636100']=600
vol$AREA_CLIMATICA_ID[vol$REMI=='34638500']=424
vol$AREA_CLIMATICA_ID[vol$REMI=='34650300']=569
vol$ANNO=as.numeric(vol$ANNO)+2000
vol$MESE=as.numeric(vol$MESE)

qpst=paste0("select GIORNO_GAS as Giorno,
nome_comune,
            AREA_CLIMATICA_ID,
            T_EFF_MIN*1.8+32 AS T_EFF_MIN_FAHR,
            T_EFF_AVG*1.8+32 AS T_EFF_AVG_FAHR,
            T_EFF_MAX*1.8+32 AS T_EFF_MAX_FAHR,
            PREC_EFF_AVG AS PREC_EFF_AVG,
            PREC_EFF_MAX AS PREC_EFF_MAX,
            RAD_EFF_MIN AS RAD_EFF_MIN,
            RAD_EFF_AVG AS RAD_EFF_AVG,
            RAD_EFF_MAX AS RAD_EFF_MAX,
            FESTIVITA
            from 
            temperature_giornaliere a
            inner join (
            select
            DT,           
            IS_FESTIVO(DT) as FESTIVITA
            from 
            (
            SELECT (date'2025-01-01' - rOWNUM) dt
            FrOM DUAL CONNECT BY ROWNUM < 5115
            )
            ) b on b.dt=a.giorno_gas
            WHERE   area_climatica_id in (600,593,590,485,590,598,424,560,569)")
            
temp=sqlQuery(conn, qpst)
temp1=data.frame(temp)
temp1$GIORNO=as.Date(temp1$GIORNO, "%Y-%m-%d",tz="CET")
#temp1$T_AVG_FAHR=as.numeric(temp1$T_AVG_FAHR)
#temp1$PREC_AVG=as.numeric(temp1$PREC_AVG)          

esito=data.frame(mese=numeric(),citta=character(),alfa=numeric(),forecast=numeric(), stringsAsFactors=F)           
risultati=data.frame(mese=numeric(),
          					 citta=character(),
          					 alfa=numeric(),
                     forecast=numeric(),
                     PREV_T_EFF_AVG_FAHR=numeric(),
                     PREV_T_EFF_MIN_FAHR=numeric(),
                     PREV_T_EFF_MAX_FAHR=numeric(),
                     PREV_PREC_EFF_AVG=numeric(),
                     PREV_PREC_EFF_MAX=numeric(),
                     PREV_RAD_EFF_AVG=numeric(),
                     PREV_RAD_EFF_MAX=numeric(),
          					 PREV_N_PDR=numeric(),
          					 PREV_C1=numeric(),
          					 PREV_C2=numeric(),
          					 PREV_C3=numeric(),
          					 PREV_C4=numeric(),
          					 PREV_C5=numeric(),
          					 PREV_T1=numeric(),
          					 PREV_T2=numeric(),
                     intercetta=numeric(),
                     T_EFF_AVG_FAHR=numeric(),
                     T_EFF_MIN_FAHR=numeric(),
                     T_EFF_MAX_FAHR=numeric(),
                     PREC_EFF_AVG=numeric(),
                     PREC_EFF_MAX=numeric(),
                     RAD_EFF_AVG=numeric(),
                     RAD_EFF_MAX=numeric(),
                     lu=numeric(), 
                     ma=numeric(), 
                     me=numeric(),
                     gi=numeric(), 
                     ve=numeric(),
                     sa=numeric() ,
                     FESTIVITA=numeric(),
          					 N_PDR=numeric(),
          					 C1=numeric(),
          					 C2=numeric(),
          					 C3=numeric(),
          					 C4=numeric(),
          					 C5=numeric(),
          					 T1=numeric(),
          					 T2=numeric(),
          					 rsquar=numeric(),stringsAsFactors = F)

l=0

for(m in 1:  length(mesi)){ 
  for (r in 1 : length(a)){ 
    for (s in 1 :  length(citta)){ 
    l=l+1
    df=paste0(getwd(),"/",citta[s],".csv")
    dataset<- read.csv2(df,header=T,sep=";",dec=",")
    names(dataset)=toupper(names(dataset))
    dataset$GIORNO=as.Date(dataset$GIORNO, "%d/%m/%Y", tz="CET")
    dataset$MESE=month(dataset$GIORNO)
    dataset$DAY=wday(dataset$GIORNO)
    dataset$ANNO=year(dataset$GIORNO)
    #dataset[,ncol(dataset)+1]=toupper(citta[s])
    #colnames(dataset)[ncol(dataset)]="AREA_CLIMATICA_ID"
    dataset$CONSUMO.M3=as.numeric(gsub(",",".",dataset$CONSUMO.M3),length=2)
    #colnames(data)[length(colnames(data))]="AREA_CLIMATICA_ID"
 
       
###############################################################
dfm=merge(x=dataset, y=temp1, by.x=c("ID", "GIORNO"),by.y=c("AREA_CLIMATICA_ID", "GIORNO"))
dfm0=na.omit(dfm)    
dfm1=merge(dfm0, vol, by.x=c("MESE","ANNO","ID"), by.y=c("MESE","ANNO","AREA_CLIMATICA_ID"), all.x=T)
#dfm1[is.na(dfm1)]=0
  if (nrow(dfm1[is.na(dfm1[,c("N_PDR")]),])==nrow(dfm1)) {dfm1[is.na(dfm1)]=0} else
   { dfm1=na.omit(dfm1)    }

#mean=sapply( dfm[,c("Consumo.m3","T_EFF_MIN_FAHR","T_EFF_AVG_FAHR","T_EFF_MAX_FAHR","PREC_EFF_AVG","PREC_EFF_MAX","RAD_EFF_AVG")],mean)
#sd=sapply( dfm[,c("Consumo.m3","T_EFF_MIN_FAHR","T_EFF_AVG_FAHR","T_EFF_MAX_FAHR","PREC_EFF_AVG","PREC_EFF_MAX","RAD_EFF_AVG")],sd)


#dfm[,c("Consumo.m3","T_EFF_MIN_FAHR","T_EFF_AVG_FAHR","T_EFF_MAX_FAHR","PREC_EFF_MIN","PREC_EFF_AVG","PREC_EFF_MAX","RAD_EFF_AVG")]= 
#scale(dfm[,c("Consumo.m3","T_EFF_MIN_FAHR","T_EFF_AVG_FAHR","T_EFF_MAX_FAHR","PREC_EFF_MIN","PREC_EFF_AVG","PREC_EFF_MAX","RAD_EFF_AVG")], center=T,scale=T)

dfm1$lu=ifelse(weekdays(dfm1$GIORNO)=="luned?",1,0)
dfm1$ma=ifelse(weekdays(dfm1$GIORNO)=="marted?",1,0)
dfm1$me=ifelse(weekdays(dfm1$GIORNO)=="mercoled?",1,0)
dfm1$gi=ifelse(weekdays(dfm1$GIORNO)=="gioved?",1,0)
dfm1$ve=ifelse(weekdays(dfm1$GIORNO)=="venerd?",1,0)
dfm1$sa=ifelse(weekdays(dfm1$GIORNO)=="sabato",1,0)

dfm1$gen=ifelse(months(dfm1$GIORNO)=="gennaio" ,1,0)
dfm1$feb=ifelse(months(dfm1$GIORNO)=="febbraio",1,0)
dfm1$dic=ifelse(months(dfm1$GIORNO)=="dicembre",1,0)
dfm1$mar=ifelse(months(dfm1$GIORNO)=="marzo" ,1,0)
#dfm1$apr=ifelse(months(dfm1$GIORNO)=="aprile",1,0)
#dfm1$mag=ifelse(months(dfm1$GIORNO)=="maggio",1,0)
#dfm1$giu=ifelse(months(dfm1$GIORNO)=="giugno",1,0)
#dfm1$lug=ifelse(months(dfm1$GIORNO)=="luglio",1,0)
#dfm1$ago=ifelse(months(dfm1$GIORNO)=="agosto",1,0)
#dfm1$set=ifelse(months(dfm1$GIORNO)=="settembre",1,0)
dfm1$ott=ifelse(months(dfm1$GIORNO)=="ottobre" ,1,0)
dfm1$nov=ifelse(months(dfm1$GIORNO)=="novembre",1,0)
#CALCOLO MAX E MIN PER OGNI COLONNA CHE USERO PER STANDARDIZZARE I DATI

#dfm$elast=log(dfm$Consumo.m3[-1]/dfm$Consumo.m3[-length(dfm$Consumo.m3)])/log((dfm$T_AVG_FAHR)[-1]/dfm$T_AVG_FAHR[-length(dfm$T_AVG_FAHR)])

#DATA WORKING
  if((mesi[m]==12 | mesi[m]==1 | mesi[m]==2)==T ) 
  {dfm6=dfm1[dfm1$MESE==12 | dfm1$MESE==1 | dfm1$MESE==2,]}   else if 

  ((mesi[m]==3 | mesi[m]==11)==T) 
    {dfm6=dfm1[dfm1$MESE==3 | dfm1$MESE==11 ,]}  else if 
      
  ((mesi[m]==10 | mesi[m]==4  | mesi[m]==5)==T)
    {dfm6=dfm1[dfm1$MESE==10 | dfm1$MESE==4 ,]}


dfm6=dfm1

#FIRST REGRESSION
model=lm(CONSUMO.M3 ~ T_EFF_MIN_FAHR+ T_EFF_AVG_FAHR+T_EFF_MAX_FAHR+ 
            PREC_EFF_AVG + PREC_EFF_MAX +  RAD_EFF_AVG + RAD_EFF_MAX+ 
           lu + ma + me+ gi+ ve+ sa  + FESTIVITA + C1 +C2+C3+C4+C5+T1+T2+N_PDR, data=dfm6 )


#model=lm(CONSUMO.M3 ~ T_EFF_MIN_FAHR+ T_EFF_AVG_FAHR+T_EFF_MAX_FAHR+ 
 #          PREC_EFF_AVG + PREC_EFF_MAX +  RAD_EFF_AVG + RAD_EFF_MAX+ 
  #         lu + ma + me+ gi+ ve+ sa + FESTIVITA + C1 +C2+C3+C4+C5+T1+T2+N_PDR, data=dfm6 )

#model=lm(Consumo.m3 ~ T_EFF_MIN_FAHR+ T_EFF_AVG_FAHR+T_EFF_MAX_FAHR+ 
   #        PREC_EFF_AVG + PREC_EFF_MAX +  RAD_EFF_AVG + RAD_EFF_MAX+ 
   #        lu + ma + me+ gi+ ve+ sa  + gen+feb+mar+apr+dic+nov+ott+ FESTIVITA, data=dfm6 )

#model=lm(Consumo.m3 ~ T_EFF_MIN_FAHR+ T_EFF_AVG_FAHR+T_EFF_MAX_FAHR+ 
 #   PREC_EFF_MIN + PREC_EFF_AVG + PREC_EFF_MAX + RAD_EFF_MIN+ RAD_EFF_AVG +RAD_EFF_MAX+
   #     lu + ma + me+ gi+ ve+ sa  + FESTIVITA, data=dfm )
AIC1=stepAIC(model, direction="backward")
summary(AIC1)
hist(AIC1$residuals)
jarque.bera.test(AIC1$residual)
plot(AIC1$residuals)

plot(dfm1$CONSUMO.M3)
lines(AIC1$fitted.values,col="red")


tprev=quantile(dfm1$T_EFF_AVG_FAHR[dfm1$MESE==mesi[m]] ,a[r])
tprevmin=quantile(dfm1$T_EFF_MIN_FAHR[dfm1$MESE==mesi[m]] ,a[r])
tprevmax=quantile(dfm1$T_EFF_MAX_FAHR[dfm1$MESE==mesi[m]] ,a[r])

prprev=quantile(dfm1$PREC_EFF_AVG[dfm1$MESE==mesi[m]] ,1-a[r])
#prmin=quantile(dfm$PREC_EFF_MIN[dfm$Mese==mesi[m]] ,1-a[r])
prmax=quantile(dfm1$PREC_EFF_MAX[dfm1$MESE==mesi[m]] ,1-a[r])

radprev=quantile(dfm1$RAD_EFF_AVG[dfm1$MESE==mesi[m]] ,a[r])
radmax=quantile(dfm1$RAD_EFF_MAX[dfm1$MESE==mesi[m]] ,a[r])


N_PDRprev=quantile(dfm1$N_PDR[dfm1$MESE==mesi[m]] ,a[r])
C1prev=quantile(dfm1$C1[dfm1$MESE==mesi[m]] ,a[r])
C2prev=quantile(dfm1$C2[dfm1$MESE==mesi[m]] ,a[r])
C3prev=quantile(dfm1$C3[dfm1$MESE==mesi[m]] ,a[r])
C4prev=quantile(dfm1$C4[dfm1$MESE==mesi[m]] ,a[r])
C5prev=quantile(dfm1$C5[dfm1$MESE==mesi[m]] ,a[r])
T1prev=quantile(dfm1$T1[dfm1$MESE==mesi[m]] ,a[r])
T2prev=quantile(dfm1$T2[dfm1$MESE==mesi[m]] ,a[r])
#elaprev=quantile(dfm$elast[dfm$Mese==mesi[m]] ,a[r])


#matmaxcoef=match(max(AIC1$coefficients[c("lu","ma","me","gi","ve","sa")]),AIC1$coefficients)
#maxdummy=names(coef(AIC1)[matmaxcoef])
#maxcoef=max(AIC1$coefficients[c("lu","ma","me","gi","ve")])


#nwdata=data.frame(T_EFF_AVG_FAHR=tprev,
 #                 T_EFF_MIN_FAHR=tprevmin,
 #                 T_EFF_MAX_FAHR=tprevmax,
  #########                PREC_EFF_AVG=prprev,
   ########               PREC_EFF_MIN=prmin,
    #######              PREC_EFF_MAX=prmax,
     ######             RAD_EFF_AVG=radprev,
      #####            #RAD_EFF_MIN=radmin,
       ####           #RAD_EFF_MAX=radmax,
        ###          FESTIVITA=0,
         ##         lu=ifelse(maxdummy=="lu",1,0),
          #        ma=ifelse(maxdummy=="ma",1,0),
          #        me=ifelse(maxdummy=="me",1,0),
          #        gi=ifelse(maxdummy=="gi",1,0),
          #        ve=ifelse(maxdummy=="ve",1,0),
          #        sa=ifelse(maxdummy=="sa",1,0),
          #        gen=ifelse(mesi[m]==1,1,0),
          #        feb=ifelse(mesi[m]==2,1,0),
          #        mar=ifelse(mesi[m]==3,1,0),
          #        apr=ifelse(mesi[m]==4,1,0),
          #        ott=ifelse(mesi[m]==10,1,0),
          #        no\v=ifelse(mesi[m]==11,1,0),
          #        dic=ifelse(mesi[m]==12,1,0))

nwdata=data.frame(T_EFF_AVG_FAHR=tprev,
                           T_EFF_MIN_FAHR=tprevmin,
                           T_EFF_MAX_FAHR=tprevmax,
                                PREC_EFF_AVG=prprev,
                                PREC_EFF_MAX=prmax,
                         RAD_EFF_AVG=radprev,
                  RAD_EFF_MAX=radmax,
                        FESTIVITA=0,
                     lu=1,
                  ma=1,
                  me=1,
                  gi=1,
                  ve=1,
                  sa=1,
                  N_PDR=N_PDRprev,
                  C1=C1prev,
                  C2=C2prev,
                  C3=C3prev,
                  C4=C4prev,
                  C5=C5prev,
                  T1=T1prev,
                  T2=T2prev)                


prev=predict(AIC1, nwdata)
#PUT OUTPUT IN DATAFRAME
  risultati[l,]=data.frame(mese=mesi[m],citta=citta[s], alfa=a[r],forecast=prev,T_EFF_AVG_FAHR=tprev,
                         PREV_T_EFF_MIN_FAHR=tprevmin,
                         PREV_T_EFF_MAX_FAHR=tprevmax,
                         PREV_PREC_EFF_AVG=prprev,
                         PREV_PREC_EFF_MAX=prmax,
                         PREV_RAD_EFF_AVG=radprev,
                        PREV_RAD_EFF_MAX=radmax,
                        N_PDR=N_PDRprev,
                        PREV_C1=C1prev,
                         PREV_C2=C2prev,
                         PREV_C3=C3prev,
                         PREV_C4=C4prev,
                         PREV_C5=C5prev,
                         PREV_T1=T1prev,
                        PREV_T2=T2prev,
  intercetta=AIC1$coefficients[names(AIC1$coefficients)=="(Intercept)"],
  if(length(AIC1$coefficients[names(AIC1$coefficients)=="T_EFF_AVG_FAHR"])==0) {T_EFF_AVG_FAH=0} else {T_EFF_AVG_FAHR=AIC1$coefficients[names(AIC1$coefficients)=="T_EFF_AVG_FAHR"]},
  if(length(AIC1$coefficients[names(AIC1$coefficients)=="T_EFF_MIN_FAHR"])==0) {T_EFF_MIN_FAHR=0} else {T_EFF_MIN_FAHR=AIC1$coefficients[names(AIC1$coefficients)=="T_EFF_MIN_FAHR"]},
  if(length(AIC1$coefficients[names(AIC1$coefficients)=="T_EFF_MAX_FAHR"])==0) {T_EFF_MAX_FAHR=0} else {T_EFF_MAX_FAHR=AIC1$coefficients[names(AIC1$coefficients)=="T_EFF_MAX_FAHR"]},
  if(length(AIC1$coefficients[names(AIC1$coefficients)=="PREC_EFF_AVG"])==0) {PREC_EFF_AVG=0} else {PREC_EFF_AVG=AIC1$coefficients[names(AIC1$coefficients)=="PREC_EFF_AVG"]},
  #if(length(AIC1$coefficients[names(AIC1$coefficients)=="PREC_EFF_MIN"])==0) {PREC_EFF_MIN=0} else {PREC_EFF_MIN=AIC1$coefficients[names(AIC1$coefficients)=="PREC_EFF_MIN"]},
  if(length(AIC1$coefficients[names(AIC1$coefficients)=="PREC_EFF_MAX"])==0) {PREC_EFF_MAX=0} else {PREC_EFF_MAX=AIC1$coefficients[names(AIC1$coefficients)=="PREC_EFF_MAX"]},
  if(length(AIC1$coefficients[names(AIC1$coefficients)=="RAD_EFF_AVG"])==0) {RAD_EFF_AVG=0} else {RAD_EFF_AVG=AIC1$coefficients[names(AIC1$coefficients)=="RAD_EFF_AVG"]},
  #if(length(AIC1$coefficients[names(AIC1$coefficients)=="RAD_EFF_MIN"])==0) {RAD_EFF_MIN=0} else {RAD_EFF_MIN=AIC1$coefficients[names(AIC1$coefficients)=="RAD_EFF_MIN"]},
  if(length(AIC1$coefficients[names(AIC1$coefficients)=="RAD_EFF_MAX"])==0) {RAD_EFF_MAX=0} else {RAD_EFF_MAX=AIC1$coefficients[names(AIC1$coefficients)=="RAD_EFF_MAX"]},
  if(length(AIC1$coefficients[names(AIC1$coefficients)=="lu"])==0) {lu=0} else {lu=AIC1$coefficients[names(AIC1$coefficients)=="lu"]},
  if(length(AIC1$coefficients[names(AIC1$coefficients)=="ma"])==0) {ma=0}else {ma=AIC1$coefficients[names(AIC1$coefficients)=="ma"]},
  if(length(AIC1$coefficients[names(AIC1$coefficients)=="me"])==0){me=0}else {me=AIC1$coefficients[names(AIC1$coefficients)=="me"]}, 
  if(length(AIC1$coefficients[names(AIC1$coefficients)=="gi"])==0) {gi=0}else {gi=AIC1$coefficients[names(AIC1$coefficients)=="gi"]},
  if(length(AIC1$coefficients[names(AIC1$coefficients)=="ve"])==0) {ve=0}else {ve=AIC1$coefficients[names(AIC1$coefficients)=="ve"]}, 
  if(length(AIC1$coefficients[names(AIC1$coefficients)=="sa"])==0)  {sa=0}else {sa=AIC1$coefficients[names(AIC1$coefficients)=="sa"]},
  if(length(AIC1$coefficients[names(AIC1$coefficients)=="FESTIVITA"])==0)  {FESTIVITA=0}else {FESTIVITA=AIC1$coefficients[names(AIC1$coefficients)=="FESTIVITA"]},
  if(length(AIC1$coefficients[names(AIC1$coefficients)=="N_PDR"])==0)  {N_PDR=0}else {N_PDR=AIC1$coefficients[names(AIC1$coefficients)=="N_PDR"]},
  if(length(AIC1$coefficients[names(AIC1$coefficients)=="C1"])==0)  {C1=0}else {C1=AIC1$coefficients[names(AIC1$coefficients)=="C1"]},
  if(length(AIC1$coefficients[names(AIC1$coefficients)=="C2"])==0)  {C2=0}else {C2=AIC1$coefficients[names(AIC1$coefficients)=="C2"]},
  if(length(AIC1$coefficients[names(AIC1$coefficients)=="C3"])==0)  {C3=0}else {C3=AIC1$coefficients[names(AIC1$coefficients)=="C3"]},
  if(length(AIC1$coefficients[names(AIC1$coefficients)=="C4"])==0)  {C4=0}else {C4=AIC1$coefficients[names(AIC1$coefficients)=="C4"]},
  if(length(AIC1$coefficients[names(AIC1$coefficients)=="C5"])==0)  {C5=0}else {C5=AIC1$coefficients[names(AIC1$coefficients)=="C5"]},
  if(length(AIC1$coefficients[names(AIC1$coefficients)=="T1"])==0)  {T1=0}else {T1=AIC1$coefficients[names(AIC1$coefficients)=="T1"]},
  if(length(AIC1$coefficients[names(AIC1$coefficients)=="T2"])==0)  {T2=0}else {T2=AIC1$coefficients[names(AIC1$coefficients)=="T2"]},
  #if(length(AIC1$coefficients[names(AIC1$coefficients)=="elast"])==0)  {elast=0}else {elast=AIC1$coefficients[names(AIC1$coefficients)=="elast"]},
  rsquar=summary(AIC1)$r.squared , stringsAsFactors = F)

  #forecast_real=prev*sd[1]+mean[1]
  #esito=data.frame(mese=numeric(), citta=numeric(), forecast=numeric(), temp_min=numeric(), temp_avg=numeric(), temp_max=numeric(),prec_min=numeric(), prec_avg=numeric(), prec_max=numeric(), rad=numeric  )
  

}

}
 }
file=paste0("Risultati_categorie_nodummy",Sys.Date(),".csv")
write.csv2(risultati,file=file)


corr=dfm1[,5:ncol(dfm1)]
corr1=corr[,-3]
corrplot(corr1)
