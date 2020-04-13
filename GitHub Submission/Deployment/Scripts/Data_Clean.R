#1. Importing the data and installing packages
args = commandArgs(trailingOnly=TRUE)

list.of.packages <- c('httr', 'stringi','stringr','readr')
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)

library('httr')
library('stringi')
library('stringr')
library('readr')


#setwd('~/DIA_Project/Input_Data/')

#Kaggle COmpetition Data
MTG <- read.csv(args[1] , stringsAsFactors = TRUE)

#Kaggle Vehicle Loan Data
RL <- read.csv(args[2] , stringsAsFactors = TRUE)



#Check for NAs
NA_Chck <- function(In_Data , exclude = NULL){
  for (i in 1:ncol(In_Data)){
    if (any(is.na(In_Data[,i])) == TRUE & any(exclude == colnames(In_Data)[i]) == FALSE & is.factor(In_Data[,i]) == FALSE) {
    print(colnames(In_Data)[i])
    }
  }
}

NA_Chck(MTG)
NA_Chck(RL)


#2.Cleaning the data
#2.1 Mortgages Data
#2.1.1 the annuity variable

#Regression fill function
NA_LM_Reg_Fill <- function(expl , preds , In_Data){
  idx_na <- is.na(In_Data[expl])
  expl_train <- In_Data[-idx_na,]
  expl_test <- In_Data[idx_na,]
  
  Preds_form <- paste(preds , collapse = '+')
  Full_form <- paste(expl , '~' , Preds_form)
  
  Formula = eval(parse(text = Full_form))

  REG <- lm(formula = Formula , data = expl_train)
  Pred <- predict(REG , newdata = expl_test)
  
  In_Data[idx_na,expl] <- Pred
  Out <- In_Data
  return(Out)
  
}

MTG <- NA_LM_Reg_Fill(expl = 'AMT_ANNUITY'
              ,preds = c('AMT_CREDIT','AMT_INCOME_TOTAL')
              ,In_Data = MTG
               )


#2.1.2 The AMT_GOODS_PRICE variable
MTG <- NA_LM_Reg_Fill(expl = 'AMT_GOODS_PRICE'
                      ,preds = 'AMT_CREDIT'
                      ,In_Data = MTG
)


#2.1.3 The Own Car variable
#Count the blanks
OCA_Idx <- is.na(MTG['OWN_CAR_AGE'])
nrow(MTG[OCA_Idx,])

#Count blanks where Car OWner is Y
OCA_Idx <- is.na(MTG['OWN_CAR_AGE']) & MTG['FLAG_OWN_CAR'] == 'Y'
nrow(MTG[OCA_Idx,])

med <- median(as.matrix(MTG['OWN_CAR_AGE']) , na.rm = TRUE)

#infill with median - Ok because number of blanks is so small
MTG[OCA_Idx,'OWN_CAR_AGE'] <- med

#Check where car age is populated and car owner is N
OCA_Idx <- !is.na(MTG['OWN_CAR_AGE']) & MTG['FLAG_OWN_CAR'] == 'N'
nrow(MTG[OCA_Idx,]) #Ok no cleaning needed!
#Blanks left to accomadate people with no car some binning may be required later.

#Set to factor
MTG$OWN_CAR_AGE <- as.factor(MTG$OWN_CAR_AGE)



#2.1.4 The number of family members
FAM_Idx <- is.na(MTG['CNT_FAM_MEMBERS'])
nrow(MTG[FAM_Idx,])
MTG[FAM_Idx , 'CNT_FAM_MEMBERS'] <- 0 #Fill with 0 because Only Two rows blank


#2.1.5 EXT_SOURCE_1, EXT_SOURCE_2, EXT_SOURCE_3 - scores from external source
#Places where they are all blank
SCR_Idx <- is.na(MTG['EXT_SOURCE_1']) & is.na(MTG['EXT_SOURCE_2']) & is.na(MTG['EXT_SOURCE_3'])
nrow(MTG[SCR_Idx,])

#identifying blanks
SCR_GB <- MTG[c('EXT_SOURCE_1','EXT_SOURCE_2','EXT_SOURCE_3')]
SCR_GB['Ind'] <- NA
SCR_GB['Ind'][is.na(MTG['EXT_SOURCE_1']) & is.na(MTG['EXT_SOURCE_2']) & is.na(MTG['EXT_SOURCE_3']),] <- 'All Blank'
SCR_GB['Ind'][!is.na(MTG['EXT_SOURCE_1']) & is.na(MTG['EXT_SOURCE_2']) & is.na(MTG['EXT_SOURCE_3']),] <- 'EXT_SOURCE 1 populated only'
SCR_GB['Ind'][is.na(MTG['EXT_SOURCE_1']) & !is.na(MTG['EXT_SOURCE_2']) & is.na(MTG['EXT_SOURCE_3']),] <- 'EXT_SOURCE 2 populated only'
SCR_GB['Ind'][is.na(MTG['EXT_SOURCE_1']) & is.na(MTG['EXT_SOURCE_2']) & !is.na(MTG['EXT_SOURCE_3']),] <- 'EXT_SOURCE 3 populated only'
SCR_GB['Ind'][!is.na(MTG['EXT_SOURCE_1']) & !is.na(MTG['EXT_SOURCE_2']) & is.na(MTG['EXT_SOURCE_3']),] <- 'EXT_SOURCE 1 & 2 populated only'
SCR_GB['Ind'][!is.na(MTG['EXT_SOURCE_1']) & is.na(MTG['EXT_SOURCE_2']) & !is.na(MTG['EXT_SOURCE_3']),] <- 'EXT_SOURCE 1 & 3 populated only'
SCR_GB['Ind'][is.na(MTG['EXT_SOURCE_1']) & !is.na(MTG['EXT_SOURCE_2']) & !is.na(MTG['EXT_SOURCE_3']),] <- 'EXT_SOURCE 2 & 3 populated only'
SCR_GB['Ind'][!is.na(MTG['EXT_SOURCE_1']) & !is.na(MTG['EXT_SOURCE_2']) & !is.na(MTG['EXT_SOURCE_3']),] <- 'All Populated'
SCR_GB <- SCR_GB['Ind']
table(SCR_GB$Ind)


#Use other scores to predict eachoter?
cor(MTG[,c('TARGET','EXT_SOURCE_1','EXT_SOURCE_2','EXT_SOURCE_3')] , use = 'complete.obs')

aggregate(MTG[, c('EXT_SOURCE_1','EXT_SOURCE_2','EXT_SOURCE_3')], list(MTG$TARGET), mean ,na.rm=TRUE)

#Use row average
MTG['Avg_Ext_Scre'] <- rowMeans(MTG[ c('EXT_SOURCE_1','EXT_SOURCE_2','EXT_SOURCE_3')] , na.rm=TRUE)
MTG[ c('Avg_Ext_Scre','EXT_SOURCE_1','EXT_SOURCE_2','EXT_SOURCE_3')]

Means <- aggregate(MTG['Avg_Ext_Scre'], list(MTG$TARGET) , mean , na.rm=TRUE)

MTG[is.na(MTG['Avg_Ext_Scre']) & MTG['TARGET'] == 1,'Avg_Ext_Scre'] <- Means[Means['Group.1'] == 1 ,'Avg_Ext_Scre']
MTG[is.na(MTG['Avg_Ext_Scre']) & MTG['TARGET'] == 0,'Avg_Ext_Scre'] <- Means[Means['Group.1'] == 0 ,'Avg_Ext_Scre']

sum(is.na(MTG['Avg_Ext_Scre']))


#2.1.6 Drop redundant columns
drops <- c('EXT_SOURCE_1','EXT_SOURCE_2','EXT_SOURCE_3')
MTG <- MTG[,!(names(MTG) %in% drops)]


#automate the dropping of columns
suffix <- c('_AVG' , '_MEDI' , '_MODE')
suffix_idx <- (str_detect(colnames(MTG) , '_AVG') | str_detect(colnames(MTG) , '_MEDI') | str_detect(colnames(MTG) , '_MODE')) & sapply(MTG, class) == 'numeric'
Vars <- str(colnames(MTG)[str_detect(colnames(MTG) , '_AVG')])

for (i in Vars){
  i_alt <- str_remove(i , '_AVG')
  Col_Idx <- (colnames(MTG) == paste(i_alt,'_AVG',sep ='') | colnames(MTG) == paste(i_alt,'_MEDI',sep ='') | colnames(MTG) == paste(i_alt,'_MODE',sep ='')) & sapply(MTG, class) == 'numeric'
  print(cor(MTG[,Col_Idx] , use = 'complete.obs'))
}

#Drop the columns
Drop_Idx <- (str_detect(colnames(MTG) , '_MEDI') | str_detect(colnames(MTG) , '_MODE')) & sapply(MTG, class) == 'numeric'
MTG <- MTG[,!Drop_Idx]


#2.1.7 "Social Circle" stuff -  replace with 0s
MTG[is.na(MTG['OBS_30_CNT_SOCIAL_CIRCLE']),'OBS_30_CNT_SOCIAL_CIRCLE'] <- 0
MTG[is.na(MTG['DEF_30_CNT_SOCIAL_CIRCLE']),'DEF_30_CNT_SOCIAL_CIRCLE'] <- 0
MTG[is.na(MTG['OBS_60_CNT_SOCIAL_CIRCLE']),'OBS_60_CNT_SOCIAL_CIRCLE'] <- 0
MTG[is.na(MTG['DEF_60_CNT_SOCIAL_CIRCLE']),'DEF_60_CNT_SOCIAL_CIRCLE'] <- 0


#2.1.8 Days since last phone change
sum(is.na(MTG['DAYS_LAST_PHONE_CHANGE']))
MTG[is.na(MTG['DAYS_LAST_PHONE_CHANGE']) , 'DAYS_LAST_PHONE_CHANGE'] <- median(as.matrix(MTG['DAYS_LAST_PHONE_CHANGE']) , na.rm = TRUE)


#2.1.9 AMT_REQ_CREDIT variables
sum(is.na(MTG['AMT_REQ_CREDIT_BUREAU_HOUR']))
sum(is.na(MTG['AMT_REQ_CREDIT_BUREAU_DAY']))
sum(is.na(MTG['AMT_REQ_CREDIT_BUREAU_WEEK']))
sum(is.na(MTG['AMT_REQ_CREDIT_BUREAU_MON']))
sum(is.na(MTG['AMT_REQ_CREDIT_BUREAU_QRT']))
sum(is.na(MTG['AMT_REQ_CREDIT_BUREAU_YEAR']))

#Put as factor
MTG$AMT_REQ_CREDIT_BUREAU_HOUR <- as.factor(MTG$AMT_REQ_CREDIT_BUREAU_HOUR)
MTG$AMT_REQ_CREDIT_BUREAU_DAY <- as.factor(MTG$AMT_REQ_CREDIT_BUREAU_DAY)
MTG$AMT_REQ_CREDIT_BUREAU_WEEK <- as.factor(MTG$AMT_REQ_CREDIT_BUREAU_WEEK)
MTG$AMT_REQ_CREDIT_BUREAU_MON <- as.factor(MTG$AMT_REQ_CREDIT_BUREAU_MON)
MTG$AMT_REQ_CREDIT_BUREAU_QRT <- as.factor(MTG$AMT_REQ_CREDIT_BUREAU_QRT)
MTG$AMT_REQ_CREDIT_BUREAU_YEAR <- as.factor(MTG$AMT_REQ_CREDIT_BUREAU_YEAR)


#2.1.10 NA check before binning
NA_Excl_Idx <- colnames(MTG)[str_detect(colnames(MTG) , '_AVG') & sapply(MTG, class) == 'numeric']
NA_Chck(MTG, exclude = NA_Excl_Idx)

#2.1.11 Check for duplicates
sum(duplicated(MTG['SK_ID_CURR'])) #No Dups



#2.2 the Vehicle Loans Dataset
#2.2.1 Check for blanks
NA_Chck(RL) #No Blank numeric variables

colnames(RL) <- lapply(colnames(RL) , toupper)
colnames(RL) <- str_replace_all(colnames(RL) , '\\.' , '_')

#2.2.2 Changing to date functions
RL$DATE_OF_BIRTH <- as.Date(RL$DATE_OF_BIRTH , format="%d-%m-%y")
RL$DISBURSAL_DATE <- as.Date(RL$DISBURSALDATE , format="%d-%m-%y")


#2.2.3 Yrs columns, Character to Numeric
Yrs_Char2Num <- function(In_Col){
  In_Col_alt <- str_remove(In_Col , 'yrs')
  In_Col_alt <- str_remove(In_Col_alt , 'mon')

  Mat <- str_split_fixed(In_Col_alt, " ", 2)
  Mat <- apply(Mat , 2, as.numeric)

  Out <- Mat[,1] + Mat[,2]/12
  return(Out)
  }

RL$AVERAGE_ACCT_AGE <- Yrs_Char2Num(RL$AVERAGE_ACCT_AGE)
RL$CREDIT_HISTORY_LENGTH <- Yrs_Char2Num(RL$CREDIT_HISTORY_LENGTH)

#2.2.4 Check ID for Duplicates
sum(duplicated(RL['UNIQUEID'])) #No Dups



#MTG <- Miss_Fill(MTG)
#RL <- Miss_Fill(RL)


#3. Writing the CSVs
write.csv(MTG , paste(args[3],'Home_Credit_Defaults.csv',sep='/') , row.names = FALSE)
write.csv(RL , paste(args[3],'Vehicle_Loan_Defaults.csv',sep='/') , row.names = FALSE)

