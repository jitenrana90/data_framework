import psycopg2
import pgdb
from pyathenajdbc import connect
from datetime import datetime, timedelta
import pandas as pd
from statsmodels.tsa.arima_model import ARIMA
import statsmodels
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
import numpy as np
import sys
import os, glob, time
from itertools import chain
import matplotlib.pyplot as plt
%matplotlib inline
plt.rcParams['figure.figsize'] = [15,10]

## This function calculates the clearing price
## Based on highest 2 bids and floor price
## It also sets a flag for various scenarios
def second_auction(gp, sp, fp):
    cp = 0
    flag = 0
    if sp == None:
        sp = 0
    if fp < 0:
        fp = 0.0
    if gp < fp:
        cp = 0
        flag = 0
    else:
        if sp >= fp:
            cp = sp
            flag = 1
        else:
            cp = fp
            flag = 2
    margin = gp - cp
    return {'cp':cp, 'margin':margin, 'flag':flag, 'cp_old':sp}
    


## The Yield calculator function calculates follwing metrics on a dataframe.
## It conducts 2nd price auction on each row of a df based on floor price passed and outputs various metrics

## Metrics computed:
## 1. pub_yield -> diff(gross price, clearing price)
## 2. pub_margin -> sum((gross price, clearing price))/1000 only on eligble bids for auction
## 3. pub_margin_pc -> % calculation of above quantity (point 2)
## 4. invalid_bids_pc -> % bids which are invalid in the floor price introduced 2nd price auction scheme
## 5. yield_improved_bids_pc -> % bids for which yield is improved due to new scheme
## 6. not_affected_bids_pc -> % bids for which yield is not affected due to new scheme
## 7. loss_due_to_invalid_bids -> sum(ap)/1000 for invalid bids.
## 8. improvement_due_to_fp -> sum(clearing price)/1000 only on the bids where we had 
##    floor price as clearing price
## 9. improvement_in_yield_pc -> improvement in yield only on the bids where clearing price was chaged due to
##    new scheme
## 10. rev_lift_in_improved_bids -> revenue lift only on the bids where clearing price was increased due to
##     new scheme
## 11. not_affected_revenue -> revenue from bids where we did not see any change in clearing price due to new
##    scheme

def safe_div(x,y):
    try:
        a = x*1.0/y
    except:
        a = 0
    return a

def yield_calculator(df, fp):
    tmp = df.apply(lambda x:second_auction(x['gp'], x['ap'], fp), axis = 1)
    cp = map(lambda x:x['cp'], tmp)
    margin = map(lambda x: x['margin'], tmp)
    flag = map(lambda x:x['flag'], tmp)
    cp_old = map(lambda x: x['cp_old'], tmp)
    newDf = pd.DataFrame({'gp':df['gp'], 'cp':cp, 'ap':df['ap'], 'flag':flag})
    pub_yield = sum(newDf['cp'])/1000
    pub_yield_old = sum(newDf['ap'])/1000
    pub_margin = sum(newDf.loc[newDf['flag'] != 0, 'gp'] - newDf.loc[newDf['flag'] != 0, 'cp'])/1000
    pub_margin_pc = safe_div(sum(newDf.loc[newDf['flag'] != 0, 'gp'] - newDf.loc[newDf['flag'] != 0, 'cp'])*100, \
                             sum(newDf.loc[newDf['flag']!=0, 'gp']))
    invalid_bids_pc = safe_div(newDf.loc[newDf['flag'] == 0].shape[0]*100, newDf.shape[0])
    yield_improved_bids_pc = safe_div(newDf.loc[newDf['flag'] == 2].shape[0]*100, newDf.shape[0])
    not_affected_bids_pc = safe_div(newDf.loc[newDf['flag'] == 1].shape[0]*100, newDf.shape[0])
    loss_due_to_invalid_bids = sum(df.loc[newDf['flag'] == 0, 'ap'])/1000
    improvement_due_to_fp = sum(newDf.loc[newDf['flag'] == 2, 'cp'])/1000
    gap_prev = sum(newDf.loc[newDf['flag'] == 2, 'gp'] - df.loc[newDf['flag'] == 2, 'ap'])/1000
    gap_now = sum(newDf.loc[newDf['flag'] == 2, 'gp'] - newDf.loc[newDf['flag'] == 2, 'cp'])/1000
    #improvement_in_yield_pc = (gap_prev - gap_now)*100/gap_prev
    rev_lift_in_improved_bids = sum(newDf.loc[newDf['flag'] == 2, 'cp'] - df.loc[newDf['flag'] == 2, 'ap'])/1000
    not_affected_rev = sum(newDf.loc[newDf['flag'] == 1, 'cp'])/1000
    return pub_yield, pub_yield_old, pub_margin, pub_margin_pc, invalid_bids_pc, yield_improved_bids_pc, loss_due_to_invalid_bids, \
           improvement_due_to_fp, rev_lift_in_improved_bids, not_affected_bids_pc, not_affected_rev 


def fp_brute_force(df):
    fp_range = range(15, 25, 1)
    fp_range = map(lambda x: x/100.0, fp_range)
    #print 'floor price {}'.format(fp_range)
    pub_yield = []
    pub_yield_old = []
    pub_margin = []
    pub_margin_pc = []
    invalid_bids_pc = []
    yield_improved_bids_pc = []
    loss_due_to_invalid_bids = []
    improvement_due_to_fp = []
    rev_lift_in_improved_bids = []
    not_affected_bids_pc = []
    not_affected_rev = []
    i = 0
    for fpr in fp_range:
        a = yield_calculator(df, fpr)
        pub_yield.append(a[0])
        #if i % 10 == 0:
        #    print 'iteration : {}, pub_yield : {}, floor_price : {}'.format(i, pub_yield[i], fpr)
        #i = i + 1
        pub_yield_old.append(a[1])
        pub_margin.append(a[2])
        pub_margin_pc.append(a[3])
        invalid_bids_pc.append(a[4])
        yield_improved_bids_pc.append(a[5])
        loss_due_to_invalid_bids.append(a[6])
        improvement_due_to_fp.append(a[7])
        rev_lift_in_improved_bids.append(a[8])
        not_affected_bids_pc.append(a[9])
        not_affected_rev.append(a[10])


    dfOut = pd.DataFrame({'fp':fp_range, 'pub_yield':pub_yield, 'pub_yield_old' : pub_yield_old, 'pub_margin':pub_margin, \
                          'pub_margin_pc':pub_margin_pc, 'invalid_bids_pc':invalid_bids_pc, 'yield_improved_bids_pc':yield_improved_bids_pc, 
                          'loss_due_to_invalid_bids':loss_due_to_invalid_bids, 'improvement_due_to_fp':improvement_due_to_fp,\
                          'rev_lift_in_improved_bids':rev_lift_in_improved_bids, 'not_affected_bids_pc':not_affected_bids_pc,\
                          'not_affected_rev':not_affected_rev})
    return dfOut

def max_yield_fp(df):
    dfOut = fp_brute_force(df)
    fp_curr_ix = np.argmax(dfOut['pub_yield'])
    fp = dfOut.loc[fp_curr_ix, 'fp']
    max_yield = dfOut.loc[fp_curr_ix, 'pub_yield']
    return fp, max_yield, dfOut


def update_fp(fp_exp_prev, beta, yield_prev, yield_curr, df_curr, alpha_h, alpha_l, adaptive_flag, batch_size, max_fp):
    if adaptive_flag == 1:
        fp_max, max_yield, dfOut = max_yield_fp(df_curr)
        fp_exp_curr = beta*fp_max + (1 - beta)*fp_exp_prev
        if (yield_curr - yield_prev) < 0:
            fp_update = fp_exp_curr + alpha_l*(yield_curr - yield_prev)*1000/batch_size
        else:
            fp_update = fp_exp_curr + alpha_h*(yield_curr - yield_prev)*1000/batch_size
        fp_update = min(max_fp, fp_update)
        diff = yield_curr - yield_prev
        #expected_yield = yield_calculator(df_curr, fp_update)[0]
    else:
        fp_update, max_yield, dfOut= max_yield_fp(df_curr)
        fp_max = fp_update
        fp_update = min(max_fp, fp_update)
        fp_exp_curr = fp_update
        diff = 0
    return fp_update, fp_exp_curr, fp_max, max_yield, diff
    


