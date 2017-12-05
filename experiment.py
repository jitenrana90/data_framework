from datetime import datetime, timedelta
import time
## Project Modules Import
import general_utility as gu
import query_module as qm
import fp_optimisation_module as fom


## SETTING UP PARAMS FOR ALGO ##
TRAIN_SIZE = 600
BATCH_SIZE = 300
#MULTIPLIER = TRAIN_SIZE/BATCH_SIZE
#NO_OF_MINIBATCHES = (df.shape[0]//TRAIN_SIZE - 1)*MULTIPLIER
#NO_OF_MINIBATCHES = 100
ALPHA_H = 0.25
ALPHA_L = 1.0
MAX_FP = 0.25
BETA = 1.0     ## Exp soothing Factor for fp_max. Has to be between 0 and 1
RECOVERY_RATE = 0.2



## EXPERIMENT PROCEDURE FUNCTION ##
def exp_code(fp_update_tmp, fp_exp_prev, fp_max, gt, lt):

	print 'processing for time period : {} - {}'.format(lt, gt)

	## QUERY DATA ##
	df = qm.query_es(qm.ELASTICSEARCH_HOST, qm.ELASTICSEARCH_BIDDERLOGS_INDEX, qm.query_file, gt, lt)

	## FILTERING AND PROCESSING ##
	dfRaw = df.loc[df['gp'] > df['ap']]
	dfRaw = dfRaw.loc[dfFRaw['gp'] <= 1]
	dfFilter = dfRaw.loc[dfRaw['pgid'] = 410]
	dfFilter = dfFilter.loc[dfFilter['adsize']=='300x250']
	## One more filter required.
	## We need to apply this only on the bids where we made changes in fp
	## so filter for fp_change flag

	## DATAFRAME READY FOR ALGO ##

	## Next line, let us see if it needs to be tuned ##
    ## train_batch = train_batch[-TRAIN_SIZE:]

    ## First yield calculation in previous batch ##
    yield_curr = fom.yield_calculator(dfFilter, fp_update_tmp)[0]
    fp_update.append(fp_update_tmp)
    fp_max_exp.append(fp_exp_prev)
    max_fp.append(fp_max)
    margin.append(sum(exp_batch['diff']))
    margin_pc.append(safe_div(sum(exp_batch['diffPc']), exp_batch.shape[0]))
    ## fp update algo ##
	fp_update_tmp, fp_exp_curr, fp_max, max_yield, diff = fom.update_fp(fp_exp_prev, BETA, yield_prev, yield_curr, train_batch, ALPHA_H, ALPHA_L, 1, TRAIN_SIZE, MAX_FP)
	fp_exp_prev = fp_exp_curr
	return fp_update_tmp, fp_exp_prev, fp_max

## REQUIRED METRICS ##
fp_update = []
yield_actual = []
yield_train = []
yield_max = []
invalid_bids = None
yield_delta = []
max_fp = []
fp_max_exp = []
margin = []
margin_pc = []
margin_new = []
margin_new_pc = []
new_batch = pd.DataFrame()
rev_loss = 0
## need to set
## fp_exp_prev, yield_prev, yield_curr
yield_prev = 0
yield_curr = 0
fp_exp_prev = 0.24
fp_update_tmp = 0.24
fp_max = 0.0
flag = 0
diff = 0

while True:	
	tic = datetime.datetime.now()
	lt = int(time.time()*1000)
	gt = int(time.time()*1000 - 60000)
	fp_update_tmp, fp_exp_prev, fp_max = exp_code(fp_update_tmp, fp_exp_prev, fp_max, gt, lt)
	exp_output = {'pgid':'410', 'adsize':'300x250', 'at':2, 'floor_price':fp_update_tmp, 'startTime':lt, 'endTime':gt}
	print 'exp_output : {}'.format(exp_output)
	toc = datetime.datetime.now()
	print 'time for iteration : {}'.format(toc - tic)
	time.sleep(60)


