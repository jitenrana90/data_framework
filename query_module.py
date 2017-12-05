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
from itertools import chain
from general_utility import *
from elasticsearch import Elasticsearch
import json


## diata pull module
def connection_citus(hostname, username, passwd, database):
	conn = psycopg2.connect(host = hostname, user = username, database = database, password = passwd)
	return conn

def connection_athena(access_key, secret_key, s3_staging_dir, region_name):
	conn = connect(
	                access_key=access_key,
	                secret_key=secret_key,
	                s3_staging_dir=s3_staging_dir,
	                region_name = region_name
	                )	
	return conn


def form_query(dict):
	table = dict['table']
	try:
		agg_sum = dict['agg']['sum']
		agg_sum = 'sum(' + '), sum('.join(agg_sum) + ')'
	except:
		agg_sum = ''
	try:
		agg_count = dict['agg']['count']
		agg_count = 'count(' + '), count('.join(agg_count) + ')'
	except:
		agg_count = ''
	dims = ', '.join(dict['dims'])
	conds = dict['interval']
	if agg_count == '':
		query = "SELECT " + str(dims) + ", " + agg_sum + " FROM " + table + " WHERE year || '_' || month || '_' || day || '_' || hh || '_' || '00' >= '" + str(conds) + "' GROUP BY " + str(dims)
		cols = [dict['dims'], dict['agg']['sum']]
	else:
		query = "SELECT " + str(dims) + ", " + agg_sum + ', ' +  agg_count + " FROM " + table + " WHERE year || '_' || month || '_' || day || '_' || hh || '_' || '00' >= '" + str(conds) + "' GROUP BY " + str(dims)
		cols = [dict['dims'], list(dict['agg']['sum']), list(dict['agg']['count'])]	
	cols = list(chain.from_iterable(cols))	
	print query
	return query, cols

#def query(hostname, username, passwd, database, table, query_params):
def query(access_key, secret_key, s3_staging_dir, region_name, database, table, query_params):
    ## Citus Connection ##
	#conn = connection_citus(hostname, username, passwd, database)
    conn = connection_athena(access_key, secret_key, s3_staging_dir, region_name)
    cur = conn.cursor()
	
	## Forming Query ##
    query_params['table'] = str(database) + '.' + str(table)
    q, cols = form_query(query_params)
	#q = "SELECT date_hr, pub_group_id, profile_id , sum(bidssent) as requests, sum(adslotrequestsent) as dsprequest, sum(bidresponses) as dspresponses, sum(bidwins) as bidssent, sum(numimpressions) as impressions, sum(totalrevenue) as totalrevenue FROM " + table + " WHERE loghour ='" + loghour + "' GROUP BY date_hr, pub_group_id, profile_id" 
    cur.execute(q)
    df = pd.DataFrame(cur.fetchall())
    print df.head()
    df.columns = ['year', 'month', 'day', 'hh', 'grouppubid', 'requests']
    df['date_hr'] = df['year'].astype('str') + '_' + df['month'].astype('str') + '_' + df['day'].astype('str') + '_' + df['hh'].astype('str')
    print cols
    df.columns = ['year', 'month', 'day', 'hh', dimcol, 'requests', 'date_hr']
    df = df[['date_hr', 'pub_group_id', 'requests']]
    return df




## ELASTIC SEARCH QUERY AND PARAMETERS ##
ELASTICSEARCH_HOST = "18.220.238.47:9200"
ELASTICSEARCH_BIDDERLOGS_INDEX = "tracker*"
QUERY_FILE = 'query.json'

def query_es(elk_host, elk_index, query_file, gt, lt):
	res = None
	es = Elasticsearch(elk_host)
	with open(QUERY_FILE) as queryFile:
		eQueryA = json.load(queryFile)
	eQueryA['query']['bool']['must'][4]['range']['@timestamp']['gte']= gt
	eQueryA['query']['bool']['must'][4]['range']['@timestamp']['lte']= lt
	eQueryA['highlight']['fields']['*']['highlight_query']['bool']['must'][4]['range']['@timestamp']['gte']= gt
	eQueryA['highlight']['fields']['*']['highlight_query']['bool']['must'][4]['range']['@timestamp']['lte']= lt
	try:
		res = es.search(index = elk_index, body = eQueryA)
	except Exception as e:
		print(str(e))
	df = make_res_df(res)
	return df

def make_res_df(res):
	ts = []
	gp = []
	ap = []
	fp = []
	for d in res['hits']['hits']:
		tmp = d['_source']
		ts.append(tmp['genTs'])
		gp.append(tmp['np'])
		ap.append(tmp['ap'])
		fp.append(tmp['bidFloor'])	
	df = pd.DataFrame({'ts':ts, 'gp':gp, 'ap':ap, 'fp':fp})
	return df


