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

# Configure email settings
SMTP_host = 'email-smtp.us-west-2.amazonaws.com'
SMTP_port = '587'
SMTP_username = 'AKIAIQ6XDBEJ5SXK5ZIQ'
SMTP_password = 'AkAGnvspLA05yNM0tLyC7ceLPe0IVWrouJaItKCybdZT'
sender = 'automated-notifications@c1exchange.com'
from_email = 'automated-notifications@c1exchange.com'
receivers_list = ['jitenkumar@c1exchange.com'] #, 'data-eng@c1exchange.com']
subject = 'SSP Global Dashboard job failure alert'

## Email Function ##
def send_email(receivers_list, subject, message):
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = 'automated-notifications@c1exchange.com'
        #msg['To'] = ','.join(receivers_list)
        msg['To'] = 'jitenkumar@c1exchange.com'
        #msg.add_header('reply-to', 'data-eng@c1exchange.com')
        body = MIMEText(message, 'html')
        msg.attach(body)
        s = smtplib.SMTP(SMTP_host, SMTP_port, 10)
        s.starttls()
        s.login(SMTP_username, SMTP_password)
        s.sendmail(from_email, receivers_list, msg.as_string())
             

## misc functions
def conv_to_str(a):
	b = str(a)
	if(len(b) == 1):
		b = '0' + b	
	return str(b)


def get_loghour(ts, sep, lag):
	time = ts - timedelta(hours = int(lag))
	year = conv_to_str(time.year)
	month = conv_to_str(time.month)
	day = conv_to_str(time.day)
	hh = conv_to_str(time.hour)
	minute = '00'.strip()
	loghour = year + sep + month + sep + day + sep + hh + sep + minute
	loghour = loghour.strip()
	return loghour