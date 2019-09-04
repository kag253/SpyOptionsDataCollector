import requests
import json
import sqlite3
import smtplib
from datetime import date, timedelta, datetime
from email.message import EmailMessage

############################################################
# This simple program retrieves and stores SPY option chains
# with expirations that are two weeks out.  The program uses
# tradier for the data.
############################################################

def create_connection():
	""" create a database connection to a SQLite database """

	db_file = './options_data.db'
	try:
	    conn = sqlite3.connect(db_file)
	    print(sqlite3.version)
	except Error as e:
	    print(e)

	return conn

def get_tradier_access_token():
	""" Retrieves the access token needed to make requests to traider's API """
	config_file = './config.json'
	with open(config_file, 'r') as f:
		config = json.load(f)
		return config['access_token']

def get_expiration_dates_to_query():
	""" 
		Returns the expiration dates to query for SPY for two weeks out. 
		SPY has expiration dates every Monday, Wednesday, and Friday, so 
		this function only returns those dates.
	"""

	# Monday = 0, Wednesday = 2, Friday = 4
	expiration_weekday_values = [0, 2, 4] 

	days_out = 14
	expiration_dates_to_query = []
	current_date = date.today()
	for i in range(days_out):
		current_date = current_date + timedelta(1)

		# If the date is a Monday, Wednesday, or Friday, add to list
		if current_date.weekday() in expiration_weekday_values:
			expiration_dates_to_query.append(current_date.isoformat())

	return expiration_dates_to_query


def send_email(status):
	""" Sends status emails"""

	# msg = EmailMessage()
	# msg.set_content('This is a test!')
	# msg['Subject'] = 'This is a test!'
	# msg['From'] = 'SPY Data Collector'
	# msg['To'] = 'kag253@gmail.com'

	# # Send the message via our own SMTP server.
	# s = smtplib.SMTP('localhost')
	# s.send_message(msg)
	# s.quit()



	mail = smtplib.SMTP('smtp.gmail.com', 587)
	# server = smtplib.SMTP_SSL
	# server.login("your username", "your password")
	mail.sendmail(
		"blah@address.com", 
		"kag253@gmail.com", 
		"this message is from python"
	)
	mail.quit()


def retrieve_options_data():
	""" Retrieves the options data from Tradier """
	
	access_token = get_tradier_access_token()
	options_data = []
	symbol = 'SPY'
	url = 'https://sandbox.tradier.com/v1/markets/options/chains'
	expiration_dates_to_query = get_expiration_dates_to_query()
	for date in expiration_dates_to_query:
		response = requests.get(url,
	    	params={'symbol': symbol, 'expiration': date},
	    	headers={'Authorization': 'Bearer ' + access_token, 'Accept': 'application/json'}
		)
		json_response = response.json()
		if json_response['options']:
			options_data.extend(json_response['options']['option'])
		else:
			# TODO change to email notification
			print('The ' + date + ' does not have data!')
			print(json_response)

	return options_data

def shape_options_data(options_data):
	"""
		Shapes the raw data into a form acceptable
		for the database (i.e. remove a lot of unneccesary fields)
	"""

	quote_timestamp = datetime.now().isoformat()
	shaped_options_data = []

	for option_obj in options_data:
		new_option_tuple = (
			option_obj['symbol'],
			option_obj['root_symbol'],
			option_obj['option_type'],
			option_obj['strike'],
			option_obj['expiration_date'],
			quote_timestamp,
			option_obj['bid'],
			option_obj['ask'],
			option_obj['bidsize'],
			option_obj['asksize']
		)

		shaped_options_data.append(new_option_tuple)
	
	return shaped_options_data

def store_options_data(shaped_data):
	""" Stores the shaped data into a sqlite database """

	print(shaped_data)
	insert_sql = 'INSERT INTO options VALUES (?,?,?,?,?,?,?,?,?,?)'
	try:
		conn = create_connection()
		c = conn.cursor()
		c.executemany(insert_sql, shaped_data)
		conn.commit()
		conn.close()
	except Error as e:
	    print(e)
	    # TODO email error



def main():
	data = retrieve_options_data()
	shaped_data = shape_options_data(data)
	store_options_data(shaped_data)


# main()
send_email('hello')

