import requests
import json
import sqlite3
import smtplib
from datetime import date, timedelta, datetime
from email.message import EmailMessage


############################################################
# This simple program retrieves and stores SPY option chains
# with expirations that are two weeks out.  The program uses
# Tradier for the data.
############################################################


class SpyDataCollector:

	def __init__(self, config_filename):
		with open(config_filename, 'r') as f:
			self.config = json.load(f)

	def create_connection(self):
		""" create a database connection to a SQLite database """

		try:
		    conn = sqlite3.connect(self.config['db_filepath'])
		    print(sqlite3.version)
		except Error as e:
		    self.send_email('SDC Update: Failed to connect to database', str(e))

		return conn

	def get_expiration_dates_to_query(self):
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


	def send_email(self, subject, message):
		""" Sends status emails"""

		gmail = self.config['email']
		password = self.config['password']
		sent_from = gmail
		to = gmail
		subject = subject
		body = message
		email_text = 'Subject: {}\n\n{}'.format(subject, body)

		try:
		    server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
		    server.ehlo()
		    server.login(gmail, password)
		    server.sendmail(sent_from, to, email_text)
		    server.close()
		except:
		    print('Something went wrong...')
		


	def retrieve_options_data(self):
		""" Retrieves the options data from Tradier """
		
		access_token = self.config['access_token']
		options_data = []
		symbol = 'SPY'
		url = 'https://sandbox.tradier.com/v1/markets/options/chains'
		expiration_dates_to_query = self.get_expiration_dates_to_query()
		for date in expiration_dates_to_query:
			response = requests.get(url,
		    	params={'symbol': symbol, 'expiration': date},
		    	headers={'Authorization': 'Bearer ' + access_token, 'Accept': 'application/json'}
			)
			json_response = response.json()
			if json_response['options']:
				options_data.extend(json_response['options']['option'])
			else:
				self.send_email('SDC Update: Date has no data', str(date))

		return options_data

	def shape_options_data(self, options_data):
		"""
			Shapes the raw data into a form acceptable
			for the database (i.e. remove a lot of unneccesary fields)
		"""

		self.quote_timestamp = datetime.now().isoformat()
		shaped_options_data = []

		for option_obj in options_data:
			new_option_tuple = (
				option_obj['symbol'],
				option_obj['root_symbol'],
				option_obj['option_type'],
				option_obj['strike'],
				option_obj['expiration_date'],
				self.quote_timestamp,
				option_obj['bid'],
				option_obj['ask'],
				option_obj['bidsize'],
				option_obj['asksize']
			)

			shaped_options_data.append(new_option_tuple)
		
		return shaped_options_data

	def store_options_data(self, shaped_data):
		""" Stores the shaped data into a sqlite database """

		sample_data = shaped_data[0]

		insert_sql = 'INSERT INTO options VALUES (?,?,?,?,?,?,?,?,?,?)'
		try:
			conn = self.create_connection()
			c = conn.cursor()
			c.executemany(insert_sql, shaped_data)
			conn.commit()
			conn.close()

			self.send_email('SDC Update: Data successfully stored for: ' + self.quote_timestamp, str(sample_data))
		except Error as e:
			self.send_email('SDC Update: Failed to insert data', str(e))


	def run(self):
		data = self.retrieve_options_data()
		shaped_data = self.shape_options_data(data)
		self.store_options_data(shaped_data)


if __name__== "__main__":
	config_filename = './config.json'
	collector = SpyDataCollector(config_filename)
	collector.run()



