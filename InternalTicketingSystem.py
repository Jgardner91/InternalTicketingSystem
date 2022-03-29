import pyodbc
import pandas as pd 
import sqlalchemy as db
from sqlalchemy import create_engine
from sqlalchemy import join
from sqlalchemy.sql import select
import warnings
warnings.filterwarnings('ignore')
import urllib.parse
from sqlalchemy import func

from sqlalchemy import event
from sqlalchemy.engine import Engine
import time
import logging
import datetime 

from sqlalchemy import Table, Column, Integer, String, Numeric,Float, MetaData,distinct,delete
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Numeric
from sqlalchemy.orm import sessionmaker

import hashlib as h
from sqlalchemy.sql import alias

from sqlalchemy import func
#importing seaborn library for plotting results
import seaborn as sns
from sqlalchemy import update
from sqlalchemy import insert
from sqlalchemy import delete

class Connection:
	'''
	This class defines the connection parameters.
	Instance variables to be adopted by Database Classes
	'''
	def __init__(self,engine,connection,metadata,raw_connect):
		self.engine = engine
		self.connection = connection
		self.metadata = metadata
		self.raw_connect = raw_connect




class Ticket(Connection):
	def __init__(self,engine,connection,metadata,raw_connect):
		super().__init__(engine,connection,metadata,raw_connect)

		self.table_list = self.engine.table_names()
		self.table_objs = [db.Table(tbl,self.metadata,autoload=True,autoload_with = self.connection) for tbl in self.table_list]
		self.column_map = {self.table_list[i]:[c.name for c in self.table_objs[i].columns] for i in range(len(self.table_list))}
	

	def CreateTicket(self):

		table = self.table_objs[0]
		create_ticket = 'yes'
		while create_ticket == 'yes':

			query = db.select([func.max(table.columns['TicketId'])])
			ResultProxy = self.connection.execute(query)
			ResultSet = ResultProxy.fetchall()
			print("TicketId must be greate than: ",ResultSet[0][0])
			ticket_id = int(input("Enter Ticket ID: "))
			date_created = input("Enter Date Created: ")
			creator = input("Creator: ")
			status_type = input("Enter Status: ")
			urgency = input("Enter Urgency: ")
			description = input("Enter Short Description(<= 200ch): ")
		
			ticket_created = table.insert().values(TicketId=ticket_id, 
                                   DateCreated=date_created,
                                   Creator=creator,
                                   StatusType=status_type,
                                   Urgency= urgency,
                                   DescriptionNote = description)

			self.engine.execute(ticket_created)
			print("*****TICKET CREATED******\n")

			create_ticket = input("Would you like to create another ticket: ")

		print("Ok, thank you. ")

	def AcceptTicket(self):
		table = self.table_objs[0]

		query = db.select([table]).where(table.columns['StatusType'] == 'Open')
		ResultProxy = self.connection.execute(query)
		ResultSet = ResultProxy.fetchall()
		Pull_ = pd.DataFrame(ResultSet, columns = ['TicketId','DateCreated','Creator','AcceptedBy',
			'DateCompleted','Urgency','StatusType','DescriptionNote','FileLocation','AssistedBy'] )

		print(Pull_[['TicketId','StatusType','DescriptionNote']])
		accept_ticket = 'yes'
		while accept_ticket == 'yes':
			credentials = input("Enter Credientials: ")
			ticket_num = int(input("Enter Number of Accepted Ticket: "))

			u = update(table)
			u = u.values({"AcceptedBy":credentials,"StatusType":'Pending'})
			u = u.where(table.columns['TicketId']==ticket_num)
			self.engine.execute(u)
			accept_ticket = input("Would you like to accept another ticket ?: ").lower()

	def CloseTicket(self):
		table = self.table_objs[0]
		close_ticket = 'yes'
		while close_ticket == 'yes':
			ticket_id = int(input("Ticket Number: "))
			file_path = input("Data File Location: ")
			date_completed = input("Enter Date Completed: ")
			assisted_by = input("Assisted By(If applicable): ")
			u = update(table)
			u = u.values({'StatusType':'Closed','FileLocation':file_path,"DateCompleted":date_completed,
				"AssistedBy":assisted_by})
			u = u.where(table.columns['TicketId']== ticket_id)
			self.engine.execute(u)
			close_ticket = input("Would you like to close another ticket: ").lower()

	def UpdateTicket(self):
		table = self.table_objs[0]
		add_update = 'yes'
		
		while add_update == 'yes':
			ticket_id = int(input("Which ticket would you like to update: "))
			update_field = input("Which field would you like to update: ").upper()
			if update_field == 'ACCEPTEDBY':
				accepted_by = input("New owner credentials: ")
				u = update(table)
				u.values({"AcceptedyBy":accepted_by})
				u.where(table.columns['TicketId']==ticket_id)
				self.engine.execute(u)

			elif update_field == 'URGENCY':
				urgency = input("New Urgency Level: ")
				u = update(table)
				u.values({"Urgency":urgency})
				u.where(table.columns['TicketId']==ticket_id)
				self.engine.execute(u)

			elif update_field == 'STATUSTYPE':
				status = input("New status type: ")
				u = update(table)
				u.values({"StatusType":status})
				u.where(table.columns['TicketId']==ticket_id)
				self.engine.execute(u)

			elif update_filed == 'DESCRIPTIONNOTE':
				note = input("Update Description: ")
				u = update(table)
				u.values({"DescriptionNote":note})
				u.where(table.columns['TicketId']==ticket_id)
				self.engine.execute(u)

			elif update_field == 'FILELOCATION':
				file_loc = input("New file location: ")
				u = update(table)
				u.values({"FileLocation":file_loc})
				u.where(table.columns['TicketId']==ticket_id)
				self.engine.execute(u)

			elif update_field == 'ASSISTEDBY':
				assist = input("New file location: ")
				u = update(table)
				u.values({"AssistedBy":assist})
				u.where(table.columns['TicketId']==ticket_id)
				self.engine.execute(u)

			else:
				print("NOT A VALID FIELD ! ")


			add_update = input("Would you like to update another field: ")