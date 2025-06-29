#!/usr/bin/env python
# coding: utf-8

#this script opts users out of separate hustle instances that aren't able to communicate directly with each other. 

from requests import request
import datetime
from parsons.hustle.column_map import LEAD_COLUMN_MAP
import logging
import petl
import parsons
import civis
from parsons import Hustle
import pandas as pd
from parsons.etl import Table


df = civis.io.read_civis(table='hustle_20.optouts_export',
                         database='redshift-ppfa', 
                         use_pandas=True)
optout_granted = df[["phone_number","first_name","last_name","group_id"]].astype({"phone_number": str})


df2 = civis.io.read_civis(table='hustle_affiliate.optouts_export',
                         database='redshift-ppfa', 
                         use_pandas=True)
optout_paid = df2[["phone_number","first_name","last_name","group_id"]].astype({"phone_number": str})


pt1=Table.from_dataframe(optout_granted)
pt2=Table.from_dataframe(optout_paid)


optout_process_granted = Hustle('email', 'SECRET')


optout_process_paid = Hustle('email', 'SECRET')



granted_optouts=optout_process_granted.create_leads(pt1,'temp pass')


for row in granted_optouts:
    optout_process_granted.update_lead(row['id'], first_name = row['firstName'], last_name = row['lastName'], 
                               global_opt_out=True)

paid_optouts=optout_process_paid.create_leads(pt2,'5yA0zWhhUc')


for row in paid_optouts:
    optout_process_paid.update_lead(row['id'], first_name = row['firstName'], last_name = row['lastName'], 
                               global_opt_out=True)


