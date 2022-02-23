# -*- coding: utf-8 -*-
"""
Created on Wed Feb 16 12:14:31 2022

@author: mxka1r54
"""

#Importar paqueterias
import os
import shutil
os.chdir(r'C:\Users\MXKA1R54\OneDrive - Kellogg Company\Documents\ARG\Pyhton') # relative path: scripts dir is under Lab

import unittest
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait # Guardar en las paginas
#from selenium.webdriver.support import expected_conditions as EC #Guardar en las paginas import unittest
#from selenium.webdriver.chrome.options import Options
from datetime import date, timedelta
import time 
import datetime
#from calendar import monthrange
import calendar
import json

import unittest
import time 
import datetime 
import pandas as pd
from datetime import date, timedelta, datetime

#Set paths  
import_path = r'C:\Users\MXKA1R54\OneDrive - Kellogg Company\Documents\ARG\LH4\INVOLVES\PLATFORMS'
catalogs_path = r'C:\Users\MXKA1R54\OneDrive - Kellogg Company\Documents\ARG\LH4\INVOLVES\CATALOGS'
export_path = r'C:\Users\MXKA1R54\OneDrive - Kellogg Company\Documents\ARG\LH4\INVOLVES\PLATFORMS\IVY' #'C:\\SUPERVALU SQL'

# Ver la fecha en que se esra ejecutando el reporte
#Fecha de hoy
today = date.today()
week_number = today.strftime("%U")

#Leer archivo platforms TENEMOS 3 FILES QUE IMPORTAR ANTES QUE ES LO QUE LLEVAMOS DE INVOLVES
data_xlsx = pd.read_csv(import_path + '\PLATFORMS_03.02.22_to_20.02.22.csv')

#Modificamos el tipo de dato de las columnas para una mejor manipulacion 
data_xlsx['ID de la encuesta'] = data_xlsx['ID de la encuesta'].astype('float64')
data_xlsx['ID del PDV'] = data_xlsx['ID del PDV'].astype('float64')
data_xlsx['Semana del año'] = data_xlsx['Semana del año'].astype('float64')
data_xlsx['Día del mes'] = data_xlsx['Día del mes'].astype('float64')
data_xlsx['Mes del año'] = data_xlsx['Mes del año'].astype('float64')
data_xlsx['Año'] = data_xlsx['Año'].astype('float64')
data_xlsx['cantidad ejecutada'] = data_xlsx['cantidad ejecutada'].fillna(0).astype('int64')
data_xlsx['Fecha y hora de la encuesta'] = data_xlsx['Fecha y hora de la encuesta'].astype('datetime64[ns]')
data_xlsx['Fecha de empiezo de la campaña'] = data_xlsx['Fecha de empiezo de la campaña'].astype('datetime64[ns]')
data_xlsx['Fecha final de la campaña'] = data_xlsx['Fecha final de la campaña'].astype('datetime64[ns]')

#convertir las fechas al formato
formatted_query_date = data_xlsx["Fecha y hora de la encuesta"].dt.strftime("%Y-%m-%d")
data_xlsx["Fecha y hora de la encuesta"] = formatted_query_date

formatted_query_date = data_xlsx["Fecha de empiezo de la campaña"].dt.strftime("%d-%m-%Y")
data_xlsx["Fecha de empiezo de la campaña"] = formatted_query_date

formatted_query_date = data_xlsx["Fecha final de la campaña"].dt.strftime("%d-%m-%Y")
data_xlsx["Fecha final de la campaña"] = formatted_query_date

#mapear y convertir los chain, retailer env y store format al formato de Ivy  
chain_catalog = pd.read_csv(catalogs_path + '\\chain.csv')
chain_catalog.dropna(inplace = True)
chain_catalog_dict = chain_catalog.set_index('Red').T.to_dict(orient = 'records') #chain_catalog_dict['CHEDRAUI'][0]  = 'CHEDRAUI'
chain_catalog_dict = chain_catalog_dict[0]

ret_env_catalog = pd.read_csv(catalogs_path + '\\ret_env.csv')
ret_env_catalog.dropna(inplace = True)
ret_env_catalog_dict = ret_env_catalog.set_index('Tipo de PDV').T.to_dict(orient = 'records') #chain_catalog_dict['CHEDRAUI'][0]  = 'CHEDRAUI'
ret_env_catalog_dict = ret_env_catalog_dict[0]

#Replace values from the catalogs into the involves dataframe
data_xlsx['Red'] = data_xlsx['Red'].replace(chain_catalog_dict)
data_xlsx['Tipo de PDV'] = data_xlsx['Tipo de PDV'].replace(ret_env_catalog_dict)

#Replace Sí por Yes 
data_xlsx['Ejecutaste el material?'] = data_xlsx['Ejecutaste el material?'].replace({"Sí": "Yes"})


#llenar un nuevo dataframe con los nombres de las columnas finales y mapearlo con el dataframe existente
df_final = pd.DataFrame(data=None, columns=['Visit Date', 'User Name', 'Region', 'Sub Region', 'Country', 'Market Segment', 'Customer Group', 'Chain', 'Retailer Environment', 'Store Format', 'Store Sub Format', "Kellogg's Store Code", 'Store Code', 'Store Name', 'Store Location', 'Start Date', 'End Date', 'Platform Name', 'Activity', 'Target', 'Received', 'Is Executed', 'Reason', 'Photo'])
df_final['Visit Date'] = data_xlsx['Fecha y hora de la encuesta']
df_final['Chain'] = data_xlsx['Red']
df_final['Retailer Environment'] = data_xlsx['Tipo de PDV']
df_final['Store Format'] = data_xlsx['Perfil del PDV']
df_final['Store Code'] = data_xlsx['Código del PDV']
df_final['Store Name'] = data_xlsx['PDV']
df_final['Start Date'] = data_xlsx['Fecha de empiezo de la campaña']
df_final['End Date'] = data_xlsx['Fecha final de la campaña']
df_final['Platform Name'] = data_xlsx['Campaña']
df_final['Activity'] = data_xlsx['Línea de producto']
df_final['Target'] = data_xlsx['Cantidad de piezas enviadas']
df_final['Received'] = data_xlsx['cantidad ejecutada']
df_final['Is Executed'] = data_xlsx['Ejecutaste el material?']
df_final['Reason'] = data_xlsx['Porqué no ejecutaste']
df_final['User Name'] = data_xlsx['Empleado']


#exportar dataframe en csv y cargar en S3
# export file 
df_final.to_csv(export_path + '\\INVOLVES_IVY_Week 08 2022.csv', index = False, encoding = 'utf-8-sig') # INVOLVES_IVY_Week %s 2022.csv'%week_number, index = False, encoding = 'utf-8-sig')


#Revisar tipo de dato de las columnas del df
df_final.dtypes

#Leer el archivo creado y subir a S3 s3://klg-kladata/dev/LH4/raw/ivy/platforms/

import boto3

ACCESS_KEY = ''
ACCESS_SECRET = ''

s3_client = boto3.client('s3', region_name='us-east-1', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=ACCESS_SECRET)

response = s3_client.upload_file(export_path + '\\INVOLVES_IVY_Week %s 2022.csv'%week_number, 'klg-kladata', 'dev/LH4/raw/ivy/platforms/INVOLVES_IVY_Week %s.csv'%week_number)
