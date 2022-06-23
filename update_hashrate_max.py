from asyncore import dispatcher
from telegram import *
from telegram.ext import *
from requests import *
from datetime import datetime
import pandas as pd
import schedule
import gspread
import requests
import os
from oauth2client.service_account import ServiceAccountCredentials

credenciales = os.environ['CREDS']
alert_telegram_channel_id = os.environ['TELEGRAMCHANNELID']
alert_telegram_bot_id = os.environ['TELEGRAMBOTID']



sheet = "Cobros - Autom"

def telegram_message(message):
    headers_telegram = {"Content-Type": "application/x-www-form-urlencoded"}
    endpoint_telegram = "https://api.telegram.org/"+alert_telegram_bot_id+"/sendMessage"
    mensaje_telegram = {"chat_id": alert_telegram_channel_id, "text": "Problemas en RIG"}
    mensaje_telegram["text"] = message
    response = requests.post(endpoint_telegram, headers=headers_telegram, data=mensaje_telegram).json()
    return response

def getSheetsDataFrame(sheet, worksheet):
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(credenciales, scope)
    client = gspread.authorize(creds)
    work_sheet = client.open(sheet)
    sheet_instance = work_sheet.worksheet(worksheet)
    records_data = sheet_instance.get_all_records()
    return (pd.DataFrame.from_dict(records_data))

def update_zabbix():
    print("ACTUALIZANDO DATA DE ZABBIX")
    dataFrame = getSheetsDataFrame(sheet, "Grafana")
    successRate = 0
    failRate = 0
    failRateWallets = []
    failRateHash = []

    for x in dataFrame["id"]:
        wallet = str(dataFrame["wallet"][x-1])
        hashrate_max = str(dataFrame["Hashrate MAX"][x-1])
        stream = os.popen("zabbix_sender -z '54.92.215.92'    -s "+wallet+" -k application.hashrate_max -o "+hashrate_max)
        output = stream.read()
        if "sent: 1" in output:
            successRate = successRate + 1
        else:
            failRate = failRate + 1
            failRateWallets.append(wallet)
            failRateHash.append(hashrate_max)
    string = ""
    for x, y in zip(failRateWallets, failRateHash):
        print("Wallet con problema: "+x+ " Hash de la wallet: "+y)
        string = string +"\n"+("Wallet con problema: "+x+ " Hash de la wallet: "+y)

    telegram_message("Se actualizaron Hashrates Maximos en Zabbix\nCantidad de envios correctos: "+ str(successRate)+"\nCantidad de envios fallidos: "+ str(failRate)+string)
    print("Cantidad de envios correctos: "+ str(successRate))
    print("Cantidad de envios fallidos: "+ str(failRate))


schedule.every().day.at("10:00").do(update_zabbix)

while True:
    schedule.run_pending()
