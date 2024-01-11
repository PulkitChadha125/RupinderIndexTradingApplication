import Algofox
import traceback
import Zerodha_Integration
from Algofox import *
from kite_trade import *
import time as sleep_time
import threading
import pyotp
import pandas as pd
from datetime import datetime,timedelta
from Zerodha_Integration import  *
from sched import scheduler
import schedule
from apscheduler.schedulers.background import BackgroundScheduler
import time
end_time_str = None
start_time_str = None
lock = threading.Lock()


def delete_file_contents(file_name):
    try:
        # Open the file in write mode, which truncates it (deletes contents)
        with open(file_name, 'w') as file:
            file.truncate(0)
        print(f"Contents of {file_name} have been deleted.")
    except FileNotFoundError:
        print(f"File {file_name} not found.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
def write_to_order_logs(message):
    with open('OrderLog.txt', 'a') as file:  # Open the file in append mode
        file.write(message + '\n')
def get_zerodha_credentials():
    delete_file_contents("OrderLog.txt")
    credentials = {}
    try:
        df = pd.read_csv('ZerodhaCredentials.csv')
        for index, row in df.iterrows():
            title = row['Title']
            value = row['Value']
            credentials[title] = value
    except pd.errors.EmptyDataError:
        print("The CSV file is empty or has no data.")
    except FileNotFoundError:
        print("The CSV file was not found.")
    except Exception as e:
        print("An error occurred while reading the CSV file:", str(e))

    return credentials

credentials_dict = get_zerodha_credentials()

user_id = credentials_dict.get('ZerodhaUserId') # Login Id
password = credentials_dict.get('ZerodhaPassword') # Login password
fakey= credentials_dict.get('Zerodha2fa')
mode=credentials_dict.get('MODE')
Algofoxid=credentials_dict.get('Algofoxid')
Algofoxpassword=credentials_dict.get('Algofoxpassword')
role=credentials_dict.get('role')
twofa = pyotp.TOTP(fakey)
twofa= twofa.now()
print(twofa)
kite = Zerodha_Integration.login(user_id, password, twofa)
AlgofoxUrl=credentials_dict.get('AlgofoxUrl')

Algofox.createurl(AlgofoxUrl)
# Algofox.login_algpfox(username=user_id,password=password,role=role)

instruments = kite.instruments("NFO")
df = pd.DataFrame(instruments)
df.to_csv("Instruments.csv")

def custom_round(price, symbol):
    rounded_price = None

    if symbol == "NIFTY":
        last_two_digits = price % 100
        if last_two_digits < 25:
            rounded_price = (price // 100) * 100
        elif last_two_digits < 75:
            rounded_price = (price // 100) * 100 + 50
        else:
            rounded_price = (price // 100 + 1) * 100
            return rounded_price

    elif symbol == "BANKNIFTY":
        last_two_digits = price % 100
        if last_two_digits < 50:
            rounded_price = (price // 100) * 100
        else:
            rounded_price = (price // 100 + 1) * 100
        return rounded_price

    elif symbol == "FINNIFTY":
        last_two_digits = price % 100
        if last_two_digits < 25:
            rounded_price = (price // 100) * 100
        elif last_two_digits < 75:
            rounded_price = (price // 100) * 100 + 50
        else:
            rounded_price = (price // 100 + 1) * 100
            return rounded_price

    else:
        pass

    return rounded_price

signal_dict={}

def read_csv_to_dictionary(file_path):
    data_dict = {}
    global signal_dict, end_time_str, start_time_str

    df = pd.read_csv(file_path)
    for index, row in df.iterrows():
        strategy_tag = row['strategytag']
        data_dict[strategy_tag] = row.to_dict()

        signal_dict[strategy_tag] = {'CALL': False, 'PUT': False, 'Target1': None, 'Target2': None, 'Target3': None,
                                     'Sl': None, 'PreviousString': None, "T1": False, "T2": False, "T3": False,
                                     "S": False, "Count": 0, "expiry": None, "SlMove1": None, "SlMove2": None, "TP1QTY": None,
                                     "TP2QTY": None, "TP3QTY": None, "strategytag": None, "EXITQTY": None, 'Trade': False,
                                     "new_time": None, "alfoxsymbol": None, "ce_atm": None, "pe_atm": None,
                                     "StopTime": None, "runonce": False}

        start_time_str = data_dict[strategy_tag]['StartTime']
        start_time_str = datetime.strptime(start_time_str, '%H:%M').time()
        end_time_str = data_dict[strategy_tag]['StopTime']
        end_time_str = datetime.strptime(end_time_str, '%H:%M').time()
        print(start_time_str)
        print(end_time_str)

    return data_dict


data_dict = read_csv_to_dictionary("TradeSettings.csv")
print('TradeSettings =',data_dict)
ce_atm=None
pe_atm=None
NextEntryTime=None

def Zerodhaweekly(original_date):
    # Convert the original date string to a datetime object
    original_date_obj = datetime.strptime(original_date, '%d-%m-%Y')

    # Extract year, month, and day
    year = str(original_date_obj.year)[-2:]  # Extract last two digits of the year
    month_first_letter = original_date_obj.strftime('%b')[0].upper()  # First letter of the month
    day = str(original_date_obj.day).zfill(2)  # Ensure day is zero-padded

    # Construct the custom date format
    custom_date_format = f"{year}{month_first_letter}{day}"

    return custom_date_format

def zerodhahistorical(original_date):
    # Convert the original date string to a datetime object
    original_date_obj = datetime.strptime(original_date, '%d-%m-%Y')

    # Format the date in the desired custom format "yyyy-mm-dd"
    custom_date_format = original_date_obj.strftime('%Y-%m-%d')

    return custom_date_format

def zerodha_ltp_index(symbol):
    if symbol == "NIFTY":
        sym = "NIFTY 50"
        usedltp = Zerodha_Integration.get_ltp(sym)

    if symbol == "BANKNIFTY":
        sym = "NIFTY BANK"
        usedltp = Zerodha_Integration.get_ltp(sym)

    if symbol == "FINNIFTY":
        sym = "FINNIFTY"
        usedltp = Zerodha_Integration.get_ltp(sym)

    return usedltp


def process_data(data_dict):
    global NextEntryTime, pe_atm, ce_atm, signal_dict, Algofoxid, Algofoxpassword, role, ce_token, pe_token
    pe_strike = None
    ce_strike = None
    try:
        for strategy_tag, data in data_dict.items():
            if pd.notna(strategy_tag):
                print(f"Data collection started for {strategy_tag}")
                StartTime = data['StartTime']
                StopTime = data['StopTime']

                StartTime = f"{StartTime}:00"
                StopTime = f"{StopTime}:00"

                entry_time = datetime.strptime(StartTime, "%H:%M:%S")
                exit_time = datetime.strptime(StopTime, "%H:%M:%S")

                signal_dict[strategy_tag]['StopTime'] = exit_time
                symbol = data['symbol']
                timeframe = data['Timeframe']
                strategytag = data['strategytag']
                Expiery = data['Expiery']
                Expiery_zerodha = Expiery
                Expiery_zerodha = datetime.strptime(Expiery_zerodha, '%d-%m-%Y')
                Expiery_zerodha = Expiery_zerodha.strftime('%Y-%m-%d')
                Expiery_zerodha = datetime.strptime(Expiery_zerodha, "%Y-%m-%d")
                Expiery_zerodha = Expiery_zerodha.strftime("%y%b").upper()
                Expiery = data['Expiery']
                expiryhistorical = zerodhahistorical(Expiery)
                Expiery = data['Expiery']
                expiery_zerodha_weekly = Zerodhaweekly(Expiery)
                # print("expiryhistorical: ",expiryhistorical)
                Expiery = data['Expiery']
                Expiery = datetime.strptime(Expiery, '%d-%m-%Y')
                Expiery = Expiery.strftime('%Y-%m-%d')
                Expiery = datetime.strptime(Expiery, "%Y-%m-%d")
                Expiery = Expiery.strftime("%y%b").upper()
                CalculationType = data['CalculationType']
                tgt1 = float(data['Tgt1'])
                Tgt1Lotsize = int(data['Tgt1Lotsize'])
                tgt2 = float(data['Tgt2'])
                Tgt2Lotsize = int(data['Tgt2Lotsize'])
                tgt3 = float(data['Tgt3'])
                Tgt3Lotsize = int(data['Tgt3Lotsize'])
                RSIValue = int(data['RSIValue'])
                RSIPeriod = int(data['RSIPeriod'])
                MAOFOI = int(data['MAOFOI'])
                MAOFVOl = int(data['MAOFVOl'])
                EntryLotsize = int(data["EntryLotsize"])

                OrderType = data["OrderType"]
                stg = data["strategytag"]
                Sl = float(data["Sl"])
                SlMove1 = float(data["SlMove1"])
                SlMove2 = float(data["SlMove2"])
                print(SlMove1)
                print(SlMove2)
                VolMultiple = float(data["VolMultiple"])
                VolLookBack = int(data["VolLookBack"])
                n = int(VolLookBack)
                TradeExpiery = data['TradeExpiery']

                TradeExpiery_zerodha = TradeExpiery
                TradeExpiery_zerodha = datetime.strptime(TradeExpiery_zerodha, '%d-%m-%Y')
                TradeExpiery_zerodha = TradeExpiery_zerodha.strftime('%Y-%m-%d')
                TradeExpiery_zerodha = datetime.strptime(TradeExpiery_zerodha, "%Y-%m-%d")
                TradeExpiery_zerodha = TradeExpiery_zerodha.strftime("%y%b").upper()
                # print("TradeExpiery_zerodha:", TradeExpiery_zerodha)

                TradeExpiery = data['TradeExpiery']
                TradeExpiery_zerodha_weekly = Zerodhaweekly(TradeExpiery)
                # print("TradeExpiery_zerodha_weekly:", TradeExpiery_zerodha_weekly)

                ExpieryContract = data['ExpieryContract']
                TradeExpieryContract = data['TradeExpieryContract']

                print("TradeExpieryContract:", TradeExpieryContract)

                datetime_obj = datetime.strptime(TradeExpiery, "%d-%m-%Y")
                abbreviated_month = datetime_obj.strftime("%b").upper()
                TradeExpiery = f"{datetime_obj.day}{abbreviated_month}{datetime_obj.year}"

                StrikeDistance = int(data['StrikeDistance'])
                option_contract_type = data['ContractType']
                NextEntryTime = int(data['NextEntryTime'])
                # print("NextEntryTime: ",type(NextEntryTime))
                current_time = datetime.now().strftime("%H:%M:%S")
                if entry_time <= datetime.strptime(current_time, "%H:%M:%S") <= exit_time:

                    if symbol == "NIFTY":
                        sym = "NIFTY 50"
                        usedltp = Zerodha_Integration.get_ltp(sym)
                        print("Instrument ltp=",usedltp )

                    if symbol == "BANKNIFTY":
                        sym = "NIFTY BANK"
                        usedltp = Zerodha_Integration.get_ltp(sym)
                        print("Instrument ltp=", usedltp)

                    if symbol == "FINNIFTY":
                        sym = "FINNIFTY"
                        usedltp = Zerodha_Integration.get_ltp(sym)
                        print("Instrument ltp=", usedltp)

                    strike = custom_round(usedltp, symbol)

                    if signal_dict[strategy_tag]['CALL'] == False and signal_dict[strategy_tag]['PUT'] == False:

                        if ExpieryContract == "MONTHLY":
                            ce_atm = Zerodha_Integration.get_sym(expiryhistorical, strike, "CE")
                            pe_atm = Zerodha_Integration.get_sym(expiryhistorical, strike, "CE")

                            ce_token = Zerodha_Integration.get_instrument_token(ce_atm, expiryhistorical, strike, "CE")
                            pe_token = Zerodha_Integration.get_instrument_token(pe_atm, expiryhistorical, strike, "PE")
                            signal_dict[strategy_tag]["processdata_call"] = ce_atm
                            signal_dict[strategy_tag]["processdata_put"] = pe_atm
                            signal_dict[strategy_tag]["ce_token"] = ce_token
                            signal_dict[strategy_tag]["pe_token"] = pe_token

                        if ExpieryContract == "WEEKLY":
                            ce_atm = Zerodha_Integration.get_sym(expiryhistorical, strike, "CE")
                            pe_atm = Zerodha_Integration.get_sym(expiryhistorical, strike, "PE")

                            ce_token = Zerodha_Integration.get_instrument_token(ce_atm, expiryhistorical, strike, "CE")
                            pe_token = Zerodha_Integration.get_instrument_token(pe_atm, expiryhistorical, strike, "PE")
                            signal_dict[strategy_tag]["processdata_call"] = ce_atm
                            signal_dict[strategy_tag]["processdata_put"] = pe_atm
                            signal_dict[strategy_tag]["ce_token"] = ce_token
                            signal_dict[strategy_tag]["pe_token"] = pe_token

                    print("Ce ATM: ", signal_dict[strategy_tag]["processdata_call"])


                    df_ce = Zerodha_Integration.get_historical_data(sym=signal_dict[strategy_tag]["processdata_call"],
                                                                    Token=signal_dict[strategy_tag]["ce_token"],
                                                                    timeframe=timeframe, exp=expiryhistorical,
                                                                    strategy_tag=strategy_tag, type="CE", strike=strike,
                                                                    RSIPeriod=RSIPeriod, MAOFOI=MAOFOI, MAOFVOl=MAOFVOl)
                    df_pe = Zerodha_Integration.get_historical_data(sym=signal_dict[strategy_tag]["processdata_put"] ,
                                                                    Token=signal_dict[strategy_tag]["pe_token"],
                                                                    timeframe=timeframe, exp=expiryhistorical,
                                                                    strategy_tag=strategy_tag, type="PE", strike=strike,
                                                                    RSIPeriod=RSIPeriod, MAOFOI=MAOFOI, MAOFVOl=MAOFVOl)

                    # df_ce.to_csv(f'CE_{strategy_tag}.csv', index=False)
                    # df_pe.to_csv(f'PE_{strategy_tag}.csv', index=False)

                    rsi_ce = df_ce['RSI'].iloc[-1]
                    vwap_ce = df_ce['VWAP'].iloc[-1]
                    maofoi_ce = df_ce['MA_OI'].iloc[-1]
                    oi_ce = df_ce['oi'].iloc[-1]
                    maofvol_ce = df_ce['MA_VOL'].iloc[-1]
                    vol_ce = df_ce['volume'].tail(n).max()
                    price_ce = df_ce['close'].iloc[-1]

                    rsi_ce_2 = df_ce['RSI'].iloc[-2]
                    vwap_ce_2 = df_ce['VWAP'].iloc[-2]
                    maofoi_ce_2 = df_ce['MA_OI'].iloc[-2]
                    oi_ce_2 = df_ce['oi'].iloc[-2]
                    maofvol_ce_2 = df_ce['MA_VOL'].iloc[-2]
                    # vol_ce_2 = df_ce['volume'].tail(n-1).max()
                    price_ce_2 = df_ce['close'].iloc[-2]

                    print("Ce ATM rsi_ce : ", rsi_ce)
                    print("Ce ATM vwap_ce : ", vwap_ce)
                    print("Ce ATM oi_ce : ", oi_ce)
                    print("Ce ATM ma_of_oi_ce : ", maofoi_ce)
                    print("Ce ATM vol_ce : ", vol_ce)
                    print("Ce ATM ma_of_vol_ce : ", maofvol_ce)
                    # rsi>60, price>vwap,oi<maofoi,vol>maofvol,

                    if (float(rsi_ce) > float(RSIValue) and float(price_ce) > float(vwap_ce) and float(oi_ce) < float(
                            maofoi_ce) and float(vol_ce) > (float(VolMultiple) * float(maofvol_ce)) and
                        signal_dict[strategy_tag]['CALL'] == False and signal_dict[strategy_tag]['PUT'] == False) and \
                            signal_dict[strategy_tag]['Trade'] == False:
                        if (float(rsi_ce_2) < float(RSIValue) or float(price_ce_2) < float(vwap_ce_2) or float(
                                oi_ce_2) > float(maofoi_ce_2)):

                            usedltp = zerodha_ltp_index(symbol)

                            if option_contract_type == "ATM":
                                strikefinal = custom_round(price=int(float(usedltp)),
                                                           symbol=data_dict[strategy_tag]['symbol'])
                                ce_strike = strikefinal
                            if option_contract_type == "ITM":
                                strikefinal = custom_round(price=int(float(usedltp)),
                                                           symbol=data_dict[strategy_tag]['symbol'])
                                ce_strike = int(strikefinal) - StrikeDistance

                            if option_contract_type == "OTM":
                                strikefinal = custom_round(price=int(float(usedltp)),
                                                           symbol=data_dict[strategy_tag]['symbol'])
                                ce_strike = int(strikefinal) + StrikeDistance

                            if TradeExpieryContract == "MONTHLY":
                                ce_strike_sym = f"{symbol}{TradeExpiery_zerodha}{ce_strike}CE"
                            if TradeExpieryContract == "WEEKLY":
                                ce_strike_sym = f"{symbol}{TradeExpiery_zerodha_weekly}{ce_strike}CE"

                            signal_dict[strategy_tag]["new_time"] = None

                            tradeprice = Zerodha_Integration.get_ltp_option(ce_strike_sym)

                            if CalculationType == "PERCENTAGE":
                                print("ltp:", tradeprice)
                                tgt1 = tgt1 * 0.01
                                tarper1 = tradeprice * tgt1
                                tgt2 = tgt2 * 0.01
                                tarper2 = tradeprice * tgt2
                                tgt3 = tgt3 * 0.01
                                tarper3 = tradeprice * tgt3
                                Sl = Sl * 0.01
                                slper = tradeprice - Sl
                                signal_dict[strategy_tag]['Sl'] = float(tradeprice) - float(slper)
                                signal_dict[strategy_tag]['Target1'] = float(tradeprice) + tarper1
                                signal_dict[strategy_tag]['Target2'] = float(tradeprice) + tarper2
                                signal_dict[strategy_tag]['Target3'] = float(tradeprice) + tarper3

                                SlMove1 = SlMove1 * 0.01
                                SlMove1per = tradeprice * SlMove1
                                SlMove2 = SlMove2 * 0.01
                                SlMov21per = tradeprice * SlMove2

                                signal_dict[strategy_tag]['SlMove1'] = float(signal_dict[strategy_tag]['Sl']) + float(
                                    SlMove1per)
                                signal_dict[strategy_tag]['SlMove2'] = float(signal_dict[strategy_tag]['SlMove1']) + float(
                                    SlMov21per)

                            if CalculationType == "POINTS":
                                print("ltp:", tradeprice)
                                signal_dict[strategy_tag]['Target1'] = float(tradeprice) + float(tgt1)
                                signal_dict[strategy_tag]['Target2'] = float(tradeprice) + float(tgt2)
                                signal_dict[strategy_tag]['Target3'] = float(tradeprice) + float(tgt3)
                                signal_dict[strategy_tag]['Sl'] = float(tradeprice) - float(Sl)
                                print("signal_dict[strategy_tag]['Sl']: ", signal_dict[strategy_tag]['Sl'])
                                signal_dict[strategy_tag]['SlMove1'] = float(signal_dict[strategy_tag]['Sl']) + float(
                                    SlMove1)
                                signal_dict[strategy_tag]['SlMove2'] = float(signal_dict[strategy_tag]['SlMove1']) + float(
                                    SlMove2)
                                print("signal_dict[strategy_tag]['SlMove1']: ", signal_dict[strategy_tag]['SlMove1'])
                                print("signal_dict[strategy_tag]['SlMove2']: ", signal_dict[strategy_tag]['SlMove2'])

                            algofox_sym = ce_strike_sym
                            signal_dict[strategy_tag]['PreviousString'] = algofox_sym
                            signal_dict[strategy_tag]['Trade'] = True
                            signal_dict[strategy_tag]['strategytag'] = strategytag
                            signal_dict[strategy_tag]['TP1QTY'] = int(Tgt1Lotsize)
                            signal_dict[strategy_tag]['TP2QTY'] = int(Tgt2Lotsize)
                            signal_dict[strategy_tag]['TP3QTY'] = int(Tgt3Lotsize)

                            signal_dict[strategy_tag]['CALL'] = True
                            signal_dict[strategy_tag]['PUT'] = False

                            signal_dict[strategy_tag]['T1'] = True
                            signal_dict[strategy_tag]['T2'] = True
                            signal_dict[strategy_tag]['T3'] = True
                            signal_dict[strategy_tag]['S'] = True
                            signal_dict[strategy_tag]['EXITQTY'] = EntryLotsize
                            algofox_sym = f"{symbol}|{TradeExpiery}|{ce_strike}|CE"
                            orderlog = f"{timestamp} Buy order executed in {algofox_sym} for lots: {signal_dict[strategy_tag]['EXITQTY']} @ price {tradeprice},Tp1= {signal_dict[strategy_tag]['Target1']}, Tp2= {signal_dict[strategy_tag]['Target2']}, Tp3= {signal_dict[strategy_tag]['Target3']}, Stoploss={signal_dict[strategy_tag]['Sl']} "
                            print(orderlog)
                            write_to_order_logs(orderlog)
                            algofox_sym = f"{symbol}|{TradeExpiery}|{ce_strike}|CE"
                            signal_dict[strategy_tag]["alfoxsymbol"]: algofox_sym
                            Algofox.Buy_order_algofox(symbol=algofox_sym, quantity=int(EntryLotsize),
                                                      instrumentType="OPTIDX",
                                                      direction="BUY",
                                                      product="MIS", strategy=stg, order_typ=OrderType,
                                                      price=float(tradeprice),
                                                      username=Algofoxid, password=Algofoxpassword, role=role)

                    print("Pe ATM: ", signal_dict[strategy_tag]["processdata_put"])

                    rsi_pe = df_pe['RSI'].iloc[-1]
                    vwap_pe = df_pe['VWAP'].iloc[-1]
                    maofoi_pe = df_pe['MA_OI'].iloc[-1]
                    oi_pe = df_pe['oi'].iloc[-1]
                    maofvol_pe = df_pe['MA_VOL'].iloc[-1]
                    vol_pe = df_pe['volume'].tail(n).max()


                    rsi_pe_2 = df_pe['RSI'].iloc[-2]
                    vwap_pe_2 = df_pe['VWAP'].iloc[-2]
                    maofoi_pe_2 = df_pe['MA_OI'].iloc[-2]
                    oi_pe_2 = df_pe['oi'].iloc[-2]
                    maofvol_pe_2 = df_pe['MA_VOL'].iloc[-2]
                    vol_pe_2 = df_pe['volume'].tail(n).max()
                    price_pe_2 = df_pe['close'].iloc[-2]

                    print("Pe ATM rsi_pe : ", rsi_pe)
                    print("Pe ATM vwap_pe : ", vwap_pe)
                    print("Pe ATM oi_pe : ", oi_pe)
                    print("Pe ATM ma_of_oi_pe : ", maofoi_pe)
                    print("Pe ATM vol_pe : ", vol_pe)
                    print("Pe ATM ma_of_vol_pe : ", maofvol_pe)

                    price_pe = df_pe['close'].iloc[-1]

                    # rsi>60, price>vwap,oi<maofoi,vol>maofvol,
                    if (float(rsi_pe) > float(RSIValue) and float(price_pe) > float(vwap_pe) and float(oi_pe) < float(
                            maofoi_pe) and float(vol_pe) > (float(VolMultiple) * float(maofvol_pe)) and
                            signal_dict[strategy_tag]['CALL'] == False and signal_dict[strategy_tag]['PUT'] == False and
                            signal_dict[strategy_tag]['Trade'] == False):
                        if float(rsi_pe_2) < float(RSIValue) or float(price_pe_2) < float(vwap_pe_2) or float(
                                oi_pe_2) > float(maofoi_pe_2):
                            usedltp = zerodha_ltp_index(symbol)
                            print("usedltp:", usedltp)
                            if option_contract_type == "ATM":
                                strikefinal = custom_round(price=int(float(usedltp)),
                                                           symbol=data_dict[strategy_tag]['symbol'])
                                pe_strike = strikefinal
                            if option_contract_type == "ITM":
                                strikefinal = custom_round(price=int(float(usedltp)),
                                                           symbol=data_dict[strategy_tag]['symbol'])
                                pe_strike = int(strikefinal) + StrikeDistance

                            if option_contract_type == "OTM":
                                strikefinal = custom_round(price=int(float(usedltp)),
                                                           symbol=data_dict[strategy_tag]['symbol'])
                                pe_strike = int(strikefinal) - StrikeDistance

                            if TradeExpieryContract == "MONTHLY":
                                pe_strike_sym = f"{symbol}{TradeExpiery_zerodha}{pe_strike}PE"
                            if TradeExpieryContract == "WEEKLY":
                                pe_strike_sym = f"{symbol}{TradeExpiery_zerodha_weekly}{pe_strike}PE"
                            signal_dict[strategy_tag]["new_time"] = None

                            tradeprice = Zerodha_Integration.get_ltp_option(pe_strike_sym)

                            if CalculationType == "PERCENTAGE":
                                print("ltp:", tradeprice)
                                tgt1 = tgt1 * 0.01
                                tarper1 = tradeprice * tgt1
                                tgt2 = tgt2 * 0.01
                                tarper2 = tradeprice * tgt2
                                tgt3 = tgt3 * 0.01
                                tarper3 = tradeprice * tgt3
                                Sl = Sl * 0.01
                                slper = tradeprice - Sl
                                signal_dict[strategy_tag]['Sl'] = float(tradeprice) - float(slper)
                                signal_dict[strategy_tag]['Target1'] = float(tradeprice) + tarper1
                                signal_dict[strategy_tag]['Target2'] = float(tradeprice) + tarper2
                                signal_dict[strategy_tag]['Target3'] = float(tradeprice) + tarper3

                                SlMove1 = SlMove1 * 0.01
                                SlMove1per = tradeprice * SlMove1
                                SlMove2 = SlMove2 * 0.01
                                SlMov21per = tradeprice * SlMove2

                                signal_dict[strategy_tag]['SlMove1'] = float(signal_dict[strategy_tag]['Sl']) + float(
                                    SlMove1per)
                                signal_dict[strategy_tag]['SlMove2'] = float(signal_dict[strategy_tag]['SlMove1']) + float(
                                    SlMov21per)

                            if CalculationType == "POINTS":
                                print("ltp:", tradeprice)
                                signal_dict[strategy_tag]['Target1'] = float(tradeprice) + float(tgt1)
                                signal_dict[strategy_tag]['Target2'] = float(tradeprice) + float(tgt2)
                                signal_dict[strategy_tag]['Target3'] = float(tradeprice) + float(tgt3)
                                signal_dict[strategy_tag]['Sl'] = float(tradeprice) - float(Sl)
                                print("signal_dict[strategy_tag]['Sl']: ", signal_dict[strategy_tag]['Sl'])
                                signal_dict[strategy_tag]['SlMove1'] = float(signal_dict[strategy_tag]['Sl']) + float(
                                    SlMove1)
                                print("signal_dict[strategy_tag]['SlMove1']: ", signal_dict[strategy_tag]['SlMove1'])
                                signal_dict[strategy_tag]['SlMove2'] = float(signal_dict[strategy_tag]['SlMove1']) + float(
                                    SlMove2)
                                print("signal_dict[strategy_tag]['SlMove2']: ", signal_dict[strategy_tag]['SlMove2'])

                            signal_dict[strategy_tag]['TP1QTY'] = int(Tgt1Lotsize)
                            signal_dict[strategy_tag]['strategytag'] = strategytag
                            signal_dict[strategy_tag]['TP2QTY'] = int(Tgt2Lotsize)
                            signal_dict[strategy_tag]['TP3QTY'] = int(Tgt3Lotsize)
                            signal_dict[strategy_tag]['T1'] = True
                            signal_dict[strategy_tag]['T2'] = True
                            signal_dict[strategy_tag]['T3'] = True
                            signal_dict[strategy_tag]['S'] = True
                            algofox_sym = pe_strike_sym
                            signal_dict[strategy_tag]['PreviousString'] = algofox_sym
                            signal_dict[strategy_tag]['CALL'] = False
                            signal_dict[strategy_tag]['PUT'] = True
                            signal_dict[strategy_tag]["EXITQTY"] = EntryLotsize
                            signal_dict[strategy_tag]['Trade'] = True
                            algofox_sym = f"{symbol}|{TradeExpiery}|{pe_strike}|PE"
                            orderlog = f"{timestamp} Buy order executed in {algofox_sym} for lots: {signal_dict[strategy_tag]['EXITQTY']}  @ price {tradeprice},Tp1= {signal_dict[strategy_tag]['Target1']}, Tp2= {signal_dict[strategy_tag]['Target2']}, Tp3= {signal_dict[strategy_tag]['Target3']}, Stoploss={signal_dict[strategy_tag]['Sl']}"
                            print(orderlog)
                            write_to_order_logs(orderlog)
                            algofox_sym = f"{symbol}|{TradeExpiery}|{pe_strike}|PE"
                            signal_dict[strategy_tag]["alfoxsymbol"]: algofox_sym
                            Algofox.Buy_order_algofox(symbol=algofox_sym, quantity=int(EntryLotsize),
                                                      instrumentType="OPTIDX",
                                                      direction="BUY",
                                                      product="MIS", strategy=stg, order_typ=OrderType,
                                                      price=float(tradeprice),
                                                      username=Algofoxid, password=Algofoxpassword, role=role)
    except Exception as e:

        traceback.print_exc()

        print(f"Error in function: {e}")

        return


def tp_and_sl(signal_dict):
    global NextEntryTime, Algofoxid, Algofoxpassword, role
    timestamp = datetime.now()
    timestamp = timestamp.strftime("%d/%m/%Y %H:%M:%S")
    try:
        for strategy_tag, data in data_dict.items():
            if pd.notna(strategy_tag):
                timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
                current_time = datetime.now().time()
                stop_time_str = signal_dict[strategy_tag]['StopTime']

                if stop_time_str is not None:
                    stop_time = signal_dict[strategy_tag]['StopTime'].time()

                    # print("stoptime",stop_time)

            #  call cycle
                    if signal_dict[strategy_tag]['T3'] == True and signal_dict[strategy_tag]['S'] == True and current_time > stop_time and signal_dict[strategy_tag]['CALL'] == True:
                            signal_dict[strategy_tag]['T3'] = False
                            signal_dict[strategy_tag]['S'] = False
                            tradeprice = Zerodha_Integration.get_ltp_option(signal_dict[strategy_tag]['PreviousString'])
                            Algofox.Sell_order_algofox(symbol=signal_dict[strategy_tag]["alfoxsymbol"],
                                                       quantity=int(signal_dict[strategy_tag]["EXITQTY"]),
                                                       instrumentType="OPTIDX",
                                                       direction="SELL", product="MIS",
                                                       strategy=signal_dict[strategy_tag]['strategytag'],
                                                       order_typ="MARKET", price=tradeprice, username=Algofoxid,
                                                       password=Algofoxpassword, role=role)
                            orderlog = f"{timestamp} Exit time acheived for  {signal_dict[strategy_tag]['PreviousString']} @ {tradeprice} ,lotsize = {signal_dict[strategy_tag]['EXITQTY']} position exited"
                            print(orderlog)
                            write_to_order_logs(orderlog)

                    if signal_dict[strategy_tag]['T3'] == True and signal_dict[strategy_tag]['S'] == True and current_time > stop_time and signal_dict[strategy_tag]['PUT'] == True:
                            signal_dict[strategy_tag]['T3'] = False
                            signal_dict[strategy_tag]['S'] = False
                            tradeprice = Zerodha_Integration.get_ltp_option(signal_dict[strategy_tag]['PreviousString'])
                            Algofox.Sell_order_algofox(symbol=signal_dict[strategy_tag]["alfoxsymbol"],
                                                       quantity=int(signal_dict[strategy_tag]["EXITQTY"]),
                                                       instrumentType="OPTIDX",
                                                       direction="SELL", product="MIS",
                                                       strategy=signal_dict[strategy_tag]['strategytag'],
                                                       order_typ="MARKET",
                                                       price=tradeprice, username=Algofoxid, password=Algofoxpassword,
                                                       role=role)
                            orderlog = f"{timestamp} Exit time acheived for  {signal_dict[strategy_tag]['PreviousString']} @ {tradeprice} ,lotsize = {signal_dict[strategy_tag]['EXITQTY']} position exited"
                            print(orderlog)
                            write_to_order_logs(orderlog)

                if signal_dict[strategy_tag]['T1'] == True and signal_dict[strategy_tag]['CALL'] == True:
                    tradeprice = Zerodha_Integration.get_ltp_option(signal_dict[strategy_tag]['PreviousString'])
                    print("tp_and_sl ltp:", tradeprice)
                    if float(tradeprice) >= float(signal_dict[strategy_tag]['Target1']):
                        Algofox.Sell_order_algofox(symbol=signal_dict[strategy_tag]["alfoxsymbol"],
                                                   quantity=int(signal_dict[strategy_tag]['TP1QTY']),
                                                   instrumentType="OPTIDX",
                                                   direction="SELL", product="MIS",
                                                   strategy=signal_dict[strategy_tag]['strategytag'],
                                                   order_typ="MARKET", price=tradeprice, username=Algofoxid,
                                                   password=Algofoxpassword, role=role)
                        signal_dict[strategy_tag]['T1'] = False
                        signal_dict[strategy_tag]['Sl'] = signal_dict[strategy_tag]['SlMove1']
                        signal_dict[strategy_tag]["EXITQTY"] = signal_dict[strategy_tag]["EXITQTY"] - \
                                                               signal_dict[strategy_tag]['TP1QTY']
                        orderlog = f"{timestamp} Target 1 executed for {signal_dict[strategy_tag]['PreviousString']} @ {tradeprice} ,lotsize = {signal_dict[strategy_tag]['TP1QTY']},newsl= {signal_dict[strategy_tag]['Sl']}"
                        print(orderlog)
                        write_to_order_logs(orderlog)

                if signal_dict[strategy_tag]['T2'] == True and signal_dict[strategy_tag]['CALL'] == True:
                    tradeprice = Zerodha_Integration.get_ltp_option(signal_dict[strategy_tag]['PreviousString'])
                    print("tp_and_sl ltp:", tradeprice)
                    if float(tradeprice) >= float(signal_dict[strategy_tag]['Target2']):
                        Algofox.Sell_order_algofox(symbol=signal_dict[strategy_tag]["alfoxsymbol"],
                                                   quantity=int(signal_dict[strategy_tag]['TP2QTY']),
                                                   instrumentType="OPTIDX",
                                                   direction="SELL", product="MIS",
                                                   strategy=signal_dict[strategy_tag]['strategytag'],
                                                   order_typ="MARKET",
                                                   price=tradeprice, username=Algofoxid,
                                                   password=Algofoxpassword, role=role)
                        signal_dict[strategy_tag]["EXITQTY"] = signal_dict[strategy_tag]["EXITQTY"] - \
                                                               signal_dict[strategy_tag]['TP2QTY']

                        signal_dict[strategy_tag]['T2'] = False
                        signal_dict[strategy_tag]['Sl'] = signal_dict[strategy_tag]['SlMove2']
                        orderlog = f"{timestamp} Target 2 executed for {signal_dict[strategy_tag]['PreviousString']} @ {tradeprice} ,lotsize = {signal_dict[strategy_tag]['TP2QTY']},newsl = {signal_dict[strategy_tag]['Sl']}"
                        print(orderlog)
                        write_to_order_logs(orderlog)

                if signal_dict[strategy_tag]['T3'] == True and signal_dict[strategy_tag]['CALL'] == True:
                    tradeprice = Zerodha_Integration.get_ltp_option(signal_dict[strategy_tag]['PreviousString'])
                    print("tp_and_sl ltp:", tradeprice)
                    if float(tradeprice) >= float(signal_dict[strategy_tag]['Target3']):
                        Algofox.Sell_order_algofox(symbol=signal_dict[strategy_tag]["alfoxsymbol"],
                                                   quantity=int(signal_dict[strategy_tag]['TP3QTY']),
                                                   instrumentType="OPTIDX",
                                                   direction="SELL", product="MIS",
                                                   strategy=signal_dict[strategy_tag]['strategytag'],
                                                   order_typ="MARKET",
                                                   price=tradeprice, username=Algofoxid, password=Algofoxpassword,
                                                   role=role)

                        signal_dict[strategy_tag]["EXITQTY"] = 0

                        current_datetime = datetime.now()
                        last_trade_time = current_datetime.time()
                        new_time_seconds = last_trade_time.hour * 3600 + last_trade_time.minute * 60 + NextEntryTime * 60
                        new_time_minutes = (new_time_seconds // 60 + 4) // 5 * 5
                        new_time_seconds = new_time_minutes * 60
                        new_time = datetime.utcfromtimestamp(new_time_seconds).time()
                        # last_trade_time = current_datetime.time()
                        # new_time_seconds = last_trade_time.hour * 3600 + last_trade_time.minute * 60 + NextEntryTime * 60
                        # new_time = datetime.utcfromtimestamp(new_time_seconds).time()
                        signal_dict[strategy_tag]["new_time"] = new_time
                        signal_dict[strategy_tag]['S'] = False
                        signal_dict[strategy_tag]['T3'] = False
                        orderlog = f"{timestamp} Target 3 executed for {signal_dict[strategy_tag]['PreviousString']} @ {tradeprice} ,lotsize = {signal_dict[strategy_tag]['TP3QTY']} , next trade time= {signal_dict[strategy_tag]['new_time']}"
                        print(orderlog)
                        write_to_order_logs(orderlog)

                if signal_dict[strategy_tag]['S'] == True and signal_dict[strategy_tag]['CALL'] == True:
                    tradeprice = Zerodha_Integration.get_ltp_option(signal_dict[strategy_tag]['PreviousString'])
                    print("tp_and_sl ltp:", tradeprice)
                    if float(tradeprice) <= float(signal_dict[strategy_tag]['Sl']):
                        Algofox.Sell_order_algofox(symbol=signal_dict[strategy_tag]["alfoxsymbol"],
                                                   quantity=int(signal_dict[strategy_tag]["EXITQTY"]),
                                                   instrumentType="OPTIDX",
                                                   direction="SELL", product="MIS",
                                                   strategy=signal_dict[strategy_tag]['strategytag'],
                                                   order_typ="MARKET",
                                                   price=tradeprice, username=Algofoxid, password=Algofoxpassword,
                                                   role=role)

                        signal_dict[strategy_tag]["EXITQTY"] = 0
                        signal_dict[strategy_tag]['S'] = False
                        signal_dict[strategy_tag]['T3'] = False
                        current_datetime = datetime.now()
                        last_trade_time = current_datetime.time()
                        new_time_seconds = last_trade_time.hour * 3600 + last_trade_time.minute * 60 + NextEntryTime * 60
                        new_time_minutes = (new_time_seconds // 60 + 4) // 5 * 5
                        new_time_seconds = new_time_minutes * 60
                        new_time = datetime.utcfromtimestamp(new_time_seconds).time()
                        # last_trade_time = current_datetime.time()
                        # new_time_seconds = last_trade_time.hour * 3600 + last_trade_time.minute * 60 + NextEntryTime * 60
                        # new_time = datetime.utcfromtimestamp(new_time_seconds).time()
                        signal_dict[strategy_tag]["new_time"] = new_time
                        orderlog = f"{timestamp} Stoploss executed for {signal_dict[strategy_tag]['PreviousString']} @ {tradeprice} , next trade time= {signal_dict[strategy_tag]['new_time']} "
                        print(orderlog)
                        write_to_order_logs(orderlog)

                        #  PUT  cycle
                if signal_dict[strategy_tag]['T1'] == True and signal_dict[strategy_tag]['PUT'] == True:
                    tradeprice = Zerodha_Integration.get_ltp_option(signal_dict[strategy_tag]['PreviousString'])
                    print("tp_and_sl ltp:", tradeprice)
                    if float(tradeprice) >= float(signal_dict[strategy_tag]['Target1']):
                        Algofox.Sell_order_algofox(symbol=signal_dict[strategy_tag]["alfoxsymbol"],
                                                   quantity=int(signal_dict[strategy_tag]['TP1QTY']),
                                                   instrumentType="OPTIDX",
                                                   direction="SELL", product="MIS",
                                                   strategy=signal_dict[strategy_tag]['strategytag'],
                                                   order_typ="MARKET",
                                                   price=tradeprice, username=Algofoxid, password=Algofoxpassword,
                                                   role=role)
                        signal_dict[strategy_tag]["EXITQTY"] = signal_dict[strategy_tag]["EXITQTY"] - \
                                                               signal_dict[strategy_tag]['TP1QTY']
                        signal_dict[strategy_tag]['T1'] = False
                        signal_dict[strategy_tag]['Sl'] = signal_dict[strategy_tag]['SlMove1']
                        orderlog = f"{timestamp} Target 1 executed for {signal_dict[strategy_tag]['PreviousString']} @ {tradeprice} ,lotsize = {signal_dict[strategy_tag]['TP1QTY']}, newsl = {signal_dict[strategy_tag]['Sl']}"
                        print(orderlog)
                        write_to_order_logs(orderlog)

                if signal_dict[strategy_tag]['T2'] == True and signal_dict[strategy_tag]['PUT'] == True:
                    tradeprice = Zerodha_Integration.get_ltp_option(signal_dict[strategy_tag]['PreviousString'])
                    print("tp_and_sl ltp:", tradeprice)
                    if float(tradeprice) >= float(signal_dict[strategy_tag]['Target2']):
                        Algofox.Sell_order_algofox(symbol=signal_dict[strategy_tag]["alfoxsymbol"],
                                                   quantity=int(signal_dict[strategy_tag]['TP2QTY']),
                                                   instrumentType="OPTIDX",
                                                   direction="SELL", product="MIS",
                                                   strategy=signal_dict[strategy_tag]['strategytag'],
                                                   order_typ="MARKET",
                                                   price=tradeprice, username=Algofoxid, password=Algofoxpassword,
                                                   role=role)
                        signal_dict[strategy_tag]["EXITQTY"] = signal_dict[strategy_tag]["EXITQTY"] - \
                                                               signal_dict[strategy_tag]['TP2QTY']

                        signal_dict[strategy_tag]['T2'] = False
                        signal_dict[strategy_tag]['Sl'] = signal_dict[strategy_tag]['SlMove2']
                        orderlog = f"{timestamp} Target 2 executed for {signal_dict[strategy_tag]['PreviousString']} @ {tradeprice} ,lotsize = {signal_dict[strategy_tag]['TP2QTY']}, newsl = {signal_dict[strategy_tag]['Sl']}"
                        print(orderlog)
                        write_to_order_logs(orderlog)

                if signal_dict[strategy_tag]['T3'] == True and signal_dict[strategy_tag]['PUT'] == True:
                    tradeprice = Zerodha_Integration.get_ltp_option(signal_dict[strategy_tag]['PreviousString'])
                    print("tp_and_sl ltp:", tradeprice)
                    if float(tradeprice) >= float(signal_dict[strategy_tag]['Target3']):
                        Algofox.Sell_order_algofox(symbol=signal_dict[strategy_tag]["alfoxsymbol"],
                                                   quantity=int(signal_dict[strategy_tag]['TP3QTY']),
                                                   instrumentType="OPTIDX",
                                                   direction="SELL", product="MIS",
                                                   strategy=signal_dict[strategy_tag]['strategytag'],
                                                   order_typ="MARKET",
                                                   price=tradeprice, username=Algofoxid, password=Algofoxpassword,
                                                   role=role)

                        signal_dict[strategy_tag]["EXITQTY"] = 0
                        current_datetime = datetime.now()
                        last_trade_time = current_datetime.time()
                        new_time_seconds = last_trade_time.hour * 3600 + last_trade_time.minute * 60 + NextEntryTime * 60
                        new_time_minutes = (new_time_seconds // 60 + 4) // 5 * 5
                        new_time_seconds = new_time_minutes * 60
                        new_time = datetime.utcfromtimestamp(new_time_seconds).time()
                        signal_dict[strategy_tag]["new_time"] = new_time

                        signal_dict[strategy_tag]['T3'] = False
                        signal_dict[strategy_tag]['S'] = False
                        signal_dict[strategy_tag]['Sl'] = signal_dict[strategy_tag]['SlMove2']
                        orderlog = f"{timestamp} Target 3 executed for {signal_dict[strategy_tag]['PreviousString']} @ {tradeprice} ,lotsize = {signal_dict[strategy_tag]['TP3QTY']} , next trade time= {signal_dict[strategy_tag]['new_time']}"
                        print(orderlog)
                        write_to_order_logs(orderlog)

                if signal_dict[strategy_tag]['S'] == True and signal_dict[strategy_tag]['PUT'] == True:
                    tradeprice = Zerodha_Integration.get_ltp_option(signal_dict[strategy_tag]['PreviousString'])
                    print("tp_and_sl ltp:", tradeprice)
                    if float(tradeprice) <= float(signal_dict[strategy_tag]['Sl']):
                        Algofox.Sell_order_algofox(symbol=signal_dict[strategy_tag]["alfoxsymbol"],
                                                   quantity=int(signal_dict[strategy_tag]['EXITQTY']),
                                                   instrumentType="OPTIDX",
                                                   direction="SELL", product="MIS",
                                                   strategy=signal_dict[strategy_tag]['strategytag'],
                                                   order_typ="MARKET",
                                                   price=tradeprice, username=Algofoxid, password=Algofoxpassword,
                                                   role=role)
                        signal_dict[strategy_tag]["EXITQTY"] = 0
                        current_datetime = datetime.now()
                        last_trade_time = current_datetime.time()
                        new_time_seconds = last_trade_time.hour * 3600 + last_trade_time.minute * 60 + NextEntryTime * 60
                        new_time_minutes = (new_time_seconds // 60 + 4) // 5 * 5
                        new_time_seconds = new_time_minutes * 60
                        new_time = datetime.utcfromtimestamp(new_time_seconds).time()
                        signal_dict[strategy_tag]["new_time"] = new_time

                        signal_dict[strategy_tag]['S'] = False
                        signal_dict[strategy_tag]['T3'] = False
                        orderlog = f"{timestamp} Stoploss executed for {signal_dict[strategy_tag]['PreviousString']} @ {tradeprice}, next trade time= {signal_dict[strategy_tag]['new_time']} "
                        print(orderlog)
                        write_to_order_logs(orderlog)

                if signal_dict[strategy_tag]['S'] == False and signal_dict[strategy_tag]['T3'] == False and \
                        signal_dict[strategy_tag]["new_time"] != None and current_time >= signal_dict[strategy_tag][
                    "new_time"] and signal_dict[strategy_tag]['Trade'] == True:
                    signal_dict[strategy_tag]["new_time"] = None
                    signal_dict[strategy_tag]['Trade'] = False
                    signal_dict[strategy_tag]['PreviousString'] = None
                    signal_dict[strategy_tag]['CALL'] = False
                    signal_dict[strategy_tag]['PUT'] = False
                    signal_dict[strategy_tag]["runonce"] = False
                    orderlog = f"{timestamp} Next Trade time acheived Strategy can open new tyrades now @ {signal_dict[strategy_tag]['PreviousString']}"
                    print(orderlog)
                    write_to_order_logs(orderlog)
    except Exception as e:

        # Handle the exception for the entire function, print the error, and continue or return as needed

        traceback.print_exc()

        print(f"Error in function: {e}")

        return


def run_process_data():
    global end_time_str, start_time_str
    timestamp = datetime.now()
    timestamp = timestamp.strftime("%d/%m/%Y %H:%M:%S")
    current_time = datetime.now().time()

    with lock:
        process_data(data_dict)


def schedule_process_data():
    scheduler = BackgroundScheduler()
    scheduler.add_job(run_process_data, 'interval', minutes=1)
    scheduler.start()


if __name__ == "__main__":
    now = datetime.now()
    seconds_until_next_minute = 60 - now.second

    initial_delay = seconds_until_next_minute + 1

    schedule_process_data()

    try:
        while True:
            timestamp = datetime.now()
            timestamp = timestamp.strftime("%d/%m/%Y %H:%M:%S")
            current_time = datetime.now().time()
            with lock:
                tp_and_sl(signal_dict)
            sleep_time.sleep(1)  # Keep the main thread alive
    except (KeyboardInterrupt, SystemExit):

        pass  # Gracefully exit the loop when interrupted
















