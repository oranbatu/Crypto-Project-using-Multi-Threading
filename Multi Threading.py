mport ccxt
import asyncio
import csv
import datetime
import os
import threading
import time
import pandas as pd
import numpy as np
import mysql.connector

exchange = ccxt.binance({'enableRateLimit': True})


def write_to_mysql_5m(result):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data_with_timestamp = [timestamp] + result
    try:
        # Connect to MySQL database
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="xxxxxxxxxx",
            database="xxxxxxxxx"
        )
        # Create a cursor
        cursor = connection.cursor()
        # Prepare the INSERT statement
        insert_query = "INSERT INTO results_1m_5m (time, symbol, symbol_incr, corr_pair, is_cp_increased, cp_incr) VALUES (%s, %s, %s, %s, %s, %s)"
        # Execute the INSERT statement
        cursor.execute(insert_query, tuple(data_with_timestamp))
        # Commit the changes
        connection.commit()
        # Close the cursor and connection
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"An error occurred while writing to MySQL: {e}")

def write_to_mysql_30m(result):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data_with_timestamp = [timestamp] + result
    try:
        # Connect to MySQL database
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="armada1997",
            database="crypto_db"
        )
        # Create a cursor
        cursor = connection.cursor()
        # Prepare the INSERT statement
        insert_query = "INSERT INTO results_1m_30m (time, symbol, symbol_incr, corr_pair, is_cp_increased, cp_incr) VALUES (%s, %s, %s, %s, %s, %s)"
        # Execute the INSERT statement
        cursor.execute(insert_query, tuple(data_with_timestamp))
        # Commit the changes
        connection.commit()
        # Close the cursor and connection
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"An error occurred while writing to MySQL: {e}")

def write_to_mysql_60m(result):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    data_with_timestamp = [timestamp] + result
    try:
        # Connect to MySQL database
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="armada1997",
            database="crypto_db"
        )
        # Create a cursor
        cursor = connection.cursor()
        # Prepare the INSERT statement
        insert_query = "INSERT INTO results_1m_60m (time, symbol, symbol_incr, corr_pair, is_cp_increased, cp_incr) VALUES (%s, %s, %s, %s, %s, %s)"
        # Execute the INSERT statement
        cursor.execute(insert_query, tuple(data_with_timestamp))
        # Commit the changes
        connection.commit()
        # Close the cursor and connection
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"An error occurred while writing to MySQL: {e}")


def fetch_candle_data(symbol,interval):
    candles = exchange.fetch_ohlcv(symbol,interval,limit=2)
    second_to_last = candles[-2]
    close_price = second_to_last[-4]
    open_price = second_to_last[1]
    increase = (close_price-open_price)/open_price

    index = symbols.index(symbol)
    correlated_pair = correlated_pairs[index]

    if increase >= percentage:
        print(f"Time: {now} Signal: {symbol} has increased by {increase * 100:.2f}%. It's correlated pair is {correlated_pair}")
        intervals = [(interval2,300),(interval3,1800),(interval4,3600)]
        for intv, breaktime in intervals:
            t = threading.Thread(target=fetch_candle_data_thread, args= (correlated_pair,intv,breaktime,increase))
            t.start()

    elif increase <= -percentage:
        print(f"Time: {now} Signal: {symbol} has decreased by {increase * 100:.2f}%. It's correlated pair is {correlated_pair}")
        intervals = [(interval2,300),(interval3,1800),(interval4,3600)]
        for intv, breaktime in intervals:
            t = threading.Thread(target=fetch_candle_data_thread, args= (correlated_pair,intv,breaktime,increase))
            t.start()

def fetch_candle_data_thread(correlated_pair, interval, breaktime, increase):
    time.sleep(breaktime)
    increase = increase
    try:
        retries = 3  # Number of retries
        while retries > 0:
            try:
                candles2 = exchange.fetch_ohlcv(correlated_pair, interval, limit=2)
                second_to_last2 = candles2[-2]
                close_price2 = second_to_last2[4]
                open_price2 = second_to_last2[1]
                increase2 = (close_price2 - open_price2) / open_price2
                result2 = [open_price2, close_price2, increase2 * 100]

                index2 = correlated_pairs.index(correlated_pair)
                symbol = symbols[index2]

                if increase > 0:
                    if increase2 >= increase * 0.5:
                        is_cp_increased = True
                        result3 = [symbol, np.round(increase, 3), correlated_pair, is_cp_increased,
                                   np.round(increase2, 3)]
                        if interval == interval2:
                            write_to_mysql_5m(result=result3)
                        elif interval == interval3:
                            write_to_mysql_30m(result=result3)
                        elif interval == interval4:
                            write_to_mysql_60m(result=result3)
                    else:
                        is_cp_increased2 = False
                        result4 = [symbol, np.round(increase, 3), correlated_pair, is_cp_increased2,
                                   np.round(increase2, 3)]
                        if interval == interval2:
                            write_to_mysql_5m(result=result4)
                        elif interval == interval3:
                            write_to_mysql_30m(result=result4)
                        elif interval == interval4:
                            write_to_mysql_60m(result=result4)

                elif increase < 0:
                    if increase2 <= increase * 0.5:
                        is_cp_decreased = True
                        result5 = [symbol, np.round(increase, 3), correlated_pair, is_cp_decreased,
                                   np.round(increase2, 3)]
                        if interval == interval2:
                            write_to_mysql_5m(result=result5)
                        elif interval == interval3:
                            write_to_mysql_30m(result=result5)
                        elif interval == interval4:
                            write_to_mysql_60m(result=result5)
                    else:
                        is_cp_decreased2 = False
                        result6 = [symbol, np.round(increase, 3), correlated_pair, is_cp_decreased2,
                                   np.round(increase2, 3)]
                        if interval == interval2:
                            write_to_mysql_5m(result=result6)
                        elif interval == interval3:
                            write_to_mysql_30m(result=result6)
                        elif interval == interval4:
                            write_to_mysql_60m(result=result6)

                break  # Exit the retry loop if the API call is successful

            except Exception as e:
                retries -= 1
                if retries == 0:
                    print(f"An error occurred while fetching candle data: {e}")
                else:
                    print(f"Retrying API call for correlated pair {correlated_pair}...")
                    time.sleep(1)  # Wait for 1 second before retrying

        print(f"{interval} Results for {correlated_pair} at time {now}: {result2}")

    except Exception as e:
        print(f"An error occurred while fetching candle data: {e}")


while True:
    symbols = ["CVP/USDT", "BOND/USDT", "HOOK/USDT", "HFT/USDT", "PHB/BUSD", "QUICK/USDT", "SANTOS/BUSD",
                   "COCOS/USDT",
                   "MINA/USDT", "FET/USDT", "MANA/USDT",
                   "AXS/USDT", "ONE/USDT", "AVAX/USDT", "LOKA/USDT", "ETH/USDT"]  # symbols for the crypto pairs
    correlated_pairs = ["DF/USDT", "WINGUSDT", "MAGIC/USDT", "HOOK/USDT", "BTC/USDT" , "FARM/USDT", "LAZIO/BUSD",
                            "COS/USDT",
                            "LRC/USDT", "OCEAN/USDT", "SAND/USDT"
            ,"SLPUSDT", "CELR/USDT", "JOE/USDT", "VOXEL/USDT", "AMB/USDT"]  # correlated pairs
    interval = "1m"
    interval2 = "5m"    #Time frame of the candles
    interval3 = "30m"
    interval4 = "60m"
    percentage = 0.00005 # threshold for the increase
    # Get the current time and calculate the number of seconds remaining until the next minute

    now = datetime.datetime.now()
    remaining_seconds = 60 - now.second
    columns = ["Time","Symbol","Symbol_increased","Correlated_pair","CP_Increased","How_many_times"]

    filename_5m = f"/Users/batuhanoran/Desktop/excel_folder/Results_1m_5m"
    filename_30m = f"/Users/batuhanoran/Desktop/excel_folder/Results_1m_30m"
    filename_60m = f"/Users/batuhanoran/Desktop/excel_folder/Results_1m_60m"
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Wait for the remaining number of seconds before executing the loop
    asyncio.get_event_loop().run_until_complete(asyncio.sleep(remaining_seconds))

    for symbol in symbols:
        index = symbols.index(symbol)
        correlated_pair = correlated_pairs[index]

        try:
            t1 = threading.Thread(target=fetch_candle_data,args=(symbol,interval))
            t1.start()
        except Exception as e:
            print(f"An error occurred while fetching candle data for symbol {symbol}: {e}")
