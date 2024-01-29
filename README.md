# Overview
This script generates 3 detailed reports on historical data and performance trends for stocks in a specified watchlist in the .env file.
The three files rendered are:
1. all company info available from yfinance for each stock in the watchlist.
2. historical data for each stock in the watchlist dating back to 2010.
3. a data table with the following columns for each stock in the watchlist (for buy/sell/hold analysis and use with ML models):
    '''symbol,trade_signal,macd_signal,rsi,macd,last closing_price,last_opening_price,bollinger_upper,bollinger_lower,last_trading_day_change_$,last_trading_day_change_%,change_2d_$,change_2d_%,change_3d_$,change_3d_%,change_4d_$,change_4d_%,change_5d_$,change_5d_%,change_6d_$,change_6d_%,change_7d_$,change_7d_%,change_8d_$,change_8d_%,change_9d_$,change_9d_%,change_10d_$,change_10d_%,change_11d_$,change_11d_%,change_12d_$,change_12d_%,change_13d_$,change_13d_%,change_14d_$,change_14d_%,change_30d_$,change_30d_%,change_60d_$,change_60d_%,change_90d_$,change_90d_%,change_180d_$,change_180d_%,change_365d_$,change_365d_%,change_730d_$,change_730d_%,change_1095d_$,change_1095d_%,change_1460d_$,change_1460d_%,change_1825d_$,change_1825d_%,change_3650d_$,change_3650d_%,last_trading_day_high,last_trading_day_low,3_day_high,3_day_low,4_day_high,4_day_low,5_day_high,5_day_low,6_day_high,6_day_low,7_day_high,7_day_low,8_day_high,8_day_low,9_day_high,9_day_low,10_day_high,10_day_low,11_day_high,11_day_low,12_day_high,12_day_low,13_day_high,13_day_low,14_day_high,14_day_low,21_day_high,21_day_low,30_day_high,30_day_low,60_day_high,60_day_low,90_day_high,90_day_low,180_day_high,180_day_low,365_day_high,365_day_low,730_day_high,730_day_low,1095_day_high,1095_day_low,1460_day_high,1460_day_low,1825_day_high,1825_day_low,3650_day_high,3650_day_low,moving_average_03,moving_average_04,moving_average_05,moving_average_06,moving_average_07,moving_average_08,moving_average_09,moving_average_10,moving_average_11,moving_average_12,moving_average_13,moving_average_14,moving_average_21,moving_average_30,moving_average_40,moving_average_50,moving_average_60,moving_average_100,moving_average_200,moving_average_300,volume_trend_03d,volume_trend_04d,volume_trend_05d,volume_trend_06d,volume_trend_07d,volume_trend_08d,volume_trend_09d,volume_trend_10d,volume_trend_11d,volume_trend_12d,volume_trend_13d,volume_trend_14d,volume_trend_21d,volume_trend_30d,volume_trend_40d,volume_trend_50d,volume_trend_60d,volume_trend_90d,atr_2d,atr_3d,atr_4d,atr_5d,atr_6d,atr_7d,atr_8d,atr_9d,atr_10d,atr_11d,atr_12d,atr_13d,atr_14d,atr_21,atr_30d,atr_60d,atr_90d,atr_180d,eps,pe_ratio,dividend_yield'''

# Features
Analyzes historical data of selected stocks.
Evaluates time-series performance for each stock in the watchlist.
Allows customization of the watchlist based on user preferences.
Saves reports automatically for user convenience.

# Setup
Requirements
Python 3.11
pip

# Installation Steps
Clone or download the script to your local system.
Install necessary dependencies:
pip install -r requirements.txt

# Configuration
Define your stock watchlist in an environment variable USER_STOCK_WATCH_LIST, formatted as a comma-separated list of stock symbols.

# Execution
Run the script from your command line within the script's directory.

# Output
Reports are saved in a designated directory, typically app_generated_files within the project structure, with timestamps for easy reference.

For more information on usage and customization, refer to the inline comments within the script.