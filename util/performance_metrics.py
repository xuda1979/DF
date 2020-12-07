import numpy as np
import statsmodels.api as sm
import yfinance as yf

class Performance:
    def __init__(self, result, c):
        result_n = result['Net Profit'].to_numpy()
        daily_return_average = np.mean(result_n) - 2*c
        std = np.std(result_n -2*c)
        sharp = 15.8*daily_return_average/std
        print("Daily mean return: ", daily_return_average)
        print("Sharp ratio: ", sharp)
        
class VIX:
    def __init__(self, result, ohlc: str):
        vix=yf.download('^VIX')
        result_vix = result.join(vix, on=['Date'])
        mod = sm.OLS(result_vix['Net Profit'], sm.add_constant(result_vix[ohlc]))
        res = mod.fit()
        print(res.summary())