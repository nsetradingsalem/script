from django.db.models.query import prefetch_related_objects
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
from django.views.decorators.csrf import csrf_protect
from django.contrib.auth.models import User,auth
from django.shortcuts import redirect,render
from django.contrib import messages
from nsepython import *
import nsepython
from myapp.models import HistoryOIChange,HistoryOITotal,LiveOIChange,LiveOITotal,LiveOITotalAllSymbol,LiveEquityResult,LiveOIPercentChange,HistoryOIPercentChange, LiveSegment,EquityThree,SuperLiveSegment
from django.db.models import Count, F, Value

import pandas as pd
from truedata_ws.websocket.TD import TD
from copy import deepcopy
import time
import logging
from datetime import datetime as dt
import datetime
from nsetools import Nse
from nsepython import *
import nsepython

from datetime import timedelta
import websocket
from datetime import date

import random
from pytz import timezone


#1 Login View - Login page
@csrf_protect
def login(request):
    
    if request.method == 'POST':

        username = request.POST['username']
        password = request.POST['password']

        if User.objects.filter(username=username).exists():
            user=auth.authenticate(username=username,password=password)
            print(user)
            if user is not None:
                auth.login(request,user)
                # messages.info(request, f"You are now logged in as {username}")
                # Directing to home page
                return redirect('sample')
            else:
                messages.info(request,'incorrect password')
                return redirect('sample')
        else:
            messages.error(request,"user doesn't exists")
            return redirect('sample')

    else:
        
        return render(request,template_name = "login.html")

#2 Logout View - User Logout
def logout(request):

    auth.logout(request)
    request.session.flush()
    print("logged out")
    messages.success(request,"Successfully logged out")
    for sesskey in request.session.keys():
        del request.session[sesskey]

    return redirect('login')   

#3 Home Page - welcome page
@login_required(login_url='login')
def home(request):
    # Importing symbol list from nse and dropdown it for selection

    # import requests
    # url = 'https://www.truedata.in/downloads/symbol_lists/13.NSE_ALL_OPTIONS.txt'
    # s = requests.get(url).content
    # stringlist=[x.decode('utf-8').split('2')[0] for x in s.splitlines()]

    # fnolist = list(set(stringlist))

    # fnolist = list(LiveSegment.objects.values_list('symbol', flat=True).distinct())

    fnolist = ['AARTIIND', 'ABBOTINDIA', 'ABFRL', 'ACC', 'ADANIPORTS', 'ALKEM', 'AMARAJABAT', 'AMBUJACEM', 'APOLLOHOSP', 'APOLLOTYRE', 'ASIANPAINT', 'ASTRAL', 'ATUL', 'AUBANK', 'AUROPHARMA', 'AXISBANK', 'BAJAJ-AUTO', 'BAJAJFINSV', 'BAJFINANCE', 'BALRAMCHIN', 'BANDHANBNK', 'BATAINDIA', 'BEL', 'BERGEPAINT', 'BHARATFORG', 'BHARTIARTL', 'BIOCON', 'BOSCHLTD', 'BPCL', 'BSOFT', 'CANBK', 'CANFINHOME', 'CHAMBLFERT', 'CHOLAFIN', 'CIPLA', 'COALINDIA', 'COFORGE', 'COLPAL', 'CONCOR', 'COROMANDEL', 'CROMPTON', 'CUMMINSIND', 'DABUR', 'DALBHARAT', 'DEEPAKNTR', 'DELTACORP', 'DIVISLAB', 'DIXON', 'DLF', 'DRREDDY', 'ESCORTS', 'GLENMARK', 'GNFC', 'GODREJCP', 'GODREJPROP', 'GRANULES', 'GRASIM', 'GSPL', 'GUJGASLTD', 'HAL', 'HAVELLS', 'HCLTECH', 'HDFC', 'HDFCAMC', 'HDFCBANK', 'HDFCLIFE', 'HINDALCO', 'HINDCOPPER', 'HINDPETRO', 'HINDUNILVR', 'HONAUT', 'IBULHSGFIN', 'ICICIBANK', 'ICICIGI', 'ICICIPRULI', 'IEX', 'IGL', 'INDHOTEL', 'INDIACEM', 'INDIAMART', 'INDIGO', 'INDUSINDBK', 'INDUSTOWER', 'INFY', 'INTELLECT', 'IPCALAB', 'IRCTC', 'ITC', 'JINDALSTEL', 'JKCEMENT', 'JSWSTEEL', 'JUBLFOOD', 'KOTAKBANK', 'LALPATHLAB', 'LAURUSLABS', 'LICHSGFIN', 'LT', 'LTI', 'LTTS', 'LUPIN', 'M&MFIN', 'MARICO', 'MARUTI', 'MCDOWELL-N', 'MCX', 'MFSL', 'MGL', 'MINDTREE', 'MOTHERSON', 'MPHASIS', 'MRF', 'MUTHOOTFIN', 'NATIONALUM', 'NAUKRI', 'NAVINFLUOR', 'NMDC', 'OBEROIRLTY', 'OFSS', 'ONGC', 'PAGEIND', 'PERSISTENT', 'PETRONET', 'PIDILITIND', 'PIIND', 'POLYCAB', 'POWERGRID', 'PVR', 'RAIN', 'RAMCOCEM', 'RELIANCE', 'SBICARD', 'SBILIFE', 'SBIN', 'SHREECEM', 'SIEMENS', 'SRF', 'SRTRANSFIN', 'SUNPHARMA', 'SUNTV', 'SYNGENE', 'TATACHEM', 'TATACOMM', 'TATACONSUM', 'TATAMOTORS', 'TATAPOWER', 'TATASTEEL', 'TECHM', 'TORNTPHARM', 'TORNTPOWER', 'TRENT', 'TVSMOTOR', 'UBL', 'ULTRACEMCO', 'UPL', 'VOLTAS', 'WHIRLPOOL', 'WIPRO', 'ZEEL', 'ZYDUSLIFE']
    return render(request,"home.html",{'fnolist':fnolist})

#4 Equity Section - Calculation
def equity(request):

    # TrueDatausername = 'tdws135'
    # TrueDatapassword = 'saaral@135'

    # nse = Nse()
    # fnolist = nse.get_fno_lot_sizes()
    # symbols = list(fnolist.keys())

    # # Default production port is 8082 in the library. Other ports may be given t oyou during trial.
    # realtime_port = 8082

    # td_app = TD(TrueDatausername, TrueDatapassword, live_port=realtime_port, historical_api=False)

    # print('Starting Real Time Feed.... ')
    # print(f'Port > {realtime_port}')

    # req_ids = td_app.start_live_data(symbols)
    # live_data_objs = {}

    # liveData = {}
    # for req_id in req_ids:
    #     # print(td_app.live_data[req_id])
    #     if (td_app.live_data[req_id].ltp) == None:
    #         continue
    #     else:
    #         liveData[td_app.live_data[req_id].symbol] = [td_app.live_data[req_id].ltp,td_app.live_data[req_id].day_open,td_app.live_data[req_id].day_high,td_app.live_data[req_id].day_low,td_app.live_data[req_id].prev_day_close,dt.now(timezone("Asia/Kolkata")).strftime('%H:%M:%S')]

    # # Graceful exit
    # td_app.stop_live_data(symbols)
    # td_app.disconnect()

    # # Finding out the pastdate
    # from datetime import datetime, timedelta
    # pastDate = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
  
    # # LiveEquityResult.objects.all().delete()
    # LiveEquityResult.objects.filter(date = pastDate).delete()

    # removeList = ["NIFTY","BANKNIFTY","FINNIFTY"]

    # callcrossedset = LiveEquityResult.objects.filter(strike__contains="Call Crossed")
    # callonepercentset = LiveEquityResult.objects.filter(strike="Call 1 percent")
    # putcrossedset = LiveEquityResult.objects.filter(strike="Put Crossed")
    # putonepercentset = LiveEquityResult.objects.filter(strike="Put 1 percent")

    # opencallcross = LiveEquityResult.objects.filter(opencrossed="call")
    # openputcross = LiveEquityResult.objects.filter(opencrossed="put")

    # callcrossedsetDict = {}
    # callonepercentsetDict = {}
    # putcrossedsetDict = {}
    # putonepercentsetDict = {}
    # opencallcrossDict = {}
    # openputcrossDict = {}

    # for i in callcrossedset:
    #     callcrossedsetDict[i.symbol] = i.time
    # for i in callonepercentset:
    #     callonepercentsetDict[i.symbol] = i.time
    # for i in putcrossedset:
    #     putcrossedsetDict[i.symbol] = i.time
    # for i in putonepercentset:
    #     putonepercentsetDict[i.symbol] = i.time
    # for i in opencallcross:
    #     opencallcrossDict[i.symbol] = i.time
    # for i in openputcross:
    #     openputcrossDict[i.symbol] = i.time

    # for e in LiveOITotalAllSymbol.objects.all():
    #     print(e.symbol)
        
    #     if e.symbol in liveData and e.symbol not in removeList:

    #         # Call
    #         if liveData[e.symbol][1] > float(e.callstrike):
    #             if e.symbol in opencallcrossDict:
    #                 LiveEquityResult.objects.filter(symbol = e.symbol).delete()
    #                 callcross = LiveEquityResult(symbol=e.symbol,open=liveData[e.symbol][1],high=liveData[e.symbol][2],low=liveData[e.symbol][3],prev_day_close=liveData[e.symbol][4],ltp=liveData[e.symbol][0],strike="Call Crossed",opencrossed="call",time=opencallcrossDict[e.symbol],date=date.today())
    #                 callcross.save()
    #             else:
    #                 callcross = LiveEquityResult(symbol=e.symbol,open=liveData[e.symbol][1],high=liveData[e.symbol][2],low=liveData[e.symbol][3],prev_day_close=liveData[e.symbol][4],ltp=liveData[e.symbol][0],strike="Call Crossed",opencrossed="call",time=liveData[e.symbol][5],date=date.today())
    #                 callcross.save()
            
    #         if liveData[e.symbol][1] < float(e.putstrike):
    #             if e.symbol in openputcrossDict:
    #                 LiveEquityResult.objects.filter(symbol = e.symbol).delete()
    #                 putcross = LiveEquityResult(symbol=e.symbol,open=liveData[e.symbol][1],high=liveData[e.symbol][2],low=liveData[e.symbol][3],prev_day_close=liveData[e.symbol][4],ltp=liveData[e.symbol][0],strike="Put Crossed",opencrossed="put",time=openputcrossDict[e.symbol],date=date.today())
    #                 putcross.save()
    #             else:
    #                 putcross = LiveEquityResult(symbol=e.symbol,open=liveData[e.symbol][1],high=liveData[e.symbol][2],low=liveData[e.symbol][3],prev_day_close=liveData[e.symbol][4],ltp=liveData[e.symbol][0],strike="Put Crossed",opencrossed="put",time=liveData[e.symbol][5],date=date.today())
    #                 putcross.save()



    #         if liveData[e.symbol][0] > float(e.callstrike) or liveData[e.symbol][1] > float(e.callstrike):
    #             if e.symbol in callcrossedsetDict:
    #                 print("Yes")
    #                 # Deleting the older
    #                 LiveEquityResult.objects.filter(symbol = e.symbol).delete()
    #                 # updating latest data
    #                 print("Yes")
    #                 callcross = LiveEquityResult(symbol=e.symbol,open=liveData[e.symbol][1],high=liveData[e.symbol][2],low=liveData[e.symbol][3],prev_day_close=liveData[e.symbol][4],ltp=liveData[e.symbol][0],strike="Call Crossed",opencrossed="Nil",time=callcrossedsetDict[e.symbol],date=date.today())
    #                 callcross.save()
    #                 continue

    #             else:
    #                 print("Call crossed")
    #                 callcross = LiveEquityResult(symbol=e.symbol,open=liveData[e.symbol][1],high=liveData[e.symbol][2],low=liveData[e.symbol][3],prev_day_close=liveData[e.symbol][4],ltp=liveData[e.symbol][0],strike="Call Crossed",opencrossed="Nil",time=liveData[e.symbol][5],date=date.today())
    #                 callcross.save()
                
    #         elif liveData[e.symbol][0] >= float(e.callone) and liveData[e.symbol][0] <= float(e.callstrike):

    #             if e.symbol in callcrossedsetDict:
    #                 print("Already crossed")
    #                 continue
    #             else:
    #                 if e.symbol in callonepercentsetDict:
    #                     print("Already crossed 1 percent")
    #                     LiveEquityResult.objects.filter(symbol = e.symbol).delete()
    #                     # updating latest data
    #                     callcross = LiveEquityResult(symbol=e.symbol,open=liveData[e.symbol][1],high=liveData[e.symbol][2],low=liveData[e.symbol][3],prev_day_close=liveData[e.symbol][4],ltp=liveData[e.symbol][0],strike="Call 1 percent",opencrossed="Nil",time=callonepercentsetDict[e.symbol],date=date.today())
    #                     callcross.save()
    #                     continue
    #                 else:
    #                     print("Call 1 percent")

    #                     callone = LiveEquityResult(symbol=e.symbol,open=liveData[e.symbol][1],high=liveData[e.symbol][2],low=liveData[e.symbol][3],prev_day_close=liveData[e.symbol][4],ltp=liveData[e.symbol][0],strike="Call 1 percent",opencrossed="Nil",time=liveData[e.symbol][5],date=date.today())
    #                     callone.save()

    #         # Put
    #         elif liveData[e.symbol][0] < float(e.putstrike) or liveData[e.symbol][2] < float(e.putstrike):
    #             if e.symbol in putcrossedsetDict:
    #                 # Deleting the older
    #                 LiveEquityResult.objects.filter(symbol =e.symbol).delete()
    #                 # updating latest data
    #                 putcross = LiveEquityResult(symbol=e.symbol,open=liveData[e.symbol][1],high=liveData[e.symbol][2],low=liveData[e.symbol][3],prev_day_close=liveData[e.symbol][4],ltp=liveData[e.symbol][0],strike="Put Crossed",opencrossed="Nil",time=putcrossedsetDict[e.symbol],date=date.today())
    #                 putcross.save()
    #                 print("put crossed updating only the data")
    #                 continue
    #             else:
    #                 print("Put crossed")
    #                 putcross = LiveEquityResult(symbol=e.symbol,open=liveData[e.symbol][1],high=liveData[e.symbol][2],low=liveData[e.symbol][3],prev_day_close=liveData[e.symbol][4],ltp=liveData[e.symbol][0],strike="Put Crossed",opencrossed="Nil",time=liveData[e.symbol][5],date=date.today())
    #                 putcross.save()


    #         elif liveData[e.symbol][0] <= float(e.putone) and liveData[e.symbol][0] >= float(e.putstrike):
    #             if e.symbol in putcrossedsetDict:
    #                 print("Already crossed put")
    #                 continue
    #             else:
    #                 if e.symbol in putonepercentsetDict:
    #                     print("Already crossed 1 percent")
    #                     LiveEquityResult.objects.filter(symbol =e.symbol).delete()
    #                     # updating latest data
    #                     putcross = LiveEquityResult(symbol=e.symbol,open=liveData[e.symbol][1],high=liveData[e.symbol][2],low=liveData[e.symbol][3],prev_day_close=liveData[e.symbol][4],ltp=liveData[e.symbol][0],strike="Put 1 percent",opencrossed="Nil",time=putonepercentsetDict[e.symbol],date=date.today())
    #                     putcross.save()
    #                     continue
    #                 else:
    #                     print("Put 1 percent")
    #                     putone = LiveEquityResult(symbol=e.symbol,open=liveData[e.symbol][1],high=liveData[e.symbol][2],low=liveData[e.symbol][3],prev_day_close=liveData[e.symbol][4],ltp=liveData[e.symbol][0],strike="Put 1 percent",opencrossed="Nil",time=liveData[e.symbol][5],date=date.today())
    #                     putone.save()
        
    OITotalValue ={}
    OIChangeValue = {}
    value1 = {}
    value2 = {}
    strikeGap = {}
    three_list = list(EquityThree.objects.all().values_list('symbol', flat=True)) 
    callOnePercent = LiveEquityResult.objects.filter(strike="Call 1 percent").filter(change_perc__gte=2).order_by('-time')
    putOnePercent = LiveEquityResult.objects.filter(strike="Put 1 percent").filter(change_perc__lte=-2).order_by('-time')
    callHalfPercent = LiveEquityResult.objects.filter(strike="Call 1/2 percent").order_by('-time')
    putHalfPercent = LiveEquityResult.objects.filter(strike="Put 1/2 percent").order_by('-time')
    callCrossed_odd = LiveEquityResult.objects.annotate(odd=F('section') % 2).filter(odd=True).filter(strike="Call Crossed").order_by('-time')
    putCrossed_odd = LiveEquityResult.objects.annotate(odd=F('section') % 2).filter(odd=True).filter(strike="Put Crossed").order_by('-time')
    callCrossed_even = LiveEquityResult.objects.annotate(odd=F('section') % 2).filter(odd=False).filter(strike="Call Crossed").order_by('-time')
    putCrossed_even = LiveEquityResult.objects.annotate(odd=F('section') % 2).filter(odd=False).filter(strike="Put Crossed").order_by('-time')
    gain = LiveSegment.objects.filter(segment__in=["above"]).order_by('-change_perc')
    loss = LiveSegment.objects.filter(segment__in=["below"]).order_by('change_perc')
    calleven = LiveEquityResult.objects.annotate(odd=F('section') % 2).filter(odd=False,change_perc__gte = 3).filter(strike="Call 1/2 percent").filter(change_perc__gte=2).order_by('section')  
    callodd = LiveEquityResult.objects.annotate(odd=F('section') % 2).filter(odd=True,change_perc__gte = 3).filter(strike="Call 1/2 percent").filter(change_perc__gte=2).order_by('section') 
    puteven = LiveEquityResult.objects.annotate(odd=F('section') % 2).filter(odd=False,change_perc__lte = -3).filter(strike="Put 1/2 percent").filter(change_perc__lte=-2).order_by('section')  
    putodd = LiveEquityResult.objects.annotate(odd=F('section') % 2).filter(odd=True,change_perc__lte = -3).filter(strike="Put 1/2 percent").filter(change_perc__lte=-2).order_by('section') 

    call_result_odd_count =  len(callodd) + len(callCrossed_odd)
    call_result_even_count = len(calleven) + len(callCrossed_even)
    put_result_odd_count =  len(putodd) + len(putCrossed_odd)
    put_result_even_count =  len(puteven) + len(putCrossed_even)

    print(callodd)
    print(calleven)
    callcrossedeven = LiveEquityResult.objects.annotate(odd=F('section') % 2).filter(odd=False).filter(strike="Call Crossed")
    putcrossedeven = LiveEquityResult.objects.annotate(odd=F('section') % 2).filter(odd=False).filter(strike="Put Crossed")
    callcrossedodd = LiveEquityResult.objects.annotate(odd=F('section') % 2).filter(odd=True).filter(strike="Call Crossed")
    putcrossedodd = LiveEquityResult.objects.annotate(odd=F('section') % 2).filter(odd=True).filter(strike="Put Crossed")

    current_time = LiveSegment.objects.order_by('time')[:1]
    equity_timing = current_time[0].time

    return render(request,"equity.html",{'equity_timing':equity_timing,'three_list':three_list,'callCrossed_odd':callCrossed_odd,'callCrossed_even':callCrossed_even,'putCrossed_even':putCrossed_even,'putCrossed_odd':putCrossed_odd,'puteven':puteven,'putodd':putodd,'put_result_even_count':put_result_even_count,'put_result_odd_count':put_result_odd_count,'call_result_even_count':call_result_even_count,'call_result_odd_count':call_result_odd_count,'callodd':callodd,'calleven':calleven,'gain':gain,'loss':loss,'OITotalValue': OITotalValue,'OIChangeValue': OIChangeValue,'value1':value1,'value2':value2,'strikeGap':strikeGap,'callOnePercent':callOnePercent,'putOnePercent':putOnePercent,'putHalfPercent':putHalfPercent,'callHalfPercent':callHalfPercent})

#5 Option chain Section - selected symbol calculation
def optionChain(request):
    # Getting the Symbol & Expiry selected by user.
    print(request)
    print(request.GET)
    
    if len(request.GET)>0:
        symbol = request.GET["symbol"]
        print("GET")
    else:
        symbol = request.POST['symbol']
        print("POST")
        print(symbol)
    # expiry = request.POST['expiry_selected']

    # Equity data
    symbol = symbol.strip()
    liveEqui = LiveEquityResult.objects.filter(symbol=symbol)
    print("printing live equi")
    print(liveEqui)

    # Optionchain data
    LiveOI = LiveOITotal.objects.filter(symbol=symbol)
    print(LiveOI)
    LiveChangeOI = LiveOIChange.objects.filter(symbol=symbol)
    print(LiveChangeOI)
    LiveChangePercentOI = LiveOIPercentChange.objects.filter(symbol=symbol)
    print(LiveChangePercentOI)

    # History data
    HistoryOITot = HistoryOITotal.objects.filter(symbol=symbol).order_by('-time')
    HistoryOIChg = HistoryOIChange.objects.filter(symbol=symbol).order_by('-time')
    HistoryOIPercentChg = HistoryOIPercentChange.objects.filter(symbol=symbol).order_by('-time')

    if len(HistoryOITot) > 0:
        early_total_oi = HistoryOITotal.objects.filter(symbol=symbol).order_by('time')[:1]
        # early_total_oi = total_oi_record[0]

    else:
        early_total_oi = LiveOITotal.objects.filter(symbol=symbol)

    if len(HistoryOIChg) > 0:
        early_change_oi = HistoryOIChange.objects.filter(symbol=symbol).order_by('time')[:1]
        #print(early_change_oi)
        # early_change_oi = HistoryOIChg[0]
        # print(f"Live : {early_change_oi}")
        # early_change_oi = change_oi_record[1]

    else:
        early_change_oi = LiveOIChange.objects.filter(symbol=symbol)

    if len(HistoryOIPercentChg) > 0:
        early_percent_change = HistoryOIPercentChange.objects.filter(symbol=symbol).order_by('time')[:1]
        print(early_percent_change)
        # early_percent_change = percent_oi_record[0]
        # early_percent_change = HistoryOIPercentChg[0]

    else:
        early_percent_change = LiveOIPercentChange.objects.filter(symbol=symbol)

    from datetime import datetime
    import pytz
    # dateToday = datetime.today().strftime('%d-%m-%Y')
    dateToday = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%d-%m-%Y')
    print(dateToday)

    lot_dict = {'ACC': 250, 'AMBUJACEM': 1800, 'AARTIIND': 850, 'ASTRAL': 275, 'AUBANK': 1000, 'ABBOTINDIA': 40, 'ABFRL': 2600, 'ALKEM': 200, 'AMARAJABAT': 1000, 'APOLLOHOSP': 125, 'ATUL': 75, 'AUROPHARMA': 1000, 'AXISBANK': 1200, 'BAJAJFINSV': 50, 'BAJFINANCE': 125, 'CHOLAFIN': 1250, 'BALRAMCHIN': 1600, 'BANDHANBNK': 1800, 'BATAINDIA': 275, 'BERGEPAINT': 1100, 'BHARTIARTL': 950, 'BIOCON': 2300, 'ASIANPAINT': 200, 'BOSCHLTD': 50, 'BPCL': 1800, 'BSOFT': 1300, 'CANFINHOME': 975, 'CIPLA': 650, 'COFORGE': 150, 'BAJAJ-AUTO': 250, 'COLPAL': 350, 'CONCOR': 1000, 'COROMANDEL': 700, 'DEEPAKNTR': 250, 'DIVISLAB': 150, 'DLF': 1650, 'DRREDDY': 125, 'BHARATFORG': 1000, 'ESCORTS': 550, 'GLENMARK': 1150, 'GNFC': 1300, 'GODREJPROP': 325, 'GRANULES': 2000, 'GUJGASLTD': 1250, 'HAL': 475, 'HCLTECH': 700, 'DABUR': 1250, 'HDFC': 300, 'HDFCAMC': 300, 'HDFCBANK': 550, 'HDFCLIFE': 1100, 'GRASIM': 475, 'HINDPETRO': 2700, 'HINDUNILVR': 300, 'HONAUT': 15, 'ICICIBANK': 1375, 'HINDALCO': 1075, 'INDIAMART': 150, 'INDUSINDBK': 900, 'INDUSTOWER': 2800, 'INFY': 300, 'INTELLECT': 750, 'IPCALAB': 650, 'IRCTC': 875, 'JINDALSTEL': 1250, 'JUBLFOOD': 1250, 'KOTAKBANK': 400, 'LALPATHLAB': 250, 'LAURUSLABS': 900, 'LICHSGFIN': 2000, 'LT': 300, 'LTI': 150, 'LTTS': 200, 'LUPIN': 850, 'MARICO': 1200, 'MARUTI': 100, 'MCDOWELL-N': 625, 'ADANIPORTS': 1250, 'MFSL': 650, 'MGL': 800, 'MINDTREE': 200, 'MPHASIS': 175, 'MRF': 10, 'MUTHOOTFIN': 375, 'NAM-INDIA': 1600, 'NAUKRI': 125, 'NAVINFLUOR': 225, 'OFSS': 200, 'ICICIPRULI': 1500, 'PAGEIND': 15, 'PERSISTENT': 150, 'PIDILITIND': 250, 'PIIND': 250, 'PVR': 407, 'RAIN': 3500, 'SBICARD': 800, 'SBILIFE': 750, 'SHREECEM': 25, 'SIEMENS': 275, 'SRF': 375, 'SUNTV': 1500, 'TATACOMM': 500, 'TATACONSUM': 900, 'TATAMOTORS': 1425, 'DALBHARAT': 500, 'TATASTEEL': 4250, 'TECHM': 600, 'TORNTPHARM': 500, 
        'TORNTPOWER': 1500, 'TRENT': 725, 'TVSMOTOR': 1400, 'UPL': 1300, 'WHIRLPOOL': 350, 'WIPRO': 1000, 'ZEEL': 3000, 'JSWSTEEL': 1350, 'OBEROIRLTY': 700, 'RELIANCE': 250, 'CHAMBLFERT': 1500, 'CROMPTON': 1500, 'CUMMINSIND': 600, 'DELTACORP': 2300, 'DIXON': 125, 'TATACHEM': 1000, 'GODREJCP': 1000, 'HAVELLS': 500, 'ICICIGI': 425, 'IGL': 1375, 'INDIGO': 300, 'JKCEMENT': 250, 'SUNPHARMA': 700, 'MCX': 400, 'POLYCAB': 300, 'RAMCOCEM': 850, 'SRTRANSFIN': 600, 'SYNGENE': 1000, 'UBL': 400, 'ULTRACEMCO': 100, 'VOLTAS': 500, 'ZYDUSLIFE': 1800, 'SBIN': 1500, 'GSPL': 2500,'APOLLOTYRE':3500,'BEL':3800,'CANBK':2700,'COALINDIA':4200,'HINDCOPPER':4300,'IBULHSGFIN':4000,'IEX':3750,'INDHOTEL':4022,'INDIACEM':2900,'ITC':3200,'M&MFIN':4000,'MOTHERSON':4500,'NATIONALUM':4250,'NMDC':3350,'ONGC':3850,'PETRONET':3000,'POWERGRID':2700,'TATAPOWER':3375}
    symbol_lot = lot_dict[symbol]
    if len(LiveOI) > 0:
        return render(request, 'optionChainSingleSymbol.html', {'early_total_oi':early_total_oi,'early_change_oi':early_change_oi,'early_percent_change':early_percent_change,'symbol_lot':symbol_lot,'dateToday':dateToday,'LiveChangePercentOI':LiveChangePercentOI,'HistoryOIPercentChg':HistoryOIPercentChg,'liveEqui':liveEqui,'symbol':symbol,'OITotalValue':LiveOI,'OIChangeValue':LiveChangeOI,'HistoryOITot':HistoryOITot,'HistoryOIChg':HistoryOIChg})
    else:
        return render(request, 'optionChainNoData.html')

def dashboard_with_pivot(request):
    return render(request, 'dashboard_with_pivot.html', {})

#Backup 1
def testhtml(request):
    print(request.GET)
    item = request.GET['symbol']
    counter = request.GET['counter']
    print(item)

    def OIPercentChange(df):
        ce = df.loc[df['type'] == "CE"]
        pe = df.loc[df['type'] == "PE"]

        # ce_oipercent_df = ce.sort_values(by=['oi_change_perc'], ascending=False)
        ce_oipercent_df = ce.where(ce['oi_change_perc'] !=0 ).sort_values(by=['oi_change_perc'], ascending=False)

        print(ce_oipercent_df)

        minvalue = ce.loc[ce['strike'] != 0].sort_values('strike', ascending=True)
        ceindex = minvalue.iloc[0].name
        peindex = ceindex.replace("CE", "PE")
        pe = pe[peindex:]

        ceoi1 = ce_oipercent_df.iloc[0]['oi_change_perc']
        cestrike = ce_oipercent_df.iloc[0]['strike']
        peoi1 = pe.loc[pe['strike']==ce_oipercent_df.iloc[0]['strike']].iloc[0]['oi_change_perc']

        print(ceoi1)
        print(cestrike)
        print(peoi1)

        pe_oipercent_df = pe.where(pe['oi_change_perc'] !=0 ).sort_values(by=['oi_change_perc'], ascending=False)

        ceoi2 = pe_oipercent_df.iloc[0]['oi_change_perc']
        pestrike = pe_oipercent_df.iloc[0]['strike']
        peoi2 = ce.loc[ce['strike']==pe_oipercent_df.iloc[0]['strike']].iloc[0]['oi_change_perc']

        print(ceoi2)
        print(pestrike)
        print(peoi2)

        celtt = ce_oipercent_df.iloc[0]['ltt']
        peltt = pe_oipercent_df.iloc[0]['ltt']


        OIPercentChange = {"celtt":celtt,"ceoi1":ceoi1,"cestrike":cestrike,"peoi1":peoi1,"peltt":peltt,"peoi2":peoi2,"pestrike":pestrike,"ceoi2":ceoi2}
        
        return OIPercentChange

    def OITotal(df,item,dte):

        ce = df.loc[df['type'] == "CE"]
        pe = df.loc[df['type'] == "PE"]

        print("before final df")

        final_df = ce.loc[ce['oi'] != 0].sort_values('oi', ascending=False)

        minvalue = ce.loc[ce['strike'] != 0].sort_values('strike', ascending=True)

        ceindex = minvalue.iloc[0].name
        peindex = ceindex.replace("CE", "PE")
        pe = pe[peindex:]

        peoi1 = pe.loc[pe['strike']==final_df.iloc[0]['strike']].iloc[0]['oi']
        count = 0

        while peoi1 == 0:
            count = count + 1
            peoi1 = pe.loc[pe['strike']==final_df.iloc[count]['strike']].iloc[0]['oi']

        cestrike = final_df.iloc[count]['strike']
        ceoi1 = final_df.iloc[count]['oi']
        celtt = final_df.iloc[count]['ltt']

        print(ceoi1)
        print(cestrike)
        print(peoi1)

        final_df = pe.loc[pe['oi'] != 0].sort_values('oi', ascending=False)

        ceoi2 = ce.loc[ce['strike']==final_df.iloc[0]['strike']].iloc[0]['oi']
        count = 0

        while ceoi2 == 0:
            count = count + 1
            ceoi2 = ce.loc[ce['strike']==final_df.iloc[count]['strike']].iloc[0]['oi']

        pestrike = final_df.iloc[count]['strike']
        peoi2 = final_df.iloc[count]['oi']
        peltt = final_df.iloc[count]['ltt']

        print(ceoi2)
        print(pestrike)
        print(peoi2)   

        OITot = {"celtt":celtt,"ceoi1":ceoi1,"cestrike":cestrike,"peoi1":peoi1,"peltt":peltt,"peoi2":peoi2,"pestrike":pestrike,"ceoi2":ceoi2}
        
        return OITot

    def OIChange(df,item,dte):

        ce = df.loc[df['type'] == "CE"]
        pe = df.loc[df['type'] == "PE"]

        print("before final df")

        final_df = ce.loc[ce['oi_change'] != 0].sort_values('oi_change', ascending=False)

        minvalue = ce.loc[ce['strike'] != 0].sort_values('strike', ascending=True)

        ceindex = minvalue.iloc[0].name
        peindex = ceindex.replace("CE", "PE")
        pe = pe[peindex:]

        peoi1 = pe.loc[pe['strike']==final_df.iloc[0]['strike']].iloc[0]['oi_change']
        count = 0

        while peoi1 == 0:
            count = count + 1
            peoi1 = pe.loc[pe['strike']==final_df.iloc[count]['strike']].iloc[0]['oi_change']

        cestrike = final_df.iloc[count]['strike']
        ceoi1 = final_df.iloc[count]['oi_change']
        celtt = final_df.iloc[count]['ltt']

        print(ceoi1)
        print(cestrike)
        print(peoi1)

        final_df = pe.loc[pe['oi_change'] != 0].sort_values('oi_change', ascending=False)

        ceoi2 = ce.loc[ce['strike']==final_df.iloc[0]['strike']].iloc[0]['oi_change']
        count = 0

        while ceoi2 == 0:
            count = count + 1
            ceoi2 = ce.loc[ce['strike']==final_df.iloc[count]['strike']].iloc[0]['oi_change']

        pestrike = final_df.iloc[count]['strike']
        peoi2 = final_df.iloc[count]['oi_change']
        peltt = final_df.iloc[count]['ltt']

        print(ceoi2)
        print(pestrike)
        print(peoi2)      

        OIChan = {"celtt":celtt,"ceoi1":ceoi1,"cestrike":cestrike,"peoi1":peoi1,"peltt":peltt,"peoi2":peoi2,"pestrike":pestrike,"ceoi2":ceoi2}
        
        return OIChan

    # Fetching the F&NO symbol list
    TrueDatausername = 'tdws127'
    TrueDatapassword = 'saaral@127'


    import pendulum
    import calendar
    from datetime import date
    import time

    sampleDict = {}
    count=1

    exceptionList = ['NIFTY','BANKNIFTY','FINNIFTY']
    # if item not in sym:
    try:
        print("Before exception list")

        if item in exceptionList:
                if calendar.day_name[date.today().weekday()] == "Thrusday":
                    expiry = date.today()
                    dte = dt.strptime(expiry, '%d-%b-%Y')
                    print("inside thursday")
                else:
                    expiry = pendulum.now().next(pendulum.THURSDAY).strftime('%d-%b-%Y')
                    dte = dt.strptime(expiry, '%d-%b-%Y')
        else:
            print("inside monthend")
            expiry = "30-Sep-2021"
            dte = dt.strptime(expiry, '%d-%b-%Y')

        print("After exception")

        print(dte)
        print(dte.year)
        print(dte.month)
        print(dte.day)

        # td_obj = TD(TrueDatausername, TrueDatapassword, log_level= logging.WARNING )
        td_obj = TD('tdws127', 'saaral@127')
        nifty_chain = td_obj.start_option_chain( item , dt(dte.year , dte.month , dte.day) ,chain_length = 100)
        time.sleep(4)
        df = nifty_chain.get_option_chain()

        nifty_chain.stop_option_chain()
        td_obj.disconnect()
        sampleDict[item] = df

        print(df)
        print(count)
        print(item)
        count = count + 1

        # Total OI Calculation from Option chain
        FutureData = {}

        # value1 = LiveOIChange.objects.all()
        # value2 = LiveOITotal.objects.all()
        print("Before changev")
        OIChangeValue = OIChange(df,item,dte)
        print("after change")
        
        if OIChangeValue == False:
            print("returning false")
            return render(request,"testhtml.html",{'symbol':item,'counter':counter})

        OITotalValue = OITotal(df,item,dte)

        if OITotalValue == False:
            print("returning false")
            return render(request,"testhtml.html",{'symbol':item,'counter':counter})

        percentChange = OIPercentChange(df)

        strikeGap =float(df['strike'].unique()[1]) - float(df['strike'].unique()[0])

        FutureData[item] = [OITotalValue['cestrike'],OITotalValue['pestrike'],strikeGap]

        print(FutureData)

        # Percentage calculation from equity data
        newDict = {}
        # for key,value in FutureData.items():
        # Call 1 percent 
        callone = float(OITotalValue['cestrike']) - (float(strikeGap))*0.1
        # Call 1/2 percent 
        callhalf = float(OITotalValue['cestrike']) - (float(strikeGap))*0.05
        # Put 1 percent
        putone = float(OITotalValue['pestrike']) + (float(strikeGap))*0.1
        # Put 1/2 percent
        puthalf = float(OITotalValue['pestrike']) + (float(strikeGap))*0.05

        newDict[item] = [float(OITotalValue['cestrike']),float(OITotalValue['pestrike']),callone,putone,callhalf,puthalf]
        
        # # Fetching today's date
        dat = dt.today()

        print("before deletiong")

        from datetime import datetime, time
        pastDate = datetime.combine(datetime.today(), time.min)
  
        # LiveEquityResult.objects.all().delete()
        LiveOITotalAllSymbol.objects.filter(time__lte = pastDate).delete()

        # # Deleting past historical data in the database
        HistoryOIChange.objects.filter(time__lte = pastDate).delete()
        HistoryOITotal.objects.filter(time__lte = pastDate).delete()
        HistoryOIPercentChange.objects.filter(time__lte = pastDate).delete()

        # Deleting live data
        LiveOITotal.objects.filter(time__lte = pastDate).delete()
        LiveOIChange.objects.filter(time__lte = pastDate).delete()
        LiveOIPercentChange.objects.filter(time__lte = pastDate).delete()

        print("After deletion")
        
        value1 = LiveOIChange.objects.filter(symbol=item)

        if len(value1) > 0:

            if (value1[0].callstrike != OIChangeValue['cestrike']) or (value1[0].putstrike != OIChangeValue['pestrike']):
                # Adding to history table
                ChangeOIHistory = HistoryOIChange(time=value1[0].time,call1=value1[0].call1,call2=value1[0].call2,put1=value1[0].put1,put2=value1[0].put2,callstrike=value1[0].callstrike,putstrike=value1[0].putstrike,symbol=value1[0].symbol,expiry=value1[0].expiry)
                ChangeOIHistory.save()

                # deleting live table data
                LiveOIChange.objects.filter(symbol=item).delete()

                # Creating in live data
                ChangeOICreation = LiveOIChange(time=OIChangeValue['celtt'],call1=OIChangeValue['ceoi1'],call2=OIChangeValue['ceoi2'],put1=OIChangeValue['peoi1'],put2=OIChangeValue['peoi2'],callstrike=OIChangeValue['cestrike'],putstrike=OIChangeValue['pestrike'],symbol=item,expiry=dte)
                ChangeOICreation.save() 

            else:
                # deleting live table data
                LiveOIChange.objects.filter(symbol=item).delete()

                # Creating in live data
                ChangeOICreation = LiveOIChange(time=OIChangeValue['celtt'],call1=OIChangeValue['ceoi1'],call2=OIChangeValue['ceoi2'],put1=OIChangeValue['peoi1'],put2=OIChangeValue['peoi2'],callstrike=OIChangeValue['cestrike'],putstrike=OIChangeValue['pestrike'],symbol=item,expiry=dte)
                ChangeOICreation.save() 
        else:
            ChangeOICreation = LiveOIChange(time=OIChangeValue['celtt'],call1=OIChangeValue['ceoi1'],call2=OIChangeValue['ceoi2'],put1=OIChangeValue['peoi1'],put2=OIChangeValue['peoi2'],callstrike=OIChangeValue['cestrike'],putstrike=OIChangeValue['pestrike'],symbol=item,expiry=dte)
            ChangeOICreation.save()


        print("value1 crossed")

        value2 = LiveOITotal.objects.filter(symbol=item)

        if len(value2) > 0:

            if (value2[0].callstrike != OITotalValue['cestrike']) or (value2[0].putstrike != OITotalValue['pestrike']):
                # Adding to history table
                TotalOIHistory = HistoryOITotal(time=value2[0].time,call1=value2[0].call1,call2=value2[0].call2,put1=value2[0].put1,put2=value2[0].put2,callstrike=value2[0].callstrike,putstrike=value2[0].putstrike,symbol=value2[0].symbol,expiry=value2[0].expiry)
                TotalOIHistory.save()

                # deleting live table data
                LiveOITotal.objects.filter(symbol=item).delete()

                # Creating in live data
                TotalOICreation = LiveOITotal(time=OITotalValue['celtt'],call1=OITotalValue['ceoi1'],call2=OITotalValue['ceoi2'],put1=OITotalValue['peoi1'],put2=OITotalValue['peoi2'],callstrike=OITotalValue['cestrike'],putstrike=OITotalValue['pestrike'],symbol=item,expiry=dte,strikegap=strikeGap)
                TotalOICreation.save()

                # Live data for equity
                LiveOITotalAllSymbol.objects.filter(symbol=item).delete()
                TotalOICreationAll = LiveOITotalAllSymbol(time=OITotalValue['celtt'],call1=OITotalValue['ceoi1'],call2=OITotalValue['ceoi2'],put1=OITotalValue['peoi1'],put2=OITotalValue['peoi2'],callstrike=OITotalValue['cestrike'],putstrike=OITotalValue['pestrike'],symbol=item,expiry=dte,callone=callone,putone=putone,callhalf=callhalf,puthalf=puthalf)
                TotalOICreationAll.save()


            else:
                # deleting live table data
                LiveOITotal.objects.filter(symbol=item).delete()

                # Creating in live data
                TotalOICreation = LiveOITotal(time=OITotalValue['celtt'],call1=OITotalValue['ceoi1'],call2=OITotalValue['ceoi2'],put1=OITotalValue['peoi1'],put2=OITotalValue['peoi2'],callstrike=OITotalValue['cestrike'],putstrike=OITotalValue['pestrike'],symbol=item,expiry=dte,strikegap=strikeGap)
                TotalOICreation.save()

                # Live data for equity
                LiveOITotalAllSymbol.objects.filter(symbol=item).delete()
                TotalOICreationAll = LiveOITotalAllSymbol(time=OITotalValue['celtt'],call1=OITotalValue['ceoi1'],call2=OITotalValue['ceoi2'],put1=OITotalValue['peoi1'],put2=OITotalValue['peoi2'],callstrike=OITotalValue['cestrike'],putstrike=OITotalValue['pestrike'],symbol=item,expiry=dte,callone=callone,putone=putone,callhalf=callhalf,puthalf=puthalf)
                TotalOICreationAll.save()

        else:
            TotalOICreation = LiveOITotal(time=OITotalValue['celtt'],call1=OITotalValue['ceoi1'],call2=OITotalValue['ceoi2'],put1=OITotalValue['peoi1'],put2=OITotalValue['peoi2'],callstrike=OITotalValue['cestrike'],putstrike=OITotalValue['pestrike'],symbol=item,expiry=dte,strikegap=strikeGap)
            TotalOICreation.save()

            # Live data for equity
            LiveOITotalAllSymbol.objects.filter(symbol=item).delete()
            TotalOICreationAll = LiveOITotalAllSymbol(time=OITotalValue['celtt'],call1=OITotalValue['ceoi1'],call2=OITotalValue['ceoi2'],put1=OITotalValue['peoi1'],put2=OITotalValue['peoi2'],callstrike=OITotalValue['cestrike'],putstrike=OITotalValue['pestrike'],symbol=item,expiry=dte,callone=callone,putone=putone,callhalf=callhalf,puthalf=puthalf)
            TotalOICreationAll.save()

        value3 = LiveOIPercentChange.objects.filter(symbol=item)

        if len(value3) > 0:

            if (value3[0].callstrike != percentChange['cestrike']) or (value3[0].putstrike != percentChange['pestrike']):
                # Adding to history table
                ChangeOIPercentHistory = HistoryOIPercentChange(time=value3[0].time,call1=value3[0].call1,call2=value3[0].call2,put1=value3[0].put1,put2=value3[0].put2,callstrike=value3[0].callstrike,putstrike=value3[0].putstrike,symbol=value3[0].symbol,expiry=value3[0].expiry)
                ChangeOIPercentHistory.save()

                # deleting live table data
                LiveOIPercentChange.objects.filter(symbol=item).delete()

                # Creating in live data
                ChangeOIPercentCreation = LiveOIPercentChange(time=percentChange['celtt'],call1=percentChange['ceoi1'],call2=percentChange['ceoi2'],put1=percentChange['peoi1'],put2=percentChange['peoi2'],callstrike=percentChange['cestrike'],putstrike=percentChange['pestrike'],symbol=item,expiry=dte)
                ChangeOIPercentCreation.save() 

            else:
                # deleting live table data
                LiveOIPercentChange.objects.filter(symbol=item).delete()

                # Creating in live data
                ChangeOIPercentCreation = LiveOIPercentChange(time=percentChange['celtt'],call1=percentChange['ceoi1'],call2=percentChange['ceoi2'],put1=percentChange['peoi1'],put2=percentChange['peoi2'],callstrike=percentChange['cestrike'],putstrike=percentChange['pestrike'],symbol=item,expiry=dte)
                ChangeOIPercentCreation.save() 
        else:
            ChangeOIPercentCreation = LiveOIPercentChange(time=percentChange['celtt'],call1=percentChange['ceoi1'],call2=percentChange['ceoi2'],put1=percentChange['peoi1'],put2=percentChange['peoi2'],callstrike=percentChange['cestrike'],putstrike=percentChange['pestrike'],symbol=item,expiry=dte)
            ChangeOIPercentCreation.save()

    except websocket.WebSocketConnectionClosedException as e:
        print('This caught the websocket exception ')
        td_obj.disconnect()
        return render(request,"testhtml.html",{'symbol':item,'counter':counter}) 
    except IndexError as e:
        print('This caught the exception')
        td_obj.disconnect()
        return render(request,"testhtml.html",{'symbol':item,'counter':counter}) 
    except Exception as e:
        print(e)
        td_obj.disconnect()
        return render(request,"testhtml.html",{'symbol':item,'counter':counter}) 

    return render(request,"testhtml.html",{'symbol':item,'counter':counter}) 

#Backup 2
def ajax_load_expiry(request):

    print(request.GET)
    entry = request.GET.get('symbol')
    print(entry)
    ex_list = expiry_list(entry)
    print(ex_list)

    return render(request,'expiry_dropdown_list_options.html',{'ex_list':ex_list})

# def gainer(request):
#     # import datetime

#     # dt = datetime.datetime.strptime("09-Sep-2021", '%d-%b-%Y')
#     # print('{0},{1},{2:02}'.format(dt.year, dt.month, dt.day % 100))

#     from datetime import datetime as dt
#     from truedata_ws.websocket.TD import TD
#     import time
#     from nsetools import Nse

#     nse = Nse()
#     fnolist = nse.get_fno_lot_sizes()

#     print(len(fnolist))
#     symbols = list(fnolist.keys()) 


#     TrueDatausername = 'tdws135'
#     TrueDatapassword = 'saaral@135'

#     # Default production port is 8082 in the library. Other ports may be given t oyou during trial.
#     realtime_port = 8082

#     td_app = TD(TrueDatausername, TrueDatapassword, live_port=realtime_port, historical_api=False)

#     # symbols = 

#     print('Starting Real Time Feed.... ')
#     print(f'Port > {realtime_port}')

#     req_ids = td_app.start_live_data(symbols)
#     live_data_objs = {}

#     time.sleep(1)

#     liveData = {}
#     for req_id in req_ids:
#         # print(td_app.live_data[req_id])
#         if (td_app.live_data[req_id].ltp) == None:
#             continue
#         else:
#             print(td_app.live_data[req_id])
#             liveData[td_app.live_data[req_id].symbol] = [td_app.live_data[req_id].ltp,td_app.live_data[req_id].day_open,td_app.live_data[req_id].day_high,td_app.live_data[req_id].day_low,td_app.live_data[req_id].prev_day_close]


#     # print(len(liveData))

#     # print(liveData)

#     top_gainers = {}
#     top_losers = {}

#     for key,value in liveData.items():
#         # print(key)
#         # print(value)
#         gainpercent = (value[4] + (value[4]*0.03))
#         losspercent = (value[4] - (value[4]*0.03))
#         if value[0] > gainpercent:
#             top_gainers[key] = value
#         elif value[0] < losspercent:
#             top_losers[key] = value

#     # print(top_gainers.keys())
#     # print(top_losers.keys())
#     # print(len(top_gainers))
#     # print(len(top_losers))

#     LiveSegment.objects.all().delete()

#     for key,value in top_gainers.items():

#         gain = LiveSegment(symbol=key,segment="gain")
#         gain.save()

#     for key,value in top_losers.items():

#         loss = LiveSegment(symbol=key,segment="loss")
#         loss.save()

   
#     fnolist = list(LiveSegment.objects.values_list('symbol', flat=True))

#     print(fnolist)

#     OITotalValue ={}
#     OIChangeValue = {}
#     value1 = {}
#     value2 = {}
#     strikeGap = {}
#     callOnePercent = LiveEquityResult.objects.filter(strike="Call 1 percent")
#     putOnePercent = LiveEquityResult.objects.filter(strike="Put 1 percent")
#     callHalfPercent = LiveEquityResult.objects.filter(strike="Call 1/2 percent")
#     putHalfPercent = LiveEquityResult.objects.filter(strike="Put 1/2 percent")
#     callCrossed = LiveEquityResult.objects.filter(strike="Call Crossed")
#     putCrossed = LiveEquityResult.objects.filter(strike="Put Crossed")

#     return render(request,"equity.html",{'OITotalValue': OITotalValue,'OIChangeValue': OIChangeValue,'value1':value1,'value2':value2,'strikeGap':strikeGap,'callOnePercent':callOnePercent,'putOnePercent':putOnePercent,'callCrossed':callCrossed,'putCrossed':putCrossed,'putHalfPercent':putHalfPercent,'callHalfPercent':callHalfPercent})

@login_required(login_url='login')
def sample(request):

    import requests
    # url = 'https://www.truedata.in/downloads/symbol_lists/13.NSE_ALL_OPTIONS.txt'
    # s = requests.get(url).content
    # stringlist=[x.decode('utf-8').split('2')[0] for x in s.splitlines()]

    # fnolist = list(set(stringlist))
    # fnolist.sort()
    # fnolist = ['1','2']

    fnolist = ['AARTIIND', 'ABBOTINDIA', 'ABFRL', 'ACC', 'ADANIPORTS', 'ALKEM', 'AMARAJABAT', 'AMBUJACEM', 'APOLLOHOSP', 'APOLLOTYRE', 'ASIANPAINT', 'ASTRAL', 'ATUL', 'AUBANK', 'AUROPHARMA', 'AXISBANK', 'BAJAJ-AUTO', 'BAJAJFINSV', 'BAJFINANCE', 'BALRAMCHIN', 'BANDHANBNK', 'BATAINDIA', 'BEL', 'BERGEPAINT', 'BHARATFORG', 'BHARTIARTL', 'BIOCON', 'BOSCHLTD', 'BPCL', 'BSOFT', 'CANBK', 'CANFINHOME', 'CHAMBLFERT', 'CHOLAFIN', 'CIPLA', 'COALINDIA', 'COFORGE', 'COLPAL', 'CONCOR', 'COROMANDEL', 'CROMPTON', 'CUMMINSIND', 'DABUR', 'DALBHARAT', 'DEEPAKNTR', 'DELTACORP', 'DIVISLAB', 'DIXON', 'DLF', 'DRREDDY', 'ESCORTS', 'GLENMARK', 'GNFC', 'GODREJCP', 'GODREJPROP', 'GRANULES', 'GRASIM', 'GSPL', 'GUJGASLTD', 'HAL', 'HAVELLS', 'HCLTECH', 'HDFC', 'HDFCAMC', 'HDFCBANK', 'HDFCLIFE', 'HINDALCO', 'HINDCOPPER', 'HINDPETRO', 'HINDUNILVR', 'HONAUT', 'IBULHSGFIN', 'ICICIBANK', 'ICICIGI', 'ICICIPRULI', 'IEX', 'IGL', 'INDHOTEL', 'INDIACEM', 'INDIAMART', 'INDIGO', 'INDUSINDBK', 'INDUSTOWER', 'INFY', 'INTELLECT', 'IPCALAB', 'IRCTC', 'ITC', 'JINDALSTEL', 'JKCEMENT', 'JSWSTEEL', 'JUBLFOOD', 'KOTAKBANK', 'LALPATHLAB', 'LAURUSLABS', 'LICHSGFIN', 'LT', 'LTI', 'LTTS', 'LUPIN', 'M&MFIN', 'MARICO', 'MARUTI', 'MCDOWELL-N', 'MCX', 'MFSL', 'MGL', 'MINDTREE', 'MOTHERSON', 'MPHASIS', 'MRF', 'MUTHOOTFIN', 'NATIONALUM', 'NAUKRI', 'NAVINFLUOR', 'NMDC', 'OBEROIRLTY', 'OFSS', 'ONGC', 'PAGEIND', 'PERSISTENT', 'PETRONET', 'PIDILITIND', 'PIIND', 'POLYCAB', 'POWERGRID', 'PVR', 'RAIN', 'RAMCOCEM', 'RELIANCE', 'SBICARD', 'SBILIFE', 'SBIN', 'SHREECEM', 'SIEMENS', 'SRF', 'SRTRANSFIN', 'SUNPHARMA', 'SUNTV', 'SYNGENE', 'TATACHEM', 'TATACOMM', 'TATACONSUM', 'TATAMOTORS', 'TATAPOWER', 'TATASTEEL', 'TECHM', 'TORNTPHARM', 'TORNTPOWER', 'TRENT', 'TVSMOTOR', 'UBL', 'ULTRACEMCO', 'UPL', 'VOLTAS', 'WHIRLPOOL', 'WIPRO', 'ZEEL', 'ZYDUSLIFE']


    #fnolist = list(LiveSegment.objects.values_list('symbol', flat=True).distinct())

    gain_list = SuperLiveSegment.objects.filter(segment__in=["gain"]).order_by('-change_perc').values_list('symbol', flat=True) 
    loss_list = SuperLiveSegment.objects.filter(segment__in=["loss"]).order_by('change_perc').values_list('symbol', flat=True) 
    super_list = list(gain_list) + list(loss_list)
    setA = set(fnolist)
    setB = set(super_list)

    # Get new set with elements that are only in a but not in b
    onlyInA = setA.difference(setB)
    fnolist = list(setB) + list(onlyInA)

    return render(request,"sample.html",{'fnolist':fnolist})
    # return render(request,"sample.html")

@login_required(login_url='login')
def bootsample(request):
    from nsetools import Nse
    nse = Nse()
    fnolist = nse.get_fno_lot_sizes()

    # fnolist = list(LiveSegment.objects.values_list('symbol', flat=True).distinct())

    return render(request,"bootsample.html",{'fnolist':fnolist})