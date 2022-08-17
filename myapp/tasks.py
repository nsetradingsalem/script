from time import sleep
from celery import shared_task
from .models import *
from nsetools import *
from datetime import datetime as dt
from truedata_ws.websocket.TD import TD
import websocket

from celery.schedules import crontab
from celery import Celery
from celery.schedules import crontab
import time
from nsetools import Nse
from myproject.celery import app
from django_celery_beat.models import PeriodicTask, PeriodicTasks
from datetime import datetime, time,timedelta
from celery.exceptions import SoftTimeLimitExceeded
from pytz import timezone
import pendulum
import calendar
from datetime import date
import time as te


@shared_task
def create_currency():

    # Deleting pastday data
    """
    1.Livesegmant
    2.Testequity result
    """
    fando_username = 'tdwsp127'
    fando_password = 'saaral@127'
    # fnolist = ['AMBUJACEM',
    #         'ATUL',
    #         'BAJAJ-AUTO',
    #         'BAJFINANCE',
    #         'DALBHARAT',
    #         'DIVISLAB',
    #         'JKCEMENT',
    #         'RAMCOCEM',
    #         'RELIANCE',
    #         'SHREECEM',
    #         'ULTRACEMCO']
    fnolist = ['AARTIIND','ABBOTINDIA','ABFRL','ACC','ADANIPORTS','ALKEM','AMARAJABAT','AMBUJACEM',
    'APOLLOHOSP','ASIANPAINT','ASTRAL','ATUL','AUBANK','AUROPHARMA','AXISBANK','BAJAJ-AUTO','BAJAJFINSV','BAJFINANCE','BALRAMCHIN','BANDHANBNK'
    ,'BATAINDIA','BERGEPAINT','BHARATFORG','BHARTIARTL','BIOCON','BOSCHLTD','BPCL','BSOFT','CANFINHOME','CHAMBLFERT','CHOLAFIN'
    ,'CIPLA','COFORGE','COLPAL','CONCOR','COROMANDEL','CROMPTON','CUMMINSIND','DABUR','DALBHARAT','DEEPAKNTR','DELTACORP','DIVISLAB','DIXON','DLF'
    ,'DRREDDY','ESCORTS','GLENMARK','GNFC','GODREJCP','GODREJPROP','GRANULES','GRASIM','GSPL','GUJGASLTD','HAL','HAVELLS','HCLTECH','HINDPETRO','HDFC','HDFCAMC'
    ,'HDFCBANK','HDFCLIFE','HINDALCO','HINDUNILVR','HONAUT','ICICIBANK','ICICIGI','ICICIPRULI','IGL'
    ,'INDIAMART','INDIGO','INDUSINDBK','INFY','INTELLECT','IPCALAB','IRCTC','JINDALSTEL','JKCEMENT','JSWSTEEL','JUBLFOOD','KOTAKBANK','LALPATHLAB','LAURUSLABS','LICHSGFIN'
    ,'LT','LTI','LTTS','LUPIN','MARICO','MARUTI','MCDOWELL-N','MCX','MFSL','MGL','MINDTREE','MPHASIS','MRF','MUTHOOTFIN','NAM-INDIA','NAUKRI'
    ,'NAVINFLUOR','OBEROIRLTY','PAGEIND','PERSISTENT','PIDILITIND','PIIND','POLYCAB','PVR','RAMCOCEM','RELIANCE','SBICARD'
    ,'SBILIFE','SBIN','SHREECEM','SIEMENS','SRF','SRTRANSFIN','SUNPHARMA','SUNTV','SYNGENE','TATACHEM','TATACOMM','TATACONSUM','TATAMOTORS','RAIN','TATASTEEL','TECHM'
    ,'TORNTPHARM','TORNTPOWER','TRENT','TVSMOTOR','UBL','ULTRACEMCO','UPL','VOLTAS','WHIRLPOOL','WIPRO','ZEEL','ZYDUSLIFE','INDUSTOWER','OFSS']
    # Today's market datetime  - Ex. 2022-08-12 09:15:00
    todays_market_datetime = datetime.combine(datetime.now(timezone('Asia/Kolkata')), time(9,15))

    # Deleting previous records in the table - if any
    LiveSegment.objects.filter(date__lt = todays_market_datetime).delete()
    TestEquityResult.objects.filter(date__lte = todays_market_datetime).delete()
    LiveEquityResult.objects.filter(date__lte = todays_market_datetime).delete()
    LiveOITotal.objects.filter(time__lte = todays_market_datetime).delete()
    LiveOIChange.objects.filter(time__lte = todays_market_datetime).delete()
    LiveOIPercentChange.objects.filter(time__lte = todays_market_datetime).delete()
    HistoryOITotal.objects.filter(time__lte = todays_market_datetime).delete()
    HistoryOIChange.objects.filter(time__lte = todays_market_datetime).delete()
    HistoryOIPercentChange.objects.filter(time__lte = todays_market_datetime).delete()


    # Daily 9.15 AM
    timenow = datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')
    # checking if first of equity is completed.
    # doneToday = LiveSegment.objects.values_list('doneToday', flat=True).distinct()

    # if len(doneToday) > 0:
    #     doneToday = doneToday[0]
    # else:
    #     doneToday = ""

    # if equity first run is not completed then the method is called.
    # if timenow > todays_market_datetime.strftime('%Y-%m-%d %H:%M:%S') and doneToday != "Yes":
    #     initialEquity(fnolist,equity_username,equity_password)
   
    fnolist = []

    gainList = list(LiveSegment.objects.filter(segment="gain").values_list('symbol', flat=True))
    lossList = list(LiveSegment.objects.filter(segment="loss").values_list('symbol', flat=True))
    segments = list(LiveSegment.objects.values_list('symbol', flat=True).distinct())
    
    fnolist.extend(gainList)
    fnolist.extend(lossList)
    fnolist.extend(segments)

    fnolist = list(set(fnolist))

    #fnolist = ['ULTRACEMCO']
    # fnolist = ['AMBUJACEM',
    #         'ATUL',
    #         'BAJAJ-AUTO',
    #         'BAJFINANCE',
    #         'DALBHARAT',
    #         'DIVISLAB',
    #         'JKCEMENT',
    #         'RAMCOCEM',
    #         'RELIANCE',
    #         'SHREECEM',
    #         'ULTRACEMCO']

    def OIPercentChange(df):
        print("percentage")
        ce = df.loc[df['type'] == "CE"]
        pe = df.loc[df['type'] == "PE"]

        # ce_oipercent_df = ce.sort_values(by=['oi_change_perc'], ascending=False)
        ce_oipercent_df = ce.where(ce['oi_change_perc'] !=0 ).sort_values(by=['oi_change_perc'], ascending=False)

        # print(ce_oipercent_df)
        
        minvalue = ce.loc[ce['strike'] != 0].sort_values('strike', ascending=True)
        
        ceindex = minvalue.iloc[0].name
        peindex = ceindex.replace("CE", "PE")
        #pe = pe[peindex:]

        ceoi1 = ce_oipercent_df.iloc[0]['oi_change_perc']
        cestrike = ce_oipercent_df.iloc[0]['strike']
        peoi1 = pe.loc[pe['strike']==ce_oipercent_df.iloc[0]['strike']].iloc[0]['oi_change_perc']

        pe_oipercent_df = pe.where(pe['oi_change_perc'] !=0 ).sort_values(by=['oi_change_perc'], ascending=False)

        ceoi2 = pe_oipercent_df.iloc[0]['oi_change_perc']
        pestrike = pe_oipercent_df.iloc[0]['strike']
        peoi2 = ce.loc[ce['strike']==pe_oipercent_df.iloc[0]['strike']].iloc[0]['oi_change_perc']

        import datetime as det
        # celtt = pe_oipercent_df.iloc[count]['ltt']
        celtt = dt.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')
        celtt = dt.strptime(str(celtt), "%Y-%m-%d %H:%M:%S").time()

        my_time_string = "15:30:00"
        my_datetime = det.datetime.strptime(my_time_string, "%H:%M:%S").time()

        if celtt > my_datetime:
            celtt = det.datetime.now().replace(hour=15,minute=30,second=00).strftime("%Y-%m-%d %H:%M:%S")
            peltt = det.datetime.now().replace(hour=15,minute=30,second=00).strftime("%Y-%m-%d %H:%M:%S")
        else:
            celtt = pe_oipercent_df.iloc[0]['ltt']
            peltt = pe_oipercent_df.iloc[0]['ltt']


        OIPercentChange = {"celtt":celtt,"ceoi1":ceoi1,"cestrike":cestrike,"peoi1":peoi1,"peltt":peltt,"peoi2":peoi2,"pestrike":pestrike,"ceoi2":ceoi2}
        return OIPercentChange

    def OITotal(df):
        
        print("total")
        ce = df.loc[df['type'] == "CE"]
        pe = df.loc[df['type'] == "PE"]

        final_df = ce.loc[ce['oi'] != 0].sort_values('oi', ascending=False)
        minvalue = ce.loc[ce['strike'] != 0].sort_values('strike', ascending=True)

        print("1")
        ceindex = minvalue.iloc[0].name
        peindex = ceindex.replace("CE", "PE")
        print(f"pe index {peindex}")
        # print(pe)
        # pe = pe[peindex:]
        # print(pe)
        


        print("2")
        print(pe)
        peoi1 = pe.loc[pe['strike']==final_df.iloc[0]['strike']].iloc[0]['oi']
        print(peoi1)
        count = 0

        while peoi1 == 0:
            count = count + 1
            peoi1 = pe.loc[pe['strike']==final_df.iloc[count]['strike']].iloc[0]['oi']

        print("3")
        cestrike = final_df.iloc[count]['strike']
        ceoi1 = final_df.iloc[count]['oi']
        
        print("4")
        import datetime as det
        celtt = final_df.iloc[count]['ltt']
        celtt = dt.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')
        celtt = dt.strptime(str(celtt), "%Y-%m-%d %H:%M:%S").time()

        my_time_string = "15:30:00"
        my_datetime = det.datetime.strptime(my_time_string, "%H:%M:%S").time()

        print("5")
        if celtt > my_datetime:
            celtt = det.datetime.now().replace(hour=15,minute=30,second=00).strftime("%Y-%m-%d %H:%M:%S")
            peltt = det.datetime.now().replace(hour=15,minute=30,second=00).strftime("%Y-%m-%d %H:%M:%S")
        else:
            celtt = final_df.iloc[0]['ltt']
            peltt = final_df.iloc[0]['ltt']

        print("6")
        final_df = pe.loc[pe['oi'] != 0].sort_values('oi', ascending=False)

        ceoi2 = ce.loc[ce['strike']==final_df.iloc[0]['strike']].iloc[0]['oi']
        count = 0

        print("7")
        while ceoi2 == 0:
            count = count + 1
            ceoi2 = ce.loc[ce['strike']==final_df.iloc[count]['strike']].iloc[0]['oi']

        print("8")
        pestrike = final_df.iloc[count]['strike']
        peoi2 = final_df.iloc[count]['oi']

        OITot = {"celtt":celtt,"ceoi1":ceoi1,"cestrike":cestrike,"peoi1":peoi1,"peltt":peltt,"peoi2":peoi2,"pestrike":pestrike,"ceoi2":ceoi2}
        return OITot

    def OIChange(df):

        print("change")
        ce = df.loc[df['type'] == "CE"]
        pe = df.loc[df['type'] == "PE"]

        final_df = ce.loc[ce['oi_change'] != 0].sort_values('oi_change', ascending=False)
        minvalue = ce.loc[ce['strike'] != 0].sort_values('strike', ascending=True)

        print("2")
        ceindex = minvalue.iloc[0].strike
        # peindex = ceindex.replace("CE", "PE")
        inde = pe[pe['strike']==ceindex].index.values
        pe = pe[inde[0]:]
        print(pe)
        print("-----------------")
        print(final_df)
 
        peoi1 = pe.loc[pe['strike']==str(final_df.iloc[0]['strike'])].iloc[0]['oi_change']
        count = 0

        print("4")
        while peoi1 == 0:
            count = count + 1
            print(f"Count :{count}")
            peoi1 = pe.loc[pe['strike']==final_df.iloc[count]['strike']].iloc[0]['oi_change']

        print("5")
        cestrike = final_df.iloc[count]['strike']
        ceoi1 = final_df.iloc[count]['oi_change']
        import datetime as det
        print("6")
        celtt = final_df.iloc[count]['ltt']
        celtt = dt.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')
        celtt = dt.strptime(str(celtt), "%Y-%m-%d %H:%M:%S").time()

        my_time_string = "15:30:00"
        my_datetime = det.datetime.strptime(my_time_string, "%H:%M:%S").time()

        if celtt > my_datetime:
            celtt = det.datetime.now().replace(hour=15,minute=30,second=00).strftime("%Y-%m-%d %H:%M:%S")
            peltt = det.datetime.now().replace(hour=15,minute=30,second=00).strftime("%Y-%m-%d %H:%M:%S")
        else:
            celtt = final_df.iloc[0]['ltt']
            peltt = final_df.iloc[0]['ltt']

        final_df = pe.loc[pe['oi_change'] != 0].sort_values('oi_change', ascending=False)
        ceoi2 = ce.loc[ce['strike']==final_df.iloc[0]['strike']].iloc[0]['oi_change']
        count = 0
        print("8")

        while ceoi2 == 0:
            count = count + 1
            ceoi2 = ce.loc[ce['strike']==final_df.iloc[count]['strike']].iloc[0]['oi_change']

        pestrike = final_df.iloc[count]['strike']
        peoi2 = final_df.iloc[count]['oi_change']
        peltt = final_df.iloc[count]['ltt']

        OIChan = {"celtt":celtt,"ceoi1":ceoi1,"cestrike":cestrike,"peoi1":peoi1,"peltt":peltt,"peoi2":peoi2,"pestrike":pestrike,"ceoi2":ceoi2}
        return OIChan

    def optionChainprocess(df,item,expiry_date):

        print(f"------------------------- {item} ----------------------------------")

       
        # Total OI Calculation from Option chain
        FutureData = {}
        #--------------------- Change OI Calculation ------------------------------
        OIChangeValue = OIChange(df)

        #--------------------- Total OI Calculation ------------------------------
        OITotalValue = OITotal(df)
        
        #--------------------- Change % Calculation ------------------------------
        percentChange = OIPercentChange(df)

        midvalue = round(len(df['strike'].unique())/2)
        strikeGap =float(df['strike'].unique()[midvalue]) - float(df['strike'].unique()[midvalue-1])

        FutureData[item] = [OITotalValue['cestrike'],OITotalValue['pestrike'],strikeGap]

        # Percentage calculation from equity data
        newDict = {}
        # for key,value in FutureData.items():
        # Call 1 percent 
        callone = float(OITotalValue['cestrike']) - (float(strikeGap))*0.99
        # Call 1/2 percent 
        callhalf = float(OITotalValue['cestrike']) - (float(strikeGap))*0.05
        # Put 1 percent
        putone = float(OITotalValue['pestrike']) + (float(strikeGap))*0.99
        # Put 1/2 percent
        puthalf = float(OITotalValue['pestrike']) + (float(strikeGap))*0.05

        newDict[item] = [float(OITotalValue['cestrike']),float(OITotalValue['pestrike']),callone,putone,callhalf,puthalf]
        
        # # Fetching today's date
        dat = dt.today()
        
        value1 = LiveOIChange.objects.filter(symbol=item)
        OIChangeValue['celtt'] = timenow

        if len(value1) > 0:

            if (value1[0].callstrike != OIChangeValue['cestrike']) or (value1[0].putstrike != OIChangeValue['pestrike']):
                # Adding to history table
                ChangeOIHistory = HistoryOIChange(time=value1[0].time,call1=value1[0].call1,call2=value1[0].call2,put1=value1[0].put1,put2=value1[0].put2,callstrike=value1[0].callstrike,putstrike=value1[0].putstrike,symbol=value1[0].symbol,expiry=value1[0].expiry)
                ChangeOIHistory.save()

                # deleting live table data
                LiveOIChange.objects.filter(symbol=item).delete()

                # Creating in live data
                ChangeOICreation = LiveOIChange(time=OIChangeValue['celtt'],call1=OIChangeValue['ceoi1'],call2=OIChangeValue['ceoi2'],put1=OIChangeValue['peoi1'],put2=OIChangeValue['peoi2'],callstrike=OIChangeValue['cestrike'],putstrike=OIChangeValue['pestrike'],symbol=item,expiry=expiry_date)
                ChangeOICreation.save() 

            else:
                # deleting live table data
                LiveOIChange.objects.filter(symbol=item).delete()

                # Creating in live data
                ChangeOICreation = LiveOIChange(time=OIChangeValue['celtt'],call1=OIChangeValue['ceoi1'],call2=OIChangeValue['ceoi2'],put1=OIChangeValue['peoi1'],put2=OIChangeValue['peoi2'],callstrike=OIChangeValue['cestrike'],putstrike=OIChangeValue['pestrike'],symbol=item,expiry=expiry_date)
                ChangeOICreation.save() 
        else:
            ChangeOICreation = LiveOIChange(time=OIChangeValue['celtt'],call1=OIChangeValue['ceoi1'],call2=OIChangeValue['ceoi2'],put1=OIChangeValue['peoi1'],put2=OIChangeValue['peoi2'],callstrike=OIChangeValue['cestrike'],putstrike=OIChangeValue['pestrike'],symbol=item,expiry=expiry_date)
            ChangeOICreation.save()

        value2 = LiveOITotal.objects.filter(symbol=item)
        OITotalValue['celtt'] = timenow
        if len(value2) > 0:

            if (value2[0].callstrike != OITotalValue['cestrike']) or (value2[0].putstrike != OITotalValue['pestrike']):
                # Adding to history table
                TotalOIHistory = HistoryOITotal(time=value2[0].time,call1=value2[0].call1,call2=value2[0].call2,put1=value2[0].put1,put2=value2[0].put2,callstrike=value2[0].callstrike,putstrike=value2[0].putstrike,symbol=value2[0].symbol,expiry=value2[0].expiry)
                TotalOIHistory.save()

                # deleting live table data
                LiveOITotal.objects.filter(symbol=item).delete()

                # Creating in live data
                TotalOICreation = LiveOITotal(time=OITotalValue['celtt'],call1=OITotalValue['ceoi1'],call2=OITotalValue['ceoi2'],put1=OITotalValue['peoi1'],put2=OITotalValue['peoi2'],callstrike=OITotalValue['cestrike'],putstrike=OITotalValue['pestrike'],symbol=item,expiry=expiry_date,strikegap=strikeGap)
                TotalOICreation.save()

                # Live data for equity
                LiveOITotalAllSymbol.objects.filter(symbol=item).delete()
                TotalOICreationAll = LiveOITotalAllSymbol(time=OITotalValue['celtt'],call1=OITotalValue['ceoi1'],call2=OITotalValue['ceoi2'],put1=OITotalValue['peoi1'],put2=OITotalValue['peoi2'],callstrike=OITotalValue['cestrike'],putstrike=OITotalValue['pestrike'],symbol=item,expiry=expiry_date,callone=callone,putone=putone,callhalf=callhalf,puthalf=puthalf)
                TotalOICreationAll.save()


            else:
                # deleting live table data
                LiveOITotal.objects.filter(symbol=item).delete()

                # Creating in live data
                TotalOICreation = LiveOITotal(time=OITotalValue['celtt'],call1=OITotalValue['ceoi1'],call2=OITotalValue['ceoi2'],put1=OITotalValue['peoi1'],put2=OITotalValue['peoi2'],callstrike=OITotalValue['cestrike'],putstrike=OITotalValue['pestrike'],symbol=item,expiry=expiry_date,strikegap=strikeGap)
                TotalOICreation.save()

                # Live data for equity
                LiveOITotalAllSymbol.objects.filter(symbol=item).delete()
                TotalOICreationAll = LiveOITotalAllSymbol(time=OITotalValue['celtt'],call1=OITotalValue['ceoi1'],call2=OITotalValue['ceoi2'],put1=OITotalValue['peoi1'],put2=OITotalValue['peoi2'],callstrike=OITotalValue['cestrike'],putstrike=OITotalValue['pestrike'],symbol=item,expiry=expiry_date,callone=callone,putone=putone,callhalf=callhalf,puthalf=puthalf)
                TotalOICreationAll.save()

        else:
            TotalOICreation = LiveOITotal(time=OITotalValue['celtt'],call1=OITotalValue['ceoi1'],call2=OITotalValue['ceoi2'],put1=OITotalValue['peoi1'],put2=OITotalValue['peoi2'],callstrike=OITotalValue['cestrike'],putstrike=OITotalValue['pestrike'],symbol=item,expiry=expiry_date,strikegap=strikeGap)
            TotalOICreation.save()

            # Live data for equity
            LiveOITotalAllSymbol.objects.filter(symbol=item).delete()
            TotalOICreationAll = LiveOITotalAllSymbol(time=OITotalValue['celtt'],call1=OITotalValue['ceoi1'],call2=OITotalValue['ceoi2'],put1=OITotalValue['peoi1'],put2=OITotalValue['peoi2'],callstrike=OITotalValue['cestrike'],putstrike=OITotalValue['pestrike'],symbol=item,expiry=expiry_date,callone=callone,putone=putone,callhalf=callhalf,puthalf=puthalf)
            TotalOICreationAll.save()

        value3 = LiveOIPercentChange.objects.filter(symbol=item)
        percentChange['celtt'] = timenow
        if len(value3) > 0:

            if (value3[0].callstrike != percentChange['cestrike']) or (value3[0].putstrike != percentChange['pestrike']):
                # Adding to history table
                ChangeOIPercentHistory = HistoryOIPercentChange(time=value3[0].time,call1=value3[0].call1,call2=value3[0].call2,put1=value3[0].put1,put2=value3[0].put2,callstrike=value3[0].callstrike,putstrike=value3[0].putstrike,symbol=value3[0].symbol,expiry=value3[0].expiry)
                ChangeOIPercentHistory.save()

                # deleting live table data
                LiveOIPercentChange.objects.filter(symbol=item).delete()

                # Creating in live data
                ChangeOIPercentCreation = LiveOIPercentChange(time=percentChange['celtt'],call1=percentChange['ceoi1'],call2=percentChange['ceoi2'],put1=percentChange['peoi1'],put2=percentChange['peoi2'],callstrike=percentChange['cestrike'],putstrike=percentChange['pestrike'],symbol=item,expiry=expiry_date)
                ChangeOIPercentCreation.save() 

            else:
                # deleting live table data
                LiveOIPercentChange.objects.filter(symbol=item).delete()

                # Creating in live data
                ChangeOIPercentCreation = LiveOIPercentChange(time=percentChange['celtt'],call1=percentChange['ceoi1'],call2=percentChange['ceoi2'],put1=percentChange['peoi1'],put2=percentChange['peoi2'],callstrike=percentChange['cestrike'],putstrike=percentChange['pestrike'],symbol=item,expiry=expiry_date)
                ChangeOIPercentCreation.save() 
        else:
            ChangeOIPercentCreation = LiveOIPercentChange(time=percentChange['celtt'],call1=percentChange['ceoi1'],call2=percentChange['ceoi2'],put1=percentChange['peoi1'],put2=percentChange['peoi2'],callstrike=percentChange['cestrike'],putstrike=percentChange['pestrike'],symbol=item,expiry=expiry_date)
            ChangeOIPercentCreation.save()


    sampleDict = {}
    count=1

    lenthree = (len(fnolist))%3

    for r,g,b in zip(*[iter(fnolist)]*3):

            # optionchain 3 symbols
            try:
                exceptionList = ['NIFTY','BANKNIFTY','FINNIFTY']
                if r in exceptionList:
                        if calendar.day_name[date.today().weekday()] == "Thrusday":
                            expiry = date.today()
                            expiry = "7-Oct-2021"
                            expiry_date = dt.strptime(expiry, '%d-%b-%Y')
                            # print("inside thursday")
                        else:
                            expiry = pendulum.now().next(pendulum.THURSDAY).strftime('%d-%b-%Y')
                            expiry = "7-Oct-2021"
                            expiry_date = dt.strptime(expiry, '%d-%b-%Y')
                else:
                    # print("inside monthend")
                    expiry = "25-Aug-2022"
                    expiry_date = dt.strptime(expiry, '%d-%b-%Y')

                # print("After exception")

                # td_obj = TD(TrueDatausername, TrueDatapassword, log_level= logging.WARNING )
                symbol1 = r
                symbol2 = g
                symbol3 = b
                try:
                    td_obj.disconnect()
                except Exception:
                    pass

                td_obj = TD(fando_username, fando_password)
                first_chain = td_obj.start_option_chain( symbol1 , dt(expiry_date.year , expiry_date.month , expiry_date.day) ,chain_length = 74)
                second_chain = td_obj.start_option_chain( symbol2 , dt(expiry_date.year , expiry_date.month , expiry_date.day) ,chain_length = 74)
                third_chain = td_obj.start_option_chain( symbol3 , dt(expiry_date.year , expiry_date.month , expiry_date.day) ,chain_length = 74)

                te.sleep(3)

                df1 = first_chain.get_option_chain()
                df2 = second_chain.get_option_chain()
                df3 = third_chain.get_option_chain()

                first_chain.stop_option_chain()
                second_chain.stop_option_chain()
                third_chain.stop_option_chain()

                td_obj.disconnect()
                td_obj.disconnect()
                # td_app.disconnect()
                sampleDict[symbol1] = df1

                count = count + 1

                optionChainprocess(df1,symbol1,expiry_date)
                optionChainprocess(df2,symbol2,expiry_date)
                optionChainprocess(df3,symbol3,expiry_date)

            except websocket.WebSocketConnectionClosedException as e:
                print('This caught the websocket exception in optionchain realtime')
                td_obj.disconnect()
                td_obj.disconnect()
            except IndexError as e:
                print('This caught the exception in option chain realtime')
                print(e)
                td_obj.disconnect()
                td_obj.disconnect()
            except Exception as e:
                print(e)
                td_obj.disconnect()
                td_obj.disconnect()

    if lenthree == 2:

        try:
            exceptionList = ['NIFTY','BANKNIFTY','FINNIFTY']
            if fnolist[-1] in exceptionList:
                    if calendar.day_name[date.today().weekday()] == "Thrusday":
                        expiry = date.today()
                        expiry = "2-Dec-2021"
                        expiry_date = dt.strptime(expiry, '%d-%b-%Y')
                        # print("inside thursday")
                    else:
                        expiry = pendulum.now().next(pendulum.THURSDAY).strftime('%d-%b-%Y')
                        expiry = "2-Dec-2021"
                        expiry_date = dt.strptime(expiry, '%d-%b-%Y')
            else:
                # print("inside monthend")
                expiry = "25-Aug-2022"
                expiry_date = dt.strptime(expiry, '%d-%b-%Y')

            # print("After exception")

            # td_obj = TD(TrueDatausername, TrueDatapassword, log_level= logging.WARNING )
            symbol1 = fnolist[-1]
            symbol2 = fnolist[-2]
            try:
                td_obj.disconnect()
            except Exception:
                pass
            td_obj = TD('tdwsp127', 'saaral@127')
            first_chain = td_obj.start_option_chain( symbol1 , dt(expiry_date.year , expiry_date.month , expiry_date.day) ,chain_length = 75)
            second_chain = td_obj.start_option_chain( symbol2 , dt(expiry_date.year , expiry_date.month , expiry_date.day) ,chain_length = 75)

            te.sleep(3)

            df1 = first_chain.get_option_chain()
            df2 = second_chain.get_option_chain()

            first_chain.stop_option_chain()
            second_chain.stop_option_chain()

            td_obj.disconnect()
            td_obj.disconnect()
            sampleDict[symbol1] = df1

            print(df1)
            print(df2)
            # print(count)
            # print(item)
            count = count + 1

            optionChainprocess(df1,symbol1,expiry_date)
            optionChainprocess(df2,symbol2,expiry_date)

            
        except websocket.WebSocketConnectionClosedException as e:
            print('This caught the websocket exception in optionchain realtime ')
            td_obj.disconnect()
            td_obj.disconnect()
        except IndexError as e:
            print('This caught the exception in optionchain realtime')
            print(e)
            td_obj.disconnect()
            td_obj.disconnect()
        except Exception as e:
            print(e)
            td_obj.disconnect()
            td_obj.disconnect()
        sleep(1)

    elif lenthree == 1:
        try:
            exceptionList = ['NIFTY','BANKNIFTY','FINNIFTY']
            if fnolist[-1] in exceptionList:
                    if calendar.day_name[date.today().weekday()] == "Thrusday":
                        expiry = date.today()
                        expiry = "7-Oct-2021"
                        expiry_date = dt.strptime(expiry, '%d-%b-%Y')
                        # print("inside thursday")
                    else:
                        expiry = pendulum.now().next(pendulum.THURSDAY).strftime('%d-%b-%Y')
                        expiry = "7-Oct-2021"
                        expiry_date = dt.strptime(expiry, '%d-%b-%Y')
            else:
                # print("inside monthend")
                expiry = "25-Aug-2022"
                expiry_date = dt.strptime(expiry, '%d-%b-%Y')

            # print("After exception")

            # td_obj = TD(TrueDatausername, TrueDatapassword, log_level= logging.WARNING )
            symbol1 = fnolist[-1]
            try:
                td_obj.disconnect()
            except Exception:
                pass
            td_obj = TD('tdwsp127', 'saaral@127')
            first_chain = td_obj.start_option_chain( symbol1 , dt(expiry_date.year , expiry_date.month , expiry_date.day) ,chain_length = 75)

            te.sleep(4)

            df1 = first_chain.get_option_chain()
            first_chain.stop_option_chain()

            td_obj.disconnect()
            td_obj.disconnect()
            sampleDict[symbol1] = df1

            print(df1)
            # print(count)
            # print(item)
            count = count + 1

            optionChainprocess(df1,symbol1,expiry_date)
   
        except websocket.WebSocketConnectionClosedException as e:
            print('This caught the websocket exception in optionchain realtime')
            td_obj.disconnect()
            td_obj.disconnect()

        except IndexError as e:
            print('This caught the exception in optionchain realtime')
            print(e)
            td_obj.disconnect() 
            td_obj.disconnect()
        except Exception as e:
            print(e)
            td_obj.disconnect()
            td_obj.disconnect()
        sleep(1)

while True:
    create_currency()
    # create_equity()
#equity_testing()

    