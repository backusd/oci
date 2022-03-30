import http.client
import ssl
import json
import time
from datetime import datetime, timedelta

class PolygonStock(object):
    def __init__(self, ticker, name, type, active, cik, composite_figi, currency_name, last_updated_utc, locale, market, primary_exchange, share_class_figi):
        self.ticker = ticker
        self.name = name
        self.type = type
        self.active = active
        self.cik = cik
        self.composite_figi = composite_figi
        self.currency_name = currency_name
        self.last_updated_utc = last_updated_utc
        self.locale = locale
        self.market = market
        self.primary_exchange = primary_exchange
        self.share_class_figi = share_class_figi

    def __str__(self):
        return "{ticker} - {name}".format(ticker = self.ticker, name = self.name)

    def __repr__(self):
        return "{ticker} - {name}".format(ticker = self.ticker, name = self.name)

class PolygonDataPointMinute(object):
    def __init__(self, open, high, low, close, number_of_trades, timestamp, volume, volume_weighted_average):
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.number_of_trades = number_of_trades
        self.timestamp = timestamp
        self.volume = volume
        self.volume_weighted_average = volume_weighted_average

    def __str__(self):
        return "{timestamp} - O: {open} | H: {high} | L: {low} | C: {close}".format(
            timestamp = self.timestamp, open = self.open, high = self.high, low = self.low, close = self.close)

    def __repr__(self):
        return "{timestamp} - O: {open} | H: {high} | L: {low} | C: {close}".format(
            timestamp = self.timestamp, open = self.open, high = self.high, low = self.low, close = self.close)

class Polygon(object):
    def __init__(self):
        with open('/home/opc/private-info/polygon-api-key.txt', 'r') as f:
            self.apiKey = f.readline().strip()

        self.host = "api.polygon.io"

    def GET(self, location):
        location = location + "&apiKey={key}".format(key = self.apiKey)

        conn = http.client.HTTPSConnection(self.host, context=ssl._create_unverified_context())
        conn.request("GET", location)
        response = conn.getresponse()

        if response.status == 429:
            print("WARNING: GET request got status 429 - Too Many Requests")
            print("WARNING: Sleeping for 60 seconds and trying again...")
            conn.close()
            time.sleep(60)

            conn = http.client.HTTPSConnection(self.host, context=ssl._create_unverified_context())
            conn.request("GET", location)
            response = conn.getresponse()

            if response.status == 429:
                print("ERROR: GET request got status 429 - Too Many Requests")
                print("ERROR: Returning no results")
                conn.close()
                return ""
            elif response.status != 200:
                print("ERROR: GET request returned status {status} - {reason}".format(status = response.status, reason = response.reason))
                print("ERROR:		URL: api.polygon.io{location}".format(location = location))
                conn.close()
                return ""

        elif response.status != 200:
            print("ERROR: GET request returned status {status} - {reason}".format(status = response.status, reason = response.reason))
            print("ERROR:		URL: api.polygon.io{location}".format(location = location))
            conn.close()
            return ""

        # MUST extract the data from the request/connection before it can be closed
        string_data = response.read().decode("utf-8")
        conn.close()
        return string_data

    def GetActiveStocks(self):
        location = "/v3/reference/tickers?type=CS&active=true&sort=ticker&order=asc&limit=1000"
        string_data = self.GET(location)
        if string_data == "":
            raise Exception("Polygon: Failed to get active tickers")

        json_response = json.loads(string_data)

        stock_list = []
        # continue until either the amount returned is less than the limit, or no "next_url" field is given
        while True:
            results = json_response["results"]

            print("Latest first result: {ticker} - {name}".format(ticker = results[0]["ticker"], name = results[0]["name"]))

            for result in results:
                stock_list.append(PolygonStock(
                    result["ticker"],
                    result["name"],
                    result["type"],
                    result["active"],
                    "" if "cik" not in result else result["cik"],
                    "" if "composite_figi" not in result else result["composite_figi"],
                    result["currency_name"],
                    result["last_updated_utc"],
                    result["locale"],
                    result["market"],
                    result["primary_exchange"],
                    "" if "share_class_figi" not in result else result["share_class_figi"]
                ))

            # If there is no "next_url", then break
            if "next_url" not in json_response:
                print("No next_url - breaking")
                break

            # If the number of results was less than the limit parameter, then there is no more data to gather
            if json_response["count"] < 1000:
                print("Count:  {count}  is less than the limit: 1000".format(count = json_response["count"]))
                break

            print("Stored {count} new entries stock_list".format(count = json_response["count"]))
            print("Stock list count: {count}".format(count = len(stock_list)))

            # Get the next batch of data
            string_data = self.GET(json_response["next_url"].replace("https://api.polygon.io", ""))
            if string_data == "":
                print("WARNING: Obtained no additional data. Returning previously acquired data")
                return stock_list
            json_response = json.loads(string_data)

        return stock_list

    def GetMostRecentTradeDate(self):
        seven_days_ago = datetime.today() - timedelta(days=7)
        recent_data = self.GetDataForTicker("MSFT", from_date=seven_days_ago, to_date=datetime.today(), trying_to_get_max_date=True)
        
        if len(recent_data) == 0:
            raise Exception("Failed to get most recent trading date over the past seven days")

        return recent_data[-1].timestamp


    def GetDataForTicker(self, ticker, timespan="minute", multiplier=1, from_date=datetime.today(), to_date=datetime.today(), adjusted=True, sort="asc", trying_to_get_max_date=False):
        if adjusted:
            adjusted = "true"
        else:
            adjusted = "false"

        data_list = []

        while from_date < to_date:

            print("from_date: {fd}    to_date: {td}".format(fd = from_date.strftime("%Y-%m-%d"), td = to_date.strftime("%Y-%m-%d")))

            location = "/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from_date}/{to_date}?adjusted={adjusted}&sort={sort}&limit=50000".format(
                ticker = ticker, multiplier = multiplier, timespan = timespan, from_date = from_date.strftime("%Y-%m-%d"), to_date = to_date.strftime("%Y-%m-%d"), adjusted = adjusted, sort = sort)

            print("GetDataForTicker(): Last location: {0}".format(location))

            string_data = self.GET(location)
            if string_data == "":
                return [] # return an empty list so we can check if any data even exists for the ticker

            json_data = json.loads(string_data)            

            # Make sure the "results" key is present in the returned data
            if "results" in json_data:
                for result in json_data["results"]:
                    data_list.append(PolygonDataPointMinute(
                        result["o"],
                        result["h"],
                        result["l"],
                        result["c"],
                        0 if "n" not in result else result["n"],
                        datetime.fromtimestamp(result["t"] / 1000),
                        result["v"],
                        0 if "vw" not in result else result["vw"]
                    ))

            print("Count of data: {c}".format(c = len(data_list)))
            print("Filling Minutely Data")
            data_list = self.FillMinutelyData(data_list, to_date)
            print("Count of data: {c}".format(c = len(data_list)))

            #yesterday = datetime(2022, 3, 28, 0, 0, 0)
            #yesterday_data = [d for d in data_list if d.timestamp > yesterday]
            #if len(yesterday_data) > 0:
            #    print("Yesterday: len = {l}".format(l = len(yesterday_data)))
            #    for d in yesterday_data:
            #        print(d)

            # If the last day only has partial data, just remove all data from that day and we will get it later
            last_full_day_of_data = datetime(data_list[-1].timestamp.year, data_list[-1].timestamp.month, data_list[-1].timestamp.day, 0, 0, 0);
            print("Last full day of data: {l}".format(l = last_full_day_of_data))
            if (len([d for d in data_list if d.timestamp > last_full_day_of_data]) < 960):		# there should be 960 data points if the full day was captured
                print("Last full day found to be incomplete...")
                data_list = [d for d in data_list if d.timestamp < last_full_day_of_data]
                last_full_day_of_data = datetime(data_list[-1].timestamp.year, data_list[-1].timestamp.month, data_list[-1].timestamp.day, 0, 0, 0)
                print("    Count of data: {c}".format(c = len(data_list)))
                print("    Last full day of data: {l}".format(l = last_full_day_of_data))

            from_date = last_full_day_of_data + timedelta(days=1)

            if trying_to_get_max_date:
                break

        return data_list

    def FillMinutelyData(self, raw_data, max_date):
        # raw_data is a List of PolygonDataPointMinute

        filled_data = []

        # don't worry about the first entry - just assume it is good and go from there
        filled_data.append(raw_data[0])

        # loop over all other raw data
        for index in range(1, len(raw_data)):
		
            # Check if the new data point is the same day as the previous
            if raw_data[index].timestamp.day == raw_data[index - 1].timestamp.day:

                # while the most recent entry in filled_data is greater than one minute from the new data point, add filler data
                while int((raw_data[index].timestamp - filled_data[-1].timestamp).total_seconds() / 60) > 1:
                    # Create a new data point
                    filled_data.append(PolygonDataPointMinute(
                        filled_data[-1].close,	# open
                        filled_data[-1].close,	# high
                        filled_data[-1].close,	# low
                        filled_data[-1].close,	# close
                        0,						# number of trades
                        filled_data[-1].timestamp + timedelta(minutes=1),	# timestamp
                        0,						# volume
                        filled_data[-1].close,	# volume weighted avg
                    ))

                # Add the next data point
                filled_data.append(raw_data[index])

                # If this is the last data point or the last data point for this day, fill any data out to 

            else:
                # Initialize a datetime object for 1am on the day of the next data point
                next_timestamp = datetime(
                    raw_data[index].timestamp.year, 
                    raw_data[index].timestamp.month, 
                    raw_data[index].timestamp.day,
                    1, 0, 0)

                # while the timestamp for the next data point has not been met yet, add filler data
                while int((raw_data[index].timestamp - next_timestamp).total_seconds() / 60) > 0:
                    # Create a new data point
                    filled_data.append(PolygonDataPointMinute(
                        filled_data[-1].close,	# open
                        filled_data[-1].close,	# high
                        filled_data[-1].close,	# low
                        filled_data[-1].close,	# close
                        0,						# number of trades
                        next_timestamp,			# timestamp
                        0,						# volume
                        filled_data[-1].close,	# volume weighted avg
                    ))

                    next_timestamp = next_timestamp + timedelta(minutes=1)

                # Add the next data point
                filled_data.append(raw_data[index])

        # If the date (not including the time) matches the max_date, then add data up to 16:59:00 so it doesn't appear to have partial data
        last_datetime = filled_data[-1].timestamp
        print("last_datetime: {y} {m} {d}".format(y = last_datetime.year, m = last_datetime.month, d = last_datetime.day))
        print("max_date:      {y} {m} {d}".format(y = max_date.year, m = max_date.month, d = max_date.day))
        if last_datetime.year == max_date.year and last_datetime.month == max_date.month and last_datetime.day == max_date.day:
            while not (last_datetime.hour == 16 and last_datetime.minute == 59):
                last_datetime = last_datetime + timedelta(minutes=1)
                print("Filling last datetime: {l}".format(l = last_datetime))
                filled_data.append(PolygonDataPointMinute(
                    filled_data[-1].close,	# open
                    filled_data[-1].close,	# high
                    filled_data[-1].close,	# low
                    filled_data[-1].close,	# close
                    0,						# number of trades
                    last_datetime,			# timestamp
                    0,						# volume
                    filled_data[-1].close,	# volume weighted avg
                ))

        return filled_data