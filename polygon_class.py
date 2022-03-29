import http.client
import ssl
import json
import time

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

class Polygon(object):
    def __init__(self):
        with open('/home/opc/private-info/polygon-api-key.txt', 'r') as f:
            self.apiKey = f.readline()

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
