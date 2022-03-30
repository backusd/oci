
from datetime import datetime, timedelta
from http.client import TEMPORARY_REDIRECT
import polygon_class
import db_class


def GetTickersFromFile():
    tickers = []
    with open('tickers.txt', 'r') as f:
        tickers = f.read().splitlines()
    return tickers

def UpdateStocks(polygon, db):
    # Get all the tickers in the tickers.txt file
    ticker_strings = GetTickersFromFile()

    # Get all tickers available from Polygon
    available_stocks = polygon.GetActiveStocks()
    print("Total Stocks: " + str(len(available_stocks)))
    
    # Get each stock object for each ticker from file
    stocks = []
    for ticker in ticker_strings:
        found = [s for s in available_stocks if s.ticker == ticker]
        if len(found) == 0:
            raise Exception("Found no stock matching the ticker: {t}".format(t = ticker))
        
        if len(found) > 1:
            raise Exception("Found multiple stocks matching ticker: {t}\nMatches: {m}".format(t = ticker, m = found))

        stocks.append(found[0])

    # Update/Insert stocks info into the database
    db.UpdateStocks(stocks)

def GatherData(polygon, db):
    
    # Get all stocks in the database with min/max dates
    ticker_dates_dict = db.GetTickersAndMinMaxDates()

    # Get the most recent date that data exists
    most_recent_trade_datetime = polygon.GetMostRecentTradeDate()

    today = datetime.today()
    two_years_ago = today - timedelta(days=770)

    # Loop over each ticker
    for key, value in ticker_dates_dict.items():
        ticker = key
        min_date = value[0]
        max_date = value[1]

        # If no max date exists, then no data has been gathered, so gather as much as possible
        if max_date is None:
            max_date = two_years_ago

        # If the most recent date available is newer, gather new data starting with the max date
        dates_diff = most_recent_trade_datetime - max_date
        if dates_diff.days > 0:
            new_data = polygon.GetDataForTicker(ticker, from_date=max_date, to_date=most_recent_trade_datetime)
            db.AddMinuteData(ticker, new_data)
    


if __name__ == "__main__":
    try:
        # Initialize the Polygon Class
        polygon = polygon_class.Polygon()

        # Initialize the database class
        db = db_class.DB('/home/opc/private-info/db-info.txt')

        # Update the tickers in the database (add new ones if applicable)
        #UpdateStocks(polygon, db)

        # Gather data for the stocks
        GatherData(polygon, db)


    except Exception as e:
        print(str(e))

