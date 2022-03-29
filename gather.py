
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
    print("nope")


if __name__ == "__main__":
    try:
        # Initialize the Polygon Class
        polygon = polygon_class.Polygon()

        # Initialize the database class
        db = db_class.DB('/home/opc/private-info/db-info.txt')

        # Update the tickers in the database (add new ones if applicable)
        UpdateStocks(polygon, db)

        # Gather data for the stocks
        GatherData(polygon, db)


    except Exception as e:
        print(str(e))

