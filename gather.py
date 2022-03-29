

tickers_file = open('tickers.txt', 'r')
tickers = tickers_file.readlines()

count = 0
# Strips the newline character
for ticker in tickers:
    count += 1
    print("Ticker{}: {}".format(count, ticker.strip()))