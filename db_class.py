import mysql.connector

class DB(object):
    def __init__(self, db_info):
        with open(db_info, 'r') as f:
            info = f.read().splitlines()
            self.host = info[0]
            self.user = info[1]
            self.password = info[2]
            self.database = info[3]

        self.db = mysql.connector.connect(
            host = self.host,
            user = self.user,
            password = self.password,
            database = self.database
        )
        self.cursor = self.db.cursor()

    def UpdateStocks(self, stocks):

        sql = """INSERT INTO stocks (`ticker`, `name`, `type`, `active`, `cik`, `composite_figi`, `currency_name`, `last_updated_utc`, `locale`, `market`, `primary_exchange`, `share_class_figi`) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) AS value_alias 
                ON DUPLICATE KEY UPDATE 
                `name` = value_alias.name,
                `type`= value_alias.type,
                `active`= value_alias.active,
                `cik`= value_alias.cik,
                `composite_figi`= value_alias.composite_figi,
                `currency_name`= value_alias.currency_name,
                `last_updated_utc`= value_alias.last_updated_utc,
                `locale`= value_alias.locale,
                `market`= value_alias.market,
                `primary_exchange`= value_alias.primary_exchange,
                `share_class_figi`= value_alias.share_class_figi;"""
        values = [(s.ticker, s.name, s.type, s.active, s.cik, s.composite_figi, s.currency_name, s.last_updated_utc, s.locale, s.market, s.primary_exchange, s.share_class_figi) for s in stocks]
	
        self.cursor.executemany(sql, values)
        self.db.commit()

    def GetTickersAndMinMaxDates(self):
        sql = """ SELECT t1.ticker, t2.mn, t2.mx FROM stocks t1
                  LEFT JOIN (SELECT ticker, MIN(`datetime`) AS mn, MAX(`datetime`) as mx FROM minute_data GROUP BY `ticker`) t2
                  ON t1.ticker = t2.ticker;
              """

        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        results_dict = {}

        for row in results:
            results_dict[row[0]] = (row[1], row[2])

        return results_dict

    def AddMinuteData(self, ticker, data):
        sql = """ INSERT INTO minute_data (`ticker`, `open`, `high`, `low`, `close`, `volume`, `number_of_trades`, `volume_weighted_avg_price`, `datetime`) 
                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) AS value_alias 
                  ON DUPLICATE KEY UPDATE 
                  `volume_weighted_avg_price` = value_alias.volume_weighted_avg_price
              """

        lower_index = 0
        delta = 40000
        upper_index = min(delta, len(data))
        while upper_index < len(data):
            print("Adding data for ticker: {t}   {l} - {u}".format(t = ticker, l = lower_index, u = upper_index))

            values = [(ticker, d.open, d.high, d.low, d.close, d.volume, d.number_of_trades, d.volume_weighted_average, d.timestamp.strftime("%Y-%m-%d %H:%M:%S")) for d in data[lower_index:upper_index]]

            self.cursor.executemany(sql, values)
            self.db.commit()

            lower_index = lower_index + delta
            upper_index = min(upper_index + delta, len(data))

        print("Adding data for ticker: {t}   {l} - {u}".format(t = ticker, l = lower_index, u = upper_index))
        values = [(ticker, d.open, d.high, d.low, d.close, d.volume, d.number_of_trades, d.volume_weighted_average, d.timestamp.strftime("%Y-%m-%d %H:%M:%S")) for d in data[lower_index:upper_index]]

        self.cursor.executemany(sql, values)
        self.db.commit()