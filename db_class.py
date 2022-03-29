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

        sql = "INSERT INTO stocks VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        values = [(s.ticker, s.name, s.type, s.active, s.cik, s.composite_figi, s.currency_name, s.last_updated_utc, s.locale, s.market, s.primary_exchange, s.share_class_figi) for s in stocks]
	
        self.cursor.executemany(sql, values)
        self.db.commit()