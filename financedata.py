import FinanceDataReader as fdr
import pandas as pd
import pymysql


def fdr_test():
    # 한국거래소 상장종목 전체
    # df_krx = fdr.StockListing('KRX')
    # print(df_krx)
    #
    # dataFrame = pd.DataFrame(df_krx.head(10))
    # dataFrame.to_csv("/Users/grange/df_krx.csv")
    # print(dataFrame)

    # print(fdr.__version__)

    # print(len(df_krx))

    df = fdr.DataReader('005930', '1900')
    df = fdr.DataReader('005930')
    # df.to_csv("/Users/grange/df.csv")

    # print(df.get_values())
    print(df)


def get_fdr_stocks():
    # df_krx = fdr.StockListing('KRX')

    return fdr.StockListing('KRX').get_values()


def get_price(symbol):
    return fdr.DataReader(symbol, '1900')


def select_test():
    # MySQL Connection 연결
    conn = pymysql.connect(host='deepcamping.iptime.org', port=13306, user='deepcamping', password='deepcamping1!',
                           db='deepcamping', charset='utf8')

    # Connection 으로부터 Cursor 생성
    curs = conn.cursor()

    # SQL문 실행
    sql = "select * from test"
    curs.execute(sql)

    # 데이타 Fetch
    rows = curs.fetchall()
    print(rows)  # 전체 rows
    # print(rows[0])  # 첫번째 row: (1, '김정수', 1, '서울')
    # print(rows[1])  # 두번째 row: (2, '강수정', 2, '서울')

    # Connection 닫기
    conn.close()


def select_stocks():
    db = pymysql.connect(host='deepcamping.iptime.org', port=13306, user='deepcamping',
                         passwd='deepcamping1!', db='deepcamping', charset='utf8')
    try:
        cursor = db.cursor()
        sql = "SELECT * from stocks where symbol > '023890'"

        cursor.execute(sql)
        rows = cursor.fetchall()
        # print(rows)
    finally:
        db.close()

    return rows


def insert_stocks(stocks):
    # print(stocks)
    db = pymysql.connect(host='deepcamping.iptime.org', port=13306, user='deepcamping',
                         passwd='deepcamping1!', db='deepcamping', charset='utf8')
    try:
        cursor = db.cursor()
        i = 0
        for symbol, name, sector, industry in stocks:
            sql = "INSERT INTO stocks (symbol, name, sector, industry) VALUES(\'{0}\', \'{1}\', \'{2}\', \'{3}\')".format(
                str(symbol), str(name), str(sector), str(industry).replace("'", "&#x27;"))
            # print(sql, i)
            # print(symbol, name, i)
            i = i + 1
            cursor.execute(sql)

        db.commit()
        print(cursor.lastrowid)
    finally:
        db.close()


def insert_price(symbol, prices):
    db = pymysql.connect(host='deepcamping.iptime.org', port=13306, user='deepcamping',
                         passwd='deepcamping1!', db='deepcamping', charset='utf8')
    try:
        cursor = db.cursor()
        for price in prices:
            sql = "INSERT INTO price (symbol, date_, open_, high_, low_, close_, volume_, change_) " \
                  "VALUES(\'{0}\', \'{1}\', {2}, {3}, {4}, {5}, {6}, {7})" \
                .format(symbol, price[0], float(price[1]), float(price[2]), float(price[3]), float(price[4]),
                        float(price[5]), \
                        float(str(price[6]).replace("nan", "0.0")))
            # print(sql)
            cursor.execute(sql)

        db.commit()
    finally:
        db.close()


def make_price_csv():
    for stock in get_fdr_stocks():
        # print(str(stock[0]))
        df = fdr.DataReader(str(stock[0]), '1900')
        df.to_csv("/Users/grange/Downloads/financedata/" + str(stock[0]) + ".csv")


def insert_price_csv():
    for stock in select_stocks():
        dataset = pd.read_csv("/Users/hb.shin/Nextcloud/Documents/financedata/" + str(stock[0]) + ".csv")
        # print(dataset.get_values())
        print(stock[0])
        insert_price(stock[0], dataset.get_values())


def main():
    # insert()
    # select_test()
    # fdr_test()
    # get_fdr_stocks()
    # insert_stocks(get_fdr_stocks())
    # select_stocks()
    # insert_price()
    # make_price_csv()
    insert_price_csv()


if __name__ == '__main__':
    main()
