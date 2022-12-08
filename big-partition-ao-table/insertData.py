#!/bin/env python3



# how to run the program
# python3 db_inflater.py 100 10 out.csv
# 写100条数据，一次10条，同时输出到 out.csv


import datetime

import shortuuid
import psycopg2
import sys
import random


# random text
class TextGen:
    length = 8

    def __init__(self, length=8):
        self.length = length

    def gen(self):
        text = shortuuid.uuid()
        while len(text) < self.length:
            text += shortuuid.uuid()
        return f"'{text[:self.length]}'"

# specified text
class FixedTextGen:
    text = ""

    def __init__(self, text):
        self.text = text

    def gen(self):
        return f"'{self.text}'"

# specified number
class SeqNumGen:
    num = 0

    def __init__(self, num=0):
        self.num = num

    def gen(self):
        ret = self.num
        self.num += 1
        return f'{ret}'

# now date
class SeqDateGen:
    date = None
    delta = None

    def __init__(
            self,
            date=datetime.date.today()):
        self.date = date

    def gen(self):
        ret = self.date.strftime("%Y/%m/%d")
        return f"'{ret}'"


class TimestampGen:
    date = None
    delta = None
    def __init__(
            self,
            date=datetime.datetime.now(),
            delta=datetime.timedelta(minutes=1)):
        self.date = date
        self.delta = delta

    def gen(self):
        ret = self.date
        self.date += self.delta
        return f"'{ret}'"



class GetSpecificDate:
    def __init__(self, date):
        self.date = date

    def getNext(self):
        ret = self.date
        self.date = self.date + datetime.timedelta(days=1)
        return ret


class Infalator:
    table = ""
    col_gen = {}
    conn = None
    csv_path = None

    def __init__(
            self,
            host, port, db, table, col_gen, usr='gpadmin', password='gpadmin',
            csv_path=None):
        self.table = table
        self.col_gen = col_gen
        self.conn = psycopg2.connect(
                host=host, port=port, user=usr, password=password, dbname=db)
        self.csv_path = csv_path

    def inflate(self, total_num=100, batch_number=10):
        csv_file = None
        if self.csv_path:
            csv_file = open(self.csv_path, 'w')
        num = 0
        while num < total_num:
            left = total_num - num
            count = batch_number
            if left < batch_number:
                count = left
            self._do_inflate(count, csv_file)
            num += count
            print(f"{num} records has been inserted")
        if csv_file:
            csv_file.close()

    def _do_inflate(self, number=10, csv_file=None):
        keys = self.col_gen.keys()
        cols = ','.join(keys)
        value_list = []
        for i in range(number):
            row = []
            for k in keys:
                row.append(self.col_gen[k].gen())
            value_list.append('(%s)' % ','.join(row))
            if csv_file:
                csv_file.write(','.join(row))
                csv_file.write('\n')
        values = ",".join(value_list)
        sql = f"INSERT INTO {self.table}({cols}) VALUES {values}"

        cur = self.conn.cursor()
        try:
            cur.execute(sql)
        except Exception as err:
            print("sql to execute: ",sql)
            print(err)
            with open('insert.log','a') as f:
                f.write(sql)
                f.write(err)
                f.write('\n')
        self.conn.commit()
        cur.close()
        return sql




if __name__ == '__main__':
    count = 2000
    bach_cont = 1000
    csv_path = None

    for idx, val in enumerate(sys.argv):
        if idx == 1:
            count = int(val)
        if idx == 2:
            bach_cont = int(val)
        if idx == 3:
            csv_path = val
    WantDate = GetSpecificDate(datetime.datetime(2021, 1, 2))
    infalator = Infalator(
        '127.0.0.1', '6000', 'gpadmin', '"sc_bloomberg"."t_equity_options_uo_v2_pricing"',
        {
            'rc': SeqNumGen(random.randint(1, 100000)),
            'id_bb_global':TextGen(10),
            'px_close_dt':TimestampGen(),
            'px_open': SeqNumGen(random.randint(1, 100000)),
            'px_high': SeqNumGen(random.randint(1, 100000)),
            'class': TextGen(5),
            'security_des':TextGen(4),
            'id_bb_unique': TextGen(10),
            'feed_source': TextGen(10),
            'opt_strike_px': SeqNumGen(12345),
            'last_update_dt':SeqDateGen(),
            'last_update_date_eod':SeqDateGen(),
            'px_open': SeqNumGen(random.randint(1, 100000)),
            'load_date':SeqDateGen(WantDate.getNext()),
            'composite_id_bb_global':TextGen(10),
            'px_scaling_factor': SeqNumGen(random.randint(1, 100000)),
            'load_time':TimestampGen(),

        },
        csv_path=csv_path
    )

    for i in range(800):
        infalator.inflate(count, bach_cont)
        infalator = Infalator(
        '127.0.0.1', '6000', 'gpadmin', '"public"."mytest"',
        {
            'rc': SeqNumGen(random.randint(1, 100000)),
            'id_bb_global':TextGen(10),
            'px_close_dt':TimestampGen(),
            'px_open': SeqNumGen(random.randint(1, 100000)),
            'px_high': SeqNumGen(random.randint(1, 100000)),
            'class': TextGen(5),
            'security_des':TextGen(4),
            'id_bb_unique': TextGen(10),
            'feed_source': TextGen(10),
            'opt_strike_px': SeqNumGen(12345),
            'last_update_dt':SeqDateGen(),
            'last_update_date_eod':SeqDateGen(),
            'px_open': SeqNumGen(random.randint(1, 100000)),
            'load_date':SeqDateGen(WantDate.getNext()),
            'composite_id_bb_global':TextGen(10),
            'px_scaling_factor': SeqNumGen(random.randint(1, 100000)),
            'load_time':TimestampGen(),
        },)
