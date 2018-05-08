# -*- coding: utf8 -*-

from example_config import *

from tablestore.asyncclient import AsyncOTSClient
from tablestore.client import OTSClient
from tablestore.metadata import *
# from tablestore import *
import time

import aiohttp
import asyncio

table_name = 'sp_etl'
step = 10000

def create_table(client):
    schema_of_primary_key = [('uid', 'INTEGER'), ('gid', 'INTEGER')]
    table_meta = TableMeta(table_name, schema_of_primary_key)
    table_options = TableOptions()
    reserved_throughput = ReservedThroughput(CapacityUnit(0, 0))
    client.create_table(table_meta, table_options, reserved_throughput)
    print ('Table has been created.')

def delete_table(client):
    client.delete_table(table_name)
    print ('Table \'%s\' has been deleted.' % table_name)

async def put_row(client, num):
    try:
        for i in range(num, num + step):
            primary_key = [('unique_key', str(i) + "23ggfdfc48b")]
            # print('Work url {0} Max Redirect {1}'.format(url, max_redirect))
            attribute_columns = [('name','测试async'+str(i)), ('growth',0.95), ('type','sub-provincial city'), ('postal code',310000), ('Alibaba', True), ('live_bj', True), ('addr', "北京市海淀区学院路29号")]
            row = Row(primary_key, attribute_columns)
            consumed, return_row = await client.put_row(table_name, row)
            print ('%s Write succeed, consume %s write cu.' % (i, consumed.write))
    except asyncio.CancelledError:
        pass
    except BaseException as be:
        print (be)

async def update_row(client, num):
    try:
        for i in range(num, num + step):
            primary_key = [('unique_key', str(i) + "23ggfdfc48b")]
            update_of_attribute_columns = {
                'PUT' : [('name','David Update'), ('addr','Hongkong')],
            }
            row = Row(primary_key, update_of_attribute_columns)
            consumed, return_row = client.update_row(table_name, row, condition) 
            print ('Update succeed, consume %s write cu.' % consumed.write)
    except asyncio.CancelledError:
        pass
    except BaseException as be:
        print (be)

    primary_key = [('gid',1), ('uid',"101")]
    update_of_attribute_columns = {
        'PUT' : [('name','David'), ('address','Hongkong')],
        'DELETE' : [('address', None, 1488436949003)],
        'DELETE_ALL' : [('mobile'), ('age')],
    }
    row = Row(primary_key, update_of_attribute_columns)
    condition = Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("age", 20, ComparatorType.EQUAL)) # update row only when this row is exist
    consumed, return_row = client.update_row(table_name, row, condition) 
    print ('Update succeed, consume %s write cu.' % consumed.write)

async def get_row(client, num):
    try:
        for i in range(num, num + step):
            primary_key = [('unique_key', str(i) + "23ggfdfc48b")]
            consumed, return_row, next_token = await client.get_row(table_name, primary_key)
            print ('Value of attribute: %s' % return_row.attribute_columns)
    except asyncio.CancelledError:
        pass
    except BaseException as be:
        print (be)

def do_work(client):
    workers = [asyncio.Task(get_row(client, j * step), loop=asyncio.get_event_loop())
                   for j in range(20)]
    return workers
    # print (len(workers))
    # for w in workers:
    #     if w:
    #         w.cancel()

if __name__ == '__main__':
    client = AsyncOTSClient("http://socialpeta.cn-beijing.ots.aliyuncs.com", "LTAIPbQ31wPvbA1U", "YrLaOZ1eYuaO1Zfc2y5o35Gfj5Wyje", "socialpeta")

    loop = asyncio.get_event_loop()
    tasks = do_work(client)
    loop.run_until_complete(asyncio.gather(*tasks))

    for w in tasks:
        if w:
            w.cancel()
    # print('Finished {0} urls in {1:.3f} secs'.format(len(crawler.done),
    #                                                  crawler.t1 - crawler.t0))
    

    loop.close()
    # try:
    #     delete_table(client)
    # except:
    #     pass
    # create_table(client)

    # time.sleep(3) # wait for table ready
    # put_row(client)
    # get_row2(client)
    # delete_table(client)
