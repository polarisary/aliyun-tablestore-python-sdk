# -*- coding: utf8 -*-

from example_config import *

from tablestore.asyncclient import AsyncOTSClient
from tablestore.client import OTSClient
from tablestore.metadata import *
# from tablestore import *
import time

import aiohttp
import asyncio

import random
import logging

logger = logging.getLogger('async_test')  
logger.setLevel(logging.INFO) 
  
fh = logging.FileHandler('tablestore_sdk_test.log')  
fh.setLevel(logging.INFO)  
  
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')  
fh.setFormatter(formatter)  
  
logger.addHandler(fh)  

table_name = 'sp_etl'
step = 100000

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
        logger.error(be)

async def update_row(client, num):
    try:
        for i in range(num, num + step):
            primary_key = [('unique_key', str(i) + "23ggfdfc48b")]
            update_of_attribute_columns = {
                'PUT' : [('name','更新测试David Update'), ('addr','Update to Hongkong')],
            }
            row = Row(primary_key, update_of_attribute_columns)
            condition = Condition(RowExistenceExpectation.IGNORE)
            consumed, return_row = await client.update_row(table_name, row, condition) 
            print ('%s Update succeed, consume %s write cu.' % (i, consumed.write))
    except asyncio.CancelledError:
        pass
    except BaseException as be:
        print (be)

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


# **************
'''
def batch_get_row(client):
    # try get 10 rows from exist table and 10 rows from not-exist table
    columns_to_get = ['name', 'mobile', 'address', 'age']
    rows_to_get = []
    for i in range(0, 10):
        primary_key = [('gid',i), ('uid',i+1)]
        rows_to_get.append(primary_key)

    cond = CompositeColumnCondition(LogicalOperator.AND)
    cond.add_sub_condition(SingleColumnCondition("name", "John", ComparatorType.EQUAL))
    cond.add_sub_condition(SingleColumnCondition("address", 'China', ComparatorType.EQUAL))

    request = BatchGetRowRequest()
    request.add(TableInBatchGetRowItem(table_name, rows_to_get, columns_to_get, cond, 1))
    request.add(TableInBatchGetRowItem('notExistTable', rows_to_get, columns_to_get, cond, 1))

    result = client.batch_get_row(request)

    print ('Result status: %s'%(result.is_all_succeed()))
    
    table_result_0 = result.get_result_by_table(table_name)
    table_result_1 = result.get_result_by_table('notExistTable')

    print ('Check first table\'s result:')     
    for item in table_result_0:
        if item.is_ok:
            print ('Read succeed, PrimaryKey: %s, Attributes: %s' % (item.row.primary_key, item.row.attribute_columns))
        else:
            print ('Read failed, error code: %s, error message: %s' % (item.error_code, item.error_message))

    print ('Check second table\'s result:')
    for item in table_result_1:
        if item.is_ok:
            print ('Read succeed, PrimaryKey: %s, Attributes: %s' % (item.row.primary_key, item.row.attribute_columns))
        else:
            print ('Read failed, error code: %s, error message: %s' % (item.error_code, item.error_message))
'''
# **************

async def batch_get_row(client, num):
    try:
        for i in range(num, num + step):
            try:
                put_row_items = []
                for i in range(i, i+10):
                    primary_key = [('unique_key', str(i) + "23ggfdfc48b")]
                    attribute_columns = [('name','somebody '+str(i)), ('address','somewhere '+str(i)), ('age', i)]
                    row = Row(primary_key, attribute_columns)
                    condition = Condition(RowExistenceExpectation.IGNORE)
                    item = PutRowItem(row, condition)
                    put_row_items.append(item)

                for i in range(i + 10, i + 20):
                    primary_key = [('unique_key',str(i) + "23ggfdfc48b")]
                    attribute_columns = {'put': [('name','somebody '+str(i)), ('address','somewhere '+str(i)), ('age',i)]}
                    row = Row(primary_key, attribute_columns)
                    condition = Condition(RowExistenceExpectation.IGNORE)
                    item = UpdateRowItem(row, condition)
                    put_row_items.append(item)

                request = BatchWriteRowRequest()
                request.add(TableInBatchWriteRowItem(table_name, put_row_items))
                result = await client.batch_write_row(request)
                # logger.info('%s batch_write_row succeed, Result status %s', i, result.is_all_succeed())
            except BaseException as be:
                logger.error(be)

    except asyncio.CancelledError:
        pass
    except BaseException as be:
        logger.error(be)

async def batch_write_row(client, num):
    try:
        for i in range(num, num + step):
            try:
                put_row_items = []
                for i in range(i, i+10):
                    primary_key = [('unique_key', str(i) + "23ggfdfc48b")]
                    attribute_columns = [('name','somebody '+str(i)), ('address','somewhere '+str(i)), ('age', i)]
                    row = Row(primary_key, attribute_columns)
                    condition = Condition(RowExistenceExpectation.IGNORE)
                    item = PutRowItem(row, condition)
                    put_row_items.append(item)

                for i in range(i + 10, i + 20):
                    primary_key = [('unique_key',str(i) + "23ggfdfc48b")]
                    attribute_columns = {'put': [('name','somebody '+str(i)), ('address','somewhere '+str(i)), ('age',i)]}
                    row = Row(primary_key, attribute_columns)
                    condition = Condition(RowExistenceExpectation.IGNORE)
                    item = UpdateRowItem(row, condition)
                    put_row_items.append(item)

                request = BatchWriteRowRequest()
                request.add(TableInBatchWriteRowItem(table_name, put_row_items))
                result = await client.batch_write_row(request)
                # logger.info('%s batch_write_row succeed, Result status %s', i, result.is_all_succeed())
            except BaseException as be:
                logger.error(be)

    except asyncio.CancelledError:
        pass
    except BaseException as be:
        logger.error(be)

async def get_range(client, num):
    try:
        for i in range(num, num + step):

            # primary_key = [('unique_key', str(i) + "23ggfdfc48b")]
            # consumed, return_row, next_token = await client.get_row(table_name, primary_key)
            # print ('Value of attribute: %s' % return_row.attribute_columns)
            rdm = int(random.random() * step) + num
            
            # rdm = random.randint(num, num + step - 1)
            limit = 20
            inclusive_start_primary_key = [('unique_key', str(rdm) + '23ggfdfc48b')]
            exclusive_end_primary_key = [('unique_key', str(rdm + limit) + '23ggfdfc48b')]
            columns_to_get = []
            
            # cond = CompositeColumnCondition(LogicalOperator.AND)
            # cond.add_sub_condition(SingleColumnCondition("address", 'China', ComparatorType.EQUAL))
            # cond.add_sub_condition(SingleColumnCondition("age", 50, ComparatorType.LESS_THAN))

            consumed, next_start_primary_key, row_list, next_token  = await client.get_range(
                        table_name, Direction.FORWARD, 
                        inclusive_start_primary_key, exclusive_end_primary_key,
                        columns_to_get, 
                        limit, 
                        column_filter = None,
                        max_version = 1
            )
            # for row in row_list:
            #     print (row.primary_key, row.attribute_columns)
            print ('%s Get_range succeed, consume %s write cu. Total rows: %s' % (i, consumed.read,len(row_list)))
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
    client = AsyncOTSClient("https://socialpeta.cn-beijing.ots.aliyuncs.com", "LTAIPbQ31wPvbA1U", "YrLaOZ1eYuaO1Zfc2y5o35Gfj5Wyje", "socialpeta")

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
