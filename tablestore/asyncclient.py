# -*- coding: utf8 -*-
# Implementation of OTSClient

import aiohttp
import asyncio
import async_timeout
from tablestore.session import Session

from tablestore.error import *
from tablestore.protocol import OTSProtocol
from tablestore.connection import ConnectionPool
from tablestore.metadata import *
from tablestore.retry import DefaultRetryPolicy

import logging
try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

try:
    import urlparse 
except ImportError:
    import urllib.parse as urlparse


class AsyncOTSClient(object):

    DEFAULT_ENCODING = 'utf8'
    DEFAULT_SOCKET_TIMEOUT = 50
    DEFAULT_LOGGER_NAME = 'tablestore-client'

    protocol_class = OTSProtocol

    def __init__(self, end_point, access_key_id, access_key_secret, instance_name, **kwargs):
        """
        初始化``AsyncOTSClient``实例。

        ``end_point``是OTS服务的地址（例如 'http://instance.cn-hangzhou.ots.aliyun.com'），必须以'http://'或'https://'开头。

        ``access_key_id``是访问OTS服务的accessid，通过官方网站申请或通过管理员获取。

        ``access_key_secret``是访问OTS服务的accesskey，通过官方网站申请或通过管理员获取。

        ``instance_name``是要访问的实例名，通过官方网站控制台创建或通过管理员获取。

        ``sts_token``是访问OTS服务的STS token，从STS服务获取，具有有效期，过期后需要重新获取。

        ``encoding``请求参数的字符串编码类型，默认是utf8。

        ``socket_timeout``是连接池中每个连接的Socket超时，单位为秒，可以为int或float。默认值为50。

        ``logger_name``用来在请求中打DEBUG日志，或者在出错时打ERROR日志。

        ``retry_policy``定义了重试策略，默认的重试策略为 DefaultRetryPolicy。你可以继承 RetryPolicy 来实现自己的重试策略，请参考 DefaultRetryPolicy 的代码。


        示例：创建一个AsyncOTSClient实例

            from tablestore.client import AsyncOTSClient

            client = AsyncOTSClient('your_instance_endpoint', 'your_user_id', 'your_user_key', 'your_instance_name')
        """

        self._validate_parameter(end_point, access_key_id, access_key_secret, instance_name)
        sts_token = kwargs.get('sts_token')

        self.encoding = kwargs.get('encoding')
        if self.encoding is None:
            self.encoding = AsyncOTSClient.DEFAULT_ENCODING

        self.socket_timeout = kwargs.get('socket_timeout')
        if self.socket_timeout is None:
            self.socket_timeout = AsyncOTSClient.DEFAULT_SOCKET_TIMEOUT

        # initialize logger
        logger_name = kwargs.get('logger_name')
        if logger_name is None:
            self.logger = logging.getLogger(AsyncOTSClient.DEFAULT_LOGGER_NAME)
            nullHandler = NullHandler()
            self.logger.addHandler(nullHandler)
        else:
            self.logger = logging.getLogger(logger_name)

        # parse end point
        scheme, netloc, path = urlparse.urlparse(end_point)[:3]
        host = scheme + "://" + netloc

        if scheme != 'http' and scheme != 'https':
            raise OTSClientError(
                "protocol of end_point must be 'http' or 'https', e.g. http://instance.cn-hangzhou.ots.aliyun.com."
            )
        if host == '':
            raise OTSClientError(
                "host of end_point should be specified, e.g. http://instance.cn-hangzhou.ots.aliyun.com."
            )

        # intialize protocol instance via user configuration
        self.protocol = self.protocol_class(
            access_key_id,
            access_key_secret, 
            sts_token,
            instance_name, 
            self.encoding, 
            self.logger
        )
        
        loop = kwargs.get('loop')
        if loop is None:
            self.loop = asyncio.get_event_loop()
        else:
            self.loop = loop
        self.session = Session(host, path, timeout=self.socket_timeout, loop=self.loop)

        # initialize the retry policy
        retry_policy = kwargs.get('retry_policy')
        if retry_policy is None:
            retry_policy = DefaultRetryPolicy()
        self.retry_policy = retry_policy

    async def get_row(self, table_name, primary_key, columns_to_get=None, 
                column_filter=None, max_version=1, time_range=None,
                start_column=None, end_column=None, token=None):
        """
        说明：获取一行数据。

        ``table_name``是对应的表名。
        ``primary_key``是主键，类型为dict。
        ``columns_to_get``是可选参数，表示要获取的列的名称列表，类型为list；如果不填，表示获取所有列。
        ``column_filter``是可选参数，表示读取指定条件的行
        ``max_version``是可选参数，表示最多读取的版本数
        ``time_range``是可选参数，表示读取额版本范围或特定版本，和max_version至少存在一个

        返回：本次操作消耗的CapacityUnit、主键列和属性列。

        ``consumed``表示消耗的CapacityUnit，是tablestore.metadata.CapacityUnit类的实例。
        ``return_row``表示行数据，包括主键列和属性列，类型都为list，如：[('PK0',value0), ('PK1',value1)]。
        ``next_token``表示宽行读取时下一次读取的位置，编码的二进制。

        示例：

            primary_key = [('gid',1), ('uid',101)]
            columns_to_get = ['name', 'address', 'age']
            consumed, return_row, next_token = client.get_row('myTable', primary_key, columns_to_get)
        """

        retval = await self._request_helper(
                    'GetRow', table_name, primary_key, columns_to_get, 
                    column_filter, max_version, time_range,
                    start_column, end_column, token
        )
        return retval

    async def put_row(self, table_name, row, condition = None, return_type = None):
        """
        说明：写入一行数据。返回本次操作消耗的CapacityUnit。

        ``table_name``是对应的表名。
        ``row``是行数据，包括主键和属性列。
        ``condition``表示执行操作前做条件检查，满足条件才执行，是tablestore.metadata.Condition类的实例。
        目前支持两种条件检测，一是对行的存在性进行检查，检查条件包括：'IGNORE'，'EXPECT_EXIST'和'EXPECT_NOT_EXIST';二是对属性列值的条件检测。
        ``return_type``表示返回类型，是tablestore.metadata.ReturnType类的实例，目前仅支持返回PrimaryKey，一般用于主键列自增中。

        返回：本次操作消耗的CapacityUnit和需要返回的行数据。

        consumed表示消耗的CapacityUnit，是tablestore.metadata.CapacityUnit类的实例。
        return_row表示返回的行数据，可能包括主键、属性列。

        示例：

            primary_key = [('gid',1), ('uid',101)]
            attribute_columns = [('name','张三'), ('mobile',111111111), ('address','中国A地'), ('age',20)]
            row = Row(primary_key, attribute_columns)
            condition = Condition('EXPECT_NOT_EXIST')
            consumed, return_row = client.put_row('myTable', row, condition)
        """

        retval = await self._request_helper(
            'PutRow', table_name, row, condition, return_type
        )
        return retval

    async def update_row(self, table_name, row, condition, return_type = None):
        """
        说明：更新一行数据。

        ``table_name``是对应的表名。
        ``row``表示更新的行数据，包括主键列和属性列，主键列是list；属性列是dict。
        ``condition``表示执行操作前做条件检查，满足条件才执行，是tablestore.metadata.Condition类的实例。
        目前支持两种条件检测，一是对行的存在性进行检查，检查条件包括：'IGNORE'，'EXPECT_EXIST'和'EXPECT_NOT_EXIST';二是对属性列值的条件检测。
        ``return_type``表示返回类型，是tablestore.metadata.ReturnType类的实例，目前仅支持返回PrimaryKey，一般用于主键列自增中。

        返回：本次操作消耗的CapacityUnit和需要返回的行数据return_row

        consumed表示消耗的CapacityUnit，是tablestore.metadata.CapacityUnit类的实例。
        return_row表示需要返回的行数据。

        示例：

            primary_key = [('gid',1), ('uid',101)]
            update_of_attribute_columns = {
                'put' : [('name','张三丰'), ('address','中国B地')],
                'delete' : [('mobile', 1493725896147)],
                'delete_all' : [('age')]
            }
            row = Row(primary_key, update_of_attribute_columns)
            condition = Condition('EXPECT_EXIST')
            consumed = client.update_row('myTable', row, condition) 
        """

        return await self._request_helper(
            'UpdateRow', table_name, row, condition, return_type
        )

    async def delete_row(self, table_name, row, condition, return_type = None):
        """
        说明：删除一行数据。

        ``table_name``是对应的表名。
        ``row``表示行数据，在delete_row仅包含主键。
        ``condition``表示执行操作前做条件检查，满足条件才执行，是tablestore.metadata.Condition类的实例。
        目前支持两种条件检测，一是对行的存在性进行检查，检查条件包括：'IGNORE'，'EXPECT_EXIST'和'EXPECT_NOT_EXIST';二是对属性列值的条件检测。

        返回：本次操作消耗的CapacityUnit和需要返回的行数据return_row

        consumed表示消耗的CapacityUnit，是tablestore.metadata.CapacityUnit类的实例。
        return_row表示需要返回的行数据。

        示例：

            primary_key = [('gid',1), ('uid',101)]
            row = Row(primary_key)
            condition = Condition('IGNORE')
            consumed, return_row = client.delete_row('myTable', row, condition) 
        """

        return await self._request_helper(
            'DeleteRow', table_name, row, condition, return_type
        )

    async def batch_get_row(self, request):
        """
        说明：批量获取多行数据。
        request = BatchGetRowRequest()

        request.add(TableInBatchGetRowItem(myTable0, primary_keys, column_to_get=None, column_filter=None))
        request.add(TableInBatchGetRowItem(myTable1, primary_keys, column_to_get=None, column_filter=None))
        request.add(TableInBatchGetRowItem(myTable2, primary_keys, column_to_get=None, column_filter=None))
        request.add(TableInBatchGetRowItem(myTable3, primary_keys, column_to_get=None, column_filter=None))

        response = client.batch_get_row(request)

        ``response``为返回的结果，类型为tablestore.metadata.BatchGetRowResponse

        示例：
            cond = CompositeColumnCondition(LogicalOperator.AND)
            cond.add_sub_condition(SingleColumnCondition("index", 0, ComparatorType.EQUAL))
            cond.add_sub_condition(SingleColumnCondition("addr", 'china', ComparatorType.EQUAL))

            request = BatchGetRowRequest()
            column_to_get = ['gid', 'uid', 'index']

            primary_keys = []
            primary_keys.append([('gid',0), ('uid',0)])
            primary_keys.append([('gid',0), ('uid',1)])
            primary_keys.append([('gid',0), ('uid',2)])
            request.add(TableInBatchGetRowItem('myTable0', primary_keys, column_to_get, cond))

            primary_keys = []
            primary_keys.append([('gid',0), ('uid',0)])
            primary_keys.append([('gid',1), ('uid',0)])
            primary_keys.append([('gid',2), ('uid',0)])
            request.add(TableInBatchGetRowItem('myTable1', primary_keys, column_to_get, cond))

            result = client.batch_get_row(request)

            table0 = result.get_result_by_table('myTable0')
            table1 = result.get_result_by_table('myTable1')
        """
        response = await self._request_helper('BatchGetRow', request)
        return BatchGetRowResponse(response)

    async def batch_write_row(self, request):
        """
        说明：批量修改多行数据。
        request = MiltiTableInBatchWriteRowItem()

        request.add(TableInBatchWriteRowItem(table0, row_items))
        request.add(TableInBatchWriteRowItem(table1, row_items))

        response = client.batch_write_row(request)

        ``response``为返回的结果，类型为tablestore.metadata.BatchWriteRowResponse

        示例：
            # put 
            row_items = []
            row = Row([('gid',0), ('uid', 0)], [('index', 6), ('addr', 'china')])
            row_items.append(PutRowItem(row,
                Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 0, ComparatorType.EQUAL))))

            # update
            row = Row([('gid',1), ('uid', 0)], {'put': [('index',9), ('addr', 'china')]})
            row_items.append(UpdateRowItem(row,
                Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 0, ComparatorType.EQUAL))))

            # delete
            row = Row([('gid', 2), ('uid', 0)])
            row_items.append(DeleteRowItem(row,
                Condition(RowExistenceExpectation.IGNORE, SingleColumnCondition("index", 3, ComparatorType.EQUAL, False)))

            request = BatchWriteRowRequest()
            request.add(TableInBatchWriteRowItem('myTable0', row_items))
            request.add(TableInBatchWriteRowItem('myTable1', row_items))

            result = self.client_test.batch_write_row(request)

            r0 = result.get_put_by_table('myTable0')
            r1 = result.get_put_by_table('myTable1')

        """

        response = await self._request_helper('BatchWriteRow', request)
        
        return BatchWriteRowResponse(request, response)


    async def get_range(self, table_name, direction, 
                  inclusive_start_primary_key, 
                  exclusive_end_primary_key, 
                  columns_to_get=None, 
                  limit=None, 
                  column_filter=None,
                  max_version=1,
                  time_range=None,
                  start_column=None,
                  end_column=None,
                  token = None):
        """
        说明：根据范围条件获取多行数据。

        ``table_name``是对应的表名。
        ``direction``表示范围的方向，字符串格式，取值包括'FORWARD'和'BACKWARD'。
        ``inclusive_start_primary_key``表示范围的起始主键（在范围内）。
        ``exclusive_end_primary_key``表示范围的结束主键（不在范围内）。
        ``columns_to_get``是可选参数，表示要获取的列的名称列表，类型为list；如果不填，表示获取所有列。
        ``limit``是可选参数，表示最多读取多少行；如果不填，则没有限制。
        ``column_filter``是可选参数，表示读取指定条件的行
        ``max_version``是可选参数，表示返回的最大版本数目，与time_range必须存在一个。
        ``time_range``是可选参数，表示返回的版本的范围，于max_version必须存在一个。
        ``start_column``是可选参数，用于宽行读取，表示本次读取的起始列。
        ``end_column``是可选参数，用于宽行读取，表示本次读取的结束列。
        ``token``是可选参数，用于宽行读取，表示本次读取的起始列位置，内容被二进制编码，来源于上次请求的返回结果中。

        返回：符合条件的结果列表。

        ``consumed``表示本次操作消耗的CapacityUnit，是tablestore.metadata.CapacityUnit类的实例。
        ``next_start_primary_key``表示下次get_range操作的起始点的主健列，类型为dict。
        ``row_list``表示本次操作返回的行数据列表，格式为：[Row, ...]。
        ``next_token``表示最后一行是否还有属性列没有读完，如果next_token不为None，则表示还有，下次get_range需要填充此值。

        示例：

            inclusive_start_primary_key = [('gid',1), ('uid',INF_MIN)] 
            exclusive_end_primary_key = [('gid',4), ('uid',INF_MAX)] 
            columns_to_get = ['name', 'address', 'mobile', 'age']
            consumed, next_start_primary_key, row_list, next_token = client.get_range(
                        'myTable', 'FORWARD', 
                        inclusive_start_primary_key, exclusive_end_primary_key,
                        columns_to_get, 100
            )
        """

        return await self._request_helper(
                    'GetRange', table_name, direction, 
                    inclusive_start_primary_key, exclusive_end_primary_key,
                    columns_to_get, limit,
                    column_filter, max_version,
                    time_range, start_column,
                    end_column, token
        )

    async def _request_helper(self, api_name, *args, **kwargs):
        query, reqheaders, reqbody = self.protocol.make_request(
            api_name, *args, **kwargs
        )

        retry_times = 0

        while True:
            try:
                status, reason, resheaders, resbody = await self.session.send_receive(
                    query, reqheaders, reqbody
                )
                self.protocol.handle_error(api_name, query, status, reason, resheaders, resbody)
                break

            except OTSServiceError as e:

                if self.retry_policy.should_retry(retry_times, e, api_name):
                    retry_delay = self.retry_policy.get_retry_delay(retry_times, e, api_name)
                    time.sleep(retry_delay)
                    retry_times += 1
                else:
                    raise e
            except BaseException as be:
                raise be

        return self.protocol.parse_response(api_name, status, resheaders, resbody)

    async def close(self):
        await self.session.close()

    def _validate_parameter(self, endpoint, access_key_id, access_key_secret, instance_name):
        if endpoint is None or len(endpoint) == 0:
            raise OTSClientError('endpoint is None or empty.')

        if access_key_id is None or len(access_key_id) == 0:
            raise OTSClientError('access_key_id is None or empty.')

        if access_key_secret is None or len(access_key_secret) == 0:
            raise OTSClientError('access_key_secret is None or empty.')

        if instance_name is None or len(instance_name) == 0:
            raise OTSClientError('instance_name is None or empty.')
            