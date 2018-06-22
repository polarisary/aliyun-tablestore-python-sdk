# -*- coding: utf8 -*-

import time
import aiohttp
import asyncio

from tablestore.error import *

_NETWORK_IO_TIME_COUNT_FLAG = False
_network_io_time = 0

class Session(object):

    def __init__(self, host, path, timeout=30, loop=None):
        self.host = host
        self.path = path
        self.timeout = 30
        self.loop = loop
        if not self.loop:
            self.loop = asyncio.get_event_loop()

        self.session = aiohttp.ClientSession(loop=self.loop)

    async def close(self):
        await self.session.close()

    async def send_receive(self, url, request_headers, request_body):

        global _network_io_time

        if _NETWORK_IO_TIME_COUNT_FLAG:
            begin = time.time()

        response = await self.session.post(
            self.host + self.path + url, 
            data=request_body, 
            headers=request_headers,
            verify_ssl=False,
        )

        if _NETWORK_IO_TIME_COUNT_FLAG:
            end = time.time()
            _network_io_time += end - begin

        # TODO error handling
        response_headers = dict(response.headers)
        response_body = await response.read() # TODO figure out why response.read() don't work
        return response.status, response.reason, response_headers, response_body