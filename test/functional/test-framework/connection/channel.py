
import asyncio
import threading
from enum import Enum
import time
import weakref

import paramiko

from core.test_run import TestRun

class ChannelType(Enum):
    STDIN = 1
    STDOUT = 2
    STDERR = 3


class GenericChannel():
    """A generic channel implementation.
    
    This class is intended to provide a single interface for monitoring
    a process and listening to its output.

    Note: interface only supports reading from process stdout/stderr, not sending input
    via process stdin.
    """

    def __init__(self):
        # nothing to do
        pass

    def ready(self) -> bool:
        """Returns True if the channel is ready to read from (has at least 1 byte available).
        
        """
        raise NotImplementedError()
    
    def read(self, size: int) -> bytes:
        """Read the specified number of bytes from the stream.
        
        If the total number of bytes available is less than the specified number,
        the available bytes will be returned. If no further data is available
        (i.e. the channel is closed) then an empty bytes object is returned.
        """
        raise NotImplementedError()
    
    def closed(self):
        """Return True if the process the channel is attached to has exited.
        
        """
        raise NotImplementedError()


def read_coroutine_with_timeout(loop,   # not sure what type loop is in asyncio
        stream_reader: asyncio.StreamReader, timeout: int = -1):
    # TODO: simplify this code

    if timeout >= 0:
        async def read_coroutine(f, s):
            try:
                read_result = await asyncio.wait_for(
                    stream_reader.read(s),
                    timeout,
                    loop=loop
                )
            except asyncio.TimeoutError: # timeout was reached
                read_result = None
            
            f.set_result(read_result)
    else:
        async def read_coroutine(f, s):
            f.set_result(await stream_reader.read(s))
    
    return read_coroutine


class LocalChannel(GenericChannel):
    
    _process: asyncio.subprocess.Process = None
    _channel_type: ChannelType = None
    _reader: asyncio.StreamReader = None
    _read_chunk_size: int = None
    _buffer: bytearray = None
    _buffer_write_position: int = None
    _buffer_read_position: int = None
    _ready: bool = None
    _loop = None

    def __init__(self, loop, process: asyncio.subprocess.Process,
                 channel_type: ChannelType = ChannelType.STDOUT,
                 buffer_size: int = 1024):
        self._process = process
        self._channel_type = channel_type
        if channel_type == ChannelType.STDIN:
            raise ValueError("STDIN not supported by LocalChannel - only STDOUT and STDERR are supported")
        if channel_type == ChannelType.STDOUT:
            if process.stdout is None:
                raise ValueError("Cannot connect to process stdout - no file handle provided by process")
            self._reader = process.stdout
        elif channel_type == ChannelType.STDERR:
            if process.stderr is None:
                raise ValueError("Cannot connect to process stderr - no file handle provided by process")
            self._reader = process.stderr
        
        self._read_chunk_size = buffer_size
        # this buffer is only used to store any initial data retrieved from the stream during
        # the self.ready() call
        # it is not used to buffer regular reads (i.e. via the self.read() call)
        self._buffer = bytearray(buffer_size)
        self._buffer_write_position = 0
        self._buffer_read_position = 0
        self._ready = False
        self._loop = loop
    
    def _read(self, size:int = -1, timeout: int = -1) -> bytes:
        # attempt to read a byte into the buffer
        # if successful, channel is ready for reading
        # if not, no bytes on the channel yet - return None
        
        fut = self._loop.create_future()
        # read_with_timeout will populate the above future's result
        self._loop.create_task(read_coroutine_with_timeout(self._loop, self._reader, timeout)(fut, size))
        return self._loop.run_until_complete(fut)

    def ready(self) -> bool:
        if self._ready:
            return True
        
        # TEMP: extra logging statements
        TestRun.LOGGER.debug("Checking if local channel is ready for reading...")

        # [CSU] This class is in a bit of an odd position as it is trying to
        # mimic the behaviour of paramiko.Channel, which as far as I can tell
        # has non-blocking means to tell whether data is available to be read
        # (Channel.recv_ready()) however actually reading the data (Channel.recv())
        # is a blocking operation with an optional timeout. So the implementation may
        # seem a bit wonky as neither subprocess or asyncio really map 100% to this
        # behaviour, however I could be wrong on that.
        # TODO: review implementation
        # TEMP: set timeout to -1 to block here, to see how long the subprocess takes
        # to be ready for reading
        # bytes_read = self._read(1, 0.2)
        TestRun.LOGGER.debug("TEMP: initiating blocking read on subprocess stdout...")
        bytes_read = self._read(1, -1)
        if bytes_read is None:
            # not ready
            TestRun.LOGGER.debug("Local channel not yet ready")
            return False
        # it is acceptable if bytes_read is empty i.e. the end of the stream was reached
        # this still indicates that the stream is/was ready for reading, and future calls to
        # read() will simply return an empty bytes object
        num_bytes_read = len(bytes_read)
        TestRun.LOGGER.debug(f"Local channel ready for reading - {num_bytes_read} bytes read")
        if num_bytes_read == 0:
            self._ready = True
            return True
        
        # store any bytes read in the buffer so that they can be returned later on read()
        self._buffer[self._buffer_write_position:self._buffer_write_position + num_bytes_read] = bytes_read
        self._buffer_write_position += num_bytes_read
        self._ready = True
        return True
    
    def read(self, size: int) -> bytes:
        if not self.ready():
            return IOError("Channel is not yet ready for reading")

        TestRun.LOGGER.debug(f"Attempting to read {size} bytes from channel...")

        # check whether there's any data remaining in the buffer that hasn't been 'read' by
        # a caller yet
        num_unread_bytes = self._buffer_write_position - self._buffer_read_position
        if num_unread_bytes > 0:
            if size <= num_unread_bytes:
                # no need to read from the underlying stream
                return_bytes = bytes(self._buffer[self._buffer_read_position:self._buffer_read_position + size])
                self._buffer_read_position += size
                TestRun.LOGGER.debug(f"Read {size} bytes from channel (requested {size}, returned {size} bytes directly from initial buffer)")
                return return_bytes
            return_bytearray = bytearray(size)
            return_bytearray[0:num_unread_bytes] = self._buffer[self._buffer_read_position:self._buffer_read_position + num_unread_bytes]
            size -= num_unread_bytes
            write_index = num_unread_bytes
            # reset buffer positions to the start of the buffer
            self._buffer_read_position = 0
            self._buffer_write_position = 0
        else:
            return_bytearray = bytearray(size)
            write_index = 0
        
        while True:
            # [CSU] no timeout set on read - I believe that mimics the default behaviour of paramiko.Channel.recv()
            next_chunk = self._read(min(size, self._read_chunk_size))
            num_bytes_read = len(next_chunk)
            if num_bytes_read == 0: # no more data in stream
                # whatever we have is all that is available
                if num_unread_bytes > 0:
                    TestRun.LOGGER.debug(f"Read {write_index} bytes from channel (requested {size}, returned {num_unread_bytes} bytes directly from initial buffer)")
                else:
                    TestRun.LOGGER.debug(f"Read {write_index} bytes from channel (requested {size})")
                return bytes(return_bytearray[0:write_index])

            return_bytearray[write_index:write_index + num_bytes_read] = next_chunk
            write_index += num_bytes_read
            size -= num_bytes_read
            if size == 0: # finished reading
                if num_unread_bytes > 0:
                    TestRun.LOGGER.debug(f"Read {size} bytes from channel (requested {size}, returned {num_unread_bytes} bytes directly from initial buffer)")
                else:
                    TestRun.LOGGER.debug(f"Read {size} bytes from channel (requested {size})")
                return bytes(return_bytearray[0:size])

    def closed(self):
        return self._process.returncode is not None
    

class SshChannel(GenericChannel):
    
    _ssh_channel: paramiko.Channel = None

    def __init__(self, ssh_channel: paramiko.Channel):
        self._ssh_channel = ssh_channel

    def ready(self):
        return self._ssh_channel.recv_ready()
    
    def read(self, size=-1):
        return self._ssh_channel.recv(size)

    def closed(self):
        return self._ssh_channel.exit_status_ready()