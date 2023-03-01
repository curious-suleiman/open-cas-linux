
import asyncio
import threading
from enum import Enum
import time
import weakref
import os
import select

import paramiko

from core.test_run import TestRun

class ChannelType(Enum):
    NONE = 0
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

def read_from_fd_coroutine(fd):
    async def read_coroutine(f, s, t):
        if t < 0:
            try:
                # note that this call may block indefinitely until data is
                # available in fd
                read_result = os.read(fd, s)
            except OSError: # fd is closed
                read_result = bytes()
        else:
            rd_available, _, _ = select.select([fd], [], [], t)
            if rd_available:
                try:
                    read_result = os.read(fd, s)
                except OSError: # fd is closed
                    read_result = bytes()
            else:
                read_result = bytes()
        
        f.set_result(read_result)
    
    return read_coroutine

def read_from_streamreader_coroutine(reader: asyncio.StreamReader):
    # TODO: simplify this code

    async def read_coroutine(f, s, t):
        if t < 0:
            read_result = await reader.read(s)
        else:
            try:
                read_result = await asyncio.wait_for(
                    reader.read(s),
                    t
                )
            except asyncio.TimeoutError: # timeout was reached
                read_result = bytes()
        
        if read_result is None:
            # streamreader is not ready yet
            # treat the same as hitting the timeout
            read_result = bytes()

        f.set_result(read_result)
    
    return read_coroutine


class LocalChannel(GenericChannel):
    
    _process: asyncio.subprocess.Process = None
    _read_func = None
    _read_chunk_size: int = None
    _buffer: bytearray = None
    _buffer_write_position: int = None
    _buffer_read_position: int = None
    _ready: bool = None
    _loop = None

    def __init__(self, loop, process: asyncio.subprocess.Process,
                 channel_type: ChannelType = ChannelType.NONE,
                 file_descriptor: int = -1,
                 buffer_size: int = 1024):
        self._process = process
        if channel_type == ChannelType.NONE and file_descriptor < 0:
            raise ValueError("Must provide one of channel_type, file_descriptor")
        if file_descriptor >= 0:
            self._read_func = read_from_fd_coroutine(file_descriptor)
        else:
            if channel_type == ChannelType.STDIN:
                raise ValueError("STDIN not supported by LocalChannel - only STDOUT and STDERR are supported")
            if channel_type == ChannelType.STDOUT:
                if process.stdout is None:
                    raise ValueError("Cannot connect to process stdout - no file handle provided by process")
                self._read_func = read_from_streamreader_coroutine(process.stdout)
            elif channel_type == ChannelType.STDERR:
                if process.stderr is None:
                    raise ValueError("Cannot connect to process stderr - no file handle provided by process")
                self._read_func = read_from_streamreader_coroutine(process.stderr)
            else:
                raise ValueError(f"Unsupported value for channel_type: {channel_type}")
        
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
        self._loop.create_task(self._read_func(fut, size, timeout))
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
        # TODO: consider subprocess.Popen + select in a busy loop?
        # TEMP: set timeout to -1 to block here, to see how long the subprocess takes
        # to be ready for reading
        bytes_read = self._read(1, 0.2)
        # TestRun.LOGGER.debug("TEMP: initiating blocking read on subprocess channel...")
        # bytes_read = self._read(1, -1)
        if len(bytes_read) == 0:
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
            return bytes()

        TestRun.LOGGER.debug(f"Attempting to read {size} bytes from channel...")

        # check whether there's any data remaining in the buffer that hasn't been 'read' by
        # a caller yet
        TestRun.LOGGER.debug("Checking for unread bytes in buffer...")
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
            # TEMP: log what was actually read
            TestRun.LOGGER.debug(f"Read {num_unread_bytes} bytes from buffer (requested {size}) - content:")
            TestRun.LOGGER.debug(return_bytearray[0:num_unread_bytes].decode('utf-8'))
            size -= num_unread_bytes
            write_index = num_unread_bytes
            # reset buffer positions to the start of the buffer
            self._buffer_read_position = 0
            self._buffer_write_position = 0
        else:
            return_bytearray = bytearray(size)
            write_index = 0
        
        TestRun.LOGGER.debug(f"Reading {size} remaining requested bytes from channel...")
        while True:
            TestRun.LOGGER.debug(f"Attempting to read {min(size, self._read_chunk_size)} bytes from channel...")
            next_chunk = self._read(min(size, self._read_chunk_size), 0.2)
            num_bytes_read = len(next_chunk)
            
            if num_bytes_read == 0: # no data available
                # first, check if process is still running
                # if so, continue to next iteration and try again
                # if not, whatever we have is all that is available
                if self.closed():
                    if num_unread_bytes > 0:
                        TestRun.LOGGER.debug(f"Read {write_index} bytes from channel (requested {size}, returned {num_unread_bytes} bytes directly from initial buffer)")
                    else:
                        TestRun.LOGGER.debug(f"Read {write_index} bytes from channel (requested {size})")
                    return bytes(return_bytearray[0:write_index])
                continue

            # TEMP: log what was actually read
            TestRun.LOGGER.debug(f"Read {num_bytes_read} from channel (requested {min(size, self._read_chunk_size)}) - content:")
            return_bytearray[write_index:write_index + num_bytes_read] = next_chunk
            TestRun.LOGGER.debug(next_chunk.decode('utf-8'))
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