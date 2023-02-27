from subprocess import Popen
from enum import Enum
from io import BufferedReader

from paramiko import Channel

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

    def ready(self):
        """Returns True if the channel is ready to read from (has at least 1 byte available).
        
        """
        raise NotImplementedError()
    
    def read(self, size=-1):
        """Read the specified number of bytes from the stream.
        
        If size is not given or is negative, read until end of data.
        """
        raise NotImplementedError()
    
    def closed(self):
        """Return True if the process the channel is attached to has exited.
        
        """
        raise NotImplementedError()
    

class LocalChannel(GenericChannel):
    
    _process: Popen = None
    _channel_type: ChannelType = None
    _reader: BufferedReader = None

    def __init__(self, process: Popen, channel_type: ChannelType = ChannelType.STDOUT):
        self._process = process
        self._channel_type = channel_type
        if channel_type == ChannelType.STDIN:
            raise ValueError("STDIN not supported by LocalChannel - only STDOUT and STDERR are supported")
        if channel_type == ChannelType.STDOUT:
            if process.stdout is None:
                raise ValueError("Cannot connect to process stdout - no file handle provided by process")
            self._reader = BufferedReader(process.stdout)
        elif channel_type == ChannelType.STDERR:
            if process.stderr is None:
                raise ValueError("Cannot connect to process stderr - no file handle provided by process")
            self._reader = BufferedReader(process.stderr)
    
    def ready(self):
        return len(self._reader.peek(1)) > 0
    
    def read(self, size=-1):
        return self._reader.read(size)

    def closed(self):
        return self._process.poll() is not None


class SshChannel(GenericChannel):
    
    _ssh_channel: Channel = None

    def __init__(self, ssh_channel: Channel):
        self._ssh_channel = ssh_channel

    def ready(self):
        return self._ssh_channel.recv_ready()
    
    def read(self, size=-1):
        return self._ssh_channel.recv(size)

    def closed(self):
        return self._ssh_channel.exit_status_ready()