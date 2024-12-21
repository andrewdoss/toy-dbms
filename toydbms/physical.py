"""Physical representation of data for storage."""


import struct
import typing as t
from abc import ABC, abstractstaticmethod
from io import BytesIO


# custom errors
class InsufficientSpaceError(ValueError):
    """Thrown when a page has insufficient space for an insert.
    
    Using this to avoid a redundant serialization with an explicit public
    'is there space?' type message. Instead, consumers should optimistically 
    try an insert and handle the exception if a new HeapPage is needed.
    """
    pass


# classes for encoding/decoding specific value data types
class DType(ABC):
    @abstractstaticmethod
    def marshall(value: t.Any) -> bytes:
        pass

    @abstractstaticmethod
    def unmarshall(buff: BytesIO) -> t.Any:
        pass

    @abstractstaticmethod
    def from_str(value: str) -> t.Any:
        pass


class UInt32(DType):
    @staticmethod
    def marshall(value: int) -> bytes:
        return struct.pack("<I", value)
    
    @staticmethod
    def unmarshall(buff: BytesIO) -> int:
        return struct.unpack("<I", buff.read(4))[0]
    
    @staticmethod
    def from_str(value: str) -> int:
        return int(value)
    

class Text(DType):
    @staticmethod
    def marshall(value: str) -> bytes:
        bin_value = value.encode("utf8")
        return struct.pack("<B", len(bin_value)) + bin_value
    
    @staticmethod
    def unmarshall(buff: BytesIO) -> str:
        data_len = struct.unpack("<B", buff.read(1))[0]
        return buff.read(data_len).decode("utf8")
    
    @staticmethod
    def from_str(value: str) -> str:
        return value


class HeapPage:
    """Object representation of a page in the heap file. 

    Supports marshalling, unmarshalling, iterating over records, and
    inserting a record.
    
    Layout:
    - uint16 for num_records
    - uint16[] for start indexes of each record
    - free space
    - records[] (bottom up ordering)

    TODO: doesn't handle concurrent iterators on same HeapPage.
    """
    def __init__(self, schema: t.List[t.Tuple[str, t.Type[DType]]], page_size: int = 4096, init_buff: t.Optional[bytes] = None):
        self._schema = schema
        if init_buff is None:
            self._buff = bytearray(page_size)
            self._record_pointers_end = 2
            self._records_start = page_size
        else:
            self._buff = bytearray(init_buff)
            self._record_pointers_end = 2 + 2 * self.num_records
            self._records_start = self._get_record_start_idx(self.num_records - 1)
        self._iter_idx = 0

    def _free_bytes(self) -> int:
        page_size = len(self._buff)
        records_size = page_size - self._records_start
        return page_size - 2 - 2 * self.num_records - records_size

    @property
    def num_records(self) -> int:
        return struct.unpack("<H", self._buff[:2])[0]
    
    def _can_fit_record(self, record_bin: bytes) -> bool:
        return 2 + len(record_bin) <= self._free_bytes() 

    def insert_record(self, record: t.List[t.Any]) -> None:
        """Optimistic record insertion.
        
        Caller must handle InsufficientSpaceError if page is too full.
        """
        record_bin = b"".join([
            dtype.marshall(val)
            for (_, dtype), val in zip(self._schema, record)
        ])
        if not self._can_fit_record(record_bin):
            raise InsufficientSpaceError
        self._records_start -= len(record_bin)
        self._record_pointers_end += 2
        self._buff[self._records_start:self._records_start+len(record_bin)] = record_bin
        self._buff[self._record_pointers_end-2:self._record_pointers_end] = struct.pack("<H", self._records_start)
        self._buff[:2] = struct.pack("<H", self.num_records + 1)

    def marshall(self) -> bytes:
        return bytes(self._buff)

    def __iter__(self):
        self._iter_idx = 0
        return self
    
    def _get_record_start_idx(self, record_num: int) -> int:
        return struct.unpack("<H", self._buff[2+2*record_num:4+2*record_num])[0]
    
    def __next__(self) -> t.List[t.Any]:
        if self._iter_idx >= self.num_records:
            raise StopIteration
        record_start = self._get_record_start_idx(self._iter_idx)
        record_end = len(self._buff) if self._iter_idx == 0 else self._get_record_start_idx(self._iter_idx-1)
        self._iter_idx += 1
        record_buff = BytesIO(self._buff[record_start:record_end])
        return [
            dtype.unmarshall(record_buff)
            for (_, dtype) in self._schema
        ]
