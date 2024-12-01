import csv
import unittest

from toydbms.execution import *
from toydbms.physical import *
from toydbms.query import *


RAW_DATA_DIR = "tests/data/raw"
ENCODED_DATA_DIR = "tests/data/encoded"


# Temp stand-in for a database catalog
DATABASE = {
    "movies": Table(
        schema=[
            ("movieId", UInt32),
            ("title", Text),
            ("genres", Text)
        ],
        data_path=f"{ENCODED_DATA_DIR}/movies.dat",
    )
}


class TestIntegration(unittest.TestCase):
    @staticmethod
    def load_csv_table_to_heapfile(tablename: str, csv_path: str) -> None:
        with open(csv_path, 'r') as fi:
            schema = DATABASE[tablename].schema
            with open(DATABASE[tablename].data_path, 'wb') as fo:
                page = HeapPage()
                reader = csv.reader(fi)
                # skip header
                next(reader)
                for row in reader:
                    record_bin = b"".join([
                        dtype.marshall(dtype.from_str(val))
                        for (_, dtype), val in zip(schema, row)
                    ])
                    if not page.can_fit_record(record_bin):
                        fo.write(page.marshall())
                        page = HeapPage()
                    page.insert_record(record_bin)
                if page.num_records:
                    fo.write(page.marshall())

    @classmethod
    def setUpClass(cls):
        TestIntegration.load_csv_table_to_heapfile("movies", f"{RAW_DATA_DIR}/movies.csv")

    def test_query_on_encoded_data(self) -> None:
        query = AbstractQuery(
            from_clause=DATABASE["movies"],
            select_clause=["movieId", "title"],
            where_clause=Filter(["genres"], lambda g: "Adventure" in g),
            order_clause=[SortColumn("title", True)],
            limit_clause=5,
        )
        got = execute(query)
        want = [
            [97757, "'Hellboy': The Seeds of Creation (2004)"],
            [6168, "10 to Midnight (1983)"],
            [58293, "10,000 BC (2008)"],
            [59834, "100 Rifles (1969)"],
            [103089, "100 Years of Evil (2010)"],
        ]
        self.assertEqual(got, want, "query result didn't match expectation")
