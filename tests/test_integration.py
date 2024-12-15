import csv
import os
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
    """TODO: Run tests sequentially"""
    @staticmethod
    def load_csv_table_to_heapfile(tablename: str, csv_path: str) -> None:
        table = DATABASE[tablename]
        execute(AbstractCreateTable(table))
        with open(csv_path, 'r') as fi:
            schema = table.schema
            with open(table.data_path, 'rb+') as fo:
                page_bin = fo.read(DEFAULT_PAGE_SIZE)
                page = HeapPage(table.schema, init_buff=page_bin)
                reader = csv.reader(fi)
                # skip header
                next(reader)
                for row in reader:
                    try:
                        page.insert_record(row)
                    except InsufficientSpaceError:
                        fo.write(page.marshall())
                        page = HeapPage(schema)
                        page.insert_record(row)
                if page.num_records:
                    fo.write(page.marshall())

    @classmethod
    def setUpClass(cls):
        TestIntegration.load_csv_table_to_heapfile("movies", os.path.join(RAW_DATA_DIR, "movies.csv"))

    @classmethod
    def tearDownClass(cls):
        for filename in os.listdir(ENCODED_DATA_DIR):
            os.unlink(os.path.join(ENCODED_DATA_DIR, filename))

    def test_query_and_inserts_on_encoded_data(self) -> None:
        """Needs refactoring, but currently one big integration test that:
        
        - 
        """
        table = DATABASE["movies"]
        EXPECTED_FIRST_FIVE_ADVENTURE_BY_TITLE = [
            [97757, "'Hellboy': The Seeds of Creation (2004)"],
            [6168, "10 to Midnight (1983)"],
            [58293, "10,000 BC (2008)"],
            [59834, "100 Rifles (1969)"],
            [103089, "100 Years of Evil (2010)"],
        ]
        query_before_insert = AbstractQuery(
            from_clause=table,
            select_clause=["movieId", "title"],
            where_clause=Filter(["genres"], lambda g: "Adventure" in g),
            order_clause=[SortColumn("title", True)],
            limit_clause=5,
        )
        got = execute(query_before_insert)
        want = EXPECTED_FIRST_FIVE_ADVENTURE_BY_TITLE
        self.assertEqual(got, want, "query result before insert didn't match expectation")
        NEW_FIRST_TWO_ADVENTURE_BY_TITLE = [
            [1000000001, "!0 New first movie by title alpha", "Adventure|Action"],
            [1000000003, "!1 New second movie by title alpha", "Drama|Adventure"],
        ]
        insert = AbstractInsert(
            into_clause=table,
            values_clause=NEW_FIRST_TWO_ADVENTURE_BY_TITLE
        )
        self.assertEqual(execute(insert), [[2]], "did not get number of expected rows inserted")
        query_after_insert = AbstractQuery(
            from_clause=DATABASE["movies"],
            select_clause=["movieId", "title"],
            where_clause=Filter(["genres"], lambda g: "Adventure" in g),
            order_clause=[SortColumn("title", True)],
            limit_clause=7,
        )
        got = execute(query_after_insert)
        want = [r[:2] for r in NEW_FIRST_TWO_ADVENTURE_BY_TITLE] + EXPECTED_FIRST_FIVE_ADVENTURE_BY_TITLE
        self.assertEqual(got, want, "query result after insert didn't match expectation")
