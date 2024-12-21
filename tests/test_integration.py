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
            reader = csv.reader(fi)
            # skip header
            next(reader)
            # TODO: could chunk this to avoid materializing all
            # records for load
            values = [r for r in reader]
            execute(AbstractInsert(table, values))

    @classmethod
    def setUpClass(cls):

        TestIntegration.load_csv_table_to_heapfile("movies", os.path.join(RAW_DATA_DIR, "movies.csv"))

    @classmethod
    def tearDownClass(cls):
        for filename in os.listdir(ENCODED_DATA_DIR):
            os.unlink(os.path.join(ENCODED_DATA_DIR, filename))

    def test_query_and_inserts_on_encoded_data(self) -> None:
        """Needs refactoring, but currently one big integration test that:
        
        - Queries a table
        - Inserts more rows
        - Queries again to confirm inserts
        """
        table = DATABASE["movies"]
        FIRST_FIVE_ADVENTURE_BY_TITLE = [
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
        want = FIRST_FIVE_ADVENTURE_BY_TITLE
        self.assertEqual(got, want, "query result before insert didn't match expectation")
        # Insert two records that should now be first
        FIRST_INSERT_RECORDS = [
            [1000000001, "!0 New first movie by title alpha", "Adventure|Action"],
            [1000000003, "!1 New second movie by title alpha", "Drama|Adventure"],
        ]
        # New query result expectation
        FIRST_SEVEN_ADVENTURE_BY_TITLE = [r[:2] for r in FIRST_INSERT_RECORDS] + FIRST_FIVE_ADVENTURE_BY_TITLE
        insert = AbstractInsert(
            into_clause=table,
            values_clause=FIRST_INSERT_RECORDS
        )
        execute(insert)
        query_after_insert = AbstractQuery(
            from_clause=DATABASE["movies"],
            select_clause=["movieId", "title"],
            where_clause=Filter(["genres"], lambda g: "Adventure" in g),
            order_clause=[SortColumn("title", True)],
            limit_clause=7,
        )
        got = execute(query_after_insert)
        want = FIRST_SEVEN_ADVENTURE_BY_TITLE
        self.assertEqual(got, want, "query result after insert didn't match expectation")
        # Now, insert records to a new table and then insert from the new table
        SECOND_INSERT_RECORDS = [
            [1000000004, "!!0 Newest first movie by title alpha", "Thriller|Adventure|Action"],
            [1000000005, "!!1 Newest second movie by title alpha", "Adventure"],
        ]
        from_table = Table(
            schema=table.schema,
            data_path=f"{ENCODED_DATA_DIR}/new_movies.dat",
        )
        execute(AbstractCreateTable(from_table))
        execute(AbstractInsert(from_table, SECOND_INSERT_RECORDS))
        execute(AbstractInsert(table, from_clause=AbstractQuery(from_table)))
        # New query result expectation
        FIRST_NINE_ADVENTURE_BY_TITLE = [r[:2] for r in SECOND_INSERT_RECORDS] + FIRST_SEVEN_ADVENTURE_BY_TITLE
        query_after_insert = AbstractQuery(
            from_clause=DATABASE["movies"],
            select_clause=["movieId", "title"],
            where_clause=Filter(["genres"], lambda g: "Adventure" in g),
            order_clause=[SortColumn("title", True)],
            limit_clause=9,
        )
        got = execute(query_after_insert)
        want = FIRST_NINE_ADVENTURE_BY_TITLE
        self.assertEqual(got, want, "query result after second insert didn't match expectation")


