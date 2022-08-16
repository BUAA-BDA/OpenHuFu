import pytest
import os

from pyonedb import TableConfig
from pyonedb import PyOneDB

class TestOneDB:
    def test_table_config(self):
        table_config = TableConfig("student1")
        table_config.add_local_table('localhost:12345', "student1")
        assert table_config.get_table_name() == "student1"

    def test_query(self):
        table_config = TableConfig("student1")
        table_config.add_local_table('localhost:12345', "student1")
        table_config.add_local_table('localhost:12346', "student1")
        table_config.add_local_table('localhost:12347', "student1")
        pyonedb = PyOneDB()
        onedb_root = os.getenv("ONEDB_ROOT")
        pyonedb.add_owner('localhost:12345', onedb_root + "/cert/ca.pem")
        pyonedb.add_owner('localhost:12346', onedb_root + "/cert/ca.pem")
        pyonedb.add_owner('localhost:12347', onedb_root + "/cert/ca.pem")

        pyonedb.add_onedb_table(table_config)

        result = pyonedb.query("select name, age from student1 limit 3")
        count = 0
        for r in result:
            count += 1
            for i in range(r.size()):
                print(r.get(i))
        assert count == 3
