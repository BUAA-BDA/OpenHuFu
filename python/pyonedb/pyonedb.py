from jnius import autoclass
import os
import jnius_config
onedb_root = os.getenv("ONEDB_ROOT")
jnius_config.set_classpath(onedb_root + "/bin/onedb_user_client.jar")


OneDB = autoclass("com.hufudb.onedb.OneDB")
GlobalTableConfig = autoclass("com.hufudb.onedb.core.table.GlobalTableConfig")
LocalTableConfig = autoclass("com.hufudb.onedb.core.table.LocalTableConfig")
ResultSet = autoclass("java.sql.ResultSet")


class TableConfig:
    def __init__(self, table_name: str):
        self._table_config = GlobalTableConfig(table_name)

    def add_local_table(self, address: str, table_name: str):
        self._table_config.addLocalTable(address, table_name)

    def get_config(self) -> GlobalTableConfig:
        return self._table_config


class Cursor(object):
    def __init__(self, rs: ResultSet):
        self._rs = rs

    def __iter__(self):
        return self

    def __next__(self):
        if self._rs.next():
            return self
        else:
            raise StopIteration

    def size(self):
        return self._rs.getMetaData().getColumnCount()

    def get(self, columnIndex: int):
        return self._rs.getObject(columnIndex + 1)


class PyOneDB:
    def __init__(self):
        self._onedb = OneDB()

    def add_owner(self, address: str):
        self._onedb.addOwner(address)

    def add_owner(self, address: str, ca_path: str):
        self._onedb.addOwner(address, ca_path)

    def add_onedb_table(self, table_config: TableConfig):
        self._onedb.createOneDBTable(table_config.get_config())

    def query(self, sql: str):
        return Cursor(self._onedb.executeQuery(sql))


if __name__ == "__main__":
    table_config = TableConfig("student1")
    table_config.add_local_table('localhost:12345', "student1")
    table_config.add_local_table('localhost:12346', "student1")
    table_config.add_local_table('localhost:12347', "student1")

    pyonedb = PyOneDB()
    pyonedb.add_owner('localhost:12345', onedb_root + "/cert/ca.pem")
    pyonedb.add_owner('localhost:12346', onedb_root + "/cert/ca.pem")
    pyonedb.add_owner('localhost:12347', onedb_root + "/cert/ca.pem")

    pyonedb.add_onedb_table(table_config)

    result = pyonedb.query("select name, age from student1 limit 3")

    for r in result:
        for i in range(r.size()):
            print(r.get(i))