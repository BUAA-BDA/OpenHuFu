import psycopg2
import csv
import json
import ast


def load_schema(schema_path):
    with open(schema_path, 'r', encoding='utf-8') as f:
        schema = json.load(f)
        return schema['databases']

def connect(host, port, user, password, dbname):
    con = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)
    return con

def build_create_table_sql(schemas):
    sqls = []
    for schema in schemas:
        sql = "DROP TABLE IF EXISTS " + schema['name']
        sqls.append(sql)
        sql = "CREATE TABLE ";
        sql += schema['name'] + "(\n"
        for column in schema['columns']:
          sql += "  {} {},\n".format(column['name'], column['type'])
        sql = sql[:-2]
        sql += "\n);"
        sqls.append(sql)
    return sqls

def create_table(con, schemas):
    cur = con.cursor()
    sqls = build_create_table_sql(schemas)
    for sql in sqls:
        print(sql)
        cur.execute(sql)
    con.commit()
    cur.close()

def load_data(con, schemas):
    cur = con.cursor()
    for schema in schemas:
        sql = "INSERT INTO " + schema['name']
        values = []
        ids = []
        columns = schema['columns']
        for column in columns:
            if column['type'] == "geometry":
                values.append("ST_GeomFromText('point(%s %s)', 4326)")
                ids.extend(column['index'][0:2])
            else:
                values.append("%s")
                ids.append(column['index'][0])
        sql += " VALUES(" + ",".join(values) + ")"
        print(sql)
        with open(schema['csv_path'], 'r', encoding='utf-8') as f:
            rows = csv.reader(f)
            for i, row in enumerate(rows):
                fields = []
                for j in ids:
                    try:
                        fields.append(ast.literal_eval(row[j]))
                    except:
                        fields.append(str(row[j]))
                cur.execute(sql, tuple(fields))
                if i != 0 and i % 1000 == 0:
                    print ("insert " + str(i+1) + " rows")
                    con.commit()
            print ("insert " + str(i+1) + " rows")
            con.commit()

def create_index(con, schemas):
    cur = con.cursor()
    for schema in schemas:
        sql = "CREATE INDEX " + schema['name'] + "_geom_idx"
        columns = schema['columns']
        for column in columns:
            if column['type'] == "geometry":
                sql = sql + " ON " + schema['name'] + " USING GIST(" + column['name'] + ")"
                print ("start create index on table " + schema['name'])
                cur.execute(sql)
                print ("create finish")
                con.commit()

def import_data(databases):
    for database in databases:
        dbname = database['database']
        host = database['host']
        port = database['port']
        username = database['username']
        password = database['password']
        con = connect(host, port, username, password, dbname)
        create_table(con, database['schemas'])
        load_data(con, database['schemas'])
        # create_index(con, database['schemas'])
        con.close()

if __name__ == "__main__":
    databases = load_schema("schema.json")
    import_data(databases)
