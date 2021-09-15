from luigi.contrib.postgres import CopyToTable, PostgresQuery, PostgresTarget



class PostgresTargetWithRows(PostgresTarget):

    rows = []

    def __init__(self, host, database, user, password, table, update_id, port=None, rows=[]):
        super(PostgresTargetWithRows, self).__init__(host, database, user, password, table, update_id, port)
        self.rows = rows

class PostgresQueryWithRows(PostgresQuery):

    rows = []

    def run(self):
        connection = self.output().connect()
        cursor = connection.cursor()
        sql = self.query

        cursor.execute(sql)

        for row in cursor.fetchall():
            self.rows.append(row)

        self.output().touch(connection)

        connection.commit()
        connection.close()

    def output(self):
        return PostgresTargetWithRows(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            table=self.table,
            update_id=self.update_id,
            rows=self.rows
        )
