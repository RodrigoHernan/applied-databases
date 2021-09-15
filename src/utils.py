from luigi.contrib.postgres import CopyToTable, PostgresQuery, PostgresTarget


class RunAlways:
    @property
    def update_id(self):
        return '146te'


class PostgresTargetWithRows(PostgresTarget):

    rows = []

    def __init__(self, host, database, user, password, table, update_id, port=None, rows=[]):
        super(PostgresTargetWithRows, self).__init__(host, database, user, password, table, update_id, port)
        self.rows = rows

class PostgresQueryWithRows(RunAlways, PostgresQuery):

    rows = []

    def run(self):
        connection = self.output().connect()
        cursor = connection.cursor()
        sql = self.query

        cursor.execute(sql)

        for row in cursor.fetchall():
            print(row)
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

class RedshiftTargetWithRows(PostgresTargetWithRows):

    marker_table = luigi.configuration.get_config().get(
        'redshift',
        'marker-table',
        'table_updates')

    use_db_timestamps = False

class RedshiftQueryWithRows(PostgresQueryWithRows):

    def run(self):
        super(RedshiftQueryWithRows, self).run()

    def output(self):
        return RedshiftTargetWithRows(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.password,
            table=self.table,
            update_id=self.update_id,
            rows=self.rows
        )
