import luigi
import luigi.contrib.postgres
from luigi.contrib.postgres import CopyToTable, PostgresQuery, PostgresTarget
import time
import random

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


class Config(luigi.Config):
    """
    Luigi configuration file containing information such as PostgreSQL connection details, database table names etc
    """
    date = 'luigi.DateParameter()'

    host = 'db'
    database = 'dw'
    user = 'postgres'
    password = 'postgres'


class CopyToTableBase(CopyToTable):
    host = Config.host
    database = Config.database
    user = Config.user
    password = Config.password


class Clean(luigi.Task):
    def requires(self):
        return None
    def output(self):
        return luigi.LocalTarget('Clean.txt')
    def run(self):
        with self.output().open('w') as outfile:
            outfile.write('Hello World!\n')




class GetData3(PostgresQueryWithRows):
    host = Config.host
    database = "postgres"
    user = Config.user
    password = Config.password
    table = "Artist"
    query = '''SELECT "Name", "ArtistId", "ArtistId" as ArtistIde
    FROM "source"."Artist";'''

    # def output(self):
    #     return luigi.LocalTarget("top_artists.tsv")

    # def run(self):

    #     # write a dummy list of words to output file
    #     words = [
    #             'apple',
    #             'banana',
    #             'grapefruit'
    #             ]

    #     with self.output().open('w') as f:
    #         for word in words:
    #             f.write('{word}\n'.format(word=word))

    # @property
    # def update_id(self):
    #     import random
    #     return random.randint(0,10000)



class CreateTables12(CopyToTableBase):
    table = 'test_table'

    columns = (('test_text', 'text'),
               ('test_int', 'int'),
               ('test_float', 'float'))

    def create_table(self, connection):
        connection.cursor().execute(
            "CREATE TABLE {table} (id SERIAL PRIMARY KEY, test_text TEXT, test_int INT, test_float FLOAT)"
            .format(table=self.table))

    def requires(self):
        return GetData3()


    def rows(self):
        """
        Return/yield tuples or lists corresponding to each row to be inserted.
        """
        # with self.input().open('r') as fobj:
        #     for line in fobj:
        #         yield line.strip('\n').split('\t')
        # raise Exception('for row in self.input().rows:', self.input().rows)
        for row in self.input().rows:
            # raise Exception(row)
            print(row)
            yield row

    # def rows(self):
    #     yield 'foo', 123, 123.45
    #     yield None, '-100', '5143.213'
    #     yield '\t\n\r\\N', 0, 0
    #     yield u'éцү我', 0, 0
    #     yield '', 0, r'\N'  # Test working default null charcter


class MainFlow(luigi.Task):

    def requires(self):
        return CreateTables12()


if __name__ == '__main__':
    luigi.run()
