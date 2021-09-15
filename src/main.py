import luigi
import luigi.contrib.postgres
from luigi.contrib.postgres import CopyToTable, PostgresQuery, PostgresTarget
import time
import random
from utils import PostgresQueryWithRows



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




class GetData(PostgresQueryWithRows):
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



class CreateTables(CopyToTableBase):
    version = luigi.IntParameter()

    table = 'test_table'

    columns = (('test_text', 'text'),
               ('test_int', 'int'),
               ('test_float', 'float'))

    def create_table(self, connection):
        connection.cursor().execute(
            "CREATE TABLE {table} (id SERIAL PRIMARY KEY, test_text TEXT, test_int INT, test_float FLOAT)"
            .format(table=self.table))

    def requires(self):
        return GetData()


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
    version = luigi.IntParameter()

    def requires(self):
        return CreateTables(self.version)


if __name__ == '__main__':
    luigi.build([MainFlow(random.randint(0, 100))])
