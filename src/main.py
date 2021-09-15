import luigi
import luigi.contrib.postgres
from luigi.contrib.postgres import CopyToTable, PostgresQuery, PostgresTarget
import time
import random
from utils import PostgresQueryWithRows


class Config(luigi.Config):
    host = 'db'
    database = 'dw'
    user = 'postgres'
    password = 'postgres'


class CopyToTableBase(CopyToTable):
    host = Config.host
    database = Config.database
    user = Config.user
    password = Config.password

    def rows(self):
        for row in self.input().rows:
            yield row


class GetTracktSoldData(PostgresQueryWithRows):
    version = luigi.IntParameter()

    host = Config.host
    database = "postgres"
    user = Config.user
    password = Config.password
    table = "Artist"
    query = '''SELECT t."Name" as "Name", t."AlbumId" as "AlbumId", il."UnitPrice" as "InvoiceUnitPrice"
                FROM "source"."Track" as t
                inner join "source"."InvoiceLine" as il ON t."TrackId" = il."TrackId";'''


class PopulateTracktSold(CopyToTableBase):
    database = "dw"
    version = luigi.IntParameter()

    table = 'fact_track_sold'

    columns = (('Name', 'text'),
               ('AlbumId', 'int'),
               ('InvoiceUnitPrice', 'int'))

    def create_table(self, connection):
        connection.cursor().execute(
            """CREATE TABLE {table} (id SERIAL PRIMARY KEY, "Name" TEXT, "AlbumId" INT, "InvoiceUnitPrice" FLOAT);"""
            .format(table=self.table))

    def requires(self):
        return GetTracktSoldData(self.version)


class MainFlow(luigi.Task):
    version = luigi.IntParameter()

    def requires(self):
        return PopulateTracktSold(self.version)


if __name__ == '__main__':
    luigi.build([MainFlow(random.randint(0, 100))])
