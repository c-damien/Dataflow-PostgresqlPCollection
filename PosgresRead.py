import argparse

import apache_beam as beam
import jaydebeapi
import logging

from contextlib import contextmanager

#from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, GoogleCloudOptions, StandardOptions, WorkerOptions

def logging_row(row):
    logging.info(row)

def parse_jdbc_entry(table_data):
    for r in table_data:
        #logging.info("-->" + '-'.str(x) for x in r)
        yield [c.value if hasattr(c, 'value') else c for c in r]

        
class QueryPostgreSQLFn(beam.DoFn):
    def parse_method(self, string_input):

        
        3logging.info("-->" + string_input)

        return string_input


@contextmanager
def dbConnection():
            database_user = "myuser"
            database_password = "mysecretpassword"
            database_host= "0.0.0.0"
            database_port = "5432"
            database_db = "mydb"

            jclassname = "org.postgresql.Driver"
            url = ("jdbc:postgresql:"+database_db)
            jars = ["/Users/cdamien/Downloads/postgresql-42.2.9.jar"] #somewhere with the jar file
            libs = None
            cnx = jaydebeapi.connect(jclassname, url, {'user': database_user, 'password':database_password}, jars=jars,libs=libs)

            try:
                yield cnx
            finally:
                cnx.close()

def run(argv=None):

    with  dbConnection() as cnx:
        cursor = cnx.cursor()
        query = "SELECT *  FROM Persons"
        cursor.execute(query)

        options = PipelineOptions()
        
        p = beam.Pipeline(options=options)
        (p
             | 'connecting to PostgreSQL' >> beam.Create(parse_jdbc_entry(cursor.fetchall()))
             | 'processing Rows' >> beam.Map(logging_row)
            # do something else ...

         )
        result = p.run()
        result.wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
