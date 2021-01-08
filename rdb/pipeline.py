# coding=utf-8
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import psycopg2


def run():

    try:
        pipeline_options = PipelineOptions()

        connection = psycopg2.connect(
            "host=xxx.xxx.xx.xxx port=xxxx dbname=xxxxx user=xxxxx password=xxxxxxx"
        )
        cur = connection.cursor()
        cur.execute("SELECT * FROM [table name]")

        p = beam.Pipeline(options=pipeline_options)
        transform_data = (
            p
            | "Read From Postgres" >> beam.Create(cur.fetchall())
            | "print" >> beam.Map(logging.error)
        )

        p.run()

    except Exception as e:
        logging.error(str(e.args))


if __name__ == "__main__":
    run()
