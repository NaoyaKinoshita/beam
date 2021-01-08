# coding=utf-8
import logging

import apache_beam as beam
from apache_beam import pvalue, window, trigger
from libs.config import ConfigAccess
from libs.transform import TimestampFieldCheck, IntegerFieldCheck, BoolFieldCheck
from options import MyOptions

DEBUN_CULUMNS_LIST = [
    "tv_id",
    "time_in",
    "time_out",
    "contents",
    "hc_ver",
    "rec_flg",
    "post_code",
    "status_data",
    "time_send",
    "station_id",
    "manufacture_id",
    "browser",
    "unique_id",
    "opt_flg",
    "identifier",
]


def _convert_json_to_dataframe(element):
    import json
    import pandas as pd
    from datetime import datetime

    # try:
    denbun_json = json.loads(element)
    denbun_df = pd.DataFrame(
        [denbun_json["data"]["receive_data"].split("|")], columns=DEBUN_CULUMNS_LIST,
    )

    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
    ip_address = denbun_json["data"]["ip_address"]
    original_data = denbun_json["data"]["receive_data"]
    extentions = []

    ext_data_df = pd.DataFrame(
        [
            {
                "timestamp": timestamp,
                "ip_address": ip_address,
                "original_data": original_data,
                "extentions": extentions,
            }
        ]
    )
    bq_data_df = pd.concat([denbun_df, ext_data_df], axis=1)

    for column_name, item in bq_data_df.iteritems():
        if column_name in ["timestamp", "time_in", "time_out", "time_send"]:
            yield pvalue.TaggedOutput("timestamp", item)
        elif column_name in ["opt_flg", "status_data", "hc_ver"]:
            yield pvalue.TaggedOutput("integer", item)
        # elif column_name in ["rec_flg"]:
        #     yield pvalue.TaggedOutput("bool", item)
        # elif column_name in ["extentions"]:
        #     yield pvalue.TaggedOutput("list", item)
        # else:
        #     yield pvalue.TaggedOutput("string", item)
    # except:
    #     yield pvalue.TaggedOutput("error", denbun_json)


def test(e):
    print("----")
    print(e)


def run():

    pubsub_data = ""
    try:
        options = MyOptions()

        config = ConfigAccess()
        bq_schema = config.get_bqschema(options.bq_schema_filepath)
        bq_err_schema = config.get_bqschema(options.bq_err_schema_filepath)

        p = beam.Pipeline(options=options)
        pubsub_data = p | "ReadFromPubSub" >> beam.io.gcp.pubsub.ReadFromPubSub(
            subscription=options.pubsub_subscription
        ).with_output_types(bytes)

        denbun_type_group = (
            pubsub_data
            | "ConvertDataFrame"
            >> beam.FlatMap(_convert_json_to_dataframe).with_outputs()
        )

        timestamp_p = denbun_type_group.timestamp | "TimestampCheck" >> beam.ParDo(
            TimestampFieldCheck()
        )
      #  timestamp_p | "yryry" >> beam.Map(print)

        integer_p = denbun_type_group.integer | "IntegerCheck" >> beam.ParDo(
            IntegerFieldCheck()
        )
      #  integer_p | "yyyy" >> beam.Map(print)

        # bool_p = denbun_type_group.bool | "BoolCheck" >> beam.ParDo(BoolFieldCheck())

        test = (
            (timestamp_p, integer_p)
            | "Flat" >> beam.Flatten()
            | "window" >> beam.WindowInto(
                window.GlobalWindows(),
                trigger=trigger.AfterCount(7),
                accumulation_mode=trigger.AccumulationMode.DISCARDING)
            | 'CreateSpammersView' >> beam.CombineGlobally(beam.combiners.ToDictCombineFn()).without_defaults()
            | "fvfv" >> beam.Map(print)
        )


        # mixed_denbun.normal | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
        #     options.bq_tablename,
        #     schema=bq_schema,
        #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        #     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        # )

        # denbun_type_group.error | "WriteToBigQueryErrorDenbun" >> beam.io.WriteToBigQuery(
        #     options.bq_err_tablename,
        #     schema=bq_err_schema,
        #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        #     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        # )

        p.run().wait_until_finish()

    except Exception as e:
        logging.error("BML Denbun is Exception;" + str(e.args))
        logging.error("BML Denbun is Content;" + pubsub_data)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.ERROR)
    run()
