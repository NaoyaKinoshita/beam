#  coding=utf-8
import apache_beam as beam
import logging

ERROR_STRING = "dummy!"
ERROR_DATETIME = "1970-01-01T00:00:00"
ERROR_INTEGER = 255
ERROR_BOOLEAN = "true"


class TimestampFieldCheck(beam.DoFn):
    """
    timestamp項目のチェック & 不正値補正
    """

    def process(self, element):
        import logging
        from dateutil import parser
        from pytz import timezone

        logging.getLogger().setLevel(logging.ERROR)
        try:
            if element.name != "timestamp":
                element.loc[0] = (
                    parser.parse(element.loc[0])
                    .astimezone(timezone("UTC"))
                    .strftime("%Y-%m-%d %H:%M:%S.%f")
                )
        except:
            logging.error(
                "[DF_ERROR]BML OK:" + element.name + "Exception;" + str(element)
            )
            element.loc[0] = ERROR_DATETIME
        yield (element.name, element.loc[0])


class IntegerFieldCheck(beam.DoFn):
    """
    integer項目のチェック & 不正値補正
    """

    def process(self, element):
        import logging

        logging.getLogger().setLevel(logging.ERROR)
        try:
            element.loc[0] = int(element.loc[0])
        except:
            logging.error(
                "[DF_ERROR]BML OK:" + element.name + "Exception;" + str(element)
            )
            element.loc[0] = ERROR_INTEGER
        yield (element.name, element.loc[0])


class BoolFieldCheck(beam.DoFn):
    """
    bool項目のチェック & 不正値補正
    """

    def process(self, element):
        import logging

        logging.getLogger().setLevel(logging.ERROR)
        try:
            element.loc[0] = "true" if bool(element.loc[0]) else "false"
        except:
            logging.error(
                "[DF_ERROR]BML OK:" + element.name + "Exception;" + str(element)
            )
            element.loc[0] = ERROR_BOOLEAN
        yield (element.name, element.loc[0])
