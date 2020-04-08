# coding=utf-8
import argparse
from apache_beam.options.pipeline_options import PipelineOptions


class MyOptions(PipelineOptions):
    """
    コマンドライン引数からオプションを受け取るためのカスタムクラス.
    """

    @classmethod
    def _add_argparse_args(cls, parser):

        parser.add_argument(
            "--pubsub_subscription",
            default="projects/using-pub-sub-emulator/subscriptions/window_test_df",
            type=str,
            help="Input Pub/Sub Subscription",
        )