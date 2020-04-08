import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions


class MyOptions(PipelineOptions):
    """カスタムオプション."""
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            '--input',
            default='./input.txt',
            help='Input path for the pipeline')

        parser.add_argument(
            '--output',
            default='./output.txt',
            help='Output path for the pipeline')


class ComputeWordLength(beam.DoFn):
    """文字数を求める変換処理."""

    def __init__(self):
        pass

    def process(self, element):
        yield len(element)


def run():
    options = MyOptions()
    # options.view_as(StandardOptions).runner = 'DirectRunner'
    p = beam.Pipeline(options=options)

    (p
     | 'ReadFromText' >> beam.io.ReadFromText(options.input)  # I/O Transform を適用して、オプションで指定したパスにデータを読み込む
     | 'ComputeWordLength' >> beam.ParDo(ComputeWordLength())  # Transform を適用
     | 'WriteToText' >> beam.io.WriteToText(options.output))  # I/O Transformを適用して、オプションで指定したパスにデータを書き込む

    p.run()


if __name__ == '__main__':
    run()


