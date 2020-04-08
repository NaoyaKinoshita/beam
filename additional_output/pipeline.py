# coding=utf-8
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import pvalue


class CheckWord(beam.DoFn):

    def __init__(self):
        pass

    def process(self, element):

        tag = 'normal'
        if element == 'test':
            tag = 'error'

        yield pvalue.TaggedOutput(tag, element)

def run():
    p = beam.Pipeline(options=PipelineOptions())

    check_result = (p
     | 'ReadFromText' >> beam.io.ReadFromText('input.txt')
     | 'CheckWord' >> beam.ParDo(CheckWord()).with_outputs()
    )

    check_result.normal | 'WriteToText' >> beam.io.WriteToText('output_normal.txt')
    check_result.error | 'WriteToTextError' >> beam.io.WriteToText('output_error.txt')
    p.run()

if __name__ == "__main__":
    run()
