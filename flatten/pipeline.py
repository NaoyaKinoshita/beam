# coding=utf-8
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import pvalue


class AddTag(beam.DoFn):

    def __init__(self):
        pass

    def process(self, element):

        tag = ''
        if element.startswith('t'):
            tag = 't'
        elif element.startswith('n'):
            tag = 'n'
        else:
            tag = 'o'

        yield pvalue.TaggedOutput(tag, element)

def run():
    p = beam.Pipeline(options=PipelineOptions())

    check_result = (p
        | 'ReadFromText' >> beam.io.ReadFromText('input.txt')
        | 'AddTag' >> beam.ParDo(AddTag()).with_outputs()
    )

    ((check_result.n, check_result.o)
        | beam.Flatten()
        | 'WriteToText' >> beam.io.WriteToText('output.txt')
    )

    p.run()

if __name__ == "__main__":
    run()
