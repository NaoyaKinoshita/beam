import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

SAMPLE_DATA = [('a', 1), ('b', 10), ('a', 2), ('a', 3), ('b', 20), ('c', 100)]

def run():
    p = beam.Pipeline(options=PipelineOptions())

    (p
        | beam.Create(SAMPLE_DATA)
        | beam.CombinePerKey(sum)
        | beam.Map(print)
    )

    p.run()

if __name__ == "__main__":
    run()
