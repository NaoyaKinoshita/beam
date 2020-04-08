import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


def run():
    p = beam.Pipeline(options=PipelineOptions())

    delimiter = p | 'Create delimiter' >> beam.Create([','])

    (
      p
        | 'Gardening plants' >> beam.Create([
                '🍓Strawberry,🥕Carrot,🍆Eggplant',
                '🍅Tomato,🥔Potato',
          ])
        | 'Split words' >> beam.FlatMap(
                  lambda text,
                  delimiter: text.split(delimiter),
                  delimiter=beam.pvalue.AsSingleton(delimiter),
                )
        | beam.Map(print))

    p.run()

if __name__ == "__main__":
    run()
