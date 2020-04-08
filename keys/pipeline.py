import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

def run():
    """
    dictのKeyを抽出
    """

    p = beam.Pipeline(options=PipelineOptions())
    (p
        | 'Garden plants' >> beam.Create([
                ('🍓', 'Strawberry'),
                ('🥕', 'Carrot'),
                ('🍆', 'Eggplant'),
                ('🍅', 'Tomato'),
                ('🥔', 'Potato'),
            ])
        | 'Keys' >> beam.Keys()
        | beam.Map(print))


    p.run()

if __name__ == "__main__":
    run()
