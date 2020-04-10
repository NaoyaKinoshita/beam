import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class GetTimestamp(beam.DoFn):
  def process(self, plant, timestamp=beam.DoFn.TimestampParam):
    yield '{} - {}'.format(timestamp.to_utc_datetime(), plant['name'])

class Transform(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        print (str(window))
        yield element


def run():
    p = beam.Pipeline(options=PipelineOptions()) 

    p1 = (p
        | 'Garden plants 1' >> beam.Create([
          ('A', 1), ('A', 2), ('B', 1), ('C', 5)
        ])
    )
    p1 | 'debug1' >> beam.Map(lambda x : print(type(x)))

    p2 = (p
        | 'Garden plants 2' >> beam.Create([
          ('D', 1), ('E', 2)
        ])
    )

    p3 = (p
        | 'Garden plants 3' >> beam.Create([
          ('B', 5), ('C', 9), ('X', 100)
        ])
    )

    p4 = ((p1, p2, p3) 
        | 'Flatten' >> beam.Flatten()
        | 'ToDict' >> beam.combiners.ToDict()
        | 'print' >> beam.Map(print)
    )

    p.run().wait_until_finish()

if __name__ == "__main__":
    run()
