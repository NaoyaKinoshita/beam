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
    (p
        | 'Garden plants' >> beam.Create([
            {'name': 'Strawberry', 'season': 1585699200}, # April, 2020
            {'name': 'Carrot', 'season': 1590969600}     # June, 2020
            {'name': 'Artichoke', 'season': 1583020800},  # March, 2020
            {'name': 'Tomato', 'season': 1588291200},     # May, 2020
            {'name': 'Potato', 'season': 1598918400},     # September, 2020
        ])
        | 'With timestamps' >> beam.Map(
            lambda plant: beam.window.TimestampedValue(plant, plant['season']))
        | "window" >> beam.WindowInto(beam.window.FixedWindows(5))
        | "transform" >> beam.ParDo(Transform())
#        | 'Get timestamp' >> beam.ParDo(GetTimestamp())
        | beam.Map(print)
    )

    p.run().wait_until_finish()

if __name__ == "__main__":
    run()
