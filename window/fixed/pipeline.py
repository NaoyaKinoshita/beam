# coding=utf-8
import apache_beam as beam
from apache_beam import window

from options import MyOptions

class Transform(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        print (str(window))
        print (element)
        yield (str(window), 1)

def run():

    options = MyOptions()
    p = beam.Pipeline(options=options)
    (p
        | "ReadFromPubSub" >> beam.io.gcp.pubsub.ReadFromPubSub(
                subscription=options.pubsub_subscription
            ).with_output_types(bytes)
        | "window" >> beam.WindowInto(window.FixedWindows(5))
        | "transform" >> beam.ParDo(Transform())
        | "Count" >> beam.combiners.Count.PerKey()
        | "Output" >> beam.io.WriteToText("output.txt")
    )

    p.run().wait_until_finish()

if __name__ == "__main__":
    run()