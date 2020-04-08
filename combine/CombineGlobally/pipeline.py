import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

pc = [1, 10, 100, 1000]
pc_str = ["1", "10", "100", "1000"]

def bounded_sum(values, bound=500):
  return min(sum(values), bound)


def run():
#    p = beam.Pipeline(options=PipelineOptions())

    # Stringの配列はCreateが必要。Intは不要
    # (p
    #     | 'CreatePcollection' >> beam.Create(pc_str)
    #     | beam.Map(print)
    # )

    small_sum = pc | beam.CombineGlobally(bounded_sum)  # [500]
    small_sum | beam.Map(print)

    large_sum = pc | beam.CombineGlobally(bounded_sum, bound=5000)  # [1111]
    large_sum | beam.Map(print)

#    p.run()

if __name__ == "__main__":
    run()
