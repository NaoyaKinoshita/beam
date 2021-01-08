import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import pvalue

class FilterMeanLengthFn(beam.DoFn):
    """平均以上の文字数を持つ文字列をフィルタリングする."""

    def __init__(self, num):
        print ('---init---')
        print (num)
        pass

    # mean_word_length は副入力
    def process(self, element, mean_word_length):
        print ('---process---')
        print (element)
        if len(element) >= mean_word_length:
            yield element


def run():
    p = beam.Pipeline(options=PipelineOptions())

    inputs = ["good morning.", "good afternoon.", "good evening."]

    # 副入力 ※副入力に利用するデータはPCollectionである必要がある。
    mean_word_length = \
    (p
        | 'ReadText1' >> beam.io.ReadFromText('input.txt')
        | "GetLength" >> beam.Map(len)
        | "Mean" >> beam.CombineGlobally(beam.combiners.MeanCombineFn())
    )

    # 主入力
    output = \
    (p
        | 'Create' >> beam.Create(inputs)
        | 'FilterMeanLength' >> beam.ParDo(FilterMeanLengthFn(11), mean_word_length=pvalue.AsSingleton(mean_word_length))
        | 'write to text' >> beam.io.WriteToText('output.txt')
    )

    p.run().wait_until_finish()

if __name__ == '__main__':
    run()
