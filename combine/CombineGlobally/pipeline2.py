import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

pc = [1, 10, 100, 1000]

class AverageFn(beam.CombineFn):
    def create_accumulator(self):
        print ('---create_accumulator---')
        return (0.0, 0) #(現在の合計, これまでに合計された値の数)の初期値

    def add_input(self, sum_count, input):
        print ('---add_input---')
        (sum, count) = sum_count
        return sum + input, count + 1

    def merge_accumulators(self, accumulators):
        print ('---merge_accumulators---')
        print (accumulators)
        sums, counts = zip(*accumulators)
        return sum(sums), sum(counts)

    def extract_output(self, sum_count):
        print ('---extract_output---')
        print (sum_count)
        (sum, count) = sum_count
        return sum / count if count else float('NaN')


def run():

    # 合計
    # （入力が空の場合にCombineが空のPCollectionを返すようにするにはwithout_defaults）
    (pc
        | beam.CombineGlobally(sum).without_defaults()
        | beam.Map(print)
    )

    # 平均（どちらでも結果は同じだが、beam.CombineFnを継承したClassを作成した方がより柔軟性が高い）
    (pc
        | beam.CombineGlobally(AverageFn())
        | beam.Map(print)
    )

    (pc
        | beam.CombineGlobally(beam.combiners.MeanCombineFn()).without_defaults()
        | beam.Map(print)
    )

if __name__ == "__main__":
    run()
