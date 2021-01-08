import apache_beam as beam

questions = [
    "nq question: where is google's headquarters",
    "nq question: what is the most populous country in the world",
    "nq question: name a member of the beatles",
    "nq question: how many teeth do humans have"
]

with beam.Pipeline() as p:
    _ = (p | beam.Create(questions)
           | beam.BatchElements(min_batch_size=3, max_batch_size=3) # バッチサイズに指定した単位で後続処理を実行できる *mixとmaxの範囲で良い感じにしてくれる（同値にすると固定）
           | beam.Map(print)
        )