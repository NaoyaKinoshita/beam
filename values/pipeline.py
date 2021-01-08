import apache_beam as beam

with beam.Pipeline() as pipeline:
  plants = (
      pipeline
      | 'Garden plants' >> beam.Create([
          ('��', 'Strawberry'),
          ('🥕', 'Carrot'),
          ('🍆', 'Eggplant'),
          ('🍅', 'Tomato'),
          ('🥔', 'Potato'),
      ])
      | 'Values' >> beam.Values()
      | beam.Map(print))

