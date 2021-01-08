import apache_beam as beam

with beam.Pipeline() as pipeline:
  plants_csv = (
      pipeline
      | 'Garden plants' >> beam.Create([
          ['🍓', 'Strawberry', 'perennial'],
          ['🥕', 'Carrot', 'biennial'],
          ['🍆', 'Eggplant', 'perennial'],
          ['🍅', 'Tomato', 'annual'],
          ['🥔', 'Potato', 'perennial'],
      ])
      | 'To string' >> beam.ToString.Iterables() # ,区切りのStringに変換
      | beam.Map(print))
