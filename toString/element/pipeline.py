import apache_beam as beam

with beam.Pipeline() as pipeline:
  plant_lists = (
      pipeline
      | 'Garden plants' >> beam.Create([
          ['🍓', 'Strawberry', 'perennial'],
          ['��', 'Carrot', 'biennial'],
          ['🍆', 'Eggplant', 'perennial'],
          ['🍅', 'Tomato', 'annual'],
          ['🥔', 'Potato', 'perennial'],
      ])
      | 'To string' >> beam.ToString.Element()
      | beam.Map(print))
