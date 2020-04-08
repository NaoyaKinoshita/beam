import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


def normalize_and_validate_durations(plant, valid_durations):
    plant["duration"] = plant["duration"].lower()
    if plant["duration"] in valid_durations:
        yield plant


def run():
    p = beam.Pipeline(options=PipelineOptions())

    valid_durations = p | "Valid durations" >> beam.Create(
        ["annual", "biennial", "perennial",]
    )

    (p
        | "Gardening plants" >> beam.Create(
                [
                    {"icon": "🍓", "name": "Strawberry", "duration": "Perennial"},
                    {"icon": "🥕", "name": "Carrot", "duration": "BIENNIAL"},
                    {"icon": "🍆", "name": "Eggplant", "duration": "perennial"},
                    {"icon": "🍅", "name": "Tomato", "duration": "annual"},
                    {"icon": "🥔", "name": "Potato", "duration": "unknown"},
                ]
            )
        | "Normalize and validate durations" >> beam.FlatMap(
                normalize_and_validate_durations,
                valid_durations=beam.pvalue.AsIter(valid_durations),
            )
        | beam.Map(print)
    )
    p.run()


if __name__ == "__main__":
    run()
