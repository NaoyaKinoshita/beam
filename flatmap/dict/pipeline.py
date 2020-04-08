import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


def replace_duration_if_valid(plant, durations):
    if plant["duration"] in durations:
        plant["duration"] = durations[plant["duration"]]
        yield plant


def run():
    p = beam.Pipeline(options=PipelineOptions())

    durations = p | "Durations dict" >> beam.Create(
        [(0, "annual"), (1, "biennial"), (2, "perennial"),]
    )

    (p
        | "Gardening plants" >> beam.Create(
            [
                {"icon": "🍓", "name": "Strawberry", "duration": 2},
                {"icon": "🥕", "name": "Carrot", "duration": 1},
                {"icon": "🍆", "name": "Eggplant", "duration": 2},
                {"icon": "🍅", "name": "Tomato", "duration": 0},
                {"icon": "🥔", "name": "Potato", "duration": -1},
            ]
        )
        | "Replace duration if valid" >> beam.FlatMap(
            replace_duration_if_valid, durations=beam.pvalue.AsDict(durations),
        )
        | beam.Map(print)
    )

    p.run()


if __name__ == "__main__":
    run()
