import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# emails_list = [
#     ('amy', 'amy@example.com'),
#     ('carl', 'carl@example.com'),
#     ('julia', 'julia@example.com'),
#     ('carl', 'carl@email.com'),
# ]
# phones_list = [
#     ('amy', '111-222-3333'),
#     ('james', '222-333-4444'),
#     ('amy', '333-444-5555'),
#     ('carl', '444-555-6666'),
# ]


def run():
    p = beam.Pipeline(options=PipelineOptions())

    #emails = p | 'CreateEmails' >> beam.Create(emails_list)
    #phones = p | 'CreatePhones' >> beam.Create(phones_list)

    emails = \
    (p
        | 'ReadText1' >> beam.io.ReadFromText('input.txt')
        | 'ConvertTuple1' >> beam.Map(lambda x: tuple(x.split(',')))
    )

    phones = \
    (p
        | 'ReadText2' >> beam.io.ReadFromText('input2.txt')
        | 'ConvertTuple2' >> beam.Map(lambda x: tuple(x.split(',')))
    )

    ({'emails': emails, 'phones': phones}
        | beam.CoGroupByKey()
        | beam.Map(print)
    )

    p.run()

if __name__ == "__main__":
    run()
