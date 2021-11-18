from __future__ import absolute_import
from IPython.core.display import display, HTML
from apache_beam.runners.runner import PipelineState
import argparse
import logging
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import os
import google.auth
from apache_beam.options import pipeline_options
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.runners import DataflowRunner
from google.cloud import bigquery


def Split(element):
    return element.split(",")
# outputs text in a readable format


def FormatText(elem):
    return 'AVERAGE RATING OF BOOKS:'+str(elem[1])
# finds the average of given list of sets


class AverageFn(beam.CombineFn):
    def create_accumulator(self):
        return (0.0, 0)  # initialize (sum, count)

    def add_input(self, sum_count, inputt):
        (summ, count) = sum_count
        (summ2, c2) = inputt
        return summ + float(summ2)*c2, count + c2

    def merge_accumulators(self, accumulators):
        ind_sums, ind_counts = zip(*accumulators)
        return sum(ind_sums), sum(ind_counts)
    # zip - [(27, 3), (39, 3), (18,
    # (84,8)

    def extract_output(self, sum_count):
        (summ, count) = sum_count
    # combine globally using CombineFn
        return summ / count if count else float('NaN')


def f_mean(element):
    (summ, count) = element
    return (summ*count, count)


def accumulate(element):
    ind_sums, ind_counts = zip(*element)
    return sum(ind_sums), sum(ind_counts)


def op(s_count):
    (summ2, c2) = ind_counts
    return summ2/c2 if s_count else float('NaN')


options = PipelineOptions()
#p = beam.Pipeline(options=options)
p = beam.Pipeline(InteractiveRunner())
p2 = beam.Pipeline(InteractiveRunner())
# Setting up the Apache Beam pipeline options.
options = pipeline_options.PipelineOptions(flags=[])
# Sets the project to the default project in your current Google Cloud environment.
_, options.view_as(GoogleCloudOptions).project = google.auth.default()
# Sets the Google Cloud Region in which Cloud Dataflow runs.
options.view_as(GoogleCloudOptions).region = 'us-west2'
dataflow_gcs_location = 'gs://dataflow_storage_1'
# % dataflow_gcs_location'
options.view_as(GoogleCloudOptions).staging_location = '%s/staging'
# Dataflow Temp Location. This location is used to store temporary files or intermediate results before finally outputting to the sink.
options.view_as(GoogleCloudOptions).temp_location = '%s/temp'
dataflow_gcs_locationclient = bigquery.Client()
dataset_id = "check1.flowtobq"
#dataset = bigquery.Dataset(dataset_id)
dataset.location = "US"
dataset.description = "dataset_books"
#dataset_ref = client.create_dataset(dataset, timeout = 30)
# split and Convert to json


def to_json(csv_str):
    fields = csv_str.split(',')

    json_str = {"Name": fields[0],
                "Author": fields[1],
                "User_Rating": fields[2],
                "Reviews": fields[3],
                "Price": fields[4],
                "Year": fields[5],
                "Genre": fields[6]
                }
    return json_str


table_schema = 'Name: STRING, Author: STRING, User_Rating: FLOAT, Reviews: INTEGER, Price: Integer, Year: Integer, Genre: STRING'
bs = (p2 | beam.io.ReadFromText("gs://case_study1_dataset/book_ratings.csv"))
(bs | 'cleaned_data to json' >> beam.Map(to_json)
 | 'write to bigquery' >> beam.io.WriteToBigQuery(
    "check1:flowtobq.t2",
     schema=table_schema,
     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
     custom_gcs_temp_location="gs://dataflow_storage_1/temp"
)
)
ret = p2.run()
if ret.state == PipelineState.DONE:
    print('Success!!!')
else:
    print('Error Running beam pipeline')
    # read data and split based on ‘,’
books = (p | beam.io.ReadFromText("gs://case_study1_dataset/book_ratings.csv") |
         beam.Map(Split))
# Filter records having fiction , map each rating as a set (rating,1), use
# combineperkey to count the number of each rating, run the average function, write
# result to Fiction_result1
res1 = (
    books
    | beam.Filter(lambda rec: rec[6] == "Fiction")
    | beam.Map(lambda rec: (rec[2], 1))
    | "Grouping keys" >> beam.CombinePerKey(sum)
    # | beam.Map(f_mean)
    # | beam.Map(accumulate)
    # | beam.Map(op)
    | "Combine Globally" >> beam.CombineGlobally(AverageFn())
    # | "Calculating mean" >>
    beam.CombineValues(beam.combiners.MeanCombineFn())
    #
    | "Apply Formatting" >> beam.Map(FormatText)
    | "write" >> beam.io.WriteToText("gs://dataflow_storage_1/Fiction_res1")
)
# Filter records having Non Fiction , map each rating as a set (rating,1), use
# combineperkey to count the number of each rating, run the average function, write
# result to Fiction_result1
Res2 = (
    books
    | beam.Filter(lambda rec: rec[6] == "Non Fiction")
    | beam.Map(lambda rec: (rec[2], 1))
    | "Grouping keys" >> beam.CombinePerKey(sum)
    # | beam.Map(f_mean)
    # | beam.Map(accumulate)
    # | beam.Map(op)| "Combine Globally" >> beam.CombineGlobally(AverageFn())
    # | "Calculating mean" >>
    beam.CombineValues(beam.combiners.MeanCombineFn())
    #
    | "Apply Formatting" >> beam.Map(FormatText)
    | "write" >> beam.io.WriteToText("gs://dataflow_storage_1/Non_Fiction_res1")
)
# map each rating as a set (rating,1), use combineperkey to count the number of
# each rating, run the average function, write result to Fiction_result1
Res3 = (
    books
    | beam.Map(lambda rec: (rec[2], 1))
    | "Grouping keys" >> beam.CombinePerKey(sum)
    | "Combine Globally" >> beam.CombineGlobally(AverageFn())
    | beam.CombineValues(beam.combiners.MeanCombineFn())
    | "Apply Formatting" >> beam.Map(FormatText)
    | "write" >> beam.io.WriteToText("gs://dataflow_storage_1/All_Result1")
)
# map each record’s 0 th column that is name with value 1 , run distinct function to
# get the distinct values of name, run top.of(5) to sort and get the last5 books
# alphabetically and store in storage bucket last5
f_res = (books | beam.Map(lambda rec: (rec[0], 1)) | beam.Distinct(
) | beam.combiners.Top.Of(5) | beam.io.WriteToText("gs://dataflow_storage_1/Last_5"))
pipeline_result = DataflowRunner().run_pipeline(p, options=options)
url = ('https://console.cloud.google.com/dataflow/jobs/%s/%s?project=%s' %
       (pipeline_result._job.location, pipeline_result._job.id, pipeline_result._job.projectId))
display(HTML('Click < a href="%s" target="_new" > here < /a > for the details of your Dataflow job!' % url))
