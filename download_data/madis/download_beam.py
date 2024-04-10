#!/usr/bin/env python3

import argparse
import apache_beam as beam
import datetime
import warnings

import logging
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from helpers import madis_download_lib
import xarray as xr


def get_date_ranges(start_date, end_date, sources):
  start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
  end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
  dates = []
  while start_date <= end_date:
    for source in sources:
      for h in range(24):
        dates.append((start_date, h, source))
    start_date += datetime.timedelta(days=1)
  return dates


def main():
  warnings.filterwarnings("ignore", module=".*xarray.conventions")
  logging.info('Starting pipeline')
  parser = argparse.ArgumentParser()
  parser.add_argument('--remote',
                      help='Whether to run locally.',
                      action='store_true',
                      default=False)
  parser.add_argument('--start_date',
                      help='Start date.',
                      type=str,
                      default='2018-01-01')
  parser.add_argument('--end_date',
                      help='End date, inclusive.',
                      type=str,
                      default='2018-01-01')
  parser.add_argument('--sources',
                      help='List of sources to download from MADIS.',
                      type=str,
                      default='mesonet,metar')
  parser.add_argument('--output_dir',
                      help='Output dir on GCS.',
                      type=str,
                      default='')
  parser.add_argument('--gcs_project',
                      help='GCS project name, if applicable.',
                      type=str,
                      default='')


  args, pipeline_args = parser.parse_known_args()
  logging.info(args)
  logging.info(pipeline_args)
  sources = args.sources.split(',')

  beam_options = PipelineOptions(pipeline_args)
  beam_options.view_as(SetupOptions).save_main_session = True

  output_dir = (args.output_dir if args.remote else '/tmp/madis')
  date_ranges = get_date_ranges(args.start_date, args.end_date, sources)
  logging.info('%s date-var combinations to get.' % len(date_ranges))
  logging.info('Storing output in %s' % output_dir)
  with beam.Pipeline(options=beam_options) as p:
    (p | beam.Create(date_ranges) | 'DownloadData' >> beam.ParDo(
        madis_download_lib.DownloadMadisDataFn(remote=args.remote,
                                               outdir=output_dir,
                                               gcs_project=args.gcs_project)))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  main()
