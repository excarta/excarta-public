import datetime
import xarray as xr
import os
import tempfile
import urllib
import logging
import gcsfs
import apache_beam as beam
from apache_beam.metrics import Metrics


def get_source_url(source: str, curdate: datetime.datetime, hour: int):
  BASEURL = curdate.strftime(
      'https://madis-data.ncep.noaa.gov/madisPublic1/data/archive/%Y/%m/%d')
  if source == 'mesonet':
    sourceurl = os.path.join(BASEURL, 'LDAD/mesonet/netCDF')
  elif source == 'metar':
    sourceurl = os.path.join(BASEURL, 'point/metar/netcdf')
  elif source == 'hfmetar':
    sourceurl = os.path.join(BASEURL, 'LDAD/hfmetar/netCDF')
  elif source == 'hcn':
    sourceurl = os.path.join(BASEURL, 'LDAD/hcn/netCDF')
  elif source == 'crn':
    sourceurl = os.path.join(BASEURL, 'LDAD/crn/netCDF')
  else:
    raise ValueError('Unrecognized source: %s' % source)
  basename = '%s_%02d00.gz' % (curdate.strftime('%Y%m%d'), hour)
  return os.path.join(sourceurl, basename)


def download_from_source(sourceurl: str, zarrpath: str, gcs_project: str):
  try:
    with tempfile.TemporaryDirectory(prefix='madis') as tmpdir:
      localpath = os.path.join(tmpdir, os.path.basename(sourceurl))
      urllib.request.urlretrieve(sourceurl, localpath)
      madis = xr.open_dataset(localpath)
      data_vars = {}
      for varname in madis.data_vars.keys():
        data_vars[varname] = xr.Variable(
            dims=madis.data_vars[varname].dims,
            data=madis.data_vars[varname],
            encoding={'chunks': madis.data_vars[varname].shape})
      newds = xr.Dataset(data_vars=data_vars)
      if zarrpath.startswith('gs://'):
        fs = gcsfs.GCSFileSystem(project=gcs_project, token=None)
        store = gcsfs.mapping.GCSMap(zarrpath, gcs=fs, check=False, create=True)
      else:
        store = zarrpath
      newds.to_zarr(store, mode='w')
  except Exception as e:
    logging.exception(e)


class DownloadMadisDataFn(beam.DoFn):

  def __init__(self, remote: bool, outdir: str, gcs_project: str):
    self.remote = remote
    self.outdir = outdir
    self.gcs_project = gcs_project
    self.existing_counter = Metrics.counter(self.__class__, 'date_vars_skipped')
    self.to_fetch_counter = Metrics.counter(self.__class__,
                                            'date_vars_to_fetch')
    self.dates_counter = Metrics.counter(self.__class__, 'date_vars_fetched')
    self.failed_counter = Metrics.counter(self.__class__, 'failed_fetches')
    self.bad_datasets_counter = Metrics.counter(self.__class__, 'bad_datasets')

  def process(self, element):
    curdate, hour, source = element
    zarrpath = os.path.join(
        self.outdir, str(curdate.year), str(curdate.month), source,
        '%s_%02d00.zarr' % (curdate.strftime('%Y%m%d'), hour))
    try:
      ds = xr.open_zarr(zarrpath, consolidated=False)
      self.existing_counter.inc()
      return
    except Exception as e:
      self.to_fetch_counter.inc()

    download_from_source(get_source_url(source, curdate, hour), zarrpath, self.gcs_project)
    self.dates_counter.inc()
