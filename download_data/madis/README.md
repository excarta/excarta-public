# What does this do?

This script is useful to download data from NOAA's MADIS (https://madis.ncep.noaa.gov/) database. There are preset quality control flags to download Level 2 QC checked data. More information on QC flags available can be found on the MADIS website (https://madis.ncep.noaa.gov/madis_qc.shtml). 

# Dependencies

you can install dependencies using the requirements file

`pip3 install -r download_requirements.txt`

# How to use this script?

the shell script `launch_madis_download.sh` can be used to provide all the required `args` including

* `start_date`: start_date of data download. E.g. 2023-06-15.
* `end_date`: (inclusive) end_date data download. E.g. 2023-07-15
* `output_dir`: (str) directory to write out data. E.g. gs://<GCS_DIRECTORY_PATH> or local path '/tmp/'
If applicable, you can specify Google Cloud Platform (GCP) project credentials 
* `gcs_project`: (str) project name on Google Cloud, if applicable.
* `region`: (str) project name on Google Cloud, if applicable. E.g. us-west1
* `temp_location`: (str) location of temporary storage of data on GCP bucket, if applicable. E.g. gs://data/tmp
* `num_workers`: (int) Number of workers on GCP, if applicable.






