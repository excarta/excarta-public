set -e

PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python3 src/download_data/madis/download_beam.py \
--start_date 2023-06-15 \
--end_date 2023-07-15 \
--output_dir \
--remote \
--requirements_file download_data/madis/download_requirements.txt  \
--setup_file download_data/madis/setup.py \
--runner=DataflowRunner \
--gcs_project \
--job_name=madisdownload \
--region=us-central1 \
--temp_location=gs://dftemp/tmp \
--experiments=use_runner_v2 \
--num_workers 20 \
--autoscaling_algorithm=NONE
