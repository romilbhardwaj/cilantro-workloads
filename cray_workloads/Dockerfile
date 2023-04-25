# FROM rayproject/ray:latest
FROM python:3.7.11-slim-buster

# Set up the development environment
WORKDIR /cray_workloads

# Install extra packages
RUN apt update && \
    yes | apt install nano wget curl

# Copy the data files 
COPY db_data ./db_data/
COPY train_data ./train_data/
COPY traces ./traces/
COPY db_bins ./db_bins/

# Install dependencies
COPY requirements.txt setup.py ./
RUN pip install -U pip && \
    pip install --no-cache-dir -r requirements.txt

RUN yes | pip install ray

# Copy the cray_workloads files
COPY cray_workloads ./cray_workloads/
# Install cilantro
RUN pip install -e .
# ADD dist/cray_workloads-0.1-py3-none-any.whl cray_workloads-0.1-py3-none-any.whl
# RUN pip install cray_workloads-0.1-py3-none-any.whl

ENV PYTHONUNBUFFERED 1

CMD ["python", "/cray_workloads/cray_workloads/drivers/cray_runscript.py"]
