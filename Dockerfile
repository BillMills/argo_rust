FROM rust:1.70.0

RUN apt-get update -y ; apt-get install -y nano netcdf-bin libhdf5-serial-dev libnetcdff-dev

WORKDIR /app
