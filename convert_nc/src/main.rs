#![allow(nonstandard_style)]
use netcdf;
use serde::{Deserialize, Serialize};
use serde_json;
use std::error::Error;

#[derive(Debug, Serialize, Deserialize)]
struct GeojsonPoint {
    #[serde(rename = "type")]
    pub point_type: String,
    pub coordinates: Vec<f64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct DataSchema {
    geolocation: GeojsonPoint,
    metadata: Vec<String>,
    CYCLE_NUMBER: i32,
    DIRECTION: String,
    DATA_STATE_INDICATOR: String,
    DATA_MODE: String,
    JULD: f64,
    JULD_QC: String,
    JULD_LOCATION: f64,
    POSITION_QC: String,
    VERTICAL_SAMPLING_SCHEME: String,
    CONFIG_MISSION_NUMBER: i32,
}

#[derive(Debug, Serialize, Deserialize)]
struct MetaSchema {
    _id: String,
    DATA_TYPE: String,
    FORMAT_VERSION: String,
    HANDBOOK_VERSION: String,
    REFERENCE_DATE_TIME: String,
    DATE_CREATION: String,
    DATE_UPDATE: String,
    PROJECT_NAME: String,   
    PI_NAME: Vec<String>,
    DATA_CENTRE: String,
    DC_REFERENCE: String,
    PLATFORM_TYPE: String,
    FLOAT_SERIAL_NO: String,
    FIRMWARE_VERSION: String,
    WMO_INST_TYPE: String,
    POSITIONING_SYSTEM: String,
}

fn trim_null_bytes(input: String) -> String {
    input.trim_end_matches('\0').trim_end_matches(' ').to_string()
}

fn unpack_string(name: &str, buflen: usize, extents: netcdf::Extents, file: &netcdf::File) -> String {
    let mut dump = vec![0_u8; buflen];
    let _ = &file.variable(name).unwrap().get_raw_values(&mut dump, extents).unwrap();
    return trim_null_bytes(String::from_utf8(dump).unwrap());
}

fn unpack_string_array(name: &str, buflen: usize, arraydim: usize, extents: netcdf::Extents, file: &netcdf::File) -> Vec<String> {
    let mut dump = vec![0_u8; buflen * arraydim];
    let _ = &file.variable(name).unwrap().get_raw_values(&mut dump, extents).unwrap();
    let strings: Vec<String> = dump
        .chunks_exact(buflen)
        .map(|chunk| trim_null_bytes(String::from_utf8_lossy(chunk).into_owned()))
        .collect();
    strings
}

fn split_string(input: String, separator: char) -> Vec<String> {
    input.split(separator).map(|s| s.to_string()).collect()
}

fn main() -> Result<(), Box<dyn Error>> {

    // open the NetCDF file
    let file = netcdf::open("data/D4900549_182.nc")?;
    let pindex = 0; // just use the first profile for now
    let STRING1: usize = 1;
    let STRING2: usize = 2;
    let STRING4: usize = 4;
    let STRING8: usize = 8;
    let STRING16: usize = 16;
    let STRING32: usize = 32;
    let STRING64: usize = 64;
    let STRING256: usize = 256;
    let DATE_TIME: usize = 14;
    let N_PROF: usize = file.dimension("N_PROF").unwrap().len();
    let N_PARAM: usize = file.dimension("N_PARAM").unwrap().len();
    let N_LEVELS: usize = file.dimension("N_LEVELS").unwrap().len();
    let N_CALIB: usize = file.dimension("N_CALIB").unwrap().len();
    let N_HISTORY: usize = file.dimension("N_HISTORY").unwrap().len();
    
    // unpack some data
    let DATA_TYPE: String = unpack_string("DATA_TYPE", STRING16, [..16].into(), &file);
    let FORMAT_VERSION: String = unpack_string("FORMAT_VERSION", STRING4, [..4].into(), &file);
    let HANDBOOK_VERSION: String = unpack_string("HANDBOOK_VERSION", STRING4, [..4].into(), &file);
    let REFERENCE_DATE_TIME: String = unpack_string("REFERENCE_DATE_TIME", DATE_TIME, [..14].into(), &file);
    let DATE_CREATION: String = unpack_string("DATE_CREATION", DATE_TIME, [..14].into(), &file);
    let DATE_UPDATE: String = unpack_string("DATE_UPDATE", DATE_TIME, [..14].into(), &file);
    let PLATFORM_NUMBER: String = unpack_string("PLATFORM_NUMBER", STRING8, [..1, ..8].into(), &file); // encoded as metadata _id
    let PROJECT_NAME: String = unpack_string("PROJECT_NAME", STRING64, [..1, ..64].into(), &file);
    let PI_NAME: String = unpack_string("PI_NAME", STRING64, [..1, ..64].into(), &file);
    let STATION_PARAMETERS: Vec<String> = unpack_string_array("STATION_PARAMETERS", STRING16, N_PARAM, [..1, ..N_PARAM, ..16].into(), &file); // encoded in data_info[0]
    let CYCLE_NUMBER: i32 = file.variable("CYCLE_NUMBER").expect("Could not find variable 'CYCLE_NUMBER'").get_value([pindex])?;
    let DIRECTION: String = unpack_string("DIRECTION", STRING1, [..1].into(), &file);
    let DATA_CENTRE: String = unpack_string("DATA_CENTRE", STRING2, [..1, ..2].into(), &file);
    let DC_REFERENCE: String = unpack_string("DC_REFERENCE", STRING32, [..1, ..32].into(), &file);
    let DATA_STATE_INDICATOR: String = unpack_string("DATA_STATE_INDICATOR", STRING4, [..1, ..4].into(), &file);
    let DATA_MODE: String = unpack_string("DATA_MODE", STRING1, [..1].into(), &file);
    let PLATFORM_TYPE: String = unpack_string("PLATFORM_TYPE", STRING32, [..1, ..32].into(), &file);
    let FLOAT_SERIAL_NO: String = unpack_string("FLOAT_SERIAL_NO", STRING32, [..1, ..32].into(), &file);
    let FIRMWARE_VERSION: String = unpack_string("FIRMWARE_VERSION", STRING32, [..1, ..32].into(), &file);
    let WMO_INST_TYPE: String = unpack_string("WMO_INST_TYPE", STRING4, [..1, ..4].into(), &file);
    let JULD: f64 = file.variable("JULD").expect("Could not find variable 'JULD'").get_value([pindex])?;
    let JULD_QC: String = unpack_string("JULD_QC", STRING1, [..1].into(), &file);
    let JULD_LOCATION: f64 = file.variable("JULD_LOCATION").expect("Could not find variable 'JULD_LOCATION'").get_value([pindex])?;
    let LATITUDE: f64 = file.variable("LATITUDE").expect("Could not find variable 'latitude'").get_value([pindex])?; // encoded in geolocation
    let LONGITUDE: f64 = file.variable("LONGITUDE").expect("Could not find variable 'longitude'").get_value([pindex])?; // encoded in geolocation
    let POSITION_QC: String = unpack_string("POSITION_QC", STRING1, [..1].into(), &file);
    let POSITIONING_SYSTEM: String = unpack_string("POSITIONING_SYSTEM", STRING8, [..1, ..8].into(), &file);
    let VERTICAL_SAMPLING_SCHEME: String = unpack_string("VERTICAL_SAMPLING_SCHEME", STRING256, [..1, ..256].into(), &file);
    let CONFIG_MISSION_NUMBER: i32 = file.variable("CONFIG_MISSION_NUMBER").expect("Could not find variable 'CONFIG_MISSION_NUMBER'").get_value([pindex])?;

    // Process the NetCDF variables and convert to JSON
    let data_object = serde_json::to_value(DataSchema {
        geolocation: GeojsonPoint {
            point_type: "Point".to_string(),
            coordinates: vec![LONGITUDE, LATITUDE],
        },
        metadata: vec![PLATFORM_NUMBER.clone()],
        CYCLE_NUMBER: CYCLE_NUMBER,
        DIRECTION: DIRECTION,
        DATA_STATE_INDICATOR: DATA_STATE_INDICATOR,
        DATA_MODE: DATA_MODE,
        JULD: JULD,
        JULD_QC: JULD_QC,
        JULD_LOCATION: JULD_LOCATION,
        POSITION_QC: POSITION_QC,
        VERTICAL_SAMPLING_SCHEME: VERTICAL_SAMPLING_SCHEME,
        CONFIG_MISSION_NUMBER: CONFIG_MISSION_NUMBER,
    })?;

    let meta_object = serde_json::to_value(MetaSchema {
        _id: PLATFORM_NUMBER.clone(),
        DATA_TYPE: DATA_TYPE,
        FORMAT_VERSION: FORMAT_VERSION,
        HANDBOOK_VERSION: HANDBOOK_VERSION,
        REFERENCE_DATE_TIME: REFERENCE_DATE_TIME,
        DATE_CREATION: DATE_CREATION,
        DATE_UPDATE: DATE_UPDATE,
        PROJECT_NAME: PROJECT_NAME,
        PI_NAME: split_string(PI_NAME, ','),
        DATA_CENTRE: DATA_CENTRE,
        DC_REFERENCE: DC_REFERENCE,
        PLATFORM_TYPE: PLATFORM_TYPE,
        FLOAT_SERIAL_NO: FLOAT_SERIAL_NO,
        FIRMWARE_VERSION: FIRMWARE_VERSION,
        WMO_INST_TYPE: WMO_INST_TYPE,
        POSITIONING_SYSTEM: POSITIONING_SYSTEM,
    })?;

    // Print the resulting JSON object
    println!("{}", serde_json::to_string_pretty(&data_object)?);
    println!("{}", serde_json::to_string_pretty(&meta_object)?);

    Ok(())
}