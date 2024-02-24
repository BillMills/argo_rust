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
}

#[derive(Debug, Serialize, Deserialize)]
struct MetaSchema {
    _id: String,
    DATA_TYPE: String,
    FORMAT_VERSION: String,
    PI_NAME: Vec<String>,
}

fn trim_null_bytes(input: String) -> String {
    input.trim_end_matches('\0').trim_end_matches(' ').to_string()
}

fn unpack_string(name: &str, buflen: usize, extents: netcdf::Extents, file: &netcdf::File) -> String {
    let mut dump = vec![0_u8; buflen];
    let _ = &file.variable(name).unwrap().get_raw_values(&mut dump, extents).unwrap();
    return trim_null_bytes(String::from_utf8(dump).unwrap());
}

fn split_string(input: String, separator: char) -> Vec<String> {
    input.split(separator).map(|s| s.to_string()).collect()
}

fn main() -> Result<(), Box<dyn Error>> {

    // open the NetCDF file
    let file = netcdf::open("data/D4900549_182.nc")?;
    let pindex = 0; // just use the first profile for now
    let STRING2: usize = 2;
    let STRING4: usize = 4;
    let STRING8: usize = 8;
    let STRING16: usize = 16;
    let STRING32: usize = 32;
    let STRING64: usize = 64;
    let STRING256: usize = 256;
    
    // unpack some data
    let DATA_TYPE: String = unpack_string("DATA_TYPE", STRING16, [..16].into(), &file);
    let FORMAT_VERSION: String = unpack_string("FORMAT_VERSION", STRING4, [..4].into(), &file);

    let LATITUDE: f64 = file.variable("LATITUDE").expect("Could not find variable 'latitude'").get_value([pindex])?;
    let LONGITUDE: f64 = file.variable("LONGITUDE").expect("Could not find variable 'longitude'").get_value([pindex])?;
    let PLATFORM_NUMBER: String = unpack_string("PLATFORM_NUMBER", STRING8, [..1, ..8].into(), &file);
    let PI_NAME: String = unpack_string("PI_NAME", STRING64, [..1, ..64].into(), &file);

    // Process the NetCDF variables and convert to JSON
    let data_object = serde_json::to_value(DataSchema {
        geolocation: GeojsonPoint {
            point_type: "Point".to_string(),
            coordinates: vec![LONGITUDE, LATITUDE],
        },
        metadata: vec![PLATFORM_NUMBER.clone()],
    })?;

    let meta_object = serde_json::to_value(MetaSchema {
        _id: PLATFORM_NUMBER.clone(),
        DATA_TYPE: DATA_TYPE,
        FORMAT_VERSION: FORMAT_VERSION,
        PI_NAME: split_string(PI_NAME, ','),
    })?;

    // Print the resulting JSON object
    println!("{}", serde_json::to_string_pretty(&data_object)?);
    println!("{}", serde_json::to_string_pretty(&meta_object)?);

    Ok(())
}