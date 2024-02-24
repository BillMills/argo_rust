use netcdf;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::error::Error;
use std::any::type_name;

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

// #[derive(Debug, Serialize, Deserialize)]
// struct MetaSchema {
//     _id: String,
// }

fn trim_null_bytes(input: String) -> String {
    input.trim_end_matches('\0').to_string()
}

fn unpack_string(name: &str, buflen: usize, file: netcdf::File) -> String {
    let mut dump = vec![0_u8; buflen];
    &file.variable(name).unwrap().get_raw_values(&mut dump, (0, ..buflen)).unwrap();
    return trim_null_bytes(String::from_utf8(dump).unwrap());
}

fn main() -> Result<(), Box<dyn Error>> {
    // open the NetCDF file
    let file = netcdf::open("data/D4900549_182.nc")?;
    let pindex = 0; // just use the first profile for now
    let STRING2: usize = 2;
    let STRING8: usize = 8;
    let mut dump: &mut [u8] = &mut [0; 1];
    
    // unpack some data
    let LATITUDE: f64 = file.variable("LATITUDE").expect("Could not find variable 'latitude'").get_value([pindex])?;
    let LONGITUDE: f64 = file.variable("LONGITUDE").expect("Could not find variable 'longitude'").get_value([pindex])?;

    // let mut PLATFORM_NUMBER = vec![0_u8; 8];
    // &file.variable("PLATFORM_NUMBER").unwrap().get_raw_values(&mut PLATFORM_NUMBER, (0, ..8)).unwrap();
    // println!("{:?}", trim_null_bytes(String::from_utf8(PLATFORM_NUMBER).unwrap()));

    let PLATFORM_NUMBER: String = unpack_string("PLATFORM_NUMBER", STRING8, file);

    // Process the NetCDF variables and convert to JSON
    let mut data_object = serde_json::to_value(DataSchema {
        geolocation: GeojsonPoint {
            point_type: "Point".to_string(),
            coordinates: vec![LONGITUDE, LATITUDE],
        },
        metadata: vec![PLATFORM_NUMBER],
    })?;

    // let mut meta_object = serde_json::to_value(MetaSchema {
    //     _id: PLATFORM_NUMBER.to_string(),
    // })?;

    // Print the resulting JSON object
    println!("{}", serde_json::to_string_pretty(&data_object)?);
    //println!("{}", serde_json::to_string_pretty(&meta_object)?);

    Ok(())
}
