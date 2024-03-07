#![allow(nonstandard_style)]
use netcdf;
use tokio;
use std::error::Error;
use chrono::Utc;
use chrono::TimeZone;
use chrono::Duration;
use std::env;
use mongodb::bson::{doc};
use mongodb::bson::DateTime;
use mongodb::{Client, options::{ClientOptions, ResolverConfig}};
use serde::{Deserialize, Serialize};
use mongodb::bson::Bson;
use std::collections::HashMap;
use std::fs;

// helper functions ///////////////////////////////////////////

fn trim_null_bytes(input: String) -> String {
    input.trim().trim_end_matches('\0').to_string()
}

fn unpack_string(name: &str, buflen: usize, extents: netcdf::Extents, file: &netcdf::File) -> String {
    let mut dump = vec![0_u8; buflen];
    if let Some(variable) = file.variable(name) {
        if let Ok(_) = variable.get_raw_values(&mut dump, extents) {
            if let Ok(string) = String::from_utf8(dump) {
                return trim_null_bytes(string);
            }
        }
    }
    String::new()
}

fn unpack_string_array(name: &str, buflen: usize, arraydim: usize, extents: netcdf::Extents, file: &netcdf::File) -> Vec<String> {
    let mut dump = vec![0_u8; buflen * arraydim];
    if let Some(variable) = file.variable(name) {
        if let Ok(_) = variable.get_raw_values(&mut dump, extents) {
            let strings: Vec<String> = dump
                .chunks_exact(buflen)
                .map(|chunk| {
                    let string: String = String::from_utf8_lossy(chunk).into_owned().parse().unwrap_or_default();
                    string.trim().to_string() // Strip leading and trailing whitespace
                })
                .collect();
            return strings;
        }
    }
    vec![String::new(); arraydim]
}

fn split_string(input: String, separator: char) -> Vec<String> {
    input.split(separator).map(|s| s.trim().to_string()).collect()
}

fn extract_variable_attributes(file: &netcdf::File, station_parameters: &[String]) -> Result<HashMap<String, (String, String)>, Box<dyn Error>> {
    let mut variable_attributes: HashMap<String, (String, String)> = HashMap::new();
    for param in station_parameters {
        let variable = file.variable(param).ok_or_else(|| format!("Could not find variable '{}'", param))?;
        let units = variable.attribute_value("units").unwrap()?;
        let long_name = variable.attribute_value("long_name").unwrap()?;
        if let netcdf::AttributeValue::Str(u) = units {
            if let netcdf::AttributeValue::Str(l) = long_name {
                variable_attributes.insert(param.clone(), (u.to_string(), l.to_string()));
            }
        }
    }
    Ok(variable_attributes)
}

////////////////////////////////////////////////////////////////

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    // mongodb setup ///////////////////////////////////////////
    // Load the MongoDB connection string from an environment variable:
    let client_uri =
       env::var("MONGODB_URI").expect("You must set the MONGODB_URI environment var!"); 

    // A Client is needed to connect to MongoDB:
    // An extra line of code to work around a DNS issue on Windows:
    let options =
       ClientOptions::parse_with_resolver_config(&client_uri, ResolverConfig::cloudflare())
          .await?;
    let client = Client::with_options(options)?; 
    let argo = client.database("argo").collection::<DataSchema>("argoX");
    let argo_meta = client.database("argo").collection::<MetaSchema>("argoMetaX");

    // structs to describe documents //////////////////////////////

    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct GeoJSONPoint {
        #[serde(rename = "type")]
        location_type: String,
        coordinates: [f64; 2],
    } 

    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct DataSchema {
        _id: String,
        geolocation: GeoJSONPoint,
        metadata: Vec<String>,
        CYCLE_NUMBER: i32,
        DIRECTION: String,
        DATA_STATE_INDICATOR: String,
        DATA_MODE: String,
        DATE_CREATION: String,
        DATE_UPDATE: String,
        JULD: f64,
        JULD_QC: String,
        JULD_LOCATION: f64,
        POSITION_QC: String,
        VERTICAL_SAMPLING_SCHEME: String,
        CONFIG_MISSION_NUMBER: i32,
        realtime_data: Option<HashMap<String, Vec<f64>>>,
        adjusted_data: Option<HashMap<String, Vec<f64>>>,
    }

    #[derive(Serialize, Deserialize, Debug, Clone)]
    struct MetaSchema {
        _id: String,
        DATA_TYPE: String,
        FORMAT_VERSION: String,
        HANDBOOK_VERSION: String,
        REFERENCE_DATE_TIME: String,
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

    // data unpacking /////////////////////////////////////////////

    let mut file_names: Vec<String> = Vec::new();
    if let Ok(entries) = fs::read_dir("data/ifremer/2901237/profiles") {
        for entry in entries {
            if let Ok(entry) = entry {
                if let Some(file_name) = entry.file_name().to_str() {
                    let file_path = format!("data/ifremer/2901237/profiles/{}", file_name);
                    file_names.push(file_path);
                }
            }
        }
    }

    let mut meta_docs: Vec<MetaSchema> = Vec::new();

    for file_name in file_names {
        println!("Processing file: {}", file_name);
        let file = netcdf::open(&file_name)?;
        let pindex = 0; // just use the first profile for now
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
        let CYCLE_NUMBER: i32 = file.variable("CYCLE_NUMBER").map(|var| var.get_value([pindex]).unwrap_or(99999)).unwrap_or(99999);
        let DIRECTION: String = unpack_string("DIRECTION", STRING1, [..1].into(), &file);
        let DATA_CENTRE: String = unpack_string("DATA_CENTRE", STRING2, [..1, ..2].into(), &file);
        let DC_REFERENCE: String = unpack_string("DC_REFERENCE", STRING32, [..1, ..32].into(), &file);
        let DATA_STATE_INDICATOR: String = unpack_string("DATA_STATE_INDICATOR", STRING4, [..1, ..4].into(), &file);
        let DATA_MODE: String = unpack_string("DATA_MODE", STRING1, [..1].into(), &file);
        let PLATFORM_TYPE: String = unpack_string("PLATFORM_TYPE", STRING32, [..1, ..32].into(), &file);
        let FLOAT_SERIAL_NO: String = unpack_string("FLOAT_SERIAL_NO", STRING32, [..1, ..32].into(), &file);
        let FIRMWARE_VERSION: String = unpack_string("FIRMWARE_VERSION", STRING32, [..1, ..32].into(), &file);
        let WMO_INST_TYPE: String = unpack_string("WMO_INST_TYPE", STRING4, [..1, ..4].into(), &file);
        let JULD: f64 = file.variable("JULD").map(|var| var.get_value([pindex]).unwrap_or(999999.0)).unwrap_or(999999.0);
        let JULD_QC: String = unpack_string("JULD_QC", STRING1, [..1].into(), &file);
        let JULD_LOCATION: f64 = file.variable("JULD_LOCATION").map(|var| var.get_value([pindex]).unwrap_or(999999.0)).unwrap_or(999999.0);
        let LATITUDE: f64 = file.variable("LATITUDE").map(|var| var.get_value([pindex]).unwrap_or(99999.0)).unwrap_or(99999.0);
        let LONGITUDE: f64 = file.variable("LONGITUDE").map(|var| var.get_value([pindex]).unwrap_or(99999.0)).unwrap_or(99999.0);
        let POSITION_QC: String = unpack_string("POSITION_QC", STRING1, [..1].into(), &file);
        let POSITIONING_SYSTEM: String = unpack_string("POSITIONING_SYSTEM", STRING8, [..1, ..8].into(), &file);
        let VERTICAL_SAMPLING_SCHEME: String = unpack_string("VERTICAL_SAMPLING_SCHEME", STRING256, [..1, ..256].into(), &file);
        let CONFIG_MISSION_NUMBER: i32 = file.variable("CONFIG_MISSION_NUMBER").map(|var| var.get_value([pindex]).unwrap_or(99999)).unwrap_or(99999);

        // fiddling with templated unpacking, tbd how to consume this downstream
        // could also turn all these into functions
        let realtime_data: Option<HashMap<String, Vec<f64>>> = if DATA_MODE == "R" {
            Some(STATION_PARAMETERS.iter()
            .map(|param| {
                let variable = file.variable(&param).expect(&format!("Could not find variable '{}'", param));
                let data: Vec<f64> = variable.get_values([..1, ..N_LEVELS])?;
                Ok((param.clone(), data))
            })
            .collect::<Result<_, Box<dyn Error>>>()?)
        } else {
            None
        };
    
        let adjusted_data: Option<HashMap<String, Vec<f64>>> = if DATA_MODE == "R" {
            None
        } else {
            Some(STATION_PARAMETERS.iter()
                .map(|param| {
                    let adjusted_variable_name = format!("{}_ADJUSTED", param);
                    let variable = file.variable(&adjusted_variable_name).expect(&format!("Could not find variable '{}'", adjusted_variable_name));
                    let data: Vec<f64> = variable.get_values([..1, ..N_LEVELS])?;
                    Ok((param.clone(), data))
                })
                .collect::<Result<_, Box<dyn Error>>>()?)
        };
    
        let profile_param_qc: HashMap<String, String> = STATION_PARAMETERS.iter()
            .map(|param| {
                let qc_variable_name = format!("PROFILE_{}_QC", param);
                let qc_value = unpack_string(&qc_variable_name, STRING1, [..1].into(), &file);
                Ok((param.clone(), qc_value))
            })
            .collect::<Result<_, Box<dyn Error>>>()?; // tbd what to do with this
    
        let level_qc: HashMap<String, Vec<String>> = STATION_PARAMETERS.iter()
            .map(|param| {
                let qc_variable_name = format!("{}_QC", param);
                let qc_vec = unpack_string_array(&qc_variable_name, STRING1, N_LEVELS, [..1, ..N_LEVELS].into(), &file);
                Ok((param.clone(), qc_vec))
            })
            .collect::<Result<_, Box<dyn Error>>>()?;
    
        let adjusted_level_qc: HashMap<String, Vec<String>> = STATION_PARAMETERS.iter()
            .map(|param| {
                let qc_variable_name = format!("{}_ADJUSTED_QC", param);
                let qc_vec = unpack_string_array(&qc_variable_name, STRING1, N_LEVELS, [..1, ..N_LEVELS].into(), &file);
                Ok((param.clone(), qc_vec))
            })
            .collect::<Result<_, Box<dyn Error>>>()?;
        
        let adjusted_level_error: HashMap<String, Vec<f64>> = STATION_PARAMETERS.iter()
            .map(|param| {
                let adjusted_variable_name = format!("{}_ADJUSTED_ERROR", param);
                let variable = file.variable(&adjusted_variable_name).expect(&format!("Could not find variable '{}'", adjusted_variable_name));
                let data: Vec<f64> = variable.get_values([..1, ..N_LEVELS])?;
                Ok((param.clone(), data))
            })
            .collect::<Result<_, Box<dyn Error>>>()?;
    
        let variable_metadata = extract_variable_attributes(&file, &STATION_PARAMETERS)?;
    
        // construct the structs for this file ///////////////////////////////
    
        let mut meta_object = MetaSchema {
            _id: PLATFORM_NUMBER.clone(),
            DATA_TYPE: DATA_TYPE,
            FORMAT_VERSION: FORMAT_VERSION,
            HANDBOOK_VERSION: HANDBOOK_VERSION,
            REFERENCE_DATE_TIME: REFERENCE_DATE_TIME,
            PROJECT_NAME: PROJECT_NAME,
            PI_NAME: split_string(PI_NAME, ','),
            DATA_CENTRE: DATA_CENTRE,
            DC_REFERENCE: DC_REFERENCE,
            PLATFORM_TYPE: PLATFORM_TYPE,
            FLOAT_SERIAL_NO: FLOAT_SERIAL_NO,
            FIRMWARE_VERSION: FIRMWARE_VERSION,
            WMO_INST_TYPE: WMO_INST_TYPE,
            POSITIONING_SYSTEM: POSITIONING_SYSTEM,
        };

        // check if this metadata object already exists in the database
        let mut meta_id = String::new();
        for meta_doc in meta_docs.iter() {
            if meta_doc.DATA_TYPE == meta_object.DATA_TYPE
                && meta_doc.FORMAT_VERSION == meta_object.FORMAT_VERSION
                && meta_doc.HANDBOOK_VERSION == meta_object.HANDBOOK_VERSION
                && meta_doc.REFERENCE_DATE_TIME == meta_object.REFERENCE_DATE_TIME
                && meta_doc.PROJECT_NAME == meta_object.PROJECT_NAME
                && meta_doc.PI_NAME == meta_object.PI_NAME
                && meta_doc.DATA_CENTRE == meta_object.DATA_CENTRE
                && meta_doc.DC_REFERENCE == meta_object.DC_REFERENCE
                && meta_doc.PLATFORM_TYPE == meta_object.PLATFORM_TYPE
                && meta_doc.FLOAT_SERIAL_NO == meta_object.FLOAT_SERIAL_NO
                && meta_doc.FIRMWARE_VERSION == meta_object.FIRMWARE_VERSION
                && meta_doc.WMO_INST_TYPE == meta_object.WMO_INST_TYPE
                && meta_doc.POSITIONING_SYSTEM == meta_object.POSITIONING_SYSTEM
            {
                meta_id = meta_doc._id.clone();
                break;
            }
        }

        if meta_id.is_empty() {
            // we found a new metadata doc
            let new_id = format!("{}_m{}", PLATFORM_NUMBER, meta_docs.len());
            meta_object._id = new_id.clone();
            meta_docs.push(meta_object.clone());
            argo_meta.insert_one(meta_object, None).await?;
            meta_id = new_id;
        }

        let data_object = DataSchema {
            _id: format!("{}_{}", PLATFORM_NUMBER, CYCLE_NUMBER),
            geolocation: GeoJSONPoint {
                location_type: "Point".to_string(),
                coordinates: [LONGITUDE, LATITUDE],
            },
            metadata: vec![meta_id.clone()],
            CYCLE_NUMBER: CYCLE_NUMBER,
            DIRECTION: DIRECTION,
            DATA_STATE_INDICATOR: DATA_STATE_INDICATOR,
            DATA_MODE: DATA_MODE,
            DATE_CREATION: DATE_CREATION,
            DATE_UPDATE: DATE_UPDATE,
            JULD: JULD,
            JULD_QC: JULD_QC,
            JULD_LOCATION: JULD_LOCATION,
            POSITION_QC: POSITION_QC,
            VERTICAL_SAMPLING_SCHEME: VERTICAL_SAMPLING_SCHEME,
            CONFIG_MISSION_NUMBER: CONFIG_MISSION_NUMBER,
            realtime_data: realtime_data,
            adjusted_data: adjusted_data,
        };
    
        argo.insert_one(data_object, None).await?;
        
    }
    
    Ok(())
}