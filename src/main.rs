mod common;
mod storage;
mod plan;
mod operators;

use std::sync::{Arc, Mutex};
use std::fs::File;
use std::io::BufReader;
use crate::plan::ExecutionTask;
use crate::storage::ScanWorker;
use crate::operators::filter::FilterWorker;
use crate::operators::projection::ProjectionWorker;
use crate::operators::zip::ZipWorker;
use crate::common::Column;

fn main() -> Result<(), String> {
    // python3 -c "import struct; f=open('age.bin','wb'); [f.write(struct.pack('<i', x)) for x in [15, 20, 25, 30, 35, 40]]; f.close()"
    let age_path = "age.bin";
    // python3 -c "import struct; f=open('salary.bin','wb'); [f.write(struct.pack('<i', x)) for x in [0, 500, 2000, 3500, 5000, 7000]]; f.close()"
    let salary_path = "salary.bin";

    let age_scanner = Arc::new(ScanWorker {
        file_handle: Mutex::new(BufReader::new(File::open(age_path).map_err(|e| e.to_string())?)),
        column_name: "age".to_string(),
    });

    let salary_scanner = Arc::new(ScanWorker {
        file_handle: Mutex::new(BufReader::new(File::open(salary_path).map_err(|e| e.to_string())?)),
        column_name: "salary".to_string(),
    });

    let merged_source = Arc::new(ZipWorker {
        left: age_scanner,
        right: salary_scanner,
    });

    let filter = Arc::new(FilterWorker {
        input: merged_source,
        predicate: |age| age > 21,
    });

    let pipeline = Arc::new(ProjectionWorker {
        input: filter, 
        indices: vec![0, 1],
    });

    // -- TABLE DISPLAY --
    println!("\n+-----+---------+");
    println!("| age | salary   |");
    println!("+-----+---------+");

    while let Some(batch) = pipeline.next_batch()? {
        let Column::Int32(ages) = &batch.columns[0];
        let Column::Int32(salaries) = &batch.columns[1];

        // zip(salaries) ensures we pair the correct age with the correct salary
        for (age, salary) in ages.iter().zip(salaries.iter()) {
            // :<3 and :<8 for spacing/alignment
            println!("| {:<3} | ${:<7} |", age, salary);
        }
    }

    println!("+-----+---------+");

    Ok(())
}