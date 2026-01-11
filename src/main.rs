mod common;
mod storage;
mod plan;
mod operators;

use crate::plan::ExecutionTask;
use crate::storage::ScanWorker;
use crate::operators::filter::FilterWorker;
use crate::operators::projection::ProjectionWorker;
use crate::operators::zip::ZipWorker;
use crate::common::Column;
use std::sync::{Arc, Mutex};
use std::fs::File;
use std::io::BufReader;


fn main() -> Result<(), String> {
    // python3 -c "import struct; f=open('age.bin','wb'); [f.write(struct.pack('<i', x)) for x in [15, 20, 25, 30, 35, 40]]; f.close()"
    // python3 -c "import struct; f=open('salary.bin','wb'); [f.write(struct.pack('<i', x)) for x in [0, 500, 2000, 3500, 5000, 7000]]; f.close()"
    // python3 -c "import struct; f=open('dept.bin','wb'); [f.write(struct.pack('<i', x)) for x in [1, 1, 0, 2, 1, 0]]; f.close()"
    
    let age_path = "age.bin";
    let salary_path = "salary.bin";
    let dept_path = "dept.bin";

    let age_scanner = Arc::new(ScanWorker {
        file_handle: Mutex::new(BufReader::new(File::open(age_path).map_err(|e| e.to_string())?)),
        column_name: "age".to_string(),
    });

    let salary_scanner = Arc::new(ScanWorker {
        file_handle: Mutex::new(BufReader::new(File::open(salary_path).map_err(|e| e.to_string())?)),
        column_name: "salary".to_string(),
    });

    let dept_scanner = Arc::new(ScanWorker {
        file_handle: Mutex::new(BufReader::new(File::open(dept_path).map_err(|e| e.to_string())?)),
        column_name: "dept".to_string(),
    });

    let age_salary_zip = Arc::new(ZipWorker {
        left: age_scanner,
        right: salary_scanner,
    });

    let merged_source = Arc::new(ZipWorker {
        left: age_salary_zip,
        right: dept_scanner,
    });

    let age_filter = Arc::new(FilterWorker {
        input: merged_source,
        predicate: |val| val > 21,
        col_idx: 0, 
    });

    let dept_filter = Arc::new(FilterWorker {
        input: age_filter,
        predicate: |val| val == 1, 
        col_idx: 2, 
    });

    let pipeline = Arc::new(ProjectionWorker {
        input: dept_filter, 
        indices: vec![0, 1, 2],
    });

    // -- TABLE DISPLAY --
    println!("\nQuery: SELECT age, salary, dept WHERE age > 21 AND dept == 1");
    println!("+-----+---------+-------+");
    println!("| age | salary  | dept  |");
    println!("+-----+---------+-------+");

    while let Some(batch) = pipeline.next_batch()? {
        let Column::Int32(ages) = &batch.columns[0];
        let Column::Int32(salaries) = &batch.columns[1];
        let Column::Int32(depts) = &batch.columns[2];

        for ((age, salary), dept) in ages.iter().zip(salaries.iter()).zip(depts.iter()) {
            let dept_label = match dept {
                0 => "Sales",
                1 => "Eng  ",
                2 => "HR   ",
                _ => "Other",
            };
            println!("| {:<3} | ${:<6} | {} |", age, salary, dept_label);
        }
    }

    println!("+-----+---------+-------+");

    Ok(())
}