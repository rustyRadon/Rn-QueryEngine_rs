use std::sync::Arc;
use crate::common::{Column, RecordBatch};
use crate::plan::ExecutionTask;

pub struct FilterWorker {
    pub input: Arc<dyn ExecutionTask>,
    pub predicate: fn(i32) -> bool,
}

impl ExecutionTask for FilterWorker {
    fn next_batch(&self) -> Result<Option<RecordBatch>, String> {

        let batch = match self.input.next_batch()? {
            Some(b) => b,
            None => return Ok(None),
        };

        let mask: Vec<bool> = match &batch.columns[0] {
            Column::Int32(data) => data.iter().map(|&v| (self.predicate)(v)).collect(),
        };

        if !mask.iter().any(|&m| m) {
            return self.next_batch(); 
        }

        let filtered_columns: Vec<Column> = batch.columns
            .into_iter()
            .map(|col| {
                let Column::Int32(data) = col;
                let new_values: Vec<i32> = data.iter()
                    .enumerate()
                    .filter(|(idx, _)| mask[*idx]) 
                    .map(|(_, &val)| val)
                    .collect();
                
                Column::Int32(Arc::new(new_values))
            })
            .collect();

        Ok(Some(RecordBatch::new(batch.names, filtered_columns)))
    }
}