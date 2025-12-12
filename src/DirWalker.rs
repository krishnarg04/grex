use crate::SearchWorker::SearchWorker as SW;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{env, fs, path::Path};

fn get_all_files_in_directory(
    path: String,
    file_buffer: Arc<Mutex<Vec<String>>>,
    dir_buffer: Arc<Mutex<Vec<String>>>,
) {
    let path = Path::new(&path);

    // Batch results locally first to minimize lock contention
    let mut local_files = Vec::new();
    let mut local_dirs = Vec::new();

    if let Ok(entries) = fs::read_dir(path) {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Ok(metadata) = entry.metadata() {
                if metadata.is_file() {
                    if let Some(path_str) = path.to_str() {
                        local_files.push(path_str.to_string());
                    }
                } else if metadata.is_dir() {
                    if let Some(path_str) = path.to_str() {
                        local_dirs.push(path_str.to_string());
                    }
                }
            }
        }
    }

    // Lock once and append all results
    if !local_files.is_empty() {
        let mut buffer_files = file_buffer.lock().unwrap();
        buffer_files.extend(local_files);
    }

    if !local_dirs.is_empty() {
        let mut buffer_dirs = dir_buffer.lock().unwrap();
        buffer_dirs.extend(local_dirs);
    }
}

pub fn dir_walk_worker(mut search_worker: SW) {
    let path = env::current_dir().unwrap();
    let file_buffer = Arc::new(Mutex::new(Vec::new()));
    let dir_buffer = Arc::new(Mutex::new(Vec::new()));

    dir_buffer
        .lock()
        .unwrap()
        .push(path.to_str().unwrap().to_string());

    let file_buffer_clone = file_buffer.clone();
    let dir_buffer_clone = dir_buffer.clone();

    let walker_handle = std::thread::spawn(move || {
        file_walk_worker(file_buffer_clone, dir_buffer_clone);
    });

    let FILE_THRESHOLD = 50;

    loop {
        // Check if there are files to process
        let file_list = {
            let mut file_buffer_obj = file_buffer.lock().unwrap();

            if file_buffer_obj.is_empty() {
                None
            } else if file_buffer_obj.len() >= FILE_THRESHOLD {
                Some(
                    file_buffer_obj
                        .drain(..FILE_THRESHOLD)
                        .collect::<Vec<String>>(),
                )
            } else {
                // Check if walker is finished before draining remaining files
                let walker_finished = walker_handle.is_finished();
                let dirs_empty = dir_buffer.lock().unwrap().is_empty();

                if walker_finished && dirs_empty {
                    // Walker is done, drain all remaining files
                    Some(file_buffer_obj.drain(..).collect::<Vec<String>>())
                } else {
                    // Still walking, wait for more files
                    None
                }
            }
        };

        if let Some(files) = file_list {
            search_worker.assign_files_to_threads(files);
        } else {
            // No files to process right now
            let walker_finished = walker_handle.is_finished();
            let dirs_empty = dir_buffer.lock().unwrap().is_empty();
            let files_remaining = file_buffer.lock().unwrap().len();

            if walker_finished && dirs_empty && files_remaining == 0 {
                break;
            }

            // Short sleep to avoid busy-waiting
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    walker_handle.join().unwrap();
    search_worker.wait_for_completion();
}

fn file_walk_worker(file_buffer: Arc<Mutex<Vec<String>>>, dir_buffer: Arc<Mutex<Vec<String>>>) {
    let thread_count = num_cpus::get().max(4);
    let mut thread_handles: Vec<std::thread::JoinHandle<()>> = Vec::new();

    loop {
        thread_handles.retain(|handle| !handle.is_finished());

        if thread_handles.len() >= thread_count {
            std::thread::sleep(Duration::from_millis(10));
            continue;
        }

        let path = {
            let mut buffer = dir_buffer.lock().unwrap();
            buffer.pop()
        };
        //println!("paths  {:?}", path);
        match path {
            Some(path) => {
                let file_buffer_clone = Arc::clone(&file_buffer);
                let dir_buffer_clone = Arc::clone(&dir_buffer);

                let handle = std::thread::spawn(move || {
                    get_all_files_in_directory(path, file_buffer_clone, dir_buffer_clone);
                });

                thread_handles.push(handle);
            }
            None => {
                if thread_handles.is_empty() {
                    break; // All done
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }

    for handle in thread_handles {
        handle.join().unwrap();
    }
}
