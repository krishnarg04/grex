use crate::PrintWorker;
use std;
use std::io::BufReader;
use std::io::Read;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};

use wide::*;

pub struct SearchWorker {
    channel_vec: Vec<Sender<Vec<String>>>,
    max_threads: usize,
    thread_holder: Vec<std::thread::JoinHandle<()>>,
    printer_tx: Option<Sender<PrintWorker::Match>>,
}

impl SearchWorker {
    pub fn new(max_threads: usize) -> Self {
        SearchWorker {
            channel_vec: Vec::new(),
            max_threads,
            thread_holder: Vec::new(),
            printer_tx: None,
        }
    }

    pub fn wait_for_completion(self) {
        drop(self.channel_vec);
        for handle in self.thread_holder {
            handle.join().unwrap();
        }
    }

    pub fn initialize_thread_and_channel(
        &mut self,
        pattern: String,
        printer_tx: Sender<PrintWorker::Match>,
    ) {
        self.printer_tx = Some(printer_tx.clone());
        for i in 0..self.max_threads {
            let (sender, receiver): (Sender<Vec<String>>, Receiver<Vec<String>>) = mpsc::channel();
            let cloned_pattern = pattern.clone();
            let cloned_printer_tx = printer_tx.clone();
            self.thread_holder.push(std::thread::spawn(move || {
                search_thread_work(receiver, cloned_pattern, cloned_printer_tx);
            }));
            self.channel_vec.push(sender);
        }
    }

    pub fn assign_files_to_threads(&mut self, files: Vec<String>) {
        let chunk_size = (files.len() / self.max_threads).max(1);
        for (i, chunk) in files.chunks(chunk_size).enumerate() {
            let thread_index = i % self.max_threads;
            let sender = &self.channel_vec[thread_index];
            sender.send(chunk.to_vec()).expect("failed to send batch");
        }
    }
}

fn search_thread_work(
    receiver: Receiver<Vec<String>>,
    pattern: String,
    printer_tx: Sender<PrintWorker::Match>,
) {
    let pat = pattern.as_bytes();
    let pat_len = pat.len();

    if pat_len == 0 {
        return;
    }

    let first = u8x32::splat(pat[0]);
    let last = if pat_len > 1 {
        u8x32::splat(pat[pat_len - 1])
    } else {
        first
    };

    let new_line_simd = u8x32::splat(b'\n');

    let mut buffer = [0u8; 8192];
    let mut overlap_buffer = Vec::new();
    while let Ok(file_paths) = receiver.recv() {
        for file_path in file_paths {
            let file_name = file_path.clone();
            let mut new_line_count = 0;
            let mut file_obj = match std::fs::File::open(&file_path) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("Failed to open {}: {}", file_path, e);
                    continue;
                }
            };
            let mut buf_reader = BufReader::new(file_obj);

            let mut byte_offset: usize = 0;
            overlap_buffer.clear();
            loop {
                let bytes_read = match buf_reader.read(&mut buffer[..]) {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(e) => {
                        eprintln!("Failed to read {}: {}", file_path, e);
                        break;
                    }
                };
                let mut search_buffer = Vec::with_capacity(overlap_buffer.len() + bytes_read);
                search_buffer.extend_from_slice(&overlap_buffer);
                search_buffer.extend_from_slice(&buffer[..bytes_read]);

                let search_len = search_buffer.len();
                let overlap_len = overlap_buffer.len();

                let mut i = 0;

                while i + 32 <= search_len {
                    let mut chunk_arr = [0u8; 32];
                    chunk_arr.copy_from_slice(&search_buffer[i..i + 32]);
                    let chunk = u8x32::from(chunk_arr);

                    let eq_mask = chunk.cmp_eq(first);
                    let new_line_mask = chunk.cmp_eq(new_line_simd);
                    let mask_arr: [u8; 32] = eq_mask.into();
                    let new_line_arr: [u8; 32] = new_line_mask.into();
                    new_line_count += new_line_arr.iter().filter(|&&x| x != 0).count();

                    for j in 0..32 {
                        if mask_arr[j] != 0 {
                            let start = i + j;
                            let end = start + pat_len;
                            if end <= search_len
                                && (pat_len == 1 || search_buffer[end - 1] == pat[pat_len - 1])
                            {
                                if &search_buffer[start..end] == pat {
                                    let actual_offset = if start < overlap_len {
                                        byte_offset + start - overlap_len
                                    } else {
                                        byte_offset + start - overlap_len
                                    };
                                    let line_start = search_buffer[..start]
                                        .iter()
                                        .rposition(|&b| b == b'\n')
                                        .map_or(0, |p| p + 1);
                                    let line_end = search_buffer[start..]
                                        .iter()
                                        .position(|&b| b == b'\n')
                                        .map_or(search_len, |p| start + p);
                                    let match_data = PrintWorker::Match {
                                        file_name: file_name.clone(),
                                        line_number: new_line_count,
                                        line_content: search_buffer[line_start..line_end].to_vec(),
                                        pattern: pattern.clone(),
                                    };

                                    if printer_tx.send(match_data).is_err() {
                                        break;
                                    }
                                    //print_match(&file_name, actual_offset, new_line_count, &search_buffer, start, &pattern);
                                }
                            }
                        }
                    }

                    i += 32;
                }

                while i < search_len {
                    let end = i + pat_len;
                    if end <= search_len {
                        if search_buffer[i] == pat[0]
                            && (pat_len == 1 || search_buffer[end - 1] == pat[pat_len - 1])
                        {
                            if &search_buffer[i..end] == pat {
                                let actual_offset = byte_offset + i - overlap_len;

                                print_match(
                                    &file_name,
                                    actual_offset,
                                    new_line_count,
                                    &search_buffer,
                                    i,
                                    &pattern,
                                );
                            }
                        }
                    }
                    i += 1;
                }

                overlap_buffer.clear();
                if bytes_read > 0 && pat_len > 1 {
                    let overlap_start = bytes_read.saturating_sub(pat_len - 1);
                    overlap_buffer.extend_from_slice(&buffer[overlap_start..bytes_read]);
                }

                byte_offset += bytes_read;
            }
        }
    }
}

fn print_match(
    file_name: &str,
    byte_pos: usize,
    line_number: usize,
    buffer: &[u8],
    match_pos: usize,
    pattern: &str,
) {
    let line_content = extract_line(buffer, match_pos);
    let line_with_color = highlight_pattern(&line_content, pattern);

    println!(
        "\x1b[35m{}\x1b[0m:\x1b[33m{}\x1b[0m:\x1b[36m{}\x1b[0m\n{}",
        file_name,
        byte_pos,
        line_number + 1,
        line_with_color
    );
}

fn extract_line(buffer: &[u8], pos: usize) -> String {
    let start = buffer[..pos]
        .iter()
        .rposition(|&b| b == b'\n')
        .map(|p| p + 1)
        .unwrap_or(0);
    let end = buffer[pos..]
        .iter()
        .position(|&b| b == b'\n')
        .map(|p| pos + p)
        .unwrap_or(buffer.len());

    String::from_utf8_lossy(&buffer[start..end]).to_string()
}

fn highlight_pattern(line: &str, pattern: &str) -> String {
    line.replace(pattern, &format!("\x1b[1;31m{}\x1b[0m", pattern))
}
