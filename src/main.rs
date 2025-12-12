mod DirWalker;
mod SearchWorker;
use SearchWorker::SearchWorker as SW;
mod PrintWorker;
use std::sync::mpsc;

#[tokio::main]
async fn main() {
    let args = std::env::args().collect::<Vec<String>>();
    let pattern = args[1].to_string();
    println!("Pattern: {}", pattern);

    let (printer_tx, printer_rx) = mpsc::channel::<PrintWorker::Match>();

    let printer_handle = tokio::spawn(async move {
        PrintWorker::printer_thread(printer_rx).await;
    });

    let mut searcher = SW::new(num_cpus::get() * 2);
    searcher.initialize_thread_and_channel(pattern, printer_tx);
    DirWalker::dir_walk_worker(searcher);
    printer_handle.await.unwrap();
}
