use std::sync::mpsc::Receiver;
pub struct Match {
    pub file_name: String,
    pub line_number: usize,
    pub line_content: Vec<u8>,
    pub pattern: String,
}

pub fn printer_thread(receiver: Receiver<Match>) {
    while let Ok(match_data) = receiver.recv() {
        let line_with_color = highlight_pattern(
            &String::from_utf8_lossy(&match_data.line_content),
            &match_data.pattern,
        );

        println!(
            "\x1b[35m{}\x1b[0m \n\x1b[36m{}\x1b[0m:{}",
            match_data.file_name,
            match_data.line_number + 1,
            line_with_color
        );
    }
}

fn highlight_pattern(line: &str, pattern: &str) -> String {
    line.replace(pattern, &format!("\x1b[1;31m{}\x1b[0m", pattern))
}
