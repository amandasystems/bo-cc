
use std::error::Error;
use std::fs;
use std::io::prelude::*;
use std::io::BufWriter;
use std::sync::mpsc;
use std::thread;

const WRITE_BACKLOG: usize = 32;
pub type BoxDynError = Box<dyn Error + Sync + Send + 'static>;

pub struct AnalysisWriter {
    inbox: Option<mpsc::SyncSender<String>>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Drop for AnalysisWriter {
    fn drop(&mut self) {
        drop(self.inbox.take());
        if let Some(thread) = self.thread.take() {
            thread.join().expect("Worker error!");
        }
    }
}

impl AnalysisWriter {
    fn process_inbox(incoming: mpsc::Receiver<String>) {
        fs::create_dir_all("forms.d").expect("Unable to create forms.d directory!");
        let mut nr_seen = 0;
        let mut index_bw =
            BufWriter::new(fs::File::create("forms.d/index").expect("Unable to open index file"));
        // FIXME do compression too!
        while let Ok(summary) = incoming.recv() {
            let archive_fn = format!("forms.d/{}", nr_seen);
            let mut archive_writer = BufWriter::new(fs::File::create(&archive_fn).expect(
                &format!("Unable to open archive dump file: {}", &archive_fn),
            ));

            writeln!(archive_writer, "{}", archive_fn).expect("Unable to write!");
            writeln!(index_bw, "{}", summary).expect("Unable to write WARC URL to index!");
            nr_seen += 1;
        }
    }
    pub fn write(&mut self, summary: String) -> Result<(), BoxDynError> {
        self.inbox.as_ref().unwrap().send(summary)?;
        Ok(())
    }
    pub fn new() -> Self {
        let (send, recieve) = mpsc::sync_channel(WRITE_BACKLOG);
        Self {
            inbox: Some(send),
            thread: Some(thread::spawn(move || Self::process_inbox(recieve))),
        }
    }
}

fn main() -> Result<(), BoxDynError> {
    let mut writer = AnalysisWriter::new();
    for i in 0..10 {
        writer.write(format!("saw: {}", i))?;
    }

    Ok(())
}
