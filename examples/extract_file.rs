//! An example of extracting a file in an archive.
//!
//! Takes a tarball on standard input, looks for an entry with a listed file
//! name as the first argument provided, and then prints the contents of that
//! file to stdout.

extern crate tar;

use std::env::args_os;
use std::path::Path;

use futures::StreamExt;
use tokio::io::{copy, stdin, stdout};

use tar::Archive;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let first_arg = args_os().skip(1).next().unwrap();
    let filename = Path::new(&first_arg);
    let mut ar = Archive::new(stdin());
    let mut entries = ar.entries().unwrap();
    while let Some(file) = entries.next().await {
        let mut f = file.unwrap();
        if f.path().unwrap() == filename {
            copy(&mut f, &mut stdout()).await.unwrap();
        }
    }
}
