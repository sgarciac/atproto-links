mod consumer;
mod storage;

use storage::Storage;

fn main() {
    let _s = Storage::new();
    println!("Hello, world!");
}
