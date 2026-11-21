use std::io::Write;

use tokio::{
    self,
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
};

#[tokio::main]
async fn main() {
    let addr = "127.0.0.1:8080".to_string();
    let mut stream = TcpStream::connect(&addr).await.unwrap();
    println!("connected to {}", addr);
    println!("commands: get <key> | put <key> <value> | commit | abort");

    let mut stdin = BufReader::new(tokio::io::stdin()).lines();
    let mut buffer = [0u8; 1024];

    // every command gets exactly one reply from the server, so send a command,
    // wait for its reply, log it and loop again
    loop {
        print!("> ");
        std::io::stdout().flush().unwrap();
        let line = match stdin.next_line().await.unwrap() {
            Some(line) => line,
            None => break,
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        stream.write_all(line.as_bytes()).await.unwrap();

        let bytes = stream.read(&mut buffer).await.unwrap();
        if bytes == 0 {
            println!("server closed the connection");
            break;
        }
        println!("{}", String::from_utf8_lossy(&buffer[0..bytes]));
    }
}
