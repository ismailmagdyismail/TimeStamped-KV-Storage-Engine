use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
};

const ADDR: &str = "127.0.0.1:8080";

async fn send_command(command: &str) -> std::io::Result<String> {
    let mut stream = TcpStream::connect(ADDR).await?;
    stream.write_all(command.as_bytes()).await?;

    // The server drops the stream after handling one command,
    // so reading until EOF gets us the whole reply (or nothing).
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    Ok(String::from_utf8_lossy(&response).into_owned())
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut lines = BufReader::new(tokio::io::stdin()).lines();

    while let Some(line) = lines.next_line().await? {
        let command = line.trim();
        if command.is_empty() {
            continue;
        }
        if command == "quit" || command == "exit" {
            break;
        }
        match send_command(command).await {
            Ok(response) if response.is_empty() => println!("(no response)"),
            Ok(response) => println!("{response}"),
            Err(e) => eprintln!("error: {e}"),
        }
    }
    Ok(())
}