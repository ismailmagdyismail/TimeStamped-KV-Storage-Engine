use std::sync::Arc;

use TimeStamped_KV_Storage_Engine::storage::{
    mvcc_storage_engine::MVCCStorageEngine, occ_comitter::OccCommitResult, transaction::Transaction,
};
use tokio::{
    self,
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

async fn handle_command<'a>(
    stream: &mut TcpStream,
    buffer: &[u8],
    mut transaction: Transaction<'a>,
) -> Option<Transaction<'a>> {
    let splitted_command: Vec<&[u8]> = buffer.split(|byte| *byte == b' ').collect();
    if splitted_command.len() < 1 {
        stream
            .write_all(
                "invalid command format, unable to parse it"
                    .to_string()
                    .as_bytes(),
            )
            .await
            .unwrap();
        return Some(transaction);
    }
    let command_name = splitted_command[0];
    let command_name = command_name.to_ascii_lowercase();
    let command_name = command_name.as_slice();
    match command_name {
        b"get" => {
            if splitted_command.len() != 2 {
                stream
                    .write_all(
                        "invalid Get command, unable to parse it"
                            .to_string()
                            .as_bytes(),
                    )
                    .await
                    .unwrap();
                return Some(transaction);
            }
            let key = String::from_utf8(splitted_command[1].to_owned()).unwrap();
            match transaction.get(&key) {
                Some((ts, val)) => {
                    stream
                        .write_all(format!("Value = {}, with ts {}", val, ts).as_bytes())
                        .await
                        .unwrap();
                }
                None => {
                    stream
                        .write_all(format!("key {} doesn't exist", key).as_bytes())
                        .await
                        .unwrap();
                }
            }
        }
        b"put" => {
            if splitted_command.len() != 3 {
                stream
                    .write_all(
                        "invalid Put Command, unable to parse it"
                            .to_string()
                            .as_bytes(),
                    )
                    .await
                    .unwrap();
                return Some(transaction);
            }
            let key = String::from_utf8(splitted_command[1].to_owned()).unwrap();
            let value = String::from_utf8(splitted_command[2].to_owned()).unwrap();
            transaction.put(key, value);
        }
        b"commit" => {
            if splitted_command.len() != 1 {
                stream
                    .write_all(
                        "invalid Commit command, unable to parse it"
                            .to_string()
                            .as_bytes(),
                    )
                    .await
                    .unwrap();
                return Some(transaction);
            }
            let begin_ts = transaction.begin_timestamp();
            match transaction.commit() {
                OccCommitResult::Comitted => {
                    stream
                        .write_all(
                            format!(
                                "transaction with begin ts {}, committed successfully",
                                begin_ts
                            )
                            .as_bytes(),
                        )
                        .await
                        .unwrap();
                }
                OccCommitResult::Aborted(abort_status) => {
                    let message = format!(
                        "transaction with begin ts {}, Aborted!! due to conflict with type {:?}, it had commit ts {}, store had ts {}",
                        begin_ts,
                        abort_status.confilict_type,
                        abort_status.ts_to_commit,
                        abort_status.stored_ts
                    );
                    stream.write_all(message.as_bytes()).await.unwrap();
                }
            }
            // commit consumed the transaction
            return None;
        }
        b"abort" => {
            if splitted_command.len() != 1 {
                stream
                    .write_all(
                        "invalid abort Comand, unable to parse it"
                            .to_string()
                            .as_bytes(),
                    )
                    .await
                    .unwrap();
                return Some(transaction);
            }
            transaction.abort();
            // abort consumed the transaction
            return None;
        }
        _ => {
            stream
                .write_all("unknown command".to_string().as_bytes())
                .await
                .unwrap();
        }
    }
    Some(transaction)
}

pub async fn handle_connection<'a>(
    mut stream: TcpStream,
    storage: &'a MVCCStorageEngine,
    mut transaction: Transaction<'a>,
) {
    let mut buffer = [b'0'; 1024];
    loop {
        let result = stream.read(&mut buffer).await;
        match result {
            // client closed the connection
            Ok(0) => break,
            Ok(bytes) => {
                transaction =
                    match handle_command(&mut stream, &buffer[0..bytes], transaction).await {
                        Some(transaction) => transaction,
                        // previous transaction was committed/aborted, start a new one
                        None => storage.begin_transaction(),
                    };
            }
            Err(err) => {
                eprint!("error when recieving bytes {}", err.to_string());
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let listner = TcpListener::bind("0.0.0.0:8080").await.unwrap();
    println!("listening on address {}", listner.local_addr().unwrap());
    let storage = Arc::new(MVCCStorageEngine::new());
    loop {
        let (stream, _) = listner.accept().await.unwrap();
        let clone = Arc::clone(&storage);
        tokio::spawn(async move {
            let tx = clone.begin_transaction();
            handle_connection(stream, &clone, tx).await;
        });
    }
}
