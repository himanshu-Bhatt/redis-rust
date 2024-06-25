use base64::Engine;
use clap::Parser;
use redis::Value;
use std::{
    collections::HashMap,
    // io::{Read, Write},
    // net::{TcpListener, TcpStream},
    sync::Arc,
};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use base64::{Engine as _, alphabet, engine::{self, general_purpose}};

/// This is my program
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Sets a custom port
    #[clap(long, default_value_t = 6379, value_parser = clap::value_parser!(u32))]
    port: u32,
    #[clap(long)]
    replicaof: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let replica = args.replicaof;

    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port))
        .await
        .unwrap();
    let map = Arc::new(Mutex::new(HashMap::new()));

    let mut isMaster = true;

    if let Some(replica) = replica {
        isMaster = false;
        let adrr = replica.split_at(9);
        // println!("{} : {}", adrr.0, adrr.1);
        // match TcpStream::connect(format!("{}:{}", adrr.0, adrr.1)) {
        match TcpStream::connect("127.0.0.1:6379").await {
            Ok(mut res) => {
                let n = res.write(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
                //    println!("{n} byres written");
                let mut buf = [0; 1024];
                if let Ok(n) = res.read(&mut buf).await {
                    // println!("{n} bytes read from stream");
                } else {
                    println!("Could not read from the stream");
                }

                // let res = String::from_utf8_lossy(&buf);
                // println!("{res}");
                if let Ok(val) = redis::parse_redis_value(&buf) {
                    // println!("{:?}", val);
                    match val {
                        Value::Status(_val) => {
                            assert_eq!(
                                _val, "PONG",
                                "Master must respond with Pong to ping of replica when connect"
                            );
                            println!("{_val}");
                        }

                        _ => {
                            eprintln!("Could not match value which master responded");
                        }
                    }
                }
                res.write(b"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n")
                    .await
                    .expect("could not respond with ok status for replconf command");
                buf = [0; 1024];
                res.read(&mut buf).await;
                if let Ok(val) = redis::parse_redis_value(&buf) {
                    match val {
                        Value::Okay => {
                            println!("{:?}", val);
                        }

                        _ => {
                            eprintln!(
                                "Master must respond with OK to replconf of replica when connect"
                            );
                        }
                    }
                }
                res.write(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
                    .await
                    .expect("could not respond with ok status for replconf command");
                buf = [0; 1024];
                res.read(&mut buf).await.expect("could not read from replica reply temp_buf");
                if let Ok(val) = redis::parse_redis_value(&buf) {
                    match val {
                        Value::Okay => {
                            println!("{:?}", val);
                        }

                        _ => {
                            eprintln!(
                                "Master must respond with OK to replconf of replica when connect"
                            );
                        }
                    }
                }
                res.write(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n").await;
                res.read(&mut buf).await;
                 if let Ok(val) = redis::parse_redis_value(&buf) {
                    match val {
                        Value::Status(_val) => {
                            println!("{:?}", _val);
                        }

                        _ => {
                            eprintln!(
                                "Master must respond with OK to replconf of replica when connect"
                            );
                        }
                    }
                }
                res.read(&mut buf).await;
                 if let Ok(val) = redis::parse_redis_value(&buf) {
                    match val {
                        Value::Data(_val) => {
                            println!("{:?}", _val);
                        }

                        _ => {
                            eprintln!(
                                "Master must respond with OK to replconf of replica when connect"
                            );
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("{}", e);
            }
        }
    }

    // let s=tokio::spawn(async move {
    //     println!("Hell0 line 30");
    while let Ok((stream, _)) = listener.accept().await {
        match stream {
            mut tcp_stream => {
                println!("accepted new connection");
                let _map = Arc::clone(&map);
                // println!("{:?}",tcp_stream);
                tokio::spawn(async move {
                    handle_conn(&mut tcp_stream, _map).await;
                });
            } // Err(e) => {
              //     println!("error: {}", e);
              // }
        }
    }
    let role;
    if isMaster {
        role = "master";
    } else {
        role = "slave";
    }
    println!("Closing connection, role = {role}");
}

async fn handle_conn(stream: &mut TcpStream, map: Arc<Mutex<HashMap<String, String>>>) {
    let mut temp_buf = [0; 512];
    let args = Args::parse();
    let replica = args.replicaof;

    loop {
        if let Ok(r_c) = stream.read(&mut temp_buf).await {
            if r_c == 0 {
                // println!("Heloooo");
                break;
            }
            if let Ok(v) = redis::parse_redis_value(&temp_buf) {
                let mut comms = Vec::new();
                match v {
                    Value::Bulk(ref items) => {
                        for item in items {
                            if let Value::Data(ref data) = item {
                                let st = String::from_utf8_lossy(data);
                                comms.push(st);
                            } else {
                                println!("Unexpected item in bulk response");
                            }
                        }
                    }
                    _ => println!("Unexpected response type"),
                }
                // println!("{:?}", comms);
                let command = &*comms[0].to_uppercase();

                match command {
                    "PING" => {
                        if let Ok(n) = stream.write(b"+PONG\r\n").await {
                            // println!("Server replied pong");
                        } else {
                            println!("Could not write to the stream");
                        }
                        // let mut buf = [0; 1024];
                        // if let Ok(n) = stream.read(&mut buf).await {
                        //     println!("{n} bytes read from stream");
                        // } else {
                        //     println!("Could not read from the stream");
                        // }
                        // let res = String::from_utf8_lossy(&buf);
                        // println!("{res}");
                    }
                    "ECHO" => {
                        let echo_this = comms
                            .iter()
                            .skip(1)
                            .map(|word| word.as_ref())
                            .collect::<Vec<&str>>()
                            .join(" ");
                        if let Err(e) = stream
                            .write(format!("${}\r\n{}\r\n", echo_this.len(), echo_this).as_bytes())
                            .await
                        {
                            eprintln!("An error occurred while writing to stream: {}", e);
                        }
                    }
                    "SET" => {
                        if comms.len() == 3 {
                            let k = comms[1].to_string();
                            let v = comms[2].to_string();
                            let mut map = map.lock().await;
                            map.insert(k, v);
                            if let Err(e) = stream.write(b"+OK\r\n").await {
                                eprintln!("An error occurred while writing to stream: {}", e);
                            }
                        } else if comms.len() == 5 && comms[3] == "px" {
                            let _tm = (&*comms[4]).to_string();
                            let tm: u64 = _tm.parse().unwrap();
                            let k = comms[1].to_string();
                            let v = comms[2].to_string();
                            {
                                let mut _map = map.lock().await;
                                _map.insert(k, v);
                            }
                            if let Err(e) = stream.write(b"+OK\r\n").await {
                                eprintln!("An error occurred while writing to stream: {}", e);
                            }

                            let _map = Arc::clone(&map);
                            let k = (&*comms[1]).to_string();
                            delete_key(k, _map, tm).await;
                        }
                    }
                    "GET" => {
                        let map = map.lock().await;
                        let k = comms[1].as_ref();
                        if let Some(res) = map.get(k) {
                            let v = format!("${}\r\n{}\r\n", res.len(), res.to_string());
                            if let Err(e) = stream.write(v.as_bytes()).await {
                                eprintln!("An error occurred while writing to stream: {}", e);
                            }
                        } else {
                            if let Err(e) = stream.write(b"-1\r\n").await {
                                eprintln!("An error occurred while writing to stream: {}", e);
                            }
                        }
                    }
                    "INFO" => {
                        let mut role = "master";
                        if let Some(_) = replica {
                            role = "slave";
                        }

                        let role = format!("role:{}", role);
                        let master_replid = format!(
                            "master_replid:{}",
                            "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
                        );
                        let master_repl_offset = format!("master_repl_offset:{}", 0);
                        // println!("{}",res.len());
                        // stream.write()
                        let len = role.len() + master_replid.len() + master_repl_offset.len();
                        let res = format!(
                            "${}\r\n{}\r\n{}\r\n{}\r\n",
                            len + 4,
                            role,
                            master_replid,
                            master_repl_offset
                        );
                        stream.write(res.as_bytes());
                    }
                    "REPLCONF" => {
                        // println!("helooo");
                        if let Ok(n) = stream.write(b"+OK\r\n").await {
                            // println!("{n} bytes writtern for replconf");
                        }
                    }
                    "PSYNC"=>{
                        stream.write(b"+FULLRESYNC ? 0\r\n").await;

                        let empty_rdb_file_content="524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
                        // let res=Engine::decode(empty_rdb_file_content);
                      let rdb_bin=  general_purpose::STANDARD.decode(empty_rdb_file_content).unwrap();
                      stream.write_all(format!("{}\r\n{:?}",rdb_bin.len(),rdb_bin).as_bytes()).await;


                    }
                    _ => {
                        println!("Unknown command");
                    }
                }
            } else {
                println!("Error while parsing");
            }
        } else {
            println!("Could not read from tcp connection");
        }
    }
}
async fn delete_key(k: String, map: Arc<Mutex<HashMap<String, String>>>, ts: u64) {
    sleep(Duration::from_millis(ts)).await;
    let k = &k[..];

    // Lock the mutex to access the HashMap
    let mut map = map.lock().await;

    // Delete the entry
    map.remove(k);
    println!("Entry for key '{}' deleted", k);
}
