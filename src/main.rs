use redis::Value;
use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
};
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let map = Arc::new(Mutex::new(HashMap::new()));
    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");
                let _map = Arc::clone(&map);
                tokio::spawn(async move {
                    handle_conn(&mut _stream, _map).await;
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
async fn handle_conn(stream: &mut TcpStream, map: Arc<Mutex<HashMap<String, String>>>) {
    let mut temp_buf = [0; 512];

    loop {
        if let Ok(r_c) = stream.read(&mut temp_buf) {
            if r_c == 0 {
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

                match &*comms[0] {
                    "PING" => {
                        if let Err(e) = stream.write(b"+PONG\r\n") {
                            eprintln!("An error occurred while writing to stream: {}", e);
                        }
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
                        {
                            eprintln!("An error occurred while writing to stream: {}", e);
                        }
                    }
                    "SET" => {
                        println!("Hello");
                        if comms.len() == 3 {
                            let k = comms[1].to_string();
                            let v = comms[2].to_string();
                            let mut map = map.lock().unwrap();
                            map.insert(k, v);
                            if let Err(e) = stream.write(b"+OK\r\n") {
                                eprintln!("An error occurred while writing to stream: {}", e);
                            }
                        } else if comms.len() == 5 && comms[3] == "px" {
                            let _tm = (&*comms[4]).to_string();
                            let tm: u64 = _tm.parse().unwrap();
                            let k = comms[1].to_string();
                            let v = comms[2].to_string();
                            {
                                let mut _map = map.lock().unwrap();
                                _map.insert(k, v);
                            }
                            if let Err(e) = stream.write(b"+OK\r\n") {
                                eprintln!("An error occurred while writing to stream: {}", e);
                            }

                            let _map = Arc::clone(&map);
                            let k = (&*comms[1]).to_string();
                            delete_key(k, _map, tm).await;
                        }
                    }
                    "GET" => {
                        let map = map.lock().unwrap();
                        let k = comms[1].as_ref();
                        if let Some(res) = map.get(k) {
                            let v = format!("${}\r\n{}\r\n", res.len(), res.to_string());
                            if let Err(e) = stream.write(v.as_bytes()) {
                                eprintln!("An error occurred while writing to stream: {}", e);
                            }
                        } else {
                            if let Err(e) = stream.write(b"-1\r\n") {
                                eprintln!("An error occurred while writing to stream: {}", e);
                            }
                        }
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
    println!("Hello");
    sleep(Duration::from_millis(ts)).await;
    let k = &k[..];

    // Lock the mutex to access the HashMap
    let mut map = map.lock().unwrap();

    // Delete the entry
    map.remove(k);
    println!("Entry for key '{}' deleted", k);
}
