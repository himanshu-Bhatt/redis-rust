// Uncomment this block to pass the first stage
// use resp::Value;
// use resp::Value::Bulk;
use std::{
    collections::HashMap,
    env,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
    thread,
};

use redis::ToRedisArgs;
use redis::Value;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    // println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let mut map = Arc::new(Mutex::new(HashMap::new()));
    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");
                let _map = Arc::clone(&map);
                thread::spawn(move || {
                    handle_conn(&mut _stream, _map);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
fn handle_conn(stream: &mut TcpStream, map: Arc<Mutex<HashMap<String, String>>>) {
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
                                // println!("{}", st);
                                comms.push(st);
                            } else {
                                println!("Unexpected item in bulk response");
                            }
                        }
                    }
                    _ => println!("Unexpected response type"),
                }

                if comms[0] == "PING" {
                    stream.write(b"+PONG\r\n");
                } else if comms[0] == "ECHO" {
                    let echo_this = comms
                        .iter()
                        .skip(1)
                        .map(|word| word.as_ref())
                        .collect::<Vec<&str>>()
                        .join(" ");
                    let resp_en = to_resp_string(echo_this);
                    stream.write(resp_en.as_bytes());
                } else if comms[0] == "SET" {
                    let k = comms[1].to_string();
                    let v = comms[2].to_string();
                    let mut map = map.lock().unwrap();
                    map.insert(k, v);
                    stream.write(b"+OK\r\n");
                } else if comms[0] == "GET" {
                     let map = map.lock().unwrap();
                    let k = comms[1].as_ref();
                    if let Some(res) = map.get(k) {
                        let v = format!("${}\r\n{}\r\n", res.len(), res.to_string());
                        stream.write(v.as_bytes());
                    } else {
                        stream.write(b"-1\r\n");
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
fn to_resp_string<T: ToRedisArgs>(value: T) -> String {
    let args = value.to_redis_args();
    println!("{:?}", args);
    let mut resp = String::new();
    for arg in args {
        resp.push_str(&format!("${}\r\n", arg.len()));
        resp.push_str(&String::from_utf8_lossy(&arg));
        resp.push_str("\r\n");
    }
    resp
}
