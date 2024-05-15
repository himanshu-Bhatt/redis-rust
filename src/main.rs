// Uncomment this block to pass the first stage
use std::{
    io::{Read, Write},
    net::TcpListener,
    thread,
};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    // println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");
                thread::spawn(move || {
                    let mut buf = [0; 512];
                    loop {
                        let r_c = _stream.read(&mut buf).unwrap();
                        if r_c == 0 {
                            break;
                        }
                       
                        _stream
                            .write(b"+PONG\r\n")
                            .expect("failed to write to stream");
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
