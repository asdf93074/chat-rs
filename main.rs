use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::thread;

type Result<T> = std::result::Result<T, ()>;

struct Message {
    addr: String,
    message: MessageType,
}

enum MessageType {
    ClientConnected(TcpStream),
    ClientDisconnected,
    Text(String),
}

fn server(messages: Receiver<Message>) -> Result<()> {
    let mut clients = HashMap::<String, TcpStream>::new();
    loop {
        let msg = messages.recv().unwrap();
        let addr = msg.addr;
        match msg.message {
            MessageType::ClientConnected(mut stream) => {
                println!("Client connected: {addr}");
                clients.insert(addr.clone(), stream.try_clone().unwrap());
                let _ = writeln!(stream, "Welcome!").map_err(|err| {
                    eprintln!("ERR: failed to write to {addr} {err}");
                });
            }
            MessageType::ClientDisconnected => {
                let stream = clients.remove(&addr).unwrap();
                let _ = stream.shutdown(Shutdown::Both);
                println!("Client disconnected: {addr}");
            }
            MessageType::Text(txt) => {
                let out = txt.trim_end_matches(&['\r', '\n'][..]).to_string();
                println!("Message from {addr}: {out}");
                for c in clients.values_mut() {
                    if c.peer_addr().unwrap().to_string() == addr {
                        continue;
                    }
                    let _ = writeln!(c, "{}", out);
                }
            }
        }
    }
}

fn handle_conn(mut stream: TcpStream, messages: Sender<Message>) -> Result<()> {
    let addr = stream.peer_addr().unwrap();
    let mut buf = vec![0u8; 512];

    let sender = stream.try_clone().unwrap();
    let _ = messages.send(Message {
        addr: addr.to_string(),
        message: MessageType::ClientConnected(sender),
    });

    loop {
        let n = stream.read(&mut buf).map_err(|err| {
            eprintln!("ERR: could not read msg from client {addr} {err}");
        })?;
        if n > 0 {
            if let Ok(msg) = String::from_utf8(buf[..n].to_vec()) {
                let _ = messages.send(Message {
                    addr: addr.to_string(),
                    message: MessageType::Text(msg),
                });
            }
        } else {
            let _ = messages.send(Message {
                addr: addr.to_string(),
                message: MessageType::ClientDisconnected,
            });
            break;
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    let address = "0.0.0.0:9001";
    let listener = TcpListener::bind(address).map_err(|err| {
        eprintln!("ERR: failed to bind to address {address} {err}");
    })?;
    println!("Listening on {address}");

    let (messages_sender, messages_receiver) = mpsc::channel();
    let _ = thread::spawn(|| server(messages_receiver));

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let sender = messages_sender.clone();
                thread::spawn(|| handle_conn(stream, sender));
            }
            Err(e) => {
                eprintln!("ERR: failed to accept connection {e}");
            }
        }
    }

    Ok(())
}
