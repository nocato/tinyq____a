// Based on https://github.com/hyperium/hyper/blob/master/examples/gateway.rs

use http_body_util::BodyExt;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::Request;
use hyper::Response;
use hyper::{server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let in_addr: SocketAddr = ([0, 0, 0, 0], 3000).into();
    let out_addr: SocketAddr = ([127, 0, 0, 1], 9200).into();

    let listener = TcpListener::bind(in_addr).await?;

    println!("Listening on http://{}", in_addr);
    println!("Proxying to http://{}", out_addr);

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);

        let service = service_fn(move |req: hyper::Request<Incoming>| async move {
            println!("-------------------------");
            let (parts, body) = req.into_parts();
            let body = body.collect().await.unwrap().to_bytes();
            let req = Request::from_parts(parts, Full::new(body));
            println!("Got request: {:#?}", req);

            let client_stream = TcpStream::connect(out_addr).await.unwrap();
            let io = TokioIo::new(client_stream);

            let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;
            tokio::task::spawn(async move {
                if let Err(err) = conn.await {
                    println!("Connection failed: {:?}", err);
                }
            });

            let res = sender.send_request(req).await;

            let (parts, body) = res.unwrap().into_parts();
            let body = body.collect().await.unwrap().to_bytes();
            println!("\nSending back: {:#?}\nwith headers: {:#?}", body, parts);
            println!("-------------------------");
            Ok::<Response<http_body_util::Full<hyper::body::Bytes>>, hyper::Error>(
                Response::from_parts(parts, Full::new(body)),
            )
        });

        tokio::task::spawn(async move {
            if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                println!("Failed to serve the connection: {:?}", err);
            }
        });
    }
}
