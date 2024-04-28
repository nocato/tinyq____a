// Based on https://github.com/hyperium/hyper/blob/master/examples/gateway.rs

use http::Response;
use http_body_util::BodyExt;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::body::Incoming;
use hyper::Request;
use hyper::{server::conn::http1, service::service_fn};
use hyper_util::rt::TokioIo;
use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::json;
use serde_json::Value;
use serde_json::Value::{Array, Object};
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};

/// Convert a Request with incoming data to a Request with the data streamed in and ready to go
async fn request_with_streamed_body(
    req: Request<Incoming>,
) -> Result<Request<Bytes>, hyper::Error> {
    let (parts, body) = req.into_parts();
    let body = body.collect().await?.to_bytes();
    Ok(Request::from_parts(parts, body))
}

/// Convert a Request<Bytes> to Request<Full<Bytes>>
fn request_with_full_body(req: &Request<Bytes>) -> Result<Request<Full<Bytes>>, hyper::Error> {
    let (parts, body) = req.clone().into_parts();
    let body = Full::new(body);
    Ok(Request::from_parts(parts, body))
}

/// Convert a Response with incoming data to a Response with the data streamed in and ready to go
async fn response_with_streamed_body(
    res: Response<Incoming>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let (parts, body) = res.into_parts();
    let body = body.collect().await?.to_bytes();
    Ok(Response::from_parts(parts, Full::new(body)))
}

/// We don't know how to handle this request, so let's forward it to OpenSearch instead
async fn forward_request_to_opensearch(
    out_addr: &SocketAddr,
    req: &Request<Bytes>,
) -> Result<Response<http_body_util::Full<hyper::body::Bytes>>, hyper::Error> {
    let client_stream = TcpStream::connect(out_addr).await.unwrap();
    let io = TokioIo::new(client_stream);

    let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;
    tokio::task::spawn(async move {
        if let Err(err) = conn.await {
            println!("Connection failed: {:?}", err);
        }
    });

    let res = sender.send_request(request_with_full_body(req)?).await?;
    let res = response_with_streamed_body(res).await?;
    Ok(res)
}

// Parsing _search request:

struct ParsedSearchRequest {
    // It's empty for now, but in the future
    // this will contain the details of the search request,
    // for example applied filters.
}

fn parse_options(
    options: &Vec<Vec<&str>>,
    _parsed: &mut ParsedSearchRequest,
) -> Result<(), String> {
    for option in options {
        match option.first() {
            // We will ignore those options for now, they don't really affect the end results
            Some(&"ignore_unavailable") => {}
            Some(&"track_total_hits") => {}
            Some(&"timeout") => {}
            Some(&"preference") => {}

            Some(option) => return Err(format!("unsupported URL option {}", option)),
            None => {}
        }
    }
    Ok(())
}

fn parse_body(body: &Value, _parsed: &mut ParsedSearchRequest) -> Result<(), String> {
    let Object(map) = body else {
        return Err(format!(
            "expected JSON object in search body but got {}",
            body
        ));
    };
    for (key, value) in map {
        match key.as_str() {
            "_source" => {
                let mut expected_source = serde_json::Map::new();
                expected_source.insert("excludes".to_string(), Array(Vec::new()));
                if *value != Object(expected_source) {
                    return Err(format!("unimplemented _source value: {}", value));
                }
            }
            "docvalue_fields" => {
                if *value != Array(Vec::new()) {
                    return Err(format!("unimplemented docvalue_fields value: {}", value));
                }
            }
            "highlight" => {
                // Just ignore it completely for now, this doesn't seem to affect the end results contents
            }
            "query" => {
                // Expect query to be:
                //     "bool": Object {
                //        "filter": Array [
                //            Object {
                //                "match_all": Object {},
                //            },
                //        ],
                //        "must": Array [],
                //        "must_not": Array [],
                //        "should": Array [],
                //    }

                let Object(query) = value else {
                    return Err(format!(
                        "unimplemented query value - expected JSON object: {}",
                        value
                    ));
                };
                let query_keys: Vec<_> = query.keys().collect();
                if query_keys != vec!["bool"] {
                    return Err(format!(
                        "unimplemented query value - many query keys: {}",
                        value
                    ));
                }

                let query = query.get("bool").unwrap();
                let Object(query) = query else {
                    return Err(format!("unimplemented query value: {}", value));
                };

                let Some(Array(filter)) = query.get("filter") else {
                    return Err(format!(
                        "unimplemented query value - expected 'filter': {}",
                        value
                    ));
                };
                if filter.len() != 1 {
                    return Err(format!(
                        "unimplemented query value - expected one element 'filter': {}",
                        value
                    ));
                }
                let Some(Object(filter)) = filter.first() else {
                    return Err(format!("unimplemented query value - expected first element of 'filter' to be JSON object: {}", value));
                };
                let filter_keys: Vec<_> = filter.keys().collect();
                if filter_keys != vec!["match_all"] {
                    return Err(format!(
                        "unimplemented query value - unexpected filter: {}",
                        value
                    ));
                }
                let Some(Object(match_all_filter)) = filter.get("match_all") else {
                    return Err(format!(
                        "unimplemented query value - unexpected match_all filter: {}",
                        value
                    ));
                };
                if !match_all_filter.keys().collect::<Vec<_>>().is_empty() {
                    return Err(format!(
                        "unimplemented query value - non-empty match_all filter: {}",
                        value
                    ));
                }

                let Some(Array(must)) = query.get("must") else {
                    return Err(format!(
                        "unimplemented query value - expected 'must': {}",
                        value
                    ));
                };
                if !must.is_empty() {
                    return Err(format!(
                        "unimplemented query value - expected empty 'must': {}",
                        value
                    ));
                }

                let Some(Array(must_not)) = query.get("must_not") else {
                    return Err(format!(
                        "unimplemented query value - expected 'must_not': {}",
                        value
                    ));
                };
                if !must_not.is_empty() {
                    return Err(format!(
                        "unimplemented query value - expected empty 'must_not': {}",
                        value
                    ));
                }

                let Some(Array(should)) = query.get("should") else {
                    return Err(format!(
                        "unimplemented query value - expected 'should': {}",
                        value
                    ));
                };
                if !should.is_empty() {
                    return Err(format!(
                        "unimplemented query value - expected empty 'should': {}",
                        value
                    ));
                }
            }
            "script_fields" => {
                if *value != Object(serde_json::Map::new()) {
                    return Err(format!("unimplemented script_fields value: {}", value));
                }
            }
            "size" => {
                // Let's ignore it for now, always returning everything...
            }
            "sort" => {
                // Let's ignore it for now, returning in any order
            }
            "stored_fields" => {
                if *value != Array(vec![Value::String("*".to_string())]) {
                    return Err(format!("unimplemented stored_fields value: {}", value));
                }
            }
            "version" => {
                // Let's ignore it for now, returning always with version
            }
            _ => {
                return Err(format!("unimplemented search parameter: {}", key));
            }
        }
    }
    Ok(())
}

/// Try to handle request to _search endpoint. If we can handle it,
/// return a hardcoded list of results, else return an error.
async fn handle_search_request(
    req: &Request<Bytes>,
) -> Result<Response<http_body_util::Full<hyper::body::Bytes>>, String> {
    let mut parsed_request: ParsedSearchRequest = ParsedSearchRequest {};

    let options: Vec<Vec<_>> = req
        .uri()
        .query()
        .unwrap_or("")
        .split('&')
        .map(|elem| elem.split('=').collect())
        .collect();
    let body: Value = serde_json::from_slice(req.body())
        .map_err(|_| "error parsing JSON body of search request")?;

    parse_options(&options, &mut parsed_request)?;
    parse_body(&body, &mut parsed_request)?;

    let result = json!({
        "took": 0,
        "timed_out": false,
        "_shards": {
            "total": 1,
            "successful": 1,
            "skipped": 0,
            "failed": 0,
        },
        "hits": {
            "total": {
                "value": 3,
                "relation": "eq",
            },
            "max_score": 0.0,
            "hits": [
                {
                    "_index":"my-first-index",
                    "_id":"1",
                    "_version":5,
                    "_score":0.0,
                    "_source":
                        {"Description": "Through the fire, to the limit, to the wall, For a chance to be with you, I'd gladly risk it all."}
                },
                {
                    "_index":"my-first-index",
                    "_id":"2",
                    "_version":1,
                    "_score":0.0,
                    "_source":
                        {"Description": "You tell me you're gonna play it smart, We're through before we start, But I believe that we've only just begun"}
                },
                {
                    "_index":"my-first-index",
                    "_id":"3",
                    "_version":1,
                    "_score":0.0,
                    "_source":
                        {"Description": "When it's this good, there's no saying no"}
                }
            ]
        }
    });

    let mut response = Response::builder();
    response = response.status(200);
    if let Some(x_opaque_id) = req.headers().get("x-opaque-id") {
        response = response.header("x-opaque-id", x_opaque_id);
    }
    response = response.header("Content-Type", "application/json; charset=UTF-8");
    response
        .body(Full::new(Bytes::from(result.to_string())))
        .map_err(|_| "error serializing response".to_string())
}

/// Handle incoming request, either by emulating _search endpoint
/// or sending the request to OpenSearch nodes as a fallback.
async fn handle_request(
    out_addr: &SocketAddr,
    req: Request<Bytes>,
) -> Result<Response<http_body_util::Full<hyper::body::Bytes>>, hyper::Error> {
    static SEARCH_ENDPOINT: Lazy<Regex> = Lazy::new(|| Regex::new(r"^/([^/]*)/_search$").unwrap());

    if SEARCH_ENDPOINT.is_match(req.uri().path()) {
        let res = handle_search_request(&req).await;
        match res {
            Ok(res) => {
                return Ok(res);
            }
            Err(err) => {
                println!("Error handling search request: {}", err);
            }
        }
    }

    forward_request_to_opensearch(out_addr, &req).await
}

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
            let req = request_with_streamed_body(req).await?;
            println!("Got request: {:#?}", req);

            let res = handle_request(&out_addr, req).await?;
            println!("Sending back: {:#?}", res);
            Ok::<Response<http_body_util::Full<hyper::body::Bytes>>, hyper::Error>(res)
        });

        tokio::task::spawn(async move {
            if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                println!("Failed to serve the connection: {:?}", err);
            }
        });
    }
}
