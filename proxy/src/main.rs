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
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug)]
struct Stats {
    search_queries_success_count: u64,
    search_queries_failure_count: u64,
    nonsearch_passed_through_count: u64,
    search_queries_failures: Vec<(String, Bytes)>,
}

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
    multi_match: String,
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

// Parse query filters like match_all or multi_match
fn parse_filter(
    value: &Value,
    filter: &serde_json::Map<String, Value>,
    parsed: &mut ParsedSearchRequest,
) -> Result<(), String> {
    let filter_keys: Vec<_> = filter.keys().collect();
    if filter_keys == vec!["match_all"] {
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
    } else if filter_keys == vec!["multi_match"] {
        let Some(Object(multi_match_filter)) = filter.get("multi_match") else {
            return Err(format!(
                "unimplemented query value - unexpected multi_match filter: {}",
                value
            ));
        };
        for (filter_key, filter_value) in multi_match_filter {
            match filter_key.as_str() {
                "lenient" => {}
                "type" => {
                    if *filter_value != "best_fields" {
                        return Err(format!(
                            "unimplemented multi_match type value: {}",
                            filter_value
                        ));
                    }
                }
                "query" => {
                    if let Value::String(filter_value) = filter_value {
                        parsed.multi_match = filter_value.clone();
                    } else {
                        return Err(format!(
                            "unimplemented multi_match query value: {}",
                            filter_value
                        ));
                    }
                }
                _ => {
                    return Err(format!(
                        "unimplemented multi_match parameter: {}",
                        filter_key
                    ));
                }
            }
        }
    } else {
        return Err(format!(
            "unimplemented query value - unexpected filter: {}",
            value
        ));
    }

    Ok(())
}

fn parse_body(body: &Value, parsed: &mut ParsedSearchRequest) -> Result<(), String> {
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
                parse_filter(value, filter, parsed)?;

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
    let mut parsed_request: ParsedSearchRequest = ParsedSearchRequest {
        multi_match: "".to_string(),
    };

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

    let mut result = vec!["Through the fire, to the limit, to the wall, For a chance to be with you, I'd gladly risk it all.", 
        "You tell me you're gonna play it smart, We're through before we start, But I believe that we've only just begun",
        "When it's this good, there's no saying no"
        ];

    if !parsed_request.multi_match.is_empty() {
        let multi_match: Vec<_> = parsed_request.multi_match.split(' ').collect();
        result.retain(|result| multi_match.iter().any(|mm| result.contains(mm)));
    }

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
                "value": result.len(),
                "relation": "eq",
            },
            "max_score": 0.0,
            "hits": result.iter().map(|r| json!({
                    "_index":"my-first-index",
                    "_id":"1",
                    "_version":5,
                    "_score":0.0,
                    "_source":
                        {"Description": r}
                })).collect::<Vec<_>>()
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
    stats: Arc<Mutex<Stats>>,
) -> Result<Response<http_body_util::Full<hyper::body::Bytes>>, hyper::Error> {
    static SEARCH_ENDPOINT: Lazy<Regex> = Lazy::new(|| Regex::new(r"^/([^/]*)/_search$").unwrap());

    if SEARCH_ENDPOINT.is_match(req.uri().path()) {
        let res = handle_search_request(&req).await;
        match res {
            Ok(res) => {
                stats.lock().unwrap().search_queries_success_count += 1;
                return Ok(res);
            }
            Err(err) => {
                let mut stats = stats.lock().unwrap();
                stats.search_queries_failure_count += 1;
                stats
                    .search_queries_failures
                    .push((err.clone(), req.body().clone()));
                println!("Error handling search request: {}", err);
            }
        }
    } else {
        stats.lock().unwrap().nonsearch_passed_through_count += 1;
    }

    forward_request_to_opensearch(out_addr, &req).await
}

fn get_queries_failures(stats: Arc<Mutex<Stats>>) -> String {
    let stats = stats.lock().unwrap();
    let mut result = "".to_owned();

    for (reason, body) in &stats.search_queries_failures {
        result.push_str(format!("<div class='failure_row'><div class='failure_reason'>{}</div> <div class='failure_body'>{:#?}</div></div>", reason, std::str::from_utf8(body).unwrap_or("")).as_str());
    }
    result
}

use axum::{routing::get, Router};
use tower_http::services::ServeFile;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Monitoring website
    let stats1 = Arc::new(Mutex::new(Stats {
        search_queries_success_count: 0,
        search_queries_failure_count: 0,
        nonsearch_passed_through_count: 0,
        search_queries_failures: Vec::new(),
    }));
    let stats2 = stats1.clone();
    let stats3 = stats1.clone();
    let stats4 = stats1.clone();
    let stats5 = stats1.clone();

    let app = Router::new()
        .route(
            "/search_queries_success_count",
            get(move || async move {
                format!("{}", stats1.lock().unwrap().search_queries_success_count)
            }),
        )
        .route(
            "/search_queries_failure_count",
            get(move || async move {
                format!("{}", stats2.lock().unwrap().search_queries_failure_count)
            }),
        )
        .route(
            "/search_queries_failures",
            get(move || async move { get_queries_failures(stats3) }),
        )
        .route(
            "/nonsearch_passed_through_count",
            get(move || async move {
                format!("{}", stats4.lock().unwrap().nonsearch_passed_through_count)
            }),
        )
        .route_service("/", ServeFile::new("../frontend/index.html"))
        .route_service("/favicon.ico", ServeFile::new("../frontend/favicon.ico"));

    let addr = SocketAddr::from(([0, 0, 0, 0], 3001));
    println!("listening on {}", addr);
    tokio::task::spawn(async move {
        hyper_server::bind(addr)
            .serve(app.into_make_service())
            .await
            .unwrap();
    });

    // Proxy
    let in_addr: SocketAddr = ([0, 0, 0, 0], 3000).into();
    let out_addr: SocketAddr = ([127, 0, 0, 1], 9200).into();

    let listener = TcpListener::bind(in_addr).await?;

    println!("Listening on http://{}", in_addr);
    println!("Proxying to http://{}", out_addr);

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);

        let stats = stats5.clone();

        let service = service_fn(move |req: hyper::Request<Incoming>| {
            let stats = stats.clone();

            async move {
                println!("-------------------------");
                let req = request_with_streamed_body(req).await?;
                println!("Got request: {:#?}", req);

                let res = handle_request(&out_addr, req, stats).await?;
                println!("Sending back: {:#?}", res);

                Ok::<Response<http_body_util::Full<hyper::body::Bytes>>, hyper::Error>(res)
            }
        });

        tokio::task::spawn(async move {
            if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                println!("Failed to serve the connection: {:?}", err);
            }
        });
    }
}
