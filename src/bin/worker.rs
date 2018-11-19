use futures::future::Future;
use futures::stream::{self, Stream};
use hyper::{client::HttpConnector, rt, Body, Client, Request, Uri};

fn main() -> std::result::Result<(), ()> {
    let next = "http://127.0.0.1:9999/next".parse::<Uri>().unwrap();
    let response = "http://127.0.0.1:9999/response".parse::<Uri>().unwrap();

    let mut http = HttpConnector::new_with_tokio_threadpool_resolver();
    http.set_nodelay(true);

    // build client
    let client_get = Client::builder()
        .keep_alive(true)
        .keep_alive_timeout(None)
        //.http2_only(true)
        .max_idle_per_host(1)
        .build(http);
    // use second client for the replies
    let client_post = client_get.clone();

    let process = stream::repeat::<(), ()>(())
        .and_then(move |_| {
            client_get
                .request(
                    Request::builder()
                        .method("GET")
                        .uri(next.clone())
                        .body(Body::empty())
                        .expect("Bad Request"),
                )
                .map_err(|e| println!("GET {:?}", e))
        })
        .and_then(move |req| {
            let id = req
                .headers()
                .get("X-Request-Id")
                .expect("RequestId missing")
                .clone();
            // Upper-case the request payload
            let body = req.into_body().map(|chunk| {
                chunk
                    .iter()
                    .map(|byte| byte.to_ascii_uppercase())
                    .collect::<Vec<u8>>()
            });
            client_post
                .request(
                    Request::builder()
                        .method("POST")
                        .header("X-Request-Id", id)
                        .uri(response.clone())
                        .body(Body::wrap_stream(body))
                        .expect("Invalid request"),
                )
                .map_err(|e| println!("POST {:?}", e))
        })
        .for_each(|_| Ok(()))
        .map_err(|_| ());

    Ok(rt::run(process))
}
