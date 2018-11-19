use broker::inspect_drop;
use futures::{future, sync::oneshot, Future};
use std::{
    cell::RefCell, collections::hash_map::HashMap, collections::VecDeque, error::Error, rc::Rc,
    time::Duration,
};
// the HTTP 1/2 stack
use hyper::{
    header::HeaderValue, service::service_fn, Body, Method, Request, Response, Server, StatusCode,
};
use log::*;
use simple_logger;
// Run single-threaded
use tokio::runtime::current_thread;
use uuid::Uuid;

/// Broker
/// 1. worker polls on `GET /next`
/// 2. client submit work to `POST /` . The work is forwarded to a worker (from 1) with request id header
/// 3. worker post response to `POST /response` with the same request id, reply is forwarded to the
///    client (2)

type RequestId = HeaderValue;
type ResponseFuture = Box<Future<Item = Response<Body>, Error = Box<Error + Send + Sync>>>;

#[derive(Clone)]
struct Broker {
    // workers waiting for a request
    worker_queue: Rc<RefCell<VecDeque<oneshot::Sender<(RequestId, Body)>>>>,
    // Map RequestId -> Response channel (oneshot)
    response_map: Rc<RefCell<HashMap<RequestId, oneshot::Sender<Response<Body>>>>>,
}

impl Broker {
    fn new() -> Broker {
        Broker {
            worker_queue: Rc::new(RefCell::new(VecDeque::new())),
            response_map: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    // Return empty response with a status
    fn empty(&self, status: StatusCode) -> ResponseFuture {
        Box::new(inspect_drop(
            future::ok(
                Response::builder()
                    .status(status)
                    .body(Body::empty())
                    .unwrap(),
            ),
            format!("{:?}", status),
        ))
    }

    // worker ask for the next job
    fn next(&self) -> ResponseFuture {
        let (tx, rx) = oneshot::channel();
        self.worker_queue.borrow_mut().push_back(tx);
        Box::new(inspect_drop(
            rx.and_then(|(id, body)| {
                debug!("Forward request id {:?}", id);
                future::ok(
                    Response::builder()
                        .header("X-Request-Id", id)
                        .status(StatusCode::OK)
                        .body(body)
                        .unwrap(),
                )
            })
            .map_err(|e| e.into()),
            "Next()".to_string(),
        ))
    }

    fn request(&self, body: Body) -> ResponseFuture {
        let id = HeaderValue::from_str(&format!("{}", Uuid::new_v4())).unwrap();
        let worker_opt = self.worker_queue.borrow_mut().pop_front();
        if let Some(worker) = worker_opt {
            info!("Found worker {:?}", worker);
            if let Err((_, body)) = worker.send((id.clone(), body)) {
                info!("Dead worker, retry");
                // worker disconnected, try next one
                self.request(body)
            } else {
                info!("Request sent");
                let (tx, rx) = oneshot::channel();
                let mut map = self.response_map.borrow_mut();
                map.insert(id, tx);
                Box::new(inspect_drop(
                    rx.map_err(|e| e.into()),
                    "Invoke sent".to_string(),
                ))
            }
        } else {
            self.empty(StatusCode::NOT_ACCEPTABLE)
        }
    }

    fn response(&self, id: RequestId, body: Body) -> ResponseFuture {
        debug!("Response for {:?}", id);
        let mut map = self.response_map.borrow_mut();
        if let Some(send) = map.remove(&id) {
            let resp = Response::builder().body(body).unwrap();
            if let Ok(_) = send.send(resp) {
                debug!("Sent id {:?}", id);
                self.empty(StatusCode::ACCEPTED)
            } else {
                error!("Error sending id {:?}", id);
                self.empty(StatusCode::INTERNAL_SERVER_ERROR)
            }
        } else {
            error!("Response for missing id {:?}", id);
            self.empty(StatusCode::NOT_FOUND)
        }
    }

    fn dispatch(&self, req: Request<Body>) -> ResponseFuture {
        info!("{:?}", req);
        match (req.method(), req.uri().path()) {
            (&Method::GET, "/next") => self.next(),
            (&Method::POST, "/") => self.request(req.into_body()),
            (&Method::POST, "/response") => {
                if let Some(id) = req.headers().get("X-Request-Id") {
                    self.response(id.clone(), req.into_body())
                } else {
                    self.empty(StatusCode::BAD_REQUEST)
                }
            }
            _ => self.empty(StatusCode::METHOD_NOT_ALLOWED),
        }
    }
}

fn main() {
    simple_logger::init_with_level(Level::Info).unwrap();

    let addr = ([127, 0, 0, 1], 9999).into();

    let broker = Box::new(Broker::new());

    let exec = current_thread::TaskExecutor::current();
    let server = Server::bind(&addr)
        .tcp_nodelay(true)
        .tcp_keepalive(Some(Duration::new(30, 0)))
        .executor(exec)
        .serve(move || {
            let broker = broker.clone();
            service_fn(move |req| broker.dispatch(req))
        })
        .map_err(|e| error!("server error: {}", e));

    info!("Listening on http://{}", addr);
    let mut runtime = current_thread::Runtime::new().expect("Failed to create runtime");
    runtime.spawn(server);
    runtime.run().expect("Failed to run");
}
