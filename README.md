Simple work-queue broker/worker
-------------------------------

1. worker polls on `GET /next`
2. client submit work to `POST /` . The work is forwarded to a worker (from 1) with request id header
3. worker post response to `POST /response` with the same request id, reply is forwarded to the
   client (2)

To send work:
```
curl -X POST http://localhost:9999/  -d "@request-body.txt"
```

This is reproduction code for:

* https://github.com/hyperium/hyper/issues/1716
* https://github.com/hyperium/hyper/issues/1717
