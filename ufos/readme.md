# UFOs

_work in progress_



----

cross-compile for raspi 1:

set up `cross`

```bash
# build and deploy

cross build --release --target arm-unknown-linux-gnueabihf && scp ../target/arm-unknown-linux-gnueabihf/release/ufos angel-hair.local:ufos
```

nginx forward proxy for websocket (run this on another host):

```nginx


worker_processes  2;

pid nginx.pid;

events {
    worker_connections   2000;

    # use [ kqueue | epoll | /dev/poll | select | poll ];
    # use kqueue;
}

http {
    default_type  application/octet-stream;


    log_format main      '$remote_addr - $remote_user [$time_local] '
                         '"$request" $status $bytes_sent '
                         '"$http_referer" "$http_user_agent" '
                         '"$gzip_ratio"';

    log_format download  '$remote_addr - $remote_user [$time_local] '
                         '"$request" $status $bytes_sent '
                         '"$http_referer" "$http_user_agent" '
                         '"$http_range" "$sent_http_content_range"';

    client_header_timeout  3m;
    client_body_timeout    3m;
    send_timeout           3m;

    client_header_buffer_size    1k;
    large_client_header_buffers  4 4k;

    gzip on;
    gzip_min_length  1100;
    gzip_buffers     4 8k;
    gzip_types       text/plain;

    output_buffers   1 32k;
    postpone_output  1460;

    sendfile         on;
    tcp_nopush       on;
    tcp_nodelay      on;
    send_lowat       12000;

    keepalive_timeout  75 20;

    upstream websocket {
        server jetstream2.us-west.bsky.network:443;
    }

    server {
        listen        8080;

        access_log /dev/null;

        location / {
            proxy_pass https://websocket;
            proxy_http_version 1.1;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection "upgrade";
        }
    }

}
```

running

```bash
RUST_LOG=info ./ufos --jetstream ws://192.168.1.139:8080/subscribe --force --data /mnt/ufos-data-blah/
```

try without info-level logs for better perf
