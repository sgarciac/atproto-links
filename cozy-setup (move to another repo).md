cozy-ucosm


## gateway

- tailscale (exit node enabled)
  -> allow ipv4 and ipv6 forwarding
- caddy

    ```bash
    apt install golang
    go install github.com/caddyserver/xcaddy/cmd/xcaddy@latest
    go/bin/xcaddy build \
      --with github.com/caddyserver/cache-handler \
      --with github.com/darkweak/storages/badger/caddy \
      --with github.com/mholt/caddy-ratelimit
    # then https://caddyserver.com/docs/running#manual-installation

    mkdir /var/cache/caddy-badger
    chown -R caddy:caddy /var/cache/caddy-badger/
    ```

    - `/etc/caddy/Caddyfile`

        ```
        {
          cache {
            badger
            api {
              prometheus
            }
          }
        }

        links.bsky.bad-example.com {
          reverse_proxy link-aggregator:6789

          @browser `{header.Origin.startsWith("Mozilla/5.0")`
          rate_limit {
            zone global_burst {
              key {remote_host}
              events 10
              window 1s
            }
            zone global_general {
              key {remote_host}
              events 100
              window 60s
              log_key true
            }
            zone website_harsh_limit {
              key {header.Origin}
              match {
                expression {header.User-Agent}.startsWith("Mozilla/5.0")
              }
              events 1000
              window 30s
              log_key true
            }
          }
          respond /souin-api/metrics "denied" 403 # does not work
          cache {
            ttl 3s
            stale 1h
            default_cache_control public, s-maxage=3
            badger {
              path /var/cache/caddy-badger/links
            }
          }
        }

        gateway:80 {
          metrics
          cache
        }
        ```
  well... the gateway fell over IMMEDIATELY with like 2 req/sec from deletions, with that ^^ config. for now i removed everything except the reverse proxy config + normal caddy metrics and it's running fine on vanilla caddy. i did try reducing the rate-limiting configs to a single, fixed-key global limit but it still ate all the ram and died. maybe badger w/ the cache config was still a problem. maybe it would have been ok on a machine with more than 1GB mem.


  alternative proxies:

    - nginx. i should probably just use this. acme-client is a piece of cake to set up, and i know how to configure it.
    - haproxy. also kind of familiar, it's old and stable. no idea how it handle low-mem (our 1gb) vs nginx.
    - sozu. popular rust thing, fast. doesn't have rate-limiting or cache feature?
    - rpxy. like caddy (auto-tls) but in rust and actually fast? has an "experimental" cache feature. but the cache feature looks good.
    - rama. build-your-own proxy. not sure that it has both cache and limiter in their standard features?
    - pingora. build-your-own cloudflare, so like, probably stable. has tools for cache and limiting. low-mem...?
      - cache stuff in pingora seems a little... hit and miss (byeeeee). only a test impl for Storage for the main cache feature?
      - but the rate-limiter has a guide: https://github.com/cloudflare/pingora/blob/main/docs/user_guide/rate_limiter.md

  what i want is low-resource reverse proxy with built-in rate-limiting and caching. but maybe cache (and/or ratelimiting) could be external to the reverse proxy
    - varnish is a dedicated cache. has https://github.com/varnish/varnish-modules/blob/master/src/vmod_vsthrottle.vcc
    - apache traffic control has experimental rate-limiting plugins


- victoriametrics

    ```bash
    curl -LO https://github.com/VictoriaMetrics/VictoriaMetrics/releases/download/v1.109.1/victoria-metrics-linux-amd64-v1.109.1.tar.gz
    tar xzf victoria-metrics-linux-amd64-v1.109.1.tar.gz
    # and then https://docs.victoriametrics.com/quick-start/#starting-vm-single-from-a-binary
    sudo mkdir /etc/victoria-metrics && sudo chown -R victoriametrics:victoriametrics /etc/victoria-metrics

    ```

    - `/etc/victoria-metrics/prometheus.yml`

        ```yaml
global:
  scrape_interval: '15s'

scrape_configs:
  - job_name: 'link_aggregator'
    static_configs:
      - targets: ['link-aggregator:8765']
  - job_name: 'gateway:caddy'
    static_configs:
      - targets: ['gateway:80/metrics']
  - job_name: 'gateway:cache'
    static_configs:
      - targets: ['gateway:80/souin-api/metrics']
        ```

    - `ExecStart` in `/etc/systemd/system/victoriametrics.service`:

        ```
        ExecStart=/usr/local/bin/victoria-metrics-prod -storageDataPath=/var/lib/victoria-metrics -retentionPeriod=90d -selfScrapeInterval=1m -promscrape.config=/etc/victoria-metrics/prometheus.yml
        ```

- grafana

    followed `https://grafana.com/docs/grafana/latest/setup-grafana/installation/debian/#install-grafana-on-debian-or-ubuntu`

    something something something then

    ```
    sudo grafana-cli --pluginUrl https://github.com/VictoriaMetrics/victoriametrics-datasource/releases/download/v0.11.1/victoriametrics-datasource-v0.11.1.zip plugins install victoriametrics
    ```

- raspi node_exporter

    ```bash
    curl -LO https://github.com/prometheus/node_exporter/releases/download/v1.8.2/node_exporter-1.8.2.linux-armv7.tar.g
    tar xzf node_exporter-1.8.2.linux-armv7.tar.gz
    sudo cp node_exporter-1.8.2.linux-armv7/node_exporter /usr/local/bin/
    sudo useradd --no-create-home --shell /bin/false node_exporter
    sudo nano /etc/systemd/system/node_exporter.service
      # [Unit]
      # Description=Node Exporter
      # Wants=network-online.target
      # After=network-online.target

      # [Service]
      # User=node_exporter
      # Group=node_exporter
      # Type=simple
      # ExecStart=/usr/local/bin/node_exporter
      # Restart=always
      # RestartSec=3

      # [Install]
      # WantedBy=multi-user.target
    sudo systemctl daemon-reload
    sudo systemctl enable node_exporter.service
    sudo systemctl start node_exporter.service
    ```

    todo: get raspi vcgencmd outputs into metrics

- nginx on gateway

    ```nginx
    # in http

    ##
    # cozy cache
    ##
    proxy_cache_path /var/cache/nginx keys_zone=cozy_zone:10m;

    ##
    # cozy limit
    ##
    limit_req_zone $binary_remote_addr zone=cozy_ip_limit:10m rate=50r/s;
    limit_req_zone $server_name zone=cozy_global_limit:10m rate=1000r/s;

    # in sites-available/linsks.bsky.bad-example.com

    upstream cozy_link_aggregator {
      server link-aggregator:6789;
      keepalive 16;
    }

    server {
      listen 8080;
      listen [::]:8080;

      server_name links.bsky.bad-example.com;

      proxy_cache cozy_zone;
      proxy_cache_background_update on;
      proxy_cache_key "$scheme$proxy_host$uri$is_args$args$http_accept";
      proxy_cache_lock on; # make simlutaneous requests for the same uri wait for it to appear in cache instead of hitting origin
      proxy_cache_lock_age 1s;
      proxy_cache_lock_timeout 2s;
      proxy_cache_valid 10s; # default -- should be explicitly set in the response headers
      proxy_cache_valid any 15s; # non-200s default
      proxy_read_timeout 5s;
      proxy_send_timeout 15s;
      proxy_socket_keepalive on;

      limit_req zone=cozy_ip_limit nodelay burst=100;
      limit_req zone=cozy_global_limit;
      limit_req_status 429;

      location / {
        proxy_pass http://cozy_link_aggregator;
        include proxy_params;
        proxy_http_version 1.1;
        proxy_set_header Connection ""; # for keepalive
      }
    }
    ```

    also `systemctl edit nginx` and paste

    ```
    [Service]
    Restart=always
    ```

    —https://serverfault.com/a/1003373


---

some todos

- [x] tailscale: exit node
  - [!] link_aggregator: use exit node
    -> worked, but reverted for now: tailscale on raspi was consuming ~50% cpu for the jetstream traffic. this might be near its max since it would have been catching up at the time (max jetstream throughput) but it feels a bit too much. we have to trust the jetstream server and link_aggregator doesn't (yet) make any other external connections, so for now the raspi connects directly from my home again.
- [x] caddy: reverse proxy
  - [x] build with cache and rate-limit plugins
  - [x] configure systemd to keep it alive
- [x] configure caddy cache
- [x] configure caddy rate-limit
- [ ] configure ~caddy~ nginx to use a health check (once it's added)
- [ ] ~configure caddy to only expose cache metrics to tailnet :/~
- [x] make some grafana dashboards
- [ ] raspi: mount /dev/sda on boot
- [ ] raspi: run link_aggregator via systemd so it starts on startup (and restarts?)

- [x] use nginx instead of caddy
- [x] nginx: enable cache
- [x] nginx: rate-limit
- [ ] nginx: get metrics
