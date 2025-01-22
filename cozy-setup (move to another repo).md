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
          respond /souin-api/metrics "denied" 403
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



---

some todos

- [x] tailscale: exit node
  - [!] link_aggregator: use exit node
    -> worked, but reverted for now: tailscale on raspi was consuming ~50% cpu for the jetstream traffic. this might be near its max since it would have been catching up at the time (max jetstream throughput) but it feels a bit too much. we have to trust the jetstream server and link_aggregator doesn't (yet) make any other external connections, so for now the raspi connects directly from my home again.
- [x] caddy: reverse proxy
  - [x] build with cache and rate-limit plugins
  - [x] configure systemd to keep it alive
- [ ] configure caddy cache
- [ ] configure caddy rate-limit
- [ ] configure caddy to use a health check (once it's added)
- [ ] configure caddy to only expose cache metrics to tailnet :/
- [ ] make some grafana dashboards

