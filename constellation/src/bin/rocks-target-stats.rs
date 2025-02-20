use bincode::config::Options;
use clap::Parser;
use std::collections::HashMap;
use std::path::PathBuf;

use constellation::storage::rocks_store::{
    Collection, DidId, RKey, RPath, Target, TargetKey, TargetLinkers, _bincode_opts,
};
use constellation::storage::RocksStorage;
use constellation::Did;

use links::parse_any_link;
use rocksdb::IteratorMode;
use std::time;

/// Aggregate links in the at-mosphere
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// where is rocksdb's data
    #[arg(short, long)]
    data: PathBuf,
}

type LinkType = String;

#[derive(Debug, Eq, Hash, PartialEq)]
struct SourceLink(Collection, RPath, LinkType);

#[derive(Debug, Default)]
struct Buckets([u64; 23]);

const BUCKETS: Buckets = Buckets([
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 16, 32, 64, 128, 256, 512, 1024, 4096, 16384, 65535, 262144,
    1048576,
]);

// b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b12, b16, b32, b64, b128, b256, b512, b1024, b4096, b16384, b65535, b262144, bmax

static DID_IDS_CF: &str = "did_ids";
static TARGET_IDS_CF: &str = "target_ids";
static TARGET_LINKERS_CF: &str = "target_links";

const REPORT_INTERVAL: usize = 50_000;

type Stats = HashMap<SourceLink, (String, String, Buckets)>;

#[derive(Debug, Default)]
struct ErrStats {
    failed_to_get_sample: usize,
    failed_to_read_target_id: usize,
    failed_to_deserialize_target_key: usize,
    failed_to_parse_target_as_link: usize,
    failed_to_get_links: usize,
    failed_to_deserialize_linkers: usize,
}

fn thousands(n: usize) -> String {
    n.to_string()
        .as_bytes()
        .rchunks(3)
        .rev()
        .map(std::str::from_utf8)
        .collect::<Result<Vec<&str>, _>>()
        .unwrap()
        .join(",")
}

fn main() {
    let args = Args::parse();

    eprintln!("starting rocksdb...");
    let rocks = RocksStorage::open_readonly(args.data).unwrap();
    eprintln!("rocks ready.");

    let RocksStorage { ref db, .. } = rocks;

    let mut stats = Stats::new();
    let mut err_stats: ErrStats = Default::default();

    let did_ids_cf = db.cf_handle(DID_IDS_CF).unwrap();
    let target_id_cf = db.cf_handle(TARGET_IDS_CF).unwrap();
    let target_links_cf = db.cf_handle(TARGET_LINKERS_CF).unwrap();

    let t0 = time::Instant::now();
    let mut t_prev = t0;

    let mut i = 0;
    for item in db.iterator_cf(&target_id_cf, IteratorMode::Start) {
        if i > 0 && i % REPORT_INTERVAL == 0 {
            let now = time::Instant::now();
            let rate = (REPORT_INTERVAL as f32) / (now.duration_since(t_prev).as_secs_f32());
            eprintln!(
                "{i}\t({}k)\t{:.2}\t{rate:.1}/s",
                thousands(i / 1000),
                t0.elapsed().as_secs_f32()
            );
            t_prev = now;
        }
        i += 1;

        let Ok((target_key, target_id)) = item else {
            err_stats.failed_to_read_target_id += 1;
            continue;
        };

        let Ok(TargetKey(Target(target), collection, rpath)) =
            _bincode_opts().deserialize(&target_key)
        else {
            err_stats.failed_to_deserialize_target_key += 1;
            continue;
        };

        let source = {
            let Some(parsed) = parse_any_link(&target) else {
                err_stats.failed_to_parse_target_as_link += 1;
                continue;
            };
            SourceLink(collection, rpath, parsed.name().into())
        };

        let Ok(Some(links_raw)) = db.get_cf(&target_links_cf, &target_id) else {
            err_stats.failed_to_get_links += 1;
            continue;
        };
        let Ok(linkers) = _bincode_opts().deserialize::<TargetLinkers>(&links_raw) else {
            err_stats.failed_to_deserialize_linkers += 1;
            continue;
        };
        let (n, _) = linkers.count();

        if n == 0 {
            continue;
        }

        let mut bucket = 0;
        for edge in BUCKETS.0 {
            if n <= edge || bucket == 22 {
                break;
            }
            bucket += 1;
        }

        stats
            .entry(source)
            .or_insert_with(|| {
                let (DidId(did_id), RKey(k)) = &linkers.0[(n - 1) as usize];
                if let Ok(Some(did_bytes)) = db.get_cf(&did_ids_cf, did_id.to_be_bytes()) {
                    if let Ok(Did(did)) = _bincode_opts().deserialize(&did_bytes) {
                        return (did, k.clone(), Default::default());
                    }
                }
                err_stats.failed_to_get_sample += 1;
                ("".into(), "".into(), Default::default())
            })
            .2
             .0[bucket] += 1;

        // if i >= 400_000 { break }
    }

    eprintln!(
        "FINISHED summarizing {} link targets in {:.1}s",
        thousands(i),
        t0.elapsed().as_secs_f32()
    );
    eprintln!("{err_stats:?}");

    for (SourceLink(Collection(c), RPath(p), t), (d, r, Buckets(b))) in stats {
        let sample_at_uri = if !(d.is_empty() || r.is_empty()) {
            format!("at://{d}/{c}/{r}")
        } else {
            "".into()
        };
        println!(
            "{c:?}, {p:?}, {t:?}, {sample_at_uri:?}, {}",
            b.map(|n| n.to_string()).join(", ")
        );
    }

    eprintln!("bye.");
}

// scan plan

// buckets (backlink count)
// 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 16, 32, 64, 128, 256, 512, 1024, 4096, 16384, 65535, 262144, 1048576+
// by
// - collection
// - json path
// - link type
// samples for each bucket for each variation
