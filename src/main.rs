use anyhow::{bail, Error, Result};
use arc_swap::ArcSwap;
use axum::{
    extract::Path,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use im::HashMap as IMHashMap;
use lazy_static::lazy_static;
use serde_json::{json, to_vec_pretty, Value as SJValue};
use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader, Seek},
    sync::{Arc, Mutex},
    thread::{self, JoinHandle},
    time::{self, Instant},
};
use std::{
    io::Read,
    net::{SocketAddr, ToSocketAddrs},
};
use tokio::{
    signal::unix::{signal, SignalKind},
    sync::oneshot::{self, Sender},
};
use toml::{Table, Value};
use tower_http::trace::{self, TraceLayer};
use tracing::{info, instrument, Level};

#[derive(Parser, Debug, Default, Clone, PartialEq)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Location of the configuration TOML file
    #[arg(short, long)]
    conf: String,

    /// Number of threads to allow for servicing.
    /// A small number if on spinning disks, larger
    /// for SSD
    #[arg(short, long)]
    threads: Option<u16>,

    /// hostname to bind to
    #[arg(long)]
    host: Vec<String>,

    /// the port to bind to
    #[arg(long, short)]
    port: Option<u16>,
}

impl Args {
    fn num_threads(&self) -> u16 {
        self.threads.unwrap_or(7)
    }

    fn conf_file(&self) -> String {
        shellexpand::tilde(&self.conf).to_string()
    }

    fn port(&self) -> u16 {
        self.port.unwrap_or(3000)
    }

    fn to_socket_addrs(&self) -> Vec<SocketAddr> {
        let lh = vec!["localhost".to_string()];
        let the_port = self.port();
        let hosts = if self.host.is_empty() {
            &lh
        } else {
            &self.host
        };
        let sa: Vec<SocketAddr> = hosts
            .iter()
            .flat_map(|host_name| {
                let ma = format!("{}:{}", host_name, the_port);
                let addr = ma.to_socket_addrs().ok();
                addr
            })
            .flatten()
            .collect();
        sa
    }
}

#[derive(Debug, Clone, Copy)]
pub struct EntryOffset {
    hash: u128,
    loc: u64,
}

#[derive(Debug)]
pub struct EntriesAndFiles {
    entries: Vec<EntryOffset>,
    file_pointers: Mutex<Vec<BufReader<File>>>,
}

impl EntriesAndFiles {
    fn get_file_pointer(&self) -> BufReader<File> {
        loop {
            {
                let mut fp = self.file_pointers.lock().unwrap();
                if fp.len() > 0 {
                    return fp.pop().unwrap();
                }
            }

            let ten_millis = time::Duration::from_millis(10);
            thread::sleep(ten_millis);
        }
    }

    fn return_file_pointer(&self, br: BufReader<File>) {
        let mut fp = self.file_pointers.lock().unwrap();
        fp.push(br);
    }

    pub fn find(&self, hash: u128) -> Option<EntryOffset> {
        find_entry(hash, &self.entries)
    }

    pub fn bytes_for(&self, eo: &EntryOffset) -> Result<Vec<u8>> {
        let mut fp = self.get_file_pointer();
        
        let content: Result<Vec<u8>> = fp
            .seek(std::io::SeekFrom::Start(eo.loc))
            .map_err(|e| e.into())
            .and({
                let mut line = String::new();
                fp.read_line(&mut line)?;
                Ok(line)
            })
            .and_then(|v| {
                let r = decode_line(&v)?;
                Ok(r.2)
            });

        self.return_file_pointer(fp);

        Ok(content?)
    }


    pub async fn find_item(&self, item: u128) -> Option<Vec<u8>> {
        let eo = self.find(item)?;
        let (tx, rx) = oneshot::channel::<Result<Vec<u8>>>();
        LOOKUP_TX.send((eo, tx)).await.ok()?;
        let answer = rx.await.ok()?.ok();
        answer
    }

    pub async fn lazy_find(&self, mut items: Vec<u128>) -> Result<Vec<u8>> {
        let lazy = std::iter::from_fn(move || {
            let item = items.pop()?;
            Some(self.find(item))
        });

        match lazy.skip_while(|p| p.is_none()).next() {
            Some(Some(eo)) => {
                let (tx, rx) = oneshot::channel::<Result<Vec<u8>>>();
                LOOKUP_TX.send((eo, tx)).await?;
                let answer = rx.await??;
                Ok(answer)
            }
            _ => anyhow::bail!("Not found"),
        }
    }

    pub fn new(entry_path: Option<String>, threads: u16) -> Result<EntriesAndFiles> {
        match &entry_path {
            Some(path)
                if {
                    let pp = std::path::Path::new(path);
                    pp.exists() && pp.is_file()
                } =>
            {
                let start = Instant::now();
                let file = File::open(path)?;
                let mut reader = BufReader::new(file);
                let mut buf = Vec::with_capacity(536870912);
                let mut cnt = 0;
                let mut offsets: Vec<EntryOffset> = Vec::new();
                loop {
                    buf.resize(0, 0u8);
                    let pos = reader.stream_position()?;
                    cnt = cnt + 1;
                    if cnt % 10000 == 0 {
                        println!("Cnt {}", cnt);
                    }
                    // let mut line = String::new();
                    
                    reader.read_until(b'\n', &mut buf)?;
                    let bl = buf.len();
                    if bl == 0 {
                        println!("Got blank line");
                        break;
                    }

                    let mut idx = 0;
                    
                    let comma = loop {
                        if idx >= bl {
                            bail!("Unable to find comma in {}", String::from_utf8_lossy(&buf));
                        }
                        if buf[idx] == b',' {
                            break idx;
                        }
                        idx += 1;
                    };

                    let hash = &buf[0..comma];

                    let hash_i = u128::from_str_radix(&String::from_utf8_lossy(hash), 16)?;
                    let entry = EntryOffset {
                        loc: pos,
                        hash: hash_i,
                    };

                    offsets.push(entry);
                }

                println!("Reading took {:?}", start.elapsed());

                let files = (1..threads)
                    .map(|_| {
                        let file = File::open(path).ok()?;
                        let reader = BufReader::new(file);
                        Some(reader)
                    })
                    .map(|a| a.unwrap())
                    .collect();

                Ok(EntriesAndFiles {
                    entries: offsets,
                    file_pointers: Mutex::new(files),
                })
            }
            Some(path) => bail!(format!("Path {} isn't a file", path)),
            None => Ok(EntriesAndFiles {
                entries: Vec::new(),
                file_pointers: Mutex::new(Vec::new()),
            }),
        }
    }
}

fn decode_line(line: &str) -> Result<(u128, String, Vec<u8>)> {
    let comma = line.find(',').ok_or(anyhow::Error::msg("no comma found"))?;
    let sep = line
        .find("||,||")
        .ok_or(Error::msg("No ||,|| separator found"))?;
    let hash = &line[0..comma];
    let path = &line[(comma + 1)..sep];
    let rest = &line[(sep + 5)..(line.len() - 1)];

    let decoded = match rbase64::decode(rest) {
        Ok(v) => v,
        Err(e) => {bail!(format!("Decode error {}", e));}
    };

    let hash_i = u128::from_str_radix(hash, 16)?;
    Ok((hash_i, path.to_string(), decoded))
}

fn find_entry(to_find: u128, offsets: &[EntryOffset]) -> Option<EntryOffset> {
    let mut low = 0;
    let mut hi = offsets.len() - 1;

    while low <= hi {
        let mid = low + (hi - low) / 2;
        match offsets.get(mid) {
            Some(entry) => {
                if entry.hash == to_find {
                    return Some(entry.clone());
                } else if entry.hash > to_find {
                    hi = mid - 1;
                } else {
                    low = mid + 1;
                }
            }
            None => return None,
        }
    }

    None
}

fn md5hash_str(st: &str) -> u128 {
    let res = md5::compute(st);

    u128::from_be_bytes(res.into())
}

fn split_path(path: String) -> Vec<String> {
    let mut segments = Vec::new();

    for s in path.split('/') {
        match s {
            "" | "." => {}
            ".." => {
                segments.pop();
            }
            s => segments.push(s.to_string()),
        }
    }

    segments
}

lazy_static! {
    static ref CONFIG_ARGS: ArcSwap<Args> = ArcSwap::new(Arc::new(Args::default()));
    static ref ENTRIES_AND_FILES: ArcSwap<EntriesAndFiles> =
        ArcSwap::new(Arc::new(EntriesAndFiles::new(None, 1).unwrap()));
    static ref LOOKUP_CHANS: (
        async_channel::Sender<(EntryOffset, Sender<Result<Vec<u8>>>)>,
        async_channel::Receiver<(EntryOffset, Sender<Result<Vec<u8>>>)>
    ) = async_channel::bounded(100);
    static ref LOOKUP_TX: async_channel::Sender<(EntryOffset, Sender<Result<Vec<u8>>>)> =
        LOOKUP_CHANS.0.clone();
    static ref LOOKUP_RX: async_channel::Receiver<(EntryOffset, Sender<Result<Vec<u8>>>)> =
        LOOKUP_CHANS.1.clone();
    static ref SERVICE_THREADS: ArcSwap<Vec<JoinHandle<()>>> = ArcSwap::new(Arc::new(Vec::new()));
}

fn not_found() -> Response {
    (StatusCode::NOT_FOUND, "Sorry\n").into_response()
}

fn _build_json(instr: &str) -> Response {
    (
        [(header::CONTENT_TYPE, "application/json")],
        instr.to_string(),
    )
        .into_response()
}

fn build_json_vec(instr: Vec<u8>) -> Response {
    ([(header::CONTENT_TYPE, "application/json")], instr).into_response()
}

fn _build_json_str(instr: String) -> Response {
    ([(header::CONTENT_TYPE, "application/json")], instr).into_response()
}

fn hex_to_u128(it: &str) -> Option<u128> {
    hex::decode(it)
        .map(|bytes| {
            if bytes.len() < std::mem::size_of::<u128>() {
                None
            } else {
                let (int_bytes, _) = bytes.split_at(std::mem::size_of::<u128>());
                let slice: [u8; 16] = int_bytes.try_into().ok()?;
                Some(u128::from_be_bytes(slice))
            }
        })
        .ok()
        .flatten()
}

fn check_md5(it: Option<&String>) -> Option<u128> {
    match it {
        Some(v) if v.len() == 32 => hex_to_u128(v),
        Some(v) if v.len() == 37 && v.ends_with(".json") => hex_to_u128(&v[0..32]),
        _ => None,
    }
}

async fn lazy_find(what: Vec<u128>) -> Result<Vec<u8>> {
    ENTRIES_AND_FILES.load().lazy_find(what).await
}

fn generate_service_thread() -> JoinHandle<()> {
    thread::spawn(|| loop {
        match LOOKUP_RX.recv_blocking() {
            Ok((eo, respond)) => {
                let thing = ENTRIES_AND_FILES.load().bytes_for(&eo);
                match respond.send(thing) {
                    Ok(_) => {}
                    Err(_e) => {
                        // FIXME log error
                    }
                }
            }
            Err(_) => break,
        }
    })
}

// #[debug_handler]
async fn serve_purl(Path(purl): Path<String>) -> Result<Response, StatusCode> {
    let mut totry = Vec::new();
    totry.push(md5hash_str(&format!("purl/{}", purl)));
    totry.push(md5hash_str(&format!("purl/{}.json", purl)));
    totry.push(md5hash_str(&purl));
    totry.push(md5hash_str(&format!("{}.json", purl)));

    let found = lazy_find(totry).await;
    match found {
        Ok(found) => Ok(build_json_vec(found)),
        _ => Ok(not_found()),
    }
}

async fn serve_gitoid(Path(path): Path<String>) -> Result<Response, StatusCode> {
    let split = split_path(path.clone());

    let mut totry = Vec::new();
    match check_md5(split.get(0)) {
        Some(i) => totry.push(i),
        _ => {}
    }

    totry.push(md5hash_str(&path));

    let found = lazy_find(totry).await;
    match found {
        Ok(found) => Ok(build_json_vec(found)),
        _ => Ok(not_found()),
    }
}

#[instrument]
async fn find_items_and_parents(inp: Vec<String>) -> HashMap<String, SJValue> {
    let mut ret = HashMap::new();
    let mut found = IMHashMap::new();
    for s in inp.iter() {
       found = found.update(s.to_string(), false);
    }

    while found.values().any(|v| !v) {
        for (item, looked_up) in found.clone() {
            if !looked_up {
                let it = ENTRIES_AND_FILES.load().find_item(md5hash_str(&item)).await;
                let mut done = false;

                if let Some(inner) = it {
                    if let Ok(json) = serde_json::from_slice::<SJValue>(&inner) {
                        done = true;
                        if let Some(contained_by) = json.get("containedBy") {
                            if let Some(items) = contained_by.as_array() {
                                for item in items {
                                    if let Some(str_item) = item.as_str() {
                                        if !found.contains_key(str_item) {
                                            found = found.update(str_item.to_string(), false);
                                        }
                                    }
                                }
                            }
                        }
                        ret.insert(item.clone(), json);
                    }
                }
                if !done {
                    ret.insert(item.clone(), SJValue::Null);
                }
                found = found.update(item, true);
            }
        }
    }

    ret
}

async fn serve_aggregate(Json(payload): Json<Value>) -> Result<Response, StatusCode> {
    match payload.as_array() {
        Some(a) => {
            let v = a
                .iter()
                .flat_map(|c| c.as_str().map(|s| s.to_string()))
                .collect();

            let items = find_items_and_parents(v).await;

            let json_thing = json!(items);
            let vec = to_vec_pretty(&json_thing).map_err(|_| StatusCode::BAD_REQUEST)?;

            Ok(build_json_vec(vec))
        }
        None => Err(StatusCode::BAD_REQUEST),
    }
}

fn read_conf_file() -> Result<Table> {
    let args = CONFIG_ARGS.load();
    let conf_file_name = args.conf_file();
    let mut conf_file = File::open(conf_file_name)?;
    let mut contents = String::new();
    conf_file.read_to_string(&mut contents)?;
    contents.parse::<Table>().map_err(|e| e.into())
}

fn update_entries_and_files() -> Result<()> {
    let args = CONFIG_ARGS.load();
    let conf = read_conf_file()?;

    let to_store = Arc::new(EntriesAndFiles::new(
        match conf.get("oc_file") {
            Some(Value::String(s)) => Some(shellexpand::tilde(s).to_string()),
            _ => bail!("'oc_file' property not set"),
        },
        args.num_threads(),
    )?);

    ENTRIES_AND_FILES.store(to_store);

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();

    println!("Args {:?}, threads {}", args, args.num_threads());

    CONFIG_ARGS.store(Arc::new(args.clone()));

    let mut sigint = signal(SignalKind::hangup())?;

    tokio::spawn(async move {
        loop {
            match sigint.recv().await {
                None => break,
                Some(_) => {
                    println!("Got sighup!");
                    thread::spawn(|| {
                    match update_entries_and_files() {
                        Ok(_) => println!("Successfully updated!"),

                        Err(e) => println!("failed to update {}", e),
                    }});
                }
            }
        }
    });

    SERVICE_THREADS.store(Arc::new(
        (1..args.num_threads())
            .map(|_| generate_service_thread())
            .collect(),
    ));

    update_entries_and_files()?;

    // build our application with a single route
    let app = Router::new()
        .route("/purl/*path", get(serve_purl))
        .route("/bulk", post(serve_aggregate))
        .route("/*path", get(serve_gitoid))
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(trace::DefaultMakeSpan::new().level(Level::INFO))
                .on_response(trace::DefaultOnResponse::new().level(Level::INFO)),
        );

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind(&*args.to_socket_addrs())
        .await
        .unwrap();
    axum::serve(listener, app).await.unwrap();
    Ok(())
}
