use async_std::{
    fs::OpenOptions,
    io::{
        prelude::{SeekExt, WriteExt},
        BufWriter, SeekFrom,
    },
    prelude::StreamExt,
    sync::{channel, Receiver, Sender},
    task,
};
use clap::{clap_app, crate_version};
use failure::{bail, format_err, Fallible};
use futures::io::AsyncReadExt;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashMap;
use surf;
use url::{ParseError, Url};

use std::process;

static PBAR_FMT: &str =
    "{msg} {spinner:.green} {percent}% [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} eta: {eta}";

#[derive(Debug)]
#[allow(unused)]
enum Event {
    Chunk { start: u64, bytes: Vec<u8> },
    Error { start: u64, end: u64 },
}

pub fn create_progress_bar(msg: &str, length: Option<u64>) -> ProgressBar {
    let progbar = match length {
        Some(len) => ProgressBar::new(len),
        None => ProgressBar::new_spinner(),
    };

    progbar.set_message(msg);
    if length.is_some() {
        progbar.set_style(
            ProgressStyle::default_bar()
                .template(PBAR_FMT)
                .progress_chars("=> "),
        );
    } else {
        progbar.set_style(ProgressStyle::default_spinner());
    }

    progbar
}

async fn get_headers(u: &Url) -> Fallible<HashMap<String, String>> {
    let mut resp = surf::head(u).await.unwrap();
    Ok(resp
        .headers()
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect::<HashMap<String, String>>())
}

async fn chunk_url(u: Url) -> Fallible<Option<Vec<(u64, u64)>>> {
    let mut resp = surf::head(u).await.unwrap();
    let headers = resp.headers();
    if Some("bytes") == headers.get("accept-ranges") {
        Ok(headers
            .get("content-length")
            .map(|ct_len_str| get_chunk_offsets(ct_len_str.parse().unwrap(), 512_000u64)))
    } else {
        Ok(headers
            .get("content-length")
            .map(|v| vec![(0u64, v.parse().unwrap())]))
    }
}

fn gen_fname(url: &Url, content_disposition: Option<&String>) -> String {
    let c_disp = content_disposition.and_then(|val| {
        if val.contains("filename=") {
            let x = val
                .rsplit(';')
                .nth(0)
                .unwrap_or("")
                .rsplit('=')
                .nth(0)
                .unwrap_or("")
                .trim_start_matches('"')
                .trim_end_matches('"');
            if x.is_empty() {
                None
            } else {
                Some(x.to_string())
            }
        } else {
            None
        }
    });
    match c_disp {
        Some(val) => val,
        None => {
            let name = url.path().split('/').last().unwrap_or("");
            if !name.is_empty() {
                match decode_percent_encoded_data(name) {
                    Ok(val) => val,
                    _ => name.to_string(),
                }
            } else {
                "index.html".to_string()
            }
        }
    }
}

async fn main_loop(url: Url, fname: Option<String>) -> Fallible<()> {
    let headers = get_headers(&url).await?;
    let cl: u64 = headers.get("content-length").unwrap().parse()?;
    let fname = fname.unwrap_or_else(|| gen_fname(&url, headers.get("content-disposition")));
    let chunks = chunk_url(url.clone()).await?.unwrap();
    let (bytes_sender, bytes_receiver) = channel(100);
    let recv_handle = task::spawn(progress_loop(bytes_receiver, Some(cl), fname));
    for offsets in chunks {
        task::spawn(download_loop(bytes_sender.clone(), offsets, url.clone()));
    }
    drop(bytes_sender);
    recv_handle.await
}

async fn download_loop(broker: Sender<Event>, mut offsets: (u64, u64), url: Url) -> Fallible<()> {
    let byte_range = format!("bytes={}-{}", offsets.0, offsets.1);
    let mut resp = surf::Request::new(surf::http::Method::GET, url)
        .set_header("Range", byte_range)
        .await
        .unwrap();
    let mut count = 0u64;
    let chunk_size = offsets.1 - offsets.0;
    loop {
        let mut buf = vec![0; chunk_size as usize];
        let byte_count = resp.read(&mut buf).await?;
        count += byte_count as u64;
        buf.truncate(byte_count);
        if !buf.is_empty() {
            broker
                .send(Event::Chunk {
                    start: offsets.0,
                    bytes: buf,
                })
                .await;
            offsets.0 += byte_count as u64;
        } else {
            break;
        }
        if count == chunk_size + 1 {
            break;
        }
    }
    Ok(())
}

async fn progress_loop(
    mut events: Receiver<Event>,
    content_length: Option<u64>,
    fname: String,
) -> Fallible<()> {
    let progress_bar = create_progress_bar(&fname, content_length);
    let mut file = None;

    while let Some(event) = events.next().await {
        match event {
            Event::Chunk { start, bytes } => {
                progress_bar.inc(bytes.len() as u64);
                if file.is_none() {
                    file = Some(BufWriter::new(
                        OpenOptions::new()
                            .write(true)
                            .create(true)
                            .open(&fname)
                            .await?,
                    ));
                }
                if let Some(ref mut f) = file {
                    f.seek(SeekFrom::Start(start)).await?;
                    f.write_all(&bytes).await?;
                    f.flush().await?;
                }
            }
            Event::Error { start, end } => println!("Got error when fetching: {}, {}", start, end),
        }
    }
    progress_bar.finish();
    Ok(())
}

fn get_chunk_offsets(ct_len: u64, chunk_size: u64) -> Vec<(u64, u64)> {
    let no_of_chunks = ct_len / chunk_size;
    let mut sizes = Vec::new();

    for chunk in 0..no_of_chunks {
        let bound = if chunk == no_of_chunks - 1 {
            ct_len
        } else {
            ((chunk + 1) * chunk_size) - 1
        };
        sizes.push((chunk * chunk_size, bound));
    }
    if sizes.is_empty() {
        sizes.push((0, ct_len));
    }

    sizes
}
pub fn parse_url(url: &str) -> Result<Url, ParseError> {
    match Url::parse(url) {
        Ok(url) => Ok(url),
        Err(error) if error == ParseError::RelativeUrlWithoutBase => {
            let url_with_base = format!("{}{}", "http://", url);
            Url::parse(url_with_base.as_str())
        }
        Err(error) => Err(error),
    }
}

pub fn gen_error(msg: String) -> Fallible<()> {
    bail!(msg)
}

pub fn decode_percent_encoded_data(data: &str) -> Fallible<String> {
    let mut unescaped_bytes: Vec<u8> = Vec::new();
    let mut bytes = data.bytes();
    while let Some(b) = bytes.next() {
        match b as char {
            '%' => {
                let bytes_to_decode = &[bytes.next().unwrap(), bytes.next().unwrap()];
                let hex_str = std::str::from_utf8(bytes_to_decode).unwrap();
                unescaped_bytes.push(u8::from_str_radix(hex_str, 16).unwrap());
            }
            _ => {
                unescaped_bytes.push(b);
            }
        }
    }
    Ok(String::from_utf8(unescaped_bytes)?)
}

fn main() {
    match run() {
        Ok(_) => {}
        Err(e) => {
            eprintln!("error: {}", e);
            process::exit(1);
        }
    }
}

fn run() -> Fallible<()> {
    let args = clap_app!(Aduma =>
    (version: crate_version!())
    (author: "Matt Gathu <mattgathu@gmail.com>")
    (about: "A minimal async file downloader")
    (@arg quiet: -q --quiet "quiet (no output)")
    (@arg continue: -c --continue "resume getting a partially-downloaded file")
    (@arg singlethread: -s --singlethread "download using only a single thread")
    (@arg headers: -H --headers "prints the headers sent by the HTTP server")
    (@arg FILE: -O --output +takes_value "write documents to FILE")
    (@arg AGENT: -U --useragent +takes_value "identify as AGENT instead of Aduma/VERSION")
    (@arg SECONDS: -T --timeout +takes_value "set all timeout values to SECONDS")
    (@arg NUM_CONNECTIONS: -n --num_connections +takes_value "maximum number of concurrent connections (default is 8)")
    (@arg URL: +required +takes_value "url to download")
    )
    .get_matches_safe().unwrap_or_else(|e| e.exit());

    let url = parse_url(
        args.value_of("URL")
            .ok_or_else(|| format_err!("missing URL argument"))?,
    )?;
    let _quiet_mode = args.is_present("quiet"); // todo
    let file_name = args.value_of("FILE");

    match url.scheme() {
        "http" | "https" => task::block_on(main_loop(url, file_name.map(|x| x.to_string()))),
        _ => gen_error(format!("unsupported url scheme '{}'", url.scheme())),
    }
}
