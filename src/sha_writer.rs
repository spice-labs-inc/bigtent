use std::{
  /*io::{Error, ErrorKind, Write},*/ io::ErrorKind,
  mem,
  task::Poll,
  thread::{self, JoinHandle},
  time,
};

use anyhow::Result;
use flume::{Receiver, Sender};
use tokio::io::AsyncWrite;

enum ShaWriterMsg {
  Bytes(Vec<u8>),
  Done,
  DigestAndReset(Sender<[u8; 32]>),
}

pub struct ShaWriter {
  initial_capacity: usize,
  backing: Vec<u8>,
  tx: Sender<ShaWriterMsg>,
  join_handle: JoinHandle<()>,
}

impl AsyncWrite for ShaWriter {
  fn poll_write(
    self: std::pin::Pin<&mut Self>,
    _cx: &mut std::task::Context<'_>,
    buf: &[u8],
  ) -> std::task::Poll<std::result::Result<usize, std::io::Error>> {
    let copy = Vec::from(buf);
    self
      .tx
      .send(ShaWriterMsg::Bytes(copy))
      .map_err(|err| std::io::Error::new(ErrorKind::Other, format!("Send error {}", err)))?;

    self.get_mut().backing.extend_from_slice(buf);
    Poll::Ready(Ok(buf.len()))
  }

  fn poll_flush(
    self: std::pin::Pin<&mut Self>,
    _cx: &mut std::task::Context<'_>,
  ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
    Poll::Ready(Ok(()))
  }

  fn poll_shutdown(
    self: std::pin::Pin<&mut Self>,
    _cx: &mut std::task::Context<'_>,
  ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
    Poll::Ready(Ok(()))
  }
}

impl Drop for ShaWriter {
  fn drop(&mut self) {
    self
      .tx
      .send(ShaWriterMsg::Done)
      .expect("Should send message to other thread");
    while !self.join_handle.is_finished() {
      thread::sleep(time::Duration::from_millis(10));
    } // .join().expect("Should be able to join the thread");
  }
}

impl ShaWriter {
  fn service(rx: Receiver<ShaWriterMsg>) -> Result<()> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();

    for msg in rx {
      match msg {
        ShaWriterMsg::Bytes(b) => {
          hasher.update(&b);
        }
        ShaWriterMsg::Done => {
          break;
        }
        ShaWriterMsg::DigestAndReset(tx) => {
          let res: [u8; 32] = hasher.finalize().into();
          hasher = Sha256::new();
          tx.send(res).expect("Should be able to send result");
        }
      }
    }
    Ok(())
  }

  pub fn finish_writing_and_reset(&mut self) -> Result<(Vec<u8>, [u8; 32])> {
    let (tx, rx) = flume::unbounded();
    self.tx.send(ShaWriterMsg::DigestAndReset(tx))?;
    let sha256 = rx.recv()?;
    let mut the_backing = Vec::with_capacity(self.initial_capacity);
    mem::swap(&mut self.backing, &mut the_backing);

    Ok((the_backing, sha256))
  }

  pub fn new(initial_capacity: usize) -> ShaWriter {
    let (tx, rx) = flume::unbounded();
    let join_handle = thread::spawn(move || {
      ShaWriter::service(rx).expect("There should be no failures in the SHA service")
    });
    ShaWriter {
      initial_capacity,
      backing: Vec::with_capacity(initial_capacity),
      tx,
      join_handle,
    }
  }

  pub fn get_buffer(&self) -> &Vec<u8> {
    &self.backing
  }

  pub fn pos(&self) -> u64 {
    self.backing.len() as u64
  }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_shaing() {
  use crate::util::sha256_for_slice;
  use rand::Rng;
  use tokio::io::AsyncWriteExt;

  let mut d1 = Vec::new();
  let mut d2 = ShaWriter::new(1000);
  let mut buf = [0u8; 4096];
  let mut rng = rand::rng();

  for _ in 0..1_0000 {
    rng.fill(&mut buf);
    std::io::Write::write_all(&mut d1, &buf).expect("Should write");
    d2.write_all(&buf).await.expect("Should write shawriter");
  }

  let d1_sha = sha256_for_slice(&d1);
  let (_, d2_sha) = d2
    .finish_writing_and_reset()
    .expect("Should be able to finish");
  assert_eq!(d1_sha, d2_sha);
}
