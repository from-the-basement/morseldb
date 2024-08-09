use std::{
    hash::Hasher,
    io,
    io::Error,
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::ready;
use pin_project_lite::pin_project;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};

pin_project! {
    pub(crate) struct HashWriter<W: AsyncWrite> {
        hasher: crc32fast::Hasher,
        #[pin]
        writer: W,
    }
}

impl<W: AsyncWrite + Unpin> HashWriter<W> {
    pub(crate) fn new(writer: W) -> Self {
        Self {
            hasher: crc32fast::Hasher::new(),
            writer,
        }
    }

    pub(crate) async fn eol(mut self) -> io::Result<usize> {
        self.writer.write(&self.hasher.finish().to_le_bytes()).await
    }
}

impl<W: AsyncWrite> AsyncWrite for HashWriter<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        let this = self.project();

        Poll::Ready(match ready!(this.writer.poll_write(cx, buf)) {
            Ok(n) => {
                this.hasher.write(&buf[..n]);
                Ok(n)
            }
            e => e,
        })
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().writer.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.project().writer.poll_shutdown(cx)
    }
}

pin_project! {
    pub(crate) struct HashReader<R: AsyncRead> {
        hasher: crc32fast::Hasher,
        #[pin]
        reader: R,
    }
}

impl<R: AsyncRead + Unpin> HashReader<R> {
    pub(crate) fn new(reader: R) -> Self {
        Self {
            hasher: crc32fast::Hasher::new(),
            reader,
        }
    }

    pub(crate) async fn checksum(mut self) -> io::Result<bool> {
        let mut hash = [0; 8];
        self.reader.read_exact(&mut hash).await?;
        let checksum = u64::from_le_bytes(hash);

        Ok(self.hasher.finish() == checksum)
    }
}

impl<R: AsyncRead> AsyncRead for HashReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.project();
        Poll::Ready(match ready!(this.reader.poll_read(cx, buf)) {
            Ok(()) => {
                this.hasher.write(buf.filled());
                Ok(())
            }
            e => e,
        })
    }
}
