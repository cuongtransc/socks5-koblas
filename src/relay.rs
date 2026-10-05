use std::io::ErrorKind;
use std::time::Duration;
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

const BUF_SIZE: usize = 8 * 1024;

#[derive(Clone, Copy, Debug)]
pub struct Timeouts {
    /// How long both directions may stay silent before the relay ends.
    pub idle: Duration,
    /// How long the open direction may stay silent once the other has closed.
    pub half_close: Duration,
}

/// Copies bytes between `client` and `upstream` until both directions close.
///
/// Unlike `io::copy_bidirectional`, a silent relay ends: after `timeouts.idle` with no bytes
/// either way, or after `timeouts.half_close` with no bytes on the open direction once the
/// other has sent EOF. Without that, a peer that never closes keeps the task (and its client
/// slot) alive forever.
///
/// Returns the bytes sent upstream and received from upstream.
///
/// # Errors
///
/// An I/O error from either socket, or `ErrorKind::TimedOut` when a timeout ends the relay.
pub async fn relay(
    client: &mut TcpStream,
    upstream: &mut TcpStream,
    timeouts: Timeouts,
) -> io::Result<(u64, u64)> {
    let (mut client_read, mut client_write) = client.split();
    let (mut upstream_read, mut upstream_write) = upstream.split();
    let mut client_buf = vec![0u8; BUF_SIZE];
    let mut upstream_buf = vec![0u8; BUF_SIZE];
    let mut client_open = true;
    let mut upstream_open = true;
    let mut sent = 0u64;
    let mut received = 0u64;

    while client_open || upstream_open {
        let wait = if client_open && upstream_open {
            timeouts.idle
        } else {
            timeouts.half_close
        };

        tokio::select! {
            read = client_read.read(&mut client_buf), if client_open => {
                let n = read?;
                if n == 0 {
                    client_open = false;
                    timeout(wait, upstream_write.shutdown()).await.map_err(timed_out)??;
                } else {
                    timeout(wait, upstream_write.write_all(&client_buf[..n])).await.map_err(timed_out)??;
                    sent += n as u64;
                }
            }
            read = upstream_read.read(&mut upstream_buf), if upstream_open => {
                let n = read?;
                if n == 0 {
                    upstream_open = false;
                    timeout(wait, client_write.shutdown()).await.map_err(timed_out)??;
                } else {
                    timeout(wait, client_write.write_all(&upstream_buf[..n])).await.map_err(timed_out)??;
                    received += n as u64;
                }
            }
            () = sleep(wait) => {
                return Err(io::Error::new(ErrorKind::TimedOut, "relay idle"));
            }
        }
    }

    Ok((sent, received))
}

fn timed_out(_: tokio::time::error::Elapsed) -> io::Error {
    io::Error::new(ErrorKind::TimedOut, "relay write stalled")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::time::timeout;

    const TIMEOUTS: Timeouts = Timeouts {
        idle: Duration::from_millis(400),
        half_close: Duration::from_millis(200),
    };
    const GUARD: Duration = Duration::from_secs(5);

    async fn pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (connected, accepted) = tokio::join!(TcpStream::connect(addr), listener.accept());
        (connected.unwrap(), accepted.unwrap().0)
    }

    // client <-> (proxy_client | proxy_upstream) <-> upstream
    async fn setup() -> (TcpStream, TcpStream, TcpStream, TcpStream) {
        let (client, proxy_client) = pair().await;
        let (proxy_upstream, upstream) = pair().await;
        (client, proxy_client, proxy_upstream, upstream)
    }

    #[tokio::test]
    async fn relays_both_ways_until_both_close() {
        let (mut client, mut proxy_client, mut proxy_upstream, mut upstream) = setup().await;
        let proxy =
            tokio::spawn(
                async move { relay(&mut proxy_client, &mut proxy_upstream, TIMEOUTS).await },
            );

        client.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        upstream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");

        upstream.write_all(b"pong!").await.unwrap();
        let mut buf = [0u8; 5];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"pong!");

        client.shutdown().await.unwrap();
        upstream.shutdown().await.unwrap();

        let (sent, received) = timeout(GUARD, proxy).await.unwrap().unwrap().unwrap();
        assert_eq!((sent, received), (4, 5));
    }

    #[tokio::test]
    async fn keeps_relaying_after_one_side_half_closes() {
        let (mut client, mut proxy_client, mut proxy_upstream, mut upstream) = setup().await;
        let proxy =
            tokio::spawn(
                async move { relay(&mut proxy_client, &mut proxy_upstream, TIMEOUTS).await },
            );

        client.write_all(b"request").await.unwrap();
        client.shutdown().await.unwrap();

        let mut request = Vec::new();
        upstream.read_to_end(&mut request).await.unwrap();
        assert_eq!(request, b"request");

        // An answer within the half-close window still reaches the client.
        tokio::time::sleep(TIMEOUTS.half_close / 4).await;
        upstream.write_all(b"response").await.unwrap();
        upstream.shutdown().await.unwrap();

        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        assert_eq!(response, b"response");
        assert!(timeout(GUARD, proxy).await.unwrap().unwrap().is_ok());
    }

    #[tokio::test]
    async fn ends_when_upstream_never_closes_after_client_half_close() {
        let (mut client, mut proxy_client, mut proxy_upstream, _upstream) = setup().await;
        let proxy =
            tokio::spawn(
                async move { relay(&mut proxy_client, &mut proxy_upstream, TIMEOUTS).await },
            );

        client.shutdown().await.unwrap();

        let start = Instant::now();
        let result = timeout(GUARD, proxy).await.expect("relay hung").unwrap();
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::TimedOut);
        assert!(start.elapsed() < TIMEOUTS.idle);
    }

    #[tokio::test]
    async fn ends_when_client_never_closes_after_upstream_half_close() {
        let (_client, mut proxy_client, mut proxy_upstream, mut upstream) = setup().await;
        let proxy =
            tokio::spawn(
                async move { relay(&mut proxy_client, &mut proxy_upstream, TIMEOUTS).await },
            );

        upstream.shutdown().await.unwrap();

        let result = timeout(GUARD, proxy).await.expect("relay hung").unwrap();
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::TimedOut);
    }

    #[tokio::test]
    async fn ends_when_both_sides_stay_silent() {
        let (_client, mut proxy_client, mut proxy_upstream, _upstream) = setup().await;
        let proxy =
            tokio::spawn(
                async move { relay(&mut proxy_client, &mut proxy_upstream, TIMEOUTS).await },
            );

        let start = Instant::now();
        let result = timeout(GUARD, proxy).await.expect("relay hung").unwrap();
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::TimedOut);
        assert!(start.elapsed() >= TIMEOUTS.idle);
    }

    #[tokio::test]
    async fn activity_resets_the_idle_timer() {
        let (mut client, mut proxy_client, mut proxy_upstream, mut upstream) = setup().await;
        let proxy =
            tokio::spawn(
                async move { relay(&mut proxy_client, &mut proxy_upstream, TIMEOUTS).await },
            );

        // Traffic every half idle period for twice the idle period keeps the relay open.
        let mut buf = [0u8; 1];
        for _ in 0..4 {
            tokio::time::sleep(TIMEOUTS.idle / 2).await;
            client.write_all(b"x").await.unwrap();
            upstream.read_exact(&mut buf).await.unwrap();
        }
        assert!(!proxy.is_finished());

        client.shutdown().await.unwrap();
        upstream.shutdown().await.unwrap();
        assert!(timeout(GUARD, proxy).await.unwrap().unwrap().is_ok());
    }
}
