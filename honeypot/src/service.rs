//! The deceptive `HoneypotService` gRPC implementation.
//!
//! Every trap endpoint returns believable synthetic data and reports the access to the admin
//! service as an intrusion event. No real data or service is ever touched.

use tonic::{Request, Response, Status};

use proto::honeypot::honeypot_service_server::HoneypotService;
use proto::honeypot::{
    BackupChunk, BackupDownloadRequest, BackupListRequest, BackupMetadata, WalletRequest,
    WalletResponse,
};

use crate::reporter::{IntrusionEvent, Reporter};
use crate::traps;

/// Maximum junk-data size a single download will stream, regardless of what the caller asks
/// for (the tarpit cap — keeps the honeypot from being turned into a memory bomb).
const MAX_DOWNLOAD_MB: u32 = 8;
const CHUNK_BYTES: usize = 64 * 1024;

pub struct HoneypotServiceImpl {
    reporter: Reporter,
}

impl HoneypotServiceImpl {
    #[must_use]
    pub fn new(reporter: Reporter) -> Self {
        Self { reporter }
    }

    /// Capture an intrusion event from an incoming request (peer IP + user-agent).
    fn capture<T>(request: &Request<T>, endpoint: &str, method: &str) -> IntrusionEvent {
        let source_ip = request
            .remote_addr()
            .map_or_else(|| "unknown".to_string(), |a| a.ip().to_string());
        let user_agent = request
            .metadata()
            .get("user-agent")
            .and_then(|v| v.to_str().ok())
            .map(ToString::to_string);
        let mut event = IntrusionEvent::new(source_ip, endpoint.to_string(), method.to_string());
        event.user_agent = user_agent;
        event
    }
}

#[tonic::async_trait]
impl HoneypotService for HoneypotServiceImpl {
    async fn get_wallet_balance(
        &self,
        request: Request<WalletRequest>,
    ) -> Result<Response<WalletResponse>, Status> {
        self.reporter
            .report(&Self::capture(
                &request,
                "/wallet/balance",
                "GetWalletBalance",
            ))
            .await;
        Ok(Response::new(WalletResponse {
            address: traps::generate_fake_wallet(),
            balance: "13.37000000".to_string(),
            currency: "BTC".to_string(),
        }))
    }

    type ListBackupsStream = tokio_stream::Iter<std::vec::IntoIter<Result<BackupMetadata, Status>>>;

    async fn list_backups(
        &self,
        request: Request<BackupListRequest>,
    ) -> Result<Response<Self::ListBackupsStream>, Status> {
        self.reporter
            .report(&Self::capture(&request, "/backups", "ListBackups"))
            .await;
        let items: Vec<Result<BackupMetadata, Status>> = traps::generate_fake_backup_list()
            .into_iter()
            .enumerate()
            .map(|(i, name)| {
                Ok(BackupMetadata {
                    name,
                    size_bytes: 1_073_741_824 + u64::try_from(i).unwrap_or(0) * 104_857_600,
                    created_at: "2025-12-08T03:14:00Z".to_string(),
                })
            })
            .collect();
        Ok(Response::new(tokio_stream::iter(items)))
    }

    type DownloadBackupStream = tokio_stream::Iter<std::vec::IntoIter<Result<BackupChunk, Status>>>;

    async fn download_backup(
        &self,
        request: Request<BackupDownloadRequest>,
    ) -> Result<Response<Self::DownloadBackupStream>, Status> {
        let event = Self::capture(&request, "/backups/download", "DownloadBackup");
        self.reporter.report(&event).await;
        let req = request.into_inner();

        // Tarpit: stream junk data in fixed-size chunks, clamped to the cap.
        let mb = req.size_mb.clamp(1, MAX_DOWNLOAD_MB) as usize;
        let total = mb * 1024 * 1024;
        let chunks: Vec<Result<BackupChunk, Status>> = (0..total)
            .step_by(CHUNK_BYTES)
            .map(|offset| {
                let len = CHUNK_BYTES.min(total - offset);
                Ok(BackupChunk {
                    data: vec![0x42u8; len],
                })
            })
            .collect();
        Ok(Response::new(tokio_stream::iter(chunks)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_stream::StreamExt;

    fn service() -> HoneypotServiceImpl {
        // No admin client configured -> reporting is local-only and inert.
        HoneypotServiceImpl::new(Reporter::default())
    }

    #[tokio::test]
    async fn get_wallet_balance_returns_fake_data() {
        let resp = service()
            .get_wallet_balance(Request::new(WalletRequest {
                wallet_id: "any".to_string(),
            }))
            .await
            .expect("wallet")
            .into_inner();
        assert!(resp.address.starts_with("bc1"));
        assert_eq!(resp.currency, "BTC");
    }

    #[tokio::test]
    async fn list_backups_streams_fake_archives() {
        let stream = service()
            .list_backups(Request::new(BackupListRequest {}))
            .await
            .expect("list")
            .into_inner();
        let items: Vec<_> = stream.collect::<Vec<_>>().await;
        assert_eq!(items.len(), 4);
        assert!(items[0].as_ref().unwrap().name.contains(".tar.gz"));
    }

    #[tokio::test]
    async fn download_backup_streams_clamped_junk() {
        let stream = service()
            .download_backup(Request::new(BackupDownloadRequest {
                name: "production_db_2025-12-08.tar.gz".to_string(),
                size_mb: 1,
            }))
            .await
            .expect("download")
            .into_inner();
        let chunks: Vec<_> = stream.collect::<Vec<_>>().await;
        let total: usize = chunks.iter().map(|c| c.as_ref().unwrap().data.len()).sum();
        assert_eq!(total, 1024 * 1024);
        // Tarpit cap: a huge request is clamped.
        let stream = service()
            .download_backup(Request::new(BackupDownloadRequest {
                name: "x".to_string(),
                size_mb: 10_000,
            }))
            .await
            .expect("download")
            .into_inner();
        let chunks: Vec<_> = stream.collect::<Vec<_>>().await;
        let total: usize = chunks.iter().map(|c| c.as_ref().unwrap().data.len()).sum();
        assert_eq!(total, (MAX_DOWNLOAD_MB as usize) * 1024 * 1024);
    }
}
