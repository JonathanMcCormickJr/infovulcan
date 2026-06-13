use anyhow::Result;
use tonic::transport::Channel;

pub use proto::db;

#[derive(Clone)]
pub struct DbClient {
    inner: db::database_client::DatabaseClient<Channel>,
}

impl DbClient {
    /// Connect to the database service
    ///
    /// # Errors
    ///
    /// Returns an error if the connection fails.
    pub async fn connect(endpoint: String) -> Result<Self> {
        let channel = proto::tls::connect(&endpoint).await?;
        Ok(Self {
            inner: db::database_client::DatabaseClient::new(channel),
        })
    }

    /// Create a client with a lazy channel (connects on first use).
    /// Useful for tests and situations where the endpoint may not be reachable immediately.
    #[cfg(test)]
    pub(crate) fn new_lazy(endpoint: &'static str) -> Self {
        let channel = Channel::from_static(endpoint).connect_lazy();
        Self {
            inner: db::database_client::DatabaseClient::new(channel),
        }
    }

    /// Put a value into the database
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    pub async fn put(&mut self, collection: &str, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let req = db::PutRequest {
            collection: collection.to_string(),
            key,
            value,
        };
        let resp = self.inner.put(req).await?;
        if resp.get_ref().success {
            Ok(())
        } else {
            anyhow::bail!(resp.get_ref().error.clone())
        }
    }

    /// Get a value from the database
    ///
    /// # Errors
    ///
    /// Returns an error if the operation fails.
    pub async fn get(&mut self, collection: &str, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let req = db::GetRequest {
            collection: collection.to_string(),
            key,
        };
        let resp = self.inner.get(req).await?;
        let r = resp.into_inner();
        if r.found { Ok(Some(r.value)) } else { Ok(None) }
    }

    // ── Domain RPCs (hybrid model: encrypted body + plaintext index fields) ──────

    /// Create a ticket; the DB assigns and returns the ticket id.
    ///
    /// # Errors
    ///
    /// Returns an error if the gRPC call fails.
    pub async fn create_ticket(
        &mut self,
        encrypted_body: Vec<u8>,
        index: db::TicketIndexFields,
    ) -> Result<u64> {
        let resp = self
            .inner
            .create_ticket(db::TicketWrite {
                ticket_id: 0,
                encrypted_body,
                index: Some(index),
            })
            .await?;
        Ok(resp.into_inner().ticket_id)
    }

    /// Fetch a ticket record (encrypted body + soft-delete state). `None` if not found.
    ///
    /// # Errors
    ///
    /// Returns an error if the gRPC call fails for any reason other than `NotFound`.
    pub async fn get_ticket(
        &mut self,
        ticket_id: u64,
        include_deleted: bool,
    ) -> Result<Option<db::TicketRecord>> {
        match self
            .inner
            .get_ticket(db::TicketLookup {
                ticket_id,
                include_deleted,
            })
            .await
        {
            Ok(resp) => Ok(Some(resp.into_inner())),
            Err(status) if status.code() == tonic::Code::NotFound => Ok(None),
            Err(status) => Err(status.into()),
        }
    }

    /// Update an existing ticket.
    ///
    /// # Errors
    ///
    /// Returns an error if the gRPC call fails.
    pub async fn update_ticket(
        &mut self,
        ticket_id: u64,
        encrypted_body: Vec<u8>,
        index: db::TicketIndexFields,
    ) -> Result<()> {
        self.inner
            .update_ticket(db::TicketWrite {
                ticket_id,
                encrypted_body,
                index: Some(index),
            })
            .await?;
        Ok(())
    }

    /// Soft-delete a ticket.
    ///
    /// # Errors
    ///
    /// Returns an error if the gRPC call fails.
    pub async fn soft_delete_ticket(&mut self, ticket_id: u64) -> Result<()> {
        self.inner
            .soft_delete_ticket(db::TicketLookup {
                ticket_id,
                include_deleted: false,
            })
            .await?;
        Ok(())
    }

    /// Query tickets, collecting the streamed records.
    ///
    /// # Errors
    ///
    /// Returns an error if the gRPC call or stream fails.
    pub async fn query_tickets(&mut self, query: db::TicketQuery) -> Result<Vec<db::TicketRecord>> {
        let mut stream = self.inner.query_tickets(query).await?.into_inner();
        let mut records = Vec::new();
        while let Some(record) = stream.message().await? {
            records.push(record);
        }
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use tokio::sync::oneshot;
    use tonic::transport::Server;

    // ── Minimal mock Database service for unit tests ──────────────────────────

    #[derive(Clone)]
    struct MockDbSvc {
        put_success: bool,
        get_found: bool,
    }

    #[tonic::async_trait]
    impl db::database_server::Database for MockDbSvc {
        // --- Domain RPC stubs (real domain behavior is covered via DbClient tests below) ---
        type QueryTicketsStream =
            tokio_stream::Iter<std::vec::IntoIter<Result<db::TicketRecord, tonic::Status>>>;
        async fn create_ticket(
            &self,
            _: tonic::Request<db::TicketWrite>,
        ) -> Result<tonic::Response<db::TicketRecord>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        async fn get_ticket(
            &self,
            _: tonic::Request<db::TicketLookup>,
        ) -> Result<tonic::Response<db::TicketRecord>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        async fn update_ticket(
            &self,
            _: tonic::Request<db::TicketWrite>,
        ) -> Result<tonic::Response<db::TicketRecord>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        async fn soft_delete_ticket(
            &self,
            _: tonic::Request<db::TicketLookup>,
        ) -> Result<tonic::Response<db::DeleteAck>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        async fn query_tickets(
            &self,
            _: tonic::Request<db::TicketQuery>,
        ) -> Result<tonic::Response<Self::QueryTicketsStream>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        async fn create_user(
            &self,
            _: tonic::Request<db::UserWrite>,
        ) -> Result<tonic::Response<db::UserRecord>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        async fn get_user(
            &self,
            _: tonic::Request<db::UserLookup>,
        ) -> Result<tonic::Response<db::UserRecord>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        async fn update_user(
            &self,
            _: tonic::Request<db::UserWrite>,
        ) -> Result<tonic::Response<db::UserRecord>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        async fn soft_delete_user(
            &self,
            _: tonic::Request<db::UserLookup>,
        ) -> Result<tonic::Response<db::DeleteAck>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }

        async fn put(
            &self,
            _req: tonic::Request<db::PutRequest>,
        ) -> Result<tonic::Response<db::PutResponse>, tonic::Status> {
            Ok(tonic::Response::new(db::PutResponse {
                success: self.put_success,
                error: if self.put_success {
                    String::new()
                } else {
                    "mock put failure".to_string()
                },
            }))
        }

        async fn get(
            &self,
            req: tonic::Request<db::GetRequest>,
        ) -> Result<tonic::Response<db::GetResponse>, tonic::Status> {
            Ok(tonic::Response::new(db::GetResponse {
                found: self.get_found,
                value: if self.get_found {
                    req.into_inner().key
                } else {
                    vec![]
                },
                error: String::new(),
            }))
        }

        async fn delete(
            &self,
            _req: tonic::Request<db::DeleteRequest>,
        ) -> Result<tonic::Response<db::DeleteResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }

        async fn list(
            &self,
            _req: tonic::Request<db::ListRequest>,
        ) -> Result<tonic::Response<db::ListResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }

        async fn exists(
            &self,
            _req: tonic::Request<db::ExistsRequest>,
        ) -> Result<tonic::Response<db::ExistsResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }

        async fn batch_put(
            &self,
            _req: tonic::Request<db::BatchPutRequest>,
        ) -> Result<tonic::Response<db::BatchPutResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }

        async fn health(
            &self,
            _req: tonic::Request<db::HealthRequest>,
        ) -> Result<tonic::Response<db::HealthResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }

        async fn cluster_status(
            &self,
            _req: tonic::Request<db::ClusterStatusRequest>,
        ) -> Result<tonic::Response<db::ClusterStatusResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("not needed"))
        }
    }

    async fn start_mock_db(svc: MockDbSvc) -> (SocketAddr, oneshot::Sender<()>) {
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local addr");
        drop(listener);
        let (tx, rx) = oneshot::channel::<()>();
        tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(db::database_server::DatabaseServer::new(svc))
                .serve_with_shutdown(addr, async {
                    let _ = rx.await;
                })
                .await;
        });
        // Wait for the server to accept connections before returning.
        let endpoint = format!("http://{addr}");
        for _ in 0..50 {
            if Channel::from_shared(endpoint.clone())
                .expect("valid uri")
                .connect()
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        (addr, tx)
    }

    #[tokio::test]
    async fn connect_rejects_invalid_endpoint() {
        let result = DbClient::connect("not-a-url".to_string()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn put_and_get_propagate_transport_errors() {
        let channel = Channel::from_static("http://127.0.0.1:9").connect_lazy();
        let mut client = DbClient {
            inner: db::database_client::DatabaseClient::new(channel),
        };

        let put_result = client.put("tickets", b"k".to_vec(), b"v".to_vec()).await;
        assert!(put_result.is_err());

        let channel2 = Channel::from_static("http://127.0.0.1:9").connect_lazy();
        let mut client2 = DbClient {
            inner: db::database_client::DatabaseClient::new(channel2),
        };
        let get_result = client2.get("tickets", b"k".to_vec()).await;
        assert!(get_result.is_err());
    }

    #[tokio::test]
    async fn connect_returns_client_for_valid_endpoint() {
        let (addr, shutdown) = start_mock_db(MockDbSvc {
            put_success: true,
            get_found: false,
        })
        .await;
        let result = DbClient::connect(format!("http://{addr}")).await;
        let _ = shutdown.send(());
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn put_returns_ok_when_server_reports_success() {
        let (addr, shutdown) = start_mock_db(MockDbSvc {
            put_success: true,
            get_found: false,
        })
        .await;
        let mut client = DbClient::connect(format!("http://{addr}"))
            .await
            .expect("connect");
        let result = client
            .put("tickets", b"key".to_vec(), b"value".to_vec())
            .await;
        let _ = shutdown.send(());
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn put_returns_error_when_server_reports_failure() {
        let (addr, shutdown) = start_mock_db(MockDbSvc {
            put_success: false,
            get_found: false,
        })
        .await;
        let mut client = DbClient::connect(format!("http://{addr}"))
            .await
            .expect("connect");
        let result = client
            .put("tickets", b"key".to_vec(), b"value".to_vec())
            .await;
        let _ = shutdown.send(());
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn get_returns_some_when_key_found() {
        let (addr, shutdown) = start_mock_db(MockDbSvc {
            put_success: true,
            get_found: true,
        })
        .await;
        let mut client = DbClient::connect(format!("http://{addr}"))
            .await
            .expect("connect");
        let result = client.get("tickets", b"mykey".to_vec()).await;
        let _ = shutdown.send(());
        let value = result.expect("should succeed").expect("should be found");
        assert_eq!(value, b"mykey"); // mock echoes the key back as value
    }

    #[tokio::test]
    async fn get_returns_none_when_key_not_found() {
        let (addr, shutdown) = start_mock_db(MockDbSvc {
            put_success: true,
            get_found: false,
        })
        .await;
        let mut client = DbClient::connect(format!("http://{addr}"))
            .await
            .expect("connect");
        let result = client.get("tickets", b"missing".to_vec()).await;
        let _ = shutdown.send(());
        assert!(result.expect("should succeed").is_none());
    }

    // ── In-memory domain mock: exercises the hybrid CreateTicket/GetTicket/QueryTickets path ──

    #[derive(Clone, Default)]
    struct DomainMockDb {
        store: std::sync::Arc<std::sync::Mutex<std::collections::BTreeMap<u64, db::TicketRecord>>>,
        seq: std::sync::Arc<std::sync::atomic::AtomicU64>,
    }

    #[tonic::async_trait]
    impl db::database_server::Database for DomainMockDb {
        type QueryTicketsStream =
            tokio_stream::Iter<std::vec::IntoIter<Result<db::TicketRecord, tonic::Status>>>;

        async fn create_ticket(
            &self,
            req: tonic::Request<db::TicketWrite>,
        ) -> Result<tonic::Response<db::TicketRecord>, tonic::Status> {
            let w = req.into_inner();
            let id = self.seq.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            let record = db::TicketRecord {
                ticket_id: id,
                encrypted_body: w.encrypted_body,
                deleted: false,
                deleted_at_unix: 0,
            };
            self.store.lock().unwrap().insert(id, record.clone());
            Ok(tonic::Response::new(record))
        }

        async fn get_ticket(
            &self,
            req: tonic::Request<db::TicketLookup>,
        ) -> Result<tonic::Response<db::TicketRecord>, tonic::Status> {
            let id = req.into_inner().ticket_id;
            self.store
                .lock()
                .unwrap()
                .get(&id)
                .cloned()
                .map(tonic::Response::new)
                .ok_or_else(|| tonic::Status::not_found("ticket not found"))
        }

        async fn update_ticket(
            &self,
            req: tonic::Request<db::TicketWrite>,
        ) -> Result<tonic::Response<db::TicketRecord>, tonic::Status> {
            let w = req.into_inner();
            let record = db::TicketRecord {
                ticket_id: w.ticket_id,
                encrypted_body: w.encrypted_body,
                deleted: false,
                deleted_at_unix: 0,
            };
            self.store
                .lock()
                .unwrap()
                .insert(w.ticket_id, record.clone());
            Ok(tonic::Response::new(record))
        }

        async fn soft_delete_ticket(
            &self,
            req: tonic::Request<db::TicketLookup>,
        ) -> Result<tonic::Response<db::DeleteAck>, tonic::Status> {
            self.store
                .lock()
                .unwrap()
                .remove(&req.into_inner().ticket_id);
            Ok(tonic::Response::new(db::DeleteAck { success: true }))
        }

        async fn query_tickets(
            &self,
            _req: tonic::Request<db::TicketQuery>,
        ) -> Result<tonic::Response<Self::QueryTicketsStream>, tonic::Status> {
            let records: Vec<_> = self
                .store
                .lock()
                .unwrap()
                .values()
                .cloned()
                .map(Ok)
                .collect();
            Ok(tonic::Response::new(tokio_stream::iter(records)))
        }

        async fn create_user(
            &self,
            _: tonic::Request<db::UserWrite>,
        ) -> Result<tonic::Response<db::UserRecord>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        async fn get_user(
            &self,
            _: tonic::Request<db::UserLookup>,
        ) -> Result<tonic::Response<db::UserRecord>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        async fn update_user(
            &self,
            _: tonic::Request<db::UserWrite>,
        ) -> Result<tonic::Response<db::UserRecord>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        async fn soft_delete_user(
            &self,
            _: tonic::Request<db::UserLookup>,
        ) -> Result<tonic::Response<db::DeleteAck>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        async fn put(
            &self,
            _: tonic::Request<db::PutRequest>,
        ) -> Result<tonic::Response<db::PutResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        async fn get(
            &self,
            _: tonic::Request<db::GetRequest>,
        ) -> Result<tonic::Response<db::GetResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        async fn delete(
            &self,
            _: tonic::Request<db::DeleteRequest>,
        ) -> Result<tonic::Response<db::DeleteResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        async fn list(
            &self,
            _: tonic::Request<db::ListRequest>,
        ) -> Result<tonic::Response<db::ListResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        async fn exists(
            &self,
            _: tonic::Request<db::ExistsRequest>,
        ) -> Result<tonic::Response<db::ExistsResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        async fn batch_put(
            &self,
            _: tonic::Request<db::BatchPutRequest>,
        ) -> Result<tonic::Response<db::BatchPutResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        async fn health(
            &self,
            _: tonic::Request<db::HealthRequest>,
        ) -> Result<tonic::Response<db::HealthResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
        async fn cluster_status(
            &self,
            _: tonic::Request<db::ClusterStatusRequest>,
        ) -> Result<tonic::Response<db::ClusterStatusResponse>, tonic::Status> {
            Err(tonic::Status::unimplemented("mock"))
        }
    }

    async fn start_domain_mock() -> (SocketAddr, oneshot::Sender<()>) {
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local addr");
        drop(listener);
        let (tx, rx) = oneshot::channel::<()>();
        tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(db::database_server::DatabaseServer::new(
                    DomainMockDb::default(),
                ))
                .serve_with_shutdown(addr, async {
                    let _ = rx.await;
                })
                .await;
        });
        let endpoint = format!("http://{addr}");
        for _ in 0..50 {
            if Channel::from_shared(endpoint.clone())
                .expect("valid uri")
                .connect()
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        (addr, tx)
    }

    fn sample_index() -> db::TicketIndexFields {
        db::TicketIndexFields {
            status: 1,
            account_uuid: "acct".to_string(),
            assigned_to_uuid: None,
            project: "proj".to_string(),
            tracking_url: None,
            created_at_unix: 0,
            updated_at_unix: 0,
        }
    }

    #[tokio::test]
    async fn create_ticket_returns_assigned_id_and_get_roundtrips() {
        let (addr, shutdown) = start_domain_mock().await;
        let mut client = DbClient::connect(format!("http://{addr}"))
            .await
            .expect("connect");

        let id1 = client
            .create_ticket(b"body-1".to_vec(), sample_index())
            .await
            .expect("create");
        let id2 = client
            .create_ticket(b"body-2".to_vec(), sample_index())
            .await
            .expect("create");
        assert_eq!((id1, id2), (1, 2));

        let record = client
            .get_ticket(id1, false)
            .await
            .expect("get")
            .expect("present");
        assert_eq!(record.encrypted_body, b"body-1");

        let all = client
            .query_tickets(db::TicketQuery::default())
            .await
            .expect("query");
        assert_eq!(all.len(), 2);

        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn get_ticket_returns_none_for_missing() {
        let (addr, shutdown) = start_domain_mock().await;
        let mut client = DbClient::connect(format!("http://{addr}"))
            .await
            .expect("connect");
        let missing = client.get_ticket(999, false).await.expect("get");
        let _ = shutdown.send(());
        assert!(missing.is_none());
    }
}
