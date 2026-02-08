# Requirements: Syslog Server - Production-Ready Centralized Logging System

> **Architecture**: Production-ready từ đầu với Kafka + ClickHouse

## 1. Overview

Hệ thống Syslog Server là một giải pháp **production-grade** tập trung hóa việc thu thập, lưu trữ và phân tích log từ nhiều nguồn (thiết bị mạng, servers, applications) theo chuẩn RFC 5424.

**Kiến trúc cốt lõi:**
- **Ingestion**: Syslog receivers (UDP/TCP/TLS) → Apache Kafka
- **Schema Management**: Confluent Schema Registry với Avro
- **Storage**: Dual-write strategy
  - PostgreSQL: Hot storage (7 ngày) cho real-time queries
  - ClickHouse: Cold storage (1 năm) cho historical analysis
- **Dashboard**: Next.js + WebSocket cho real-time monitoring

## 2. User Stories

- **As a System Administrator**, I want to collect logs from all network devices centrally so that I can troubleshoot issues faster.
- **As a Developer**, I want real-time log streaming on a dashboard so that I can debug during deployment immediately.
- **As a Security Engineer**, I want alerts for critical errors (Severity 0-2) so that I can respond to security threats.
- **As an Auditor**, I want to search historical logs (up to 1 year) for compliance requirements.
- **As a DevOps Engineer**, I want the system to handle 50k msg/s bursts without data loss.
- **As a Compliance Officer**, I want tamper-proof logs with integrity verification (Merkle Tree).

## 3. Technical Requirements

### 3.1 Ingestion Layer

#### 3.1.1 Syslog Receivers (Go with gnet)
- **Networking Framework**: `gnet` (event-driven, non-blocking)
  - **Architecture**: Multiple Reactors pattern
  - **Mechanism**: epoll (Linux) / kqueue (BSD/macOS)
  - **Advantage**: Hàng triệu kết nối đồng thời với memory footprint cực thấp
  - **Anti-DoS**: Non-blocking queue ngay cả khi bị Log DoS attack
- **Protocol Support**: RFC 5424 compliant
- **Transport**:
  - UDP listener on port 514 (fast, connectionless)
  - TCP listener on port 514 (reliable delivery)
  - TLS listener on port 6514 (encrypted transport)
- **Parser**: Extract all RFC 5424 fields (PRI, VERSION, TIMESTAMP, HOSTNAME, APP-NAME, PROCID, MSGID, SD, MSG)
  - Library: `go-syslog` hoặc custom parser
- **Structured Logging**: `Zap` (Uber) cho zero-allocation logging
  - Giảm GC pressure
  - Sub-microsecond latency
  - Structured JSON output cho audit trail
- **Performance**: Handle 10k msg/s sustained, 50k burst
- **Concurrency**: Worker pool từ gnet với configurable reactor count

#### 3.1.2 Message Queue (Apache Kafka)
- **Topic**: `syslog-events`
  - Partitions: 12 (scalable to 24)
  - Replication factor: 3
  - Retention: 7 days
- **Partitioning Strategy**: Semantic partitioning by `hostname`
  - Ensures causal ordering per host
  - Prevents message reordering for same entity
- **Producer Config**:
  - Acknowledgment: `acks=all` (durability)
  - Compression: `lz4` (balance speed/ratio)
  - Idempotence: enabled (exactly-once semantics)
- **Dead Letter Queue**:
  - Topic: `syslog-dlq`
  - For messages failing after 3 retries
  - Alert ops when DLQ size > 100 messages

### 3.2 Data Transformation Layer

#### 3.2.1 Vector.dev (Data Aggregator & Router)
- **Purpose**: Transform, enrich, và route log data
- **Language**: Rust (performance gần C++, memory-safe)
- **Capabilities**:
  - **VRL (Vector Remap Language)**: Parse và transform dữ liệu
  - **Geo-IP Enrichment**: Gắn thông tin địa lý từ IP address
  - **PII Filtering**: Loại bỏ hoặc mask sensitive fields trước khi lưu
  - **Backpressure Management**: Tự động buffer khi downstream chậm
  - **Disk Buffering**: Lưu tạm vào disk nếu Kafka/ClickHouse không khả dụng
- **Pipeline**:
  ```
  Kafka Topic (syslog-events) → Vector → Transform (VRL) → Router
    ├─> PostgreSQL (hot)
    ├─> ClickHouse (cold)
    └─> Redis (real-time)
  ```
- **Example VRL Transform**:
  ```coffeescript
  # Parse RFC 5424 và thêm Geo location
  .parsed = parse_syslog!(.message)
  .geo = get_enrichment_table_record!("geoip", { "ip": .parsed.hostname })
  .parsed.country = .geo.country_code
  
  # Mask PII
  if exists(.parsed.email) {
    .parsed.email = redact(.parsed.email, filters: ["email"])
  }
  ```

### 3.3 Schema Management

#### 3.3.1 Schema Registry (Confluent)
- **Schema Format**: Avro (binary serialization)
- **Compatibility**: Forward compatibility
  - Allows adding new fields without breaking consumers
  - Old consumers ignore unknown fields
- **Validation**: Producer validates against schema before sending
- **Versioning**: Automatic schema versioning

#### 3.2.2 SyslogMessage Schema

```json
{
  "type": "record",
  "name": "SyslogMessage",
  "namespace": "com.syslog.v1",
  "fields": [
    {"name": "event_time", "type": "long", "logicalType": "timestamp-millis"},
    {"name": "facility", "type": "int"},
    {"name": "severity", "type": "int"},
    {"name": "hostname", "type": "string"},
    {"name": "app_name", "type": ["null", "string"], "default": null},
    {"name": "process_id", "type": ["null", "string"], "default": null},
    {"name": "message_id", "type": ["null", "string"], "default": null},
    {"name": "structured_data", "type": ["null", "string"], "default": null},
    {"name": "message", "type": "string"},
    {"name": "raw_message", "type": "string"}
  ]
}
```

### 3.3 Storage Layer

#### 3.3.1 PostgreSQL (Hot Storage)
- **Purpose**: Real-time queries cho dashboard
- **Retention**: 7 ngày (auto-delete older records)
- **Table**: `syslog_messages`
  - Columns: id, event_time, facility, severity, hostname, app_name, process_id, message_id, structured_data, message, raw_message
  - Indexes: 
    - `idx_event_time` (BRIN for time-series)
    - `idx_severity` (B-tree)
    - `idx_hostname` (B-tree)
    - `idx_message_gin` (GIN for full-text search)
- **Partitioning**: By day (PARTITION BY RANGE)
- **Consumer**: Kafka consumer group `pg-consumer`

#### 3.3.2 ClickHouse (Cold Storage)
- **Purpose**: Long-term storage và analytics
- **Retention**: 1 năm
- **Cluster**: 3-node cluster với replication
- **Table Engine**: `ReplicatedReplacingMergeTree`
  - Deduplication by `_version` field
  - Partition by month: `PARTITION BY toYYYYMM(event_time)`
  - **Ordering Key** (optimized): `ORDER BY (tenant_id, service_name, toStartOfHour(event_time), severity)`
    - **Lý do**: Query thường filter theo tenant + service → Sparse index skip billions rows
- **Data Types**:
  - `LowCardinality(String)` cho severity, facility, hostname, app_name
  - **Lợi ích**: Nén hiệu quả bằng dictionary encoding
- **Compression**: ZSTD(3) cho `message` field, LZ4 cho metadata
- **Materialized Views** (Pre-aggregated Analytics):
  - `logs_stats_by_minute`: COUNT(*), AVG(severity) GROUP BY toStartOfMinute(event_time)
  - `logs_by_severity`: COUNT(*) GROUP BY severity, toStartOfHour(event_time)
  - **Lợi ích**: Dashboard query pre-computed tables thay vì scan raw logs
- **Tiered Storage**:
  - Hot tier (SSD): 30 ngày gần nhất
  - Cold tier (S3): Ngày 31 → 1 năm
- **Consumer**: Vector.dev (thay vì Kafka consumer trực tiếp)

### 3.4 Real-time Layer

#### 3.4.1 Redis
- **Purpose**: Real-time log broadcasting
- **Pattern**: Pub/Sub
  - Channel: `syslog-realtime`
  - TTL: 5 minutes (in-memory only)
- **Consumer**: Kafka consumer group `redis-consumer`

#### 3.4.2 WebSocket Server (Go)
- **Port**: 8080/ws
- **Protocol**: WebSocket
- **Source**: Subscribe to Redis channel
- **Auth**: JWT token validation
- **Rate limit**: 1000 messages/second per client

### 3.5 API & Dashboard

#### 3.5.1 REST API (Go)
- **Endpoints**:
  - `GET /api/logs` - Query logs (auto-route: PG for recent, CH for old)
  - `GET /api/stats` - Aggregation stats
  - `GET /api/health` - Health check
- **Query Optimization**:
  - Time range < 7 days → Query PostgreSQL
  - Time range ≥ 7 days → Query ClickHouse
  - Pagination: max 1000 rows per request
- **Authentication**: JWT-based

#### 3.5.2 Dashboard (Next.js)
- **Features**:
  - Real-time log streaming (WebSocket)
  - Historical search (REST API)
  - Severity filtering (Emergency → Debug)
  - Hostname filtering (autocomplete)
  - Time range picker (last 1 hour → 1 year)
  - Full-text search
  - Export to CSV/JSON
  - **Trace Correlation**: Click log entry → View distributed trace
- **Performance Optimization**:
  - **TanStack Virtual**: List virtualization cho hàng triệu rows
    - Chỉ render visible items + overscan buffer
    - Reuse DOM elements → 60 FPS scrolling
    - Handle 100k+ rows mượt mà
  - **TanStack Table v8**: Advanced filtering, sorting, column grouping
- **Visualization**:
  - **ECharts** hoặc **Recharts**: Time-series charts, heatmaps
  - Pre-aggregated data từ ClickHouse Materialized Views
- **UI Components**: shadcn/ui
- **Styling**: Tailwind CSS with dark mode

## 4. Resilience & Error Handling

### 4.1 Dead Letter Queue (DLQ)
- **Trigger**: Message fails after 3 retries
- **Action**: 
  - Route to `syslog-dlq` topic
  - Log error with original message + stack trace
  - Send alert to ops team (PagerDuty)
- **Monitoring**: Alert if DLQ size > 100 messages

### 4.2 Poison Pill Handling
- **ErrorHandlingDeserializer**: Wrap Avro deserializer
- **Behavior**: 
  - Catch deserialization exceptions
  - Log malformed message
  - Skip to next message (no crash loop)

### 4.3 Circuit Breaker
- **PostgreSQL**: 
  - If connection fails → Circuit open → Buffer in Kafka (retention 7 days)
  - Auto-retry every 30 seconds
- **ClickHouse**: 
  - If insert fails → Circuit open → Backlog in Kafka
  - Alert ops if backlog > 1 million messages

## 5. Security & Compliance

### 5.1 GDPR Compliance (Crypto-Shredding)
- **Challenge**: Audit logs bất biến vs Right to be Forgotten
- **Solution**:
  - Encrypt PII fields (email, IP) with user-specific key
  - Store keys in Key Management Service (KMS)
  - On deletion request: Delete key in KMS (not log data)
  - Result: PII becomes unreadable gibberish
- **Implementation**:
  - KMS: HashiCorp Vault
  - Algorithm: AES-256-GCM
  - Key rotation: Every 90 days

### 5.2 Tamper-Proof (Merkle Tree)
- **Purpose**: Prove logs haven't been altered (tamper-evident)
- **Implementation Details**:
  - **Leaf Nodes**: Mỗi log entry được hash với SHA-256
  - **Tree Construction**: Pair-wise hashing đến khi có Merkle Root
  - **Batch Size**: 10 phút hoặc 100,000 log entries (configurable)
  - **Digital Signature**: Sign Merkle Root với Ed25519 private key
  - **Anchoring**: Publish signed root to immutable storage:
    - Option 1: Amazon S3 với Object Lock (WORM mode)
    - Option 2: Blockchain (Ethereum, Hyperledger)
  - **Inclusion Proof**: Cung cấp path từ leaf → root để verify (log2(N) hashes)
- **Verification Process**:
  1. Auditor request proof cho log entry X
  2. System returns: log X + sibling hashes + Merkle Root + signature
  3. Auditor recomputes hash từ X lên root
  4. Auditor verify signature với public key
  5. Auditor đối chiếu root với bản lưu trên S3/blockchain
- **Security Guarantee**: Ngay cả khi attacker có root access, không thể sửa log mà không bị phát hiện

### 5.3 Access Control
- **RBAC**: Role-based access control
  - Admin: Full access
  - Developer: Read-only, filtered by app_name
  - Auditor: Read-only, no PII fields
- **Row-Level Security** (ClickHouse):
  ```sql
  CREATE ROW POLICY filter_by_team ON syslog_db.logs
  FOR SELECT
  USING team = currentUser()
  TO developer_role;
  ```

## 6. Performance & Scalability

### 6.1 Throughput Targets
- **Sustained**: 10,000 msg/s
- **Burst**: 50,000 msg/s (5 minutes)
- **Latency**: 
  - Ingestion → Kafka: < 5ms (p99)
  - Kafka → Storage: < 50ms (p99)
  - Dashboard update: < 100ms (p99)

### 6.2 Resource Sizing

**Kafka Cluster (3 brokers):**
- CPU: 8 vCPU per broker
- RAM: 32 GB per broker
- Disk: 1 TB NVMe SSD per broker

**PostgreSQL (Primary + Replica):**
- CPU: 16 vCPU
- RAM: 64 GB
- Disk: 500 GB SSD (rotating 7 days)

**ClickHouse Cluster (3 nodes):**
- CPU: 16 vCPU per node
- RAM: 128 GB per node
- Disk: 5 TB SSD (hot) + S3 (cold)

**Estimated Costs** (AWS us-east-1):
- Kafka: ~$500/month
- PostgreSQL: ~$300/month
- ClickHouse: ~$800/month
- S3: ~$50/month (1 year retention, 30TB)
- **Total: ~$1,650/month**

## 7. Edge Cases

### 7.1 Data Integrity
- **Duplicate Messages**: ClickHouse ReplacingMergeTree auto-deduplicates
- **Message Loss**: Kafka replication factor 3 ensures durability
- **Out-of-Order**: Partitioning by hostname ensures ordering per entity

### 7.2 Operational
- **Kafka Broker Failure**: Auto-failover to replica
- **Hot Partition**: Monitor lag; re-partition if needed
- **ClickHouse "Too Many Parts"**: Monitor `system.parts`; tune batch size
- **Schema Evolution**: Forward compatibility allows gradual rollout

### 7.3 Network
- **UDP Packet Loss**: Acceptable for syslog (best-effort delivery)
- **TCP Backpressure**: Buffered channels in Go receiver
- **TLS Handshake Overhead**: Connection pooling

## 8. Monitoring & Observability

### 8.1 Metrics (Prometheus)
- **Ingestion**:
  - `syslog_messages_received_total` (counter by protocol)
  - `syslog_parse_errors_total` (counter by error type)
- **Kafka**:
  - `kafka_producer_record_send_total`
  - `kafka_consumer_lag` (by partition)
- **Storage**:
  - `pg_insert_duration_seconds` (histogram)
  - `ch_insert_duration_seconds` (histogram)
  - `dlq_size` (gauge)

### 8.2 Dashboards (Grafana)
- **Overview**: Throughput, latency, error rate
- **Kafka**: Partition lag, consumer group health
- **Storage**: Insert rate, query latency, disk usage
- **Alerts**: DLQ size, circuit breaker open, partition lag > 10k

### 8.3 Logging
- **Format**: Structured JSON
- **Level**: INFO (production), DEBUG (development)
- **Destination**: stdout → Fluentd → Elasticsearch

## 9. Deployment

### 9.1 Infrastructure
- **Platform**: Kubernetes (EKS/GKE)
- **Namespaces**: 
  - `syslog-prod` (production)
  - `syslog-staging` (testing)

### 9.2 Services
- **syslog-receiver**: StatefulSet (sticky sessions for TLS)
- **kafka**: Helm chart (Strimzi operator)
- **schema-registry**: Deployment
- **postgresql**: StatefulSet with persistent volumes
- **clickhouse**: StatefulSet (ClickHouse Operator)
- **api-server**: Deployment (horizontal scaling)
- **dashboard**: Deployment (CDN for static assets)

### 9.3 CI/CD
- **Pipeline**: GitLab CI
- **Stages**: Build → Test → Security Scan → Deploy
- **Tests**: Unit (80%+ coverage), Integration, Load test
- **Deployment**: Blue-green deployment

## 10. Success Criteria

### 10.1 Functional
- ✅ Receive logs via UDP/TCP/TLS (RFC 5424)
- ✅ Parse and validate with Avro schema
- ✅ Store in dual-storage (PG + CH)
- ✅ Real-time dashboard updates (< 100ms)
- ✅ Historical search (1 year retention)
- ✅ GDPR compliance (crypto-shredding)
- ✅ Tamper-proof (Merkle Tree)

### 10.2 Non-Functional
- ✅ Throughput: 10k msg/s sustained, 50k burst
- ✅ Latency: < 100ms end-to-end (p99)
- ✅ Availability: 99.9% uptime
- ✅ Data loss: Zero (Kafka durability)
- ✅ Cost: < $2,000/month (AWS)

## 11. Assumptions

- Team có kinh nghiệm vận hành Kafka và ClickHouse
- Budget cho infrastructure (~$1,650/month)
- Kubernetes cluster đã có sẵn
- Network cho phép UDP/TCP/TLS inbound traffic
- Syslog clients tuân thủ RFC 5424 (hoặc gần đúng)


## 2. User Stories
- **As a System Administrator**, I want to collect logs from all network devices in one place so that I can troubleshoot issues faster.
- **As a Developer**, I want to see my application logs in real-time on a dashboard so that I can debug errors immediately during deployment.
- **As a Security Engineer**, I want to be alerted when critical system errors (Severity 0-2) occur so that I can respond to potential security threats.
- **As an Auditor**, I want to search and filter historical logs by hostname and time range to comply with compliance requirements.
- **As a DevOps Engineer**, I want the system to handle traffic spikes (10k+ msg/s) without losing messages.

## 3. Technical Requirements

### 3.1 Phase 1: MVP (Learning Focus)

#### 3.1.1 Receiver (Backend - Go)
- **Protocol Support**: Hỗ trợ đầy đủ định dạng RFC 5424.
- **Transport**:
  - UDP (Port 514): Cho các gói tin log không yêu cầu độ tin cậy kết nối.
  - TCP (Port 514): Cho truyền tải log tin cậy.
  - TLS (Port 6514): Cho truyền tải log mã hóa an toàn.
- **Performance**: Xử lý được ít nhất 1,000 messages/giây (MVP target).
- **Concurrency**: Sử dụng Goroutines và Worker Pool để xử lý song song.

#### 3.1.2 Storage & Analysis (MVP)
- **Database**: PostgreSQL để lưu trữ log có cấu trúc (7 ngày retention).
- **Indexing**: Index vào `timestamp`, `severity`, `hostname`, `app_name`.
- **Real-time**: Redis Pub/Sub để phát tán log mới đến frontend qua WebSocket.

#### 3.1.3 Dashboard (Frontend - Next.js)
- **Log Streaming**: Hiển thị log mới ngay lập tức mà không cần refresh.
- **Filtering**: Lọc theo Severity (Emergency → Debug).
- **Search**: Tìm kiếm full-text trong nội dung message.
- **Visuals**: Màu sắc phân biệt severity levels.

### 3.2 Phase 2: Production-Ready (Scale Focus)

> ⚠️ **Architecture Change**: Migrate sang Kafka + ClickHouse để giải quyết vấn đề "Vacuum Death Spiral" và cost optimization.

#### 3.2.1 Message Queue Layer (NEW)
- **Apache Kafka**:
  - Topic: `syslog-events` (partitioned by hostname)
  - Đảm bảo message ordering per hostname (Semantic Partitioning)
  - Dead Letter Queue (DLQ) topic: `syslog-dlq` cho malformed messages
  - Retention: 7 days (buffer)

#### 3.2.2 Schema Management (NEW)
- **Schema Registry**:
  - Avro schema cho SyslogMessage
  - Forward compatibility strategy
  - Producer validation: Từ chối message không đúng schema
  - **Lý do**: Tránh "Data Swamp" - sau 2 năm không ai nhớ trường `ts` là giây hay mili-giây

#### 3.2.3 Dual Storage Strategy
- **PostgreSQL** (Hot Storage):
  - Retention: 7 ngày
  - Use case: Real-time dashboard queries (<100ms latency)
  - Auto-archival: Kafka consumer chuyển data sang ClickHouse sau 7 ngày
  
- **ClickHouse** (Cold Storage):
  - Retention: 1 năm
  - Use case: Historical analysis, compliance reports
  - Columnar compression: ~10-20x vs PostgreSQL
  - Tiered Storage: SSD (30 days) → S3 (1 year)

#### 3.2.4 Resilience & Error Handling (NEW)
- **Dead Letter Queue**:
  - Retry failed messages 3 times
  - Sau đó route vào DLQ topic
  - Alert ops team khi DLQ size > threshold
  
- **Poison Pill Handling**:
  - ErrorHandlingDeserializer wrapper
  - Tránh consumer crash loop

#### 3.2.5 Security & Compliance (NEW)
- **Crypto-Shredding for GDPR**:
  - PII fields (email, IP) được encrypt với user-specific key
  - Khi user yêu cầu xóa → Xóa key trong KMS (không xóa log)
  - Log vẫn tồn tại nhưng PII trở thành gibberish
  
- **Tamper-Proof Mechanism**:
  - Merkle Tree: Compute hourly hash root
  - Publish root to immutable storage (blockchain/WORM)
  - Auditor có thể verify log integrity

## 4. Edge Cases

### 4.1 MVP Edge Cases
- **Buffer Overflow**: Xử lý khi lượng log vượt quá worker pool capacity (buffered channels).
- **Malformed Messages**: Không crash server; log lỗi và discard hoặc lưu vào `corrupted_logs` table.
- **Database Downtime**: Tạm lưu trong Redis queue nếu PostgreSQL ngắt kết nối.
- **Very Long Messages**: Xử lý messages > 2KiB.

### 4.2 Production Edge Cases
- **Kafka Broker Failure**: Consumer tự động failover sang replica broker.
- **Hot Partition**: Monitor partition lag; alert nếu 1 partition nhận > 2x avg messages.
- **ClickHouse "Too Many Parts"**: Monitor `system.parts`; alert nếu active parts > 300.
- **Schema Evolution**: Producer thêm trường mới không làm crash Consumer cũ (Forward Compatibility).

## 5. Constraints & Success Metrics

### 5.1 MVP Metrics
- **Latency**: Message processing < 50ms (99th percentile)
- **Throughput**: 1,000 messages/second sustained
- **Dashboard**: < 200ms response time
- **Uptime**: 99% availability

### 5.2 Production Metrics
- **Latency**: < 10ms processing, < 100ms dashboard update
- **Throughput**: 10,000 messages/second sustained, 50k burst
- **Storage Cost**: < $50/TB/month (with ClickHouse compression)
- **Uptime**: 99.9% availability
- **Data Loss**: Zero message loss (Kafka durability)

## 6. Assumptions

### 6.1 MVP Assumptions
- Client gửi log tuân thủ RFC 5424 (hoặc gần đúng).
- Network hỗ trợ UDP và TCP đến server.
- Traffic < 1,000 msg/s (Development/Testing environment).
- Storage cho 7 ngày log là đủ.

### 6.2 Production Assumptions
- Traffic có thể burst lên 50k msg/s (production environment).
- Cần lưu trữ 1 năm cho compliance.
- Budget cho Kafka cluster (3 brokers) + ClickHouse cluster (3 nodes).
- DevOps team có kinh nghiệm vận hành Kafka và ClickHouse.

## 7. Migration Strategy (MVP → Production)

**Week 1-3**: Implement MVP (PostgreSQL + Redis)
- Focus: Học AI-assisted workflow
- Deliverable: Working Syslog server với real-time dashboard

**Week 4-5**: Add Kafka Layer
- Refactor: Receivers → Kafka → PostgreSQL
- Implement DLQ
- Test throughput với 10k msg/s

**Week 6-7**: Add ClickHouse
- Setup cluster
- Dual-write: Kafka → both PG + CH
- Configure auto-archival (7 days TTL in PG)

**Week 8**: Production Hardening
- Schema Registry integration
- Crypto-shredding implementation
- Merkle Tree integrity verification
- Monitoring & Alerting (Grafana + Prometheus)

## 8. Non-Functional Requirements

### 8.1 Code Quality
- **Test Coverage**: 80%+ (unit + integration tests)
- **Documentation**: Swagger/OpenAPI cho REST API
- **Logging**: Structured logging (JSON format)

### 8.2 Deployment
- **Containerization**: Docker images cho tất cả services
- **Orchestration**: Docker Compose (MVP), Kubernetes (Production)
- **CI/CD**: GitLab CI pipeline với automated tests

### 8.3 Monitoring
- **Metrics**: Prometheus metrics cho throughput, latency, error rate
- **Dashboards**: Grafana dashboards cho ops team
- **Alerts**: PagerDuty integration cho critical errors
