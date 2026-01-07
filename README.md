A distributed object replication system with a centralized control plane and multiple data plane nodes, supporting chunked object ingestion, content-addressed deduplication, and asynchronous migration jobs.

This project demonstrates real-world patterns used in distributed storage systems, control-plane/data-plane separation, and asynchronous job orchestration.

Key Features

Control Plane (FastAPI)
Node registration & discovery
Asynchronous job scheduling
Migration job lifecycle management
Persistent metadata store (SQLite)
Prometheus-style metrics endpoint

Data Plane Nodes
Object ingestion via streaming upload
Chunk-based storage
SHA-256 content-addressed chunks
Manifest-based object reconstruction

Replication & Migration
Pull-based object migration
Chunk deduplication during transfer
Integrity verification via hashes
Async execution with failure isolation

Production-Oriented Design
Dockerized services
Background job runner
Idempotent operations
Clean API contracts

Architecture Overview
                ┌─────────────────────┐
                │   Control Plane     │
                │  (FastAPI + SQLite) │
                │                     │
                │  - Node Registry    │
                │  - Job Scheduler    │
                │  - Migration Logic  │
                └─────────┬───────────┘
                          │
        ┌─────────────────┴─────────────────┐
        │                                   │
┌───────────────┐                   ┌───────────────┐
│ Data Plane    │                   │ Data Plane    │
│ Node 1        │                   │ Node 2        │
│ (Object Store)│                   │ (Object Store)│
│               │                   │               │
│ - Chunks      │   ───────────▶    │ - Chunks      │
│ - Manifests   │   Replication     │ - Manifests   │
└───────────────┘                   └───────────────┘




Project Structure

distributed-data-replication-system/
│
├── control-plane/       
│   ├── api/              
│   ├── services/        
│   ├── db/              
│   └── main.py           
│
├── data-plane/          
│   ├── storage/         
│   └── main.py
│
├── docker/              
├── proto/               
├── scripts/              
├── docker-compose.yml  
└── README.md



Prerequisites
Docker
Docker Compose


Start all services
docker compose up

Services exposed:

Control Plane: http://localhost:8000

Data Node 1: http://localhost:9001

Data Node 2: http://localhost:9002


API Usage Walkthrough
curl -X POST http://localhost:8000/nodes/register \
  -H "Content-Type: application/json" \
  -d '{"name":"node1","base_url":"http://node1:9001"}'

curl -X POST http://localhost:8000/nodes/register \
  -H "Content-Type: application/json" \
  -d '{"name":"node2","base_url":"http://node2:9002"}'


Create Migration Job
curl -X POST http://localhost:8000/jobs/migrate \
  -H "Content-Type: application/json" \
  -d '{"src_node":"node1","dst_node":"node2","object_id":"demo.bin"}'


Track Job Status
curl http://localhost:8000/jobs
curl http://localhost:8000/jobs/1


Technologies Used

Python 3.11
FastAPI
SQLAlchemy
aiohttp
Docker / Docker Compose
SQLite
AsyncIO


Reliability & Design Considerations

Content-addressed chunking avoids duplicate transfers
Jobs are persisted for crash recovery
Control plane failures do not corrupt data plane
Async execution isolates long-running migrations
Clear separation of concerns (control vs data)

Known Limitations
SQLite used for simplicity (can be replaced with Postgres)
No authentication (intentionally omitted)
Single control-plane instance (can be HA-enabled)

Future Improvements
gRPC-based data-plane communication
Parallel chunk transfers
Retry & exponential backoff
Object versioning
Checksummed streaming verification
Leader election for control plane HA
