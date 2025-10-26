# distributed-analytics-engine
- Consistent hashing for shard placement
- gRPC between nodes (local demo over loopback)
- SQLite used as backing store to avoid external deps

## Quickstart
Run two nodes:
```
go run ./cmd/node --addr :7001 --peers :7002
go run ./cmd/node --addr :7002 --peers :7001
```
Query:
```
curl -X POST localhost:7001/query -d '{"sql":"select 1+1"}'
```
