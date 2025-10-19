# etcd-light

etcd-light is a lightweight version of etcd that is not feature complete and does not tolerate failures. It implements only what was needed to explore optimization strategies, and is not meant for production usage. etcd-light focuses on improvement strategies such as partitioned WAL files, partitioned databases, and fast path writes which don't wait on the database to respond to the client.

## Building and running

etcd-light should only be built and ran on linux.
go version must be 1.23.0 or later
The server's `-max-db-index` flag must be greater than the client's `-ops`

### Server flags
- `-node` *(int)* — Node index, must start at 0 and be incrementing across machines
- `-peer-connections` *(int)* — Number of peer connections
- `-peer-listen` *(address)* — Peer listen address (e.g. `0.0.0.0:0000`)
- `-client-listen` *(address)* — Client listen address (e.g. `0.0.0.0:0000`)
- `-peer-addresses` *([]address)* — List of peer addresses, comma separated (e.g. `0.0.0.0:0001,0.0.0.0:0002`)
- `-wal-file-count` *(int)* — Number of WAL files to partition across
- `-fsync` *(bool)* — Enable or disable fsync call after hardstate files or WAL is written to
- `-memory` *(bool)* — Enable or disable file accessing
- `-fast-path-writes` *(bool)* — Enable or disable waiting for database to apply before responding to client
- `-num-dbs` *(int)* — Number of databases to partition across
- `-max-db-index` *(int)* — Maximum index for database to partition from
- `-profile` *(bool)* — Enable or disable profiling

### Client flags
- `-addresses` *([]address)* — List of node addresses, comma separated (e.g. `0.0.0.0:0001,0.0.0.0:0002`)
- `-data-size` *(int)* — Size of data in bytes
- `-ops` *(int)* — Number of operations total
- `-read-ratio` *(decimal)* — Ratio of read operations, *(0.0-1.0)*
- `-clients` *(int)* — Number of concurrent connections
- `-read-mem` *(bool)* — Read from memory only
- `-write-mem` *(bool)* — Write to memory only
- `-find-leader` *(bool)* — Enforces leader only communication