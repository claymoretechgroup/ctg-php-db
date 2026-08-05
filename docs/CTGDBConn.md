# CTGDBConn

Dedicated PDO connection lifecycle manager. `CTGDBConn` creates and owns one
PDO handle, provides guarded prepared-statement execution, reports connection
state, manages transaction boundaries, and permanently invalidates connections
whose state can no longer be trusted.

It does not build SQL or provide CRUD/query-builder semantics. Those
responsibilities remain with `CTGDB` and `CTGDBQuery`. It does bind supplied
values and consume statement results because keeping those operations inside
the connection boundary prevents PDO handles and live statements from escaping.

### Properties

| Property | Type | Description |
|----------|------|-------------|
| _PDO | ?\PDO | Private PDO handle; set to `null` on invalidation |
| _persistent | bool | Whether persistent PDO pooling was requested |
| _invalidated | bool | Whether the object has been permanently poisoned |

---

## Construction

### CONSTRUCTOR :: ["host" => STRING, "database" => STRING, "username" => STRING, "password" => STRING, "charset"? => STRING, "timeout"? => ?INT, "persistent"? => BOOL] -> ctgdbConn

Creates a PDO MySQL connection with exception error handling, associative
fetching, native prepared statements, and UTF-8 defaults. Configuration uses
one associative array.

```php
use CTG\DB\CTGDBConn;

$connection = new CTGDBConn([
    'host' => 'localhost',
    'database' => 'myapp',
    'username' => 'user',
    'password' => 'pass',
    'charset' => 'utf8mb4',
    'timeout' => 5,
    'persistent' => false,
]);
```

### Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| host | string | yes | — | Database hostname or IP address |
| database | string | yes | — | Database/schema name |
| username | string | yes | — | Database account name |
| password | string | yes | — | Database account password; may be empty |
| charset | string | no | `utf8mb4` | PDO MySQL connection charset |
| timeout | ?int | no | `null` | PDO connection timeout in seconds |
| persistent | bool | no | `false` | Whether PDO persistent pooling is requested |

The optional defaults are exposed as `CTGDBConn::DEFAULT_OPTIONS`. Unknown
fields are rejected so misspelled options cannot silently fall back to an
unsafe or unintended default. Required strings, charset syntax, timeout range,
and persistent-flag type are validated before constructing the PDO DSN.

Authentication failures, timeouts, and general connection failures are mapped
to the existing `CTGDBError` connection types. Passwords are never retained in
instance fields or included in error context.

### CTGDBConn.init :: ["host" => STRING, "database" => STRING, "username" => STRING, "password" => STRING, "charset"? => STRING, "timeout"? => ?INT, "persistent"? => BOOL] -> ctgdbConn

Static factory method. Returns `new static(...)` so connection-aware subclasses
retain the factory behavior.

```php
$connection = CTGDBConn::init([
    'host' => 'localhost',
    'database' => 'myapp',
    'username' => 'user',
    'password' => 'pass',
]);
```

---

## Execution Methods

### ctgdbConn.query :: STRING, ARRAY -> ARRAY

Prepares one SQL statement, binds positional or named values, executes it, and
materializes every returned row without exposing PDO or `PDOStatement`. A
query with no matching rows returns `[]`. Statements that do not produce
columns are rejected with `INVALID_QUERY_STATE`.

`CTGDB::run()` delegates to this row-only boundary. The statement cursor is
closed after all rows have been consumed.

```php
$rows = $connection->query(
    'SELECT id, email FROM users WHERE active = ?',
    [['type' => 'bool', 'value' => true]]
);
```

### ctgdbConn.execute :: STRING, ARRAY -> INT

Prepares and executes one non-row statement and returns its affected-row
count. Row-producing statements are rejected with `INVALID_QUERY_STATE`.
`CTGDB::execute()`, `CTGDB::update()`, and `CTGDB::delete()` delegate to this
boundary.

```php
$affected = $connection->execute(
    'UPDATE users SET active = ? WHERE last_login < ?',
    [false, '2025-01-01']
);
```

### ctgdbConn.insert :: STRING, ARRAY -> INT|STRING

Prepares and executes one insert and returns the last insert identifier.
Row-producing statements are rejected with `INVALID_QUERY_STATE`.
`CTGDB::create()` delegates to this boundary.

```php
$id = $connection->insert(
    'INSERT INTO users (email) VALUES (?)',
    ['user@example.com']
);
```

Connection loss during preparation, execution, fetching, row-count lookup, or
insert-ID lookup permanently invalidates the connection. Constraint and
duplicate-entry errors retain the public `CTGDBError` classifications. Every
statement cursor is closed after its result has been consumed.

### ctgdbConn.process :: STRING, (ARRAY, MIXED -> MIXED), MIXED, ARRAY -> MIXED

Prepares and executes a row-producing statement, passes each row and current
state to the processor, and returns the final state. It does not materialize
rows internally and does not expose the live PDO statement. The cursor is
closed even when the processor throws, and the original processor exception is
preserved. Use `query()` for materialized rows, `execute()` for affected-row
commands, or `insert()` for insert identifiers.

```php
$count = $connection->process(
    'SELECT id FROM users WHERE active = ?',
    fn($row, $result) => $result + 1,
    0,
    [['type' => 'bool', 'value' => true]]
);
```

---

## Transaction Methods

### ctgdbConn.beginTransaction :: VOID -> VOID

Starts a transaction. Nested transactions are rejected with
`INVALID_QUERY_STATE` without poisoning the valid, active transaction.

### ctgdbConn.commit :: VOID -> VOID

Commits the active transaction. Calling it without an active transaction is
rejected with `INVALID_QUERY_STATE`.

If the commit result cannot be confirmed, the connection is invalidated and a
`QUERY_FAILED` error is thrown with `connection_invalidated => true`.

### ctgdbConn.rollBack :: VOID -> VOID

Rolls back the active transaction. Calling it without an active transaction is
rejected with `INVALID_QUERY_STATE`.

If the rollback result cannot be confirmed, the connection is invalidated and
a `QUERY_FAILED` error is thrown with `connection_invalidated => true`.

### ctgdbConn.inTransaction :: VOID -> BOOL

Returns whether PDO reports an active transaction. A failure to determine
transaction state invalidates the connection and throws `CONNECTION_FAILED`.

---

## Lifecycle Methods

### ctgdbConn.invalidate :: VOID -> VOID

Releases the wrapper's PDO handle and permanently poisons the connection
object. The operation is idempotent. Later guarded access fails closed with
`CONNECTION_FAILED`.

Persistent PDO connections must not be used when physical connection disposal
is part of the security guarantee: releasing a persistent handle can return
the underlying connection to PDO's pool.

### ctgdbConn.isInvalidated :: VOID -> BOOL

Reports whether the connection has been permanently invalidated.

### ctgdbConn.isPersistent :: VOID -> BOOL

Reports whether persistent PDO pooling was requested at construction.

---

## PDO Encapsulation

There is no public or protected PDO accessor. `CTGDBConn::query()`,
`execute()`, `insert()`, and `process()` are narrow driver boundaries that
never return a raw PDO handle or live `PDOStatement`. This prevents callers and
subclasses from retaining a driver object that could continue operating after
the wrapper is invalidated.

`CTGDB` composes a `CTGDBConn` and uses `query()` for `run()`, `insert()` for
`create()`, and `execute()` for custom commands, `update()`, and `delete()`. It
exposes the connection's transaction operations, but not persistence state or
invalidation. Callers needing fail-closed invalidation control retain the
connection they inject into `CTGDB`.
