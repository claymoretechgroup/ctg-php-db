# CTGDB

Main database API operating over a composed `CTGDBConn`, with safe CRUD
methods, materialized execution through `run()`, custom non-row execution
through `execute()`, and incremental row handling through `process()`.
`CTGDBConn` owns PDO construction, guarded statement execution, connection
state, invalidation, and low-level transaction mechanics. All queries use PDO
prepared statements.
Identifiers are regex-validated. Keywords are allowlisted.
Read queries should use `CTGDBQuery` (see [CTGDBQuery.md](CTGDBQuery.md))
for safe-by-default, structured query building — it validates all
identifiers, allowlists all operators, and parameterizes all values by
construction.

## Construction

### CONSTRUCTOR :: ctgdbConn -> ctgdb

Creates the main database API over an existing `CTGDBConn`. This keeps `CTGDB`
stateless while all CRUD and query operations use the supplied connection.

```php
$connection = CTGDBConn::init([
    'host' => 'localhost',
    'database' => 'myapp',
    'username' => 'user',
    'password' => 'pass',
    'charset' => 'utf8mb4',
    'timeout' => 5,
    'persistent' => false,
]);
$db = new CTGDB($connection);
```

### CTGDB.init :: ["host" => STRING, "database" => STRING, "username" => STRING, "password" => STRING, "charset"? => STRING, "timeout"? => ?INT, "persistent"? => BOOL] -> ctgdb

Convenience factory that creates a validated `CTGDBConn` from the config and
passes it to `new static(...)`. This is the concise path for callers that need
ordinary query and CRUD APIs. Transactional callers construct and retain a
`CTGDBConn` so their application coordinator can operate on the same connection
used by `CTGDB`.

```php
$db = CTGDB::init([
    'host' => 'localhost',
    'database' => 'myapp',
    'username' => 'user',
    'password' => 'pass',
]);
```

---

## Instance Methods

### ctgdb.run :: STRING|ctgdbQuery, ARRAY -> ARRAY

Executes a raw SQL string with optional bound values or a `CTGDBQuery` and
returns its complete materialized row array. Values support both typed form
(`['type' => 'int', 'value' => 42]`) and PHP type inference. No matching rows
returns `[]`; a non-row statement is rejected with `INVALID_QUERY_STATE`.
Raw SQL identifiers are the caller's responsibility.

```php
$users = $db->run('SELECT * FROM users');

$admins = $db->run(
    'SELECT * FROM users WHERE role = ?',
    [['type' => 'str', 'value' => 'admin']]
);

$admins = $db->run(
    CTGDBQuery::from('users')->where('role', '=', 'admin', 'str')
);
```

### ctgdb.execute :: STRING, ARRAY -> INT

Executes a custom non-row SQL statement with optional bound values and returns
its affected-row count. Use this for conditional writes, upserts, DDL, and
session commands that are not represented by the standard CRUD methods.
Row-producing statements are rejected with `INVALID_QUERY_STATE`.

```php
$affected = $db->execute(
    'UPDATE access_tokens SET used_at = ? WHERE id = ? AND used_at IS NULL',
    [$usedAt, $tokenId]
);
```

The SQL structure is trusted application code and remains the caller's
responsibility. Never concatenate user-controlled identifiers or values into
it; pass values separately so PDO binds them.

### ctgdb.process :: STRING|ctgdbQuery, (ARRAY, MIXED -> MIXED), MIXED, ARRAY -> MIXED

Executes a row-producing raw SQL string or `CTGDBQuery`, passes each row and
the current state to the processor, and returns the final state. This avoids
materializing rows inside `CTGDB`, although memory usage still depends on what
the processor retains and on the connection's PDO buffering mode. Statement
cursors are closed after processing, including when the processor throws.
Use `create()`, `update()`, or `delete()` for standard writes and `execute()`
for custom statements that do not return rows.

```php
$emails = $db->process(
    'SELECT email FROM users WHERE active = ?',
    fn($record, $result) => [...$result, $record['email']],
    [],
    values: [true]
);

$count = $db->process(
    CTGDBQuery::from('users')->where('active', '=', true, 'bool'),
    fn($record, $result) => $result + 1,
    0
);
```

### ctgdb.create :: STRING, ARRAY -> INT|STRING

Inserts a single row. Validates table and column names. Returns the
last insert ID. Data keys are column names, values are typed or
untyped.

```php
$id = $db->create('guitars', [
    'make' => ['type' => 'str', 'value' => 'PRS'],
    'model' => ['type' => 'str', 'value' => 'Custom 24'],
    'color' => ['type' => 'str', 'value' => 'Violet'],
    'year_purchased' => ['type' => 'int', 'value' => 2025]
]);
```

### ctgdb.read :: ctgdbQuery -> ARRAY

Executes a structured `CTGDBQuery` and returns its materialized rows. The query
object provides validated, parameterized SELECT construction with no raw SQL
strings.

```php
$guitars = $db->read(CTGDBQuery::from('guitars'));

$fenders = $db->read(
    CTGDBQuery::from('guitars')
        ->columns('id', 'model', 'color')
        ->where('make', '=', 'Fender', 'str')
        ->orderBy('year_purchased', 'DESC')
        ->limit(10)
);

$db->read(
    CTGDBQuery::from('guitars')
        ->innerJoin('pickups', ['guitars.id' => 'pickups.guitar_id'])
        ->columns('guitars.model', 'pickups.type')
);
```

See [CTGDBQuery.md](CTGDBQuery.md) for the full builder documentation. Use
`run()` when a row-producing SELECT cannot be expressed by the structured
builder.

### ctgdb.update :: STRING, ARRAY, ARRAY -> INT

Updates rows matching WHERE conditions. Validates table and column
names. Returns affected row count. Data keys are SET columns, where
keys are WHERE conditions (AND-joined, equality only).

```php
$affected = $db->update('guitars',
    ['color' => ['type' => 'str', 'value' => 'Sunburst']],
    ['id' => ['type' => 'int', 'value' => 1]]
);
```

### ctgdb.delete :: STRING, ARRAY -> INT

Deletes rows matching WHERE conditions. Validates table and column
names. Returns affected row count. Throws `EMPTY_WHERE_DELETE` if
`$where` is empty — no accidental full-table deletes.

```php
$affected = $db->delete('pickups', [
    'guitar_id' => ['type' => 'int', 'value' => 1]
]);
```

### ctgdb.paginate :: ctgdbQuery, ARRAY -> ARRAY

Paginates a `CTGDBQuery`. Runs a count query and a data query, returning
materialized `data` and `pagination` metadata. The `total` config option skips
the count query when the total is already known.

When `sort`/`order` are provided in config, they **replace** (not append to)
any existing ORDER BY on the query. `page` and `per_page` override the query's
pagination.

```php
$query = CTGDBQuery::from('guitars')
    ->where('make', '=', 'Fender', 'str')
    ->where('year_purchased', '>=', 2020, 'int');

$result = $db->paginate($query, [
    'sort' => 'year_purchased',
    'order' => 'DESC',
    'page' => 1,
    'per_page' => 5
]);

// $result['data'] — array of rows
// $result['pagination'] — {page, per_page, total_rows, total_pages, has_previous, has_next}
```

## Transactions

`CTGDB` does not expose transaction lifecycle methods. Retain its supplied
connection and perform transaction operations there; every `CTGDB` operation
then participates in that connection's active transaction:

```php
$connection = CTGDBConn::init($config);
$db = new CTGDB($connection);
$connection->beginTransaction();

try {
    $db->execute($conditionalWrite, $values);
} catch (Throwable $error) {
    try {
        if ($connection->inTransaction()) {
            $connection->rollBack();
        }
    } catch (Throwable $rollbackError) {
        $connection->invalidate();
    }
    throw $error;
}

$connection->commit();
```

Application-specific coordinators remain responsible for isolation policy,
commit context, rollback-or-throw behavior, and whether execution should be
allowed after a transaction ends. `CTGDBConn` invalidates itself when a begin,
commit, rollback, or state-check result cannot be trusted.

---

## Protected Methods

These methods are `protected` to support subclassing for
application-specific optimization.

### ctgdb.calcPaginationInfo :: INT, INT, INT -> ARRAY

Calculates pagination information from page, per_page, and total.
Returns `{page, per_page, total_rows, total_pages, has_previous, has_next}`.

SQL identifier quoting and reusable SQL construction are owned by
`CTGDBQuery`. `CTGDB` uses `CTGDBQuery::quoteIdentifier()` for write
identifiers and `CTGDBQuery::buildWhere()` for equality-only write predicates.

PDO access is deliberately absent from `CTGDB`. Preparation, binding,
execution, and result consumption cross the narrow `CTGDBConn::query()`,
`insert()`, `execute()`, and `process()` boundaries. `CTGDBConn` privately owns
each live prepared-statement lifecycle, so neither PDO statements nor PDO
handles can escape invalidation.
