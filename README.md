# ctg-php-db

`ctg-php-db` is a minimal PHP database library built on PDO. `CTGDB` provides
safe CRUD methods, materialized execution through `run()`, and incremental row
handling through `process()`. `CTGDBConn` owns connection lifecycle and exposes
the low-level transaction primitives used by application coordinators.
`CTGDBQuery` is the default read path — a
structured query builder where every column,
operator, and value is validated and parameterized by construction. All
values are bound through PDO prepared statements. All identifiers are
validated before interpolation.

**Key Features:**

* **Explicit execution modes** — `run()` always returns a complete row array,
  `execute()` returns affected-row counts for custom non-row statements, and
  `process()` handles result rows incrementally
* **Explicit connection lifecycle** — `CTGDBConn` owns PDO creation,
  statement execution, persistence state, and fail-closed invalidation without
  exposing PDO, including direct transaction boundary operations
* **Process results** — callers control incremental output through a row
  processor and initial state
* **Composable** — `CTGDBQuery` combines column selection, WHERE conditions,
  joins, ordering, and pagination into a single structured object that
  `read()` and `paginate()` accept directly
* **AI-safe query building** — `CTGDBQuery` eliminates raw SQL strings
  from the read path, so generated code cannot interpolate user input
  into queries
* **Explicit types** — parameter types map directly to PDO constants,
  no inference surprises
* **SQL injection hardened** — values via PDO binding, identifiers via
  regex validation, keywords via hardcoded allowlists
* **Composable connection layer** — `CTGDB` operates over an injectable
  `CTGDBConn` instead of being a connection subtype

## Install

Add the GitHub repository to your `composer.json`:

```json
{
    "repositories": [
        { "type": "vcs", "url": "https://github.com/claymoretechgroup/ctg-php-db" }
    ]
}
```

Then require the package:

```
composer require ctg/php-db
```

## Connection lifecycle

Version 1.1 introduced an explicit fail-closed lifecycle for security-sensitive
transaction coordinators. Version 2.0 moves connection invalidation and
persistence state into `CTGDBConn`. Transaction mechanics remain explicit on
the connection so application-specific coordinators can apply their own policy:

```php
$connection = CTGDBConn::init($config);
if ($connection->isPersistent()) {
    throw new RuntimeException('This operation requires a nonpersistent connection');
}

$db = new CTGDB($connection);
$connection->beginTransaction();

try {
    // ... perform database operations through $db ...
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

`invalidate()` releases the wrapper's PDO handle and permanently poisons the
composed connection; all later queries fail with `CONNECTION_FAILED`. Call it when
rollback cannot be confirmed or commit acknowledgement is indeterminate.
Reject persistent connections before starting transactions that require
physical connection disposal. Higher-level requirements such as isolation
verification, audit context, and rollback-or-throw behavior belong to the
application transaction coordinator.

## Examples

### Connecting

Version 2.0 replaces the positional constructor and `connect()` factory with
an injectable `CTGDBConn` constructor and a config-based `init()` factory.

```php
use CTG\DB\CTGDB;

$db = CTGDB::init([
    'host' => 'localhost',
    'database' => 'myapp',
    'username' => 'user',
    'password' => 'pass',
]);
```

Use an explicitly injected connection when persistence or fail-closed
invalidation control is required:

```php
use CTG\DB\CTGDBConn;

$connection = CTGDBConn::init($config);
$db = new CTGDB($connection);
```

### Basic CRUD

```php
use CTG\DB\CTGDBQuery;

$id = $db->create('guitars', [
    'make' => ['type' => 'str', 'value' => 'PRS'],
    'model' => ['type' => 'str', 'value' => 'Custom 24'],
    'color' => ['type' => 'str', 'value' => 'Violet'],
    'year_purchased' => ['type' => 'int', 'value' => 2025]
]);

$guitars = $db->read(
    CTGDBQuery::from('guitars')
        ->columns('id', 'make', 'model')
        ->where('make', '=', 'Fender', 'str')
        ->orderBy('year_purchased', 'DESC')
        ->limit(10)
);

$db->update('guitars',
    ['color' => ['type' => 'str', 'value' => 'Sunburst']],
    ['id' => ['type' => 'int', 'value' => 1]]
);

$db->delete('pickups', [
    'guitar_id' => ['type' => 'int', 'value' => 1]
]);
```

`read()` accepts only `CTGDBQuery`, so all structured SELECT syntax is generated
and validated in one place.

### Raw Queries and Incremental Processing

`run()` materializes a complete row array. A query with no matching rows
returns `[]`. `process()` handles rows one at a time:

```php
$activeUsers = $db->run(
    'SELECT * FROM users WHERE active = ?',
    [true]
);

$emails = $db->process(
    'SELECT email FROM users WHERE active = ?',
    fn($record, $result) => [...$result, $record['email']],
    [],
    values: [true]
);

$byId = $db->process(
    CTGDBQuery::from('guitars')->orderBy('id'),
    fn($record, $result) => $result + [$record['id'] => $record],
    []
);
```

`run()` and `process()` accept only row-producing statements. Use `create()`,
`update()`, or `delete()` for standard writes, or `execute()` for a custom
parameterized non-row statement:

```php
$affected = $db->execute(
    'UPDATE access_tokens SET used_at = ? WHERE id = ? AND used_at IS NULL',
    [$usedAt, $tokenId]
);
```

Raw SQL structure passed to `execute()` is the caller's responsibility; values
must be supplied separately for PDO binding. Statement cursors are closed after
execution, including when a processor throws.

### Filtering

Build reusable query conditions with full operator support:

```php
use CTG\DB\CTGDBQuery;

$query = CTGDBQuery::from('guitars')
    ->where('make', '=', 'Fender', 'str')
    ->where('year_purchased', '>=', 2020, 'int')
    ->where('model', 'LIKE', '%Strat%', 'str');

$page1 = $db->paginate($query, ['sort' => 'model', 'page' => 1]);
$page2 = $db->paginate($query, ['sort' => 'model', 'page' => 2]);
```

### Joins

```php
use CTG\DB\CTGDBQuery;

$db->read(
    CTGDBQuery::from('guitars')
        ->innerJoin('pickups', ['guitars.id' => 'pickups.guitar_id'])
        ->columns('guitars.model', 'pickups.position', 'pickups.type')
);

$db->read(
    CTGDBQuery::from('guitars')
        ->leftJoin('pickups', ['guitars.id' => 'pickups.guitar_id'])
        ->columns('guitars.model', 'pickups.position')
);
```

### Pagination

Paginate a `CTGDBQuery`; the same structured query produces both the count and
data statements:

```php
$result = $db->paginate(CTGDBQuery::from('guitars'), [
    'sort' => 'year_purchased',
    'order' => 'DESC',
    'page' => 1,
    'per_page' => 5
]);

// $result['data'] — array of rows
// $result['pagination'] — {page, per_page, total_rows, total_pages, has_previous, has_next}
```

### Composing Filter + Join + Paginate

```php
use CTG\DB\CTGDBQuery;

$query = CTGDBQuery::from('guitars')
    ->innerJoin('pickups', ['guitars.id' => 'pickups.guitar_id'])
    ->columns('guitars.model', 'pickups.type')
    ->where('guitars.year_purchased', '>=', 2020, 'int');

$result = $db->paginate($query, [
    'sort' => 'model',
    'order' => 'ASC',
    'page' => 1,
    'per_page' => 10
]);
```

### Error Handling

```php
use CTG\DB\CTGDBError;

try {
    $db->create('guitars', $data);
} catch (CTGDBError $e) {
    $e->on('DUPLICATE_ENTRY', fn($e) => respondConflict($e->data))
      ->on('CONSTRAINT_VIOLATION', fn($e) => respondBadRequest($e->msg))
      ->on('INVALID_TABLE', fn($e) => log("Bad table: " . $e->data['identifier']))
      ->otherwise(fn($e) => respondServerError($e->msg));
}
```

## Notice

`ctg-php-db` is under active development. The core API is stable.
Schema-based identifier validation may be added as a configurable
option in a future version.
