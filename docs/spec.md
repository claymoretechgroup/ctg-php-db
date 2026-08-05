# ctg-php-db — Library Specification

## Overview

A minimal, opinionated PHP database library built on PDO. `CTGDB`
provides safe CRUD methods, materialized execution with `run()`, custom
non-row execution with `execute()`, and incremental result handling with `process()`
(`create`, `read`, `update`, `delete`) built on top. `CTGDBConn`
separates PDO access, guarded execution, connection lifecycle, and transaction
mechanics from query construction and CRUD semantics. Application-specific
coordinators own higher-level transaction policy. Filtering and pagination are
separate, composable operations that work on any result set — including joins,
subqueries, and aggregations.

Supports parameterized queries with explicit typing and row-by-row processing
with caller-defined state.

`CTGDBQuery` is the structured query builder for read operations —
the default path for all SELECT queries. It validates identifiers,
allowlists operators, and parameterizes all values by construction.
Queries that `CTGDBQuery` cannot express use `run()` directly.

---

## Design Principles

1. **Explicit execution modes** — `run()` materializes complete results,
   `execute()` returns affected-row counts for non-row commands, and `process()`
   handles rows incrementally
2. **Structured reads** — `CTGDBQuery` represents validated application reads,
   while raw SQL remains an explicit escape hatch
3. **Process results** — callers control incremental output through a row
   processor and initial state
4. **Explicit types** — parameter types are declared, not inferred,
   mapping directly to PDO param constants
5. **Separation of concerns** — `CTGDB` owns CRUD semantics, `CTGDBQuery` owns
   SQL construction, and `CTGDBConn` owns PDO connection, statement, and
   low-level transaction lifecycles
6. **Pagination is set-agnostic** — operates on any result set
   regardless of how it was produced (table, filter, join, raw SQL)
7. **Subclass-friendly** — methods are `protected` where needed so
   application-specific optimizations can override default behavior
8. **Safe-by-default reads** — `CTGDBQuery` is the default read path;
   all identifiers are validated, all operators are allowlisted, and
   all values are parameterized by construction

---

## Class Interface

```php
namespace CTG\DB;

class CTGDB
{
    private CTGDBConn $_connection;

    public function __construct(CTGDBConn $connection);

    // ─── Low-level ─────────────────────────────────────────

    // :: STRING|ctgdbQuery, ARRAY -> ARRAY
    // Execute a row-producing query and return all rows as a materialized array
    public function run(string|CTGDBQuery $query, array $values = []): array;

    // :: STRING, ARRAY -> INT
    // Execute a custom non-row statement and return its affected-row count
    public function execute(string $sql, array $values = []): int;

    // :: STRING|ctgdbQuery, (ARRAY, MIXED -> MIXED), MIXED, ARRAY -> MIXED
    // Process row-producing query results one row at a time and return final state
    public function process(string|CTGDBQuery $query, callable $processor, mixed $initial = null, array $values = []): mixed;

    // ─── CRUD ──────────────────────────────────────────────

    // :: STRING, ARRAY -> INT|STRING
    // Insert a single row, returns last insert ID
    public function create(string $table, array $data): int|string;

    // :: ctgdbQuery -> ARRAY
    // Execute a structured SELECT query and return materialized rows
    public function read(CTGDBQuery $query): array;

    // :: STRING, ARRAY, ARRAY -> INT
    // Update rows matching WHERE conditions, returns affected count
    public function update(string $table, array $data, array $where): int;

    // :: STRING, ARRAY -> INT
    // Delete rows matching WHERE conditions, returns affected count
    public function delete(string $table, array $where): int;

    // ─── Pagination ────────────────────────────────────────

    // :: ctgdbQuery, ARRAY -> ARRAY
    // Paginate any result set with metadata
    public function paginate(CTGDBQuery $source, array $config = []): array;

    public static function init(array $config): static;
}

class CTGDBConn
{
    public function beginTransaction(): void;
    public function commit(): void;
    public function rollBack(): void;
    public function inTransaction(): bool;
    public function query(string $sql, array $values = []): array;
    public function execute(string $sql, array $values = []): int;
    public function insert(string $sql, array $values = []): int|string;
    public function process(string $sql, callable $processor, mixed $initial = null, array $values = []): mixed;
}
```

---

## Constructor & Factory

```php
// Convenience factory with one associative connection configuration
$db = CTGDB::init([
    'host' => 'localhost',
    'database' => 'ctg_staging',
    'username' => 'ctg_dev',
    'password' => 'password',
    'charset' => 'utf8mb4',        // default: utf8mb4
    'timeout' => 5,                // default: null
    'persistent' => false,         // default: false
]);
```

`CTGDB::init()` creates a `CTGDBConn` and passes it to `new static(...)`.
Applications may instead inject an existing connection with
`new CTGDB($connection)`. Transactional applications retain that connection and
perform low-level transaction operations on it; all operations through the
composed `CTGDB` use the same PDO session. See `docs/CTGDBConn.md` for the
complete connection contract.

---

## Type System

Parameter types map to PDO constants:

| CTG type | PDO constant | Use for |
|----------|-------------|---------|
| `'str'` | `PDO::PARAM_STR` | Strings, dates, text |
| `'int'` | `PDO::PARAM_INT` | Integers, foreign keys |
| `'bool'` | `PDO::PARAM_BOOL` | Boolean flags |
| `'null'` | `PDO::PARAM_NULL` | Explicit NULL values |
| `'float'` | `PDO::PARAM_STR` | Floats (PDO has no float type, cast to string) |

Values can be expressed in two forms:

```php
// Typed — explicit associative form (preferred)
['type' => 'int', 'value' => 42]
['type' => 'str', 'value' => 'alice@example.com']
['type' => 'float', 'value' => 29.99]

// Untyped — type inferred from PHP type (convenience)
42          // inferred as 'int'
'alice'     // inferred as 'str'
3.14        // inferred as 'float'
true        // inferred as 'bool'
null        // inferred as 'null'
```

The typed associative form is preferred. Detection:

```php
if (is_array($value) && isset($value['type'], $value['value'])) {
    // typed value — use declared type
} else {
    // untyped — infer from PHP type
}
```

---

## run() — Materialized Execution

### Signature

```php
// :: STRING|ctgdbQuery, ARRAY -> ARRAY
// Execute a row-producing query and return all rows as a materialized array
public function run(string|CTGDBQuery $query, array $values = []): array;
```

### Query Formats

```php
// Plain SQL string (no parameters)
$db->run('SELECT * FROM guitars');

// Raw SQL with positional placeholders
$db->run(
    'SELECT * FROM guitars WHERE id = ? AND year_purchased > ?',
    [
        ['type' => 'int', 'value' => 1],
        ['type' => 'int', 'value' => 2010]
    ]
);

// Raw SQL with named placeholders
$db->run(
    'SELECT * FROM guitars WHERE id = :id AND make = :make',
    [
        'id' => ['type' => 'int', 'value' => 1],
        'make' => ['type' => 'str', 'value' => 'Fender']
    ]
);

// Untyped convenience
$db->run('SELECT * FROM guitars WHERE id = ?', [42]);

// Fluent structured query; its values are resolved by CTGDBQuery
$db->run(
    CTGDBQuery::from('guitars')->where('id', '=', 42, 'int')
);
```

Supplying a separate `$values` array with `CTGDBQuery` is rejected because the
query object owns its SQL and bound values.

### Return Behavior

- SELECT and other row-producing statements return a materialized row array.
- A statement with no returned rows returns `[]`.
- Non-row statements are rejected with `INVALID_QUERY_STATE`; writes use the
  dedicated CRUD methods or `execute()`.

---

## execute() — Custom Non-Row Execution

### Signature

```php
// :: STRING, ARRAY -> INT
// Execute a custom non-row statement and return its affected-row count
public function execute(string $sql, array $values = []): int;
```

Use `execute()` for conditional writes, upserts, DDL, and session commands that
the standard CRUD methods do not represent. Row-producing statements are
rejected with `INVALID_QUERY_STATE`.

```php
$affected = $db->execute(
    'UPDATE access_tokens SET used_at = ? WHERE id = ? AND used_at IS NULL',
    [$usedAt, $tokenId]
);
```

The SQL structure is trusted application code. User-controlled values must be
supplied separately for PDO binding and must never be concatenated into SQL.

---

## process() — Incremental Result Handling

### Signature

```php
// :: STRING|ctgdbQuery, (ARRAY, MIXED -> MIXED), MIXED, ARRAY -> MIXED
// Process query results one row at a time and return final state
public function process(string|CTGDBQuery $query, callable $processor, mixed $initial = null, array $values = []): mixed;
```

For each row, `$processor` receives the current associative row and state. Its
return value becomes the state passed to the next row. The final state is
returned when all rows have been consumed. Statement cursors are closed after
processing, including when the processor throws. `process()` is intended for
SELECT and other row-producing statements; use CRUD methods or `execute()` for
statements that do not return rows.

```
$processor signature: fn(array $record, mixed $result): mixed
```

```php
// Extract a single column
$makes = $db->process(
    'SELECT make FROM guitars',
    fn($record, $result) => [...$result, $record['make']],
    []
);

// Key by ID
$byId = $db->process(
    CTGDBQuery::from('guitars'),
    fn($record, $result) => $result + [$record['id'] => $record],
    []
);

// Sum a column
$total = $db->process(
    'SELECT year_purchased FROM guitars WHERE make = ?',
    fn($record, $sum) => $sum + $record['year_purchased'],
    0,
    values: ['Fender']
);
```

`process()` avoids accumulating rows inside `CTGDB`. End-to-end memory behavior
still depends on what the processor retains and whether PDO buffering is
enabled for the connection.

---

## CRUD Methods

All CRUD methods build SQL and bound-value arrays internally and delegate to
typed `CTGDBConn` execution boundaries.

### create()

```php
// :: STRING, ARRAY -> INT|STRING
// Insert a single row, returns last insert ID
public function create(string $table, array $data): int|string;
```

```php
$id = $db->create('guitars', [
    'make' => ['type' => 'str', 'value' => 'PRS'],
    'model' => ['type' => 'str', 'value' => 'Custom 24'],
    'color' => ['type' => 'str', 'value' => 'Violet'],
    'year_purchased' => ['type' => 'int', 'value' => 2025]
]);
// Generates: INSERT INTO guitars (make, model, color, year_purchased) VALUES (?, ?, ?, ?)
// Returns: 10 (the new row's auto-increment ID)

// Untyped convenience
$id = $db->create('guitars', [
    'make' => 'PRS',
    'model' => 'Custom 24',
    'color' => 'Violet',
    'year_purchased' => 2025
]);
```

### read()

```php
// :: ctgdbQuery -> ARRAY
// Execute a structured SELECT query
public function read(CTGDBQuery $query): array;
```

The general-purpose structured read method. `CTGDBQuery` owns all SELECT syntax
and executes directly through the row-only query boundary.

### CTGDBQuery Usage

The query object contains all configuration: columns, WHERE, JOINs, ORDER BY,
GROUP BY, LIMIT, and OFFSET.

```php
// Simple read
$guitars = $db->read(
    CTGDBQuery::from('guitars')
        ->where('make', '=', 'Fender', 'str')
        ->orderBy('model')
        ->limit(10)
);

// Join read
$rows = $db->read(
    CTGDBQuery::from('guitars')
        ->innerJoin('pickups', ['guitars.id' => 'pickups.guitar_id'])
        ->columns('guitars.model', 'pickups.type as pickup_type')
        ->where('guitars.year_purchased', '>=', 2020, 'int')
);

// Incremental processing is a separate operation
$byMake = $db->process(
    CTGDBQuery::from('guitars'),
    fn($row, $result) => $result + [$row['make'] => $row],
    []
);
```

`innerJoin()`, `leftJoin()`, `rightJoin()`, and `crossJoin()` are convenience
methods that delegate to the canonical `join()` implementation. A generic
`outerJoin()` is intentionally omitted because its direction would be
ambiguous and MariaDB does not directly support `FULL OUTER JOIN`.

**Join + paginate via CTGDBQuery:**

Use `CTGDBQuery` to build join queries for pagination:

```php
$query = CTGDBQuery::from('guitars')
    ->innerJoin('pickups', ['guitars.id' => 'pickups.guitar_id'])
    ->columns('guitars.*', 'pickups.type as pickup_type');

$result = $db->paginate($query, [
    'sort' => 'guitars.make',
    'page' => 1,
    'per_page' => 5
]);
```

**Composing join + filter via CTGDBQuery:**

```php
$result = $db->read(
    CTGDBQuery::from('guitars')
        ->innerJoin('pickups', ['guitars.id' => 'pickups.guitar_id'])
        ->columns('guitars.model', 'pickups.type')
        ->where('guitars.year_purchased', '>=', 2020, 'int')
);
```

### update()

```php
// :: STRING, ARRAY, ARRAY -> INT
// Update rows matching WHERE conditions, returns affected count
public function update(string $table, array $data, array $where): int;
```

```php
$affected = $db->update('guitars',
    ['color' => ['type' => 'str', 'value' => 'Sunburst']],
    ['id' => ['type' => 'int', 'value' => 1]]
);
// Generates: UPDATE guitars SET color = ? WHERE id = ?
// Returns: 1
```

### delete()

```php
// :: STRING, ARRAY -> INT
// Delete rows matching WHERE conditions, returns affected count
public function delete(string $table, array $where): int;
```

```php
$affected = $db->delete('pickups', [
    'guitar_id' => ['type' => 'int', 'value' => 1]
]);
```

**Safety: `delete` throws CTGDBError if `$where` is empty.**

---

## paginate() — Paging Any Result Set

### Signature

```php
// :: ctgdbQuery, ARRAY -> ARRAY
// Paginate any result set with metadata
public function paginate(CTGDBQuery $source, array $config = []): array;
```

### Source

`$source` is a structured query:

```php
$query = CTGDBQuery::from('guitars')
    ->innerJoin('pickups', ['guitars.id' => 'pickups.guitar_id'])
    ->columns('guitars.model', 'pickups.type as pickup_type')
    ->where('guitars.year_purchased', '>=', 2020, 'int');

$result = $db->paginate($query, [
    'sort' => 'guitars.model',
    'page' => 1,
    'per_page' => 10
]);
```

String table names, arrays, raw query arrays, and prior `as_query` output are
not accepted. Construct a `CTGDBQuery` explicitly.

### Config Options

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `sort` | `string` | `null` | Column to sort by |
| `order` | `string` | `'ASC'` | Sort direction: `'ASC'` or `'DESC'` |
| `page` | `int` | `1` | Current page (1-based) |
| `per_page` | `int` | `20` | Rows per page |
| `total` | `int` | `null` | Pre-computed total (skips count query) |

### Return Structure

Always returns this shape:

```php
[
    'data' => [
        ['id' => 1, 'make' => 'Ibanez', ...],
        ['id' => 2, 'make' => 'Schecter', ...],
    ],
    'pagination' => [
        'page' => 1,
        'per_page' => 20,
        'total_rows' => 9,
        'total_pages' => 1,
        'has_previous' => false,
        'has_next' => false
    ]
]
```

Pagination always materializes the current page in `data`. Transformations can
be applied explicitly after pagination when needed.

### Internal Behavior

`CTGDBQuery::toCountStatement()` builds a count statement from the same JOIN,
WHERE, and GROUP BY structure. The cloned query receives the requested page,
page size, and optional replacement sort before producing the data statement:

```sql
-- Count
SELECT COUNT(*) as total FROM guitars WHERE make = ?

-- Data
SELECT * FROM guitars WHERE make = ?
ORDER BY model ASC LIMIT 5 OFFSET 0
```

The `total` config option lets callers skip the count query when they
already know the total. This is the primary extension point for
performance optimization in subclasses.

---

## Transaction Boundaries

`CTGDBConn` exposes the four direct PDO transaction primitives. `CTGDB` does
not forward or own their lifecycle, but all its operations participate when
its supplied connection has an active transaction:

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

Nested begins and terminal operations without an active transaction are
rejected with `INVALID_QUERY_STATE`. Indeterminate begin, commit, rollback, or
state-check outcomes invalidate `CTGDBConn`. Application-specific coordinators
own isolation requirements, commit context, retry behavior, and explicit
rollback policy.

---

## WHERE Clause Behavior

Use `CTGDBQuery::from()->where()` for all read query WHERE conditions.
`CTGDBQuery` supports the full operator set (`=`, `>`, `<`, `>=`, `<=`,
`!=`, `LIKE`, `NOT LIKE`, `IN`, `NOT IN`, `IS`, `IS NOT`, `BETWEEN`).

`update()` and `delete()` continue to accept equality-only associative WHERE
arrays, which `CTGDBQuery::buildWhere()` converts into bound predicates. Use
`run()` for row-producing statements that `CTGDBQuery` cannot express.

---

## SQL Injection Prevention

The library follows a strict security model: **no user-provided value
ever touches SQL without going through PDO's prepared statement
binding.**

### Values — PDO Prepared Statements

All values are bound through PDO with explicit types. The query
structure and the data are sent to the database separately. Injection
is impossible regardless of what the value contains.

### Identifiers — Regex Validation

Table names, column names, and aliases are validated and quoted by the
canonical `CTGDBQuery::quoteIdentifier()` implementation. It accepts bare or
table-qualified identifiers containing letters, numbers, underscores, and
dots. Anything else is rejected with a `CTGDBError`.

```php
// :: STRING -> STRING
// Validates and backtick-quotes a bare or table-qualified identifier
public static function quoteIdentifier(string $identifier): string;
```

Column references with table prefixes (`guitars.model`), aliases
(`guitars.model as guitar_model`), and wildcards (`*`, `guitars.*`) are handled
by `CTGDBQuery::columns()`, which parses and validates each component.

### Keyword Allowlists

SQL keywords that get interpolated are validated against hardcoded
allowlists:

Join types, sort directions, and filter operators are validated by private
`CTGDBQuery` allowlists as their corresponding builder methods are called.

### Where Each Validation Runs

| Method | What's validated |
|--------|-----------------|
| `create()` | Table name, column names (from data keys) |
| `read()` | Accepts only `CTGDBQuery`; validation occurs in the builder |
| `update()` | Table name, column names (data keys + where keys) |
| `delete()` | Table name, column names (where keys) |
| `paginate()` | Clones `CTGDBQuery`; sort validation occurs in the builder |
| `CTGDBQuery` | Table name, column names, join types, operators, sort direction — **all validated by construction** |
| `run()` | **No identifier validation** — raw SQL is the caller's responsibility |
| `execute()` | **No identifier validation** — raw SQL is the caller's responsibility; supplied values are bound |

`CTGDBQuery` provides safe-by-default query building: every identifier
is validated, every operator is allowlisted, and every value is
parameterized. It is the recommended path for all read operations,
particularly for AI-generated code where raw SQL strings pose an
injection risk.

### Future: Schema Validation (v2)

A future version may add optional INFORMATION_SCHEMA-based validation
that checks table and column names against the live database schema.
This would be configurable via a constructor option (e.g.,
`'validate_schema' => true`) since the INFORMATION_SCHEMA query has a
performance cost not all use cases need.

---

## Error Handling — CTGDBError

All errors thrown by the library are instances of `CTG\DB\CTGDBError`.
PDO exceptions are caught internally and re-thrown as `CTGDBError` with
the appropriate type code and context data. No raw `PDOException`
escapes the library boundary.

### CTGDBError Class

```php
namespace CTG\DB;

class CTGDBError extends \Exception
{
    const TYPES = [
        // 1xxx — Connection
        'CONNECTION_FAILED'    => 1000,
        'CONNECTION_TIMEOUT'   => 1001,
        'AUTH_FAILED'          => 1002,
        // 2xxx — Query execution
        'QUERY_FAILED'         => 2000,
        'DUPLICATE_ENTRY'      => 2001,
        'CONSTRAINT_VIOLATION' => 2002,
        // 3xxx — Validation
        'INVALID_TABLE'        => 3000,
        'INVALID_COLUMN'       => 3001,
        'INVALID_OPERATOR'     => 3002,
        'INVALID_JOIN_TYPE'    => 3003,
        'INVALID_SORT'         => 3004,
        'INVALID_ARGUMENT'     => 3005,
        'EMPTY_WHERE_DELETE'   => 3006,
        'INVALID_IDENTIFIER'   => 3007,
        'INVALID_AGGREGATE'    => 3008,
        'INVALID_QUERY_STATE'  => 3009,
    ];

    public readonly int    $code;
    public readonly string $type;
    public readonly string $msg;
    public readonly mixed  $data;

    private bool $_handled = false;

    // CONSTRUCTOR :: STRING|INT, ?STRING, MIXED -> $this
    // Creates a new error — accepts type name or code
    public function __construct(
        string|int $type,
        ?string    $msg = null,
        mixed      $data = null
    );

    // :: STRING|INT -> INT|STRING|NULL
    // Bidirectional lookup — name to code or code to name
    public static function lookup(string|int $key): int|string|null;

    // :: STRING|INT, (ctgdbError -> VOID) -> $this
    // Handle error if it matches the given type. Chainable.
    public function on(string|int $type, callable $handler): static;

    // :: (ctgdbError -> VOID) -> VOID
    // Handle error if no previous on() matched
    public function otherwise(callable $handler): void;
}
```

### Usage

```php
// Chainable handler — short-circuits on first match
try {
    $id = $db->create('guitars', $data);
} catch (CTGDBError $e) {
    $e->on('DUPLICATE_ENTRY', fn($e) => respondConflict($e->data))
      ->on('CONSTRAINT_VIOLATION', fn($e) => respondBadRequest($e->msg))
      ->on('INVALID_TABLE', fn($e) => log("Bad table: " . $e->data['identifier']))
      ->otherwise(fn($e) => respondServerError($e->msg));
}

// Bidirectional lookup
CTGDBError::lookup('DUPLICATE_ENTRY');  // 2001
CTGDBError::lookup(2001);               // 'DUPLICATE_ENTRY'
CTGDBError::lookup('NONEXISTENT');      // null
```

### How the Library Throws CTGDBError

Error classification uses driver error codes (`errorInfo[1]`) as the
primary discriminant, SQLSTATE as a fallback grouping, and message text
only as a last resort. This ensures classification is robust across
driver versions and server locales.

Connection and query failures are classified inside `CTGDBConn`, the only
class with PDO access. `CTGDB::run()` resolves raw SQL or `CTGDBQuery` and
delegates materialized row execution through its supplied `CTGDBConn`.
`CTGDB::create()` delegates inserts through the same connection, while custom
execution, updates, and deletes delegate to `execute()`. `CTGDB::process()`
delegates incremental result handling to `process()`. These paths map duplicate
and constraint failures and permanently invalidate the connection when driver
codes 2006 or 2013 show that it was lost.

```php
$rows = $this->_connection->query($sql, $values);
$insertId = $this->_connection->insert($sql, $values);
$affected = $this->_connection->execute($sql, $values);
$state = $this->_connection->process($sql, $processor, $initial, $values);

// Safety guard
throw new CTGDBError('EMPTY_WHERE_DELETE',
    "delete() requires a WHERE clause",
    ['table' => $table]
);
```

### Code Ranges

| Range | Category | Types |
|-------|----------|-------|
| 1xxx | Connection | `CONNECTION_FAILED`, `CONNECTION_TIMEOUT`, `AUTH_FAILED` |
| 2xxx | Query execution | `QUERY_FAILED`, `DUPLICATE_ENTRY`, `CONSTRAINT_VIOLATION` |
| 3xxx | Validation | `INVALID_TABLE`, `INVALID_COLUMN`, `INVALID_OPERATOR`, `INVALID_JOIN_TYPE`, `INVALID_SORT`, `INVALID_ARGUMENT`, `EMPTY_WHERE_DELETE`, `INVALID_IDENTIFIER`, `INVALID_AGGREGATE`, `INVALID_QUERY_STATE` |

---

## Protected Internals

The following methods are `protected` to support subclassing:

| Method | Purpose |
|--------|---------|
| `calcPaginationInfo()` | Pagination information calculation |

Equality-only write predicates are constructed by the public static
`CTGDBQuery::buildWhere()` helper so SQL construction does not remain on the
`CTGDB` execution façade.

PDO statement preparation, binding, type resolution, result access, and cursor
cleanup are private connection-bound details implemented by `CTGDBConn`. No PDO
accessor or live statement is exposed to `CTGDB`, subclasses, or application
callers.

---

## File Structure

```
ctg-php-db/
├── composer.json
├── docs/
│   ├── CTGDBConn.md
│   └── spec.md
├── src/
│   ├── CTGDB.php
│   ├── CTGDBConn.php
│   ├── CTGDBError.php
│   └── CTGDBQuery.php
├── tests/
│   ├── CTGDBConnTest.php
│   ├── CTGDBErrorTest.php
│   ├── CTGDBTest.php
│   ├── CTGDBIntegrationTest.php
│   └── CTGDBQueryTest.php
├── staging/                        # gitignored
└── README.md
```

### composer.json

```json
{
    "name": "ctg/php-db",
    "description": "Minimal PDO database library with safe CRUD and incremental result processing",
    "type": "library",
    "license": "MIT",
    "autoload": {
        "psr-4": {
            "CTG\\DB\\": "src/"
        }
    },
    "require": {
        "php": ">=8.1",
        "ext-pdo": "*",
        "ext-pdo_mysql": "*"
    }
}
```

---

## Implementation Order

1. **CTGDBError** — standalone error class, no dependencies
2. **CTGDBConn** — PDO setup, guarded execution, lifecycle, and transaction primitives
3. **CTGDB composition** — main query API delegates to one connection
4. **CTGDBQuery** — the sole SELECT builder and canonical identifier validator
5. **run()** — materialized row execution with an array-only return contract
6. **execute()** — affected-row execution for custom non-row statements
7. **process()** — incremental row handling
8. **CRUD** — create, read, update, delete (delegates to typed execution operations)
9. **paginate()** — paging with metadata
10. **Integration** — CRUD and query APIs participate in transactions on their supplied connection
