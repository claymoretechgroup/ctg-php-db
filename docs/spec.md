# ctg-php-db — Library Specification

## Overview

A minimal, opinionated PHP database library built on PDO. `CTGDB`
provides safe CRUD methods, materialized execution with `run()`, and
incremental result handling with `process()`
(`create`, `read`, `update`, `delete`) built on top. `CTGDBConn`
separates PDO access, guarded execution, connection lifecycle, and transaction
state from query construction and CRUD semantics. Filtering and pagination are
separate, composable operations that work on any result set — including
joins, subqueries, and aggregations.

Supports parameterized queries with explicit typing and row-by-row processing
with caller-defined state.

`CTGDBQuery` is the structured query builder for read operations —
the default path for all SELECT queries. It validates identifiers,
allowlists operators, and parameterizes all values by construction.
Queries that `CTGDBQuery` cannot express use `run()` directly.

---

## Design Principles

1. **Explicit execution modes** — `run()` materializes complete results while
   `process()` handles rows incrementally
2. **Structured reads** — `CTGDBQuery` represents validated application reads,
   while raw SQL remains an explicit escape hatch
3. **Process results** — callers control incremental output through a row
   processor and initial state
4. **Explicit types** — parameter types are declared, not inferred,
   mapping directly to PDO param constants
5. **Separation of concerns** — `CTGDB` owns CRUD semantics, `CTGDBQuery` owns
   SQL construction, and a composed `CTGDBConn` exclusively owns PDO,
   execution, and connection lifecycle
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

    // :: STRING|ctgdbQuery, (ARRAY, MIXED -> MIXED), MIXED, ARRAY -> MIXED
    // Process row-producing query results one row at a time and return final state
    public function process(string|CTGDBQuery $query, callable $processor, mixed $initial = null, array $values = []): mixed;

    // ─── CRUD ──────────────────────────────────────────────

    // :: STRING, ARRAY -> INT|STRING
    // Insert a single row, returns last insert ID
    public function create(string $table, array $data): int|string;

    // :: STRING|ARRAY|ctgdbQuery, ARRAY -> ARRAY
    // Build or accept a CTGDBQuery and return materialized rows
    // When $tables is a CTGDBQuery, $config is ignored
    public function read(string|array|CTGDBQuery $tables, array $config = []): array;

    // :: STRING, ARRAY, ARRAY -> INT
    // Update rows matching WHERE conditions, returns affected count
    public function update(string $table, array $data, array $where): int;

    // :: STRING, ARRAY -> INT
    // Delete rows matching WHERE conditions, returns affected count
    public function delete(string $table, array $where): int;

    // ─── Pagination ────────────────────────────────────────

    // :: STRING|ctgdbQuery, ARRAY -> ARRAY
    // Paginate any result set with metadata
    // CTGDBQuery is the preferred source type
    public function paginate(string|CTGDBQuery $source, array $config = []): array;

    public static function init(array $config): static;
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
`new CTGDB($connection)`. `CTGDB` intentionally does not forward transaction,
persistence-state, or invalidation methods. Callers needing those operations
retain the injected connection and invoke its lifecycle API directly. See
`docs/CTGDBConn.md` for the complete connection contract.

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
  dedicated CRUD methods.

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
SELECT and other row-producing statements; use CRUD methods for statements
that do not return rows.

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
`run()`.

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
// :: STRING|ARRAY|ctgdbQuery, ARRAY -> ARRAY
// Read rows from one or more tables
public function read(string|array|CTGDBQuery $tables, array $config = []): array;
```

The general-purpose read method. A supplied `CTGDBQuery` executes directly.
String and array table forms are translated into `CTGDBQuery` method calls, so
all SELECT syntax comes from one builder.

### CTGDBQuery — Preferred Usage

`CTGDBQuery` is the preferred way to use `read()`. When `$tables` is a
`CTGDBQuery` instance, `$config` is ignored — the query object contains
all configuration (columns, WHERE, JOINs, ORDER BY, LIMIT).

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

### Compatibility Usage

The table/config forms remain available, but they do not construct SQL inside
`CTGDB`. `read()` converts them into a `CTGDBQuery` before execution.

**When `$tables` is a string** — single table, simple query:

```php
$guitars = $db->read('guitars');

$fenders = $db->read('guitars', [
    'columns' => ['id', 'model', 'color'],
    'where' => [
        'make' => ['type' => 'str', 'value' => 'Fender']
    ],
    'order' => 'year_purchased DESC',
    'limit' => 10
]);
```

**When `$tables` is an array** — multi-table join:

```php
// Two tables, inner join
$db->read(['guitars', 'pickups'], [
    'join' => 'inner',
    'on' => [
        ['guitars.id' => 'pickups.guitar_id']
    ],
    'columns' => ['guitars.make', 'guitars.model', 'pickups.position', 'pickups.type']
]);

// Three+ tables with mixed join types (array form)
$db->read(['articles', 'categories', 'users'], [
    'join' => [
        ['type' => 'left',  'on' => ['articles.category_id' => 'categories.id']],
        ['type' => 'inner', 'on' => ['articles.author_id' => 'users.id']],
    ],
    'columns' => ['articles.title', 'categories.name as category', 'users.name as author']
]);
```

**Config options (single table):**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `columns` | `array` | `['*']` | Columns to select |
| `where` | `array` | `[]` | WHERE conditions (associative array only). String form removed — use `CTGDBQuery` |
| `values` | — | — | Removed (was used with string `where`) |
| `order` | `string` | `null` | ORDER BY clause |
| `limit` | `int` | `null` | Max rows to return |

**Additional config options (multi-table):**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `join` | `string\|array` | *required* | Join type(s) |
| `on` | `array` | *required* | Join conditions |
| `where_raw` | — | — | Removed — use `CTGDBQuery` |
| `group` | `string` | `null` | GROUP BY clause |
| `having` | — | — | Removed — use `CTGDBQuery` |
| `as_query` | — | — | Removed — use `CTGDBQuery` directly |

**Join type formats:**

```php
// String — same join type for all tables
'join' => 'left'
'join' => 'inner'

// Array — per-table join type and conditions
'join' => [
    ['type' => 'left',  'on' => ['a.col' => 'b.col']],
    ['type' => 'inner', 'on' => ['a.col' => 'c.col']],
]
```

**On condition formats:**

When `join` is a string (uniform type), `on` is a separate array
where each entry corresponds to each joined table:

```php
// Entry [0] joins table[1] to table[0]
// Entry [1] joins table[2] to the result set
'on' => [
    ['guitars.id' => 'pickups.guitar_id'],
]
```

When `join` is an array (mixed types), each entry contains its own
`on` — see mixed join example above.

Each `on` entry supports multiple conditions for composite keys:

```php
['type' => 'inner', 'on' => [
    'orders.user_id' => 'users.id',
    'orders.tenant_id' => 'users.tenant_id'
]]
// Generates: INNER JOIN users ON orders.user_id = users.id
//            AND orders.tenant_id = users.tenant_id
```

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
// :: STRING|ctgdbQuery, ARRAY -> ARRAY
// Paginate any result set with metadata
public function paginate(string|CTGDBQuery $source, array $config = []): array;
```

### Source Types

`$source` accepts two forms:

```php
// 1. CTGDBQuery — the default, safe path
$query = CTGDBQuery::from('guitars')
    ->innerJoin('pickups', ['guitars.id' => 'pickups.guitar_id'])
    ->columns('guitars.model', 'pickups.type as pickup_type')
    ->where('guitars.year_purchased', '>=', 2020, 'int');

$result = $db->paginate($query, [
    'sort' => 'guitars.model',
    'page' => 1,
    'per_page' => 10
]);

// 2. Table name — paginate all rows from a single table
$result = $db->paginate('guitars', [
    'sort' => 'year_purchased',
    'order' => 'DESC',
    'page' => 1
]);
```

Array sources (filter results, raw query arrays, `as_query` output) are
no longer accepted. Use `CTGDBQuery` for filtered/joined pagination, or
`run()` directly for queries that `CTGDBQuery` cannot express.

### Config Options

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `columns` | `array` | `['*']` | Columns to select (table/filter sources only) |
| `sort` | `string` | `null` | Column to sort by |
| `order` | `string` | `'ASC'` | Sort direction: `'ASC'` or `'DESC'` |
| `page` | `int` | `1` | Current page (1-based) |
| `per_page` | `int` | `20` | Rows per page |
| `total` | `int` | `null` | Pre-computed total (skips count query) |

### Return Structure

Always the same shape regardless of source type:

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

For **table name** and **filter** sources, paginate builds the SQL
directly:

```sql
-- Count
SELECT COUNT(*) as total FROM guitars WHERE make = ?

-- Data
SELECT * FROM guitars WHERE make = ?
ORDER BY model ASC LIMIT 5 OFFSET 0
```

For **raw query** and **join query** sources, paginate wraps in a
subquery:

```sql
-- Count
SELECT COUNT(*) as total FROM (
    SELECT g.model, p.make as pickup_make
    FROM guitars g
    INNER JOIN pickups p ON g.id = p.guitar_id
    WHERE g.year_purchased > ?
) as _paginated

-- Data
SELECT * FROM (
    SELECT g.model, p.make as pickup_make
    FROM guitars g
    INNER JOIN pickups p ON g.id = p.guitar_id
    WHERE g.year_purchased > ?
) as _paginated
ORDER BY model ASC LIMIT 20 OFFSET 0
```

The `total` config option lets callers skip the count query when they
already know the total. This is the primary extension point for
performance optimization in subclasses.

---

## WHERE Clause Behavior

Use `CTGDBQuery::from()->where()` for all read query WHERE conditions.
`CTGDBQuery` supports the full operator set (`=`, `>`, `<`, `>=`, `<=`,
`!=`, `LIKE`, `NOT LIKE`, `IN`, `NOT IN`, `IS`, `IS NOT`, `BETWEEN`).

The `where` config in `read()` accepts only the associative array form
for backward compatibility with `update()` and `delete()`:

```php
'where' => [
    'make' => ['type' => 'str', 'value' => 'Fender'],
    'year_purchased' => ['type' => 'int', 'value' => 2019]
]
// Generates: WHERE make = ? AND year_purchased = ?
```

String `where`, `where_raw`, and raw `having` are **removed** — passing
a string to `where` throws `INVALID_ARGUMENT`. Use `CTGDBQuery` for all
read queries, or `run()` for anything `CTGDBQuery` cannot express.

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
| `read()` | Translates compatibility config into `CTGDBQuery`; validation occurs in the builder |
| `update()` | Table name, column names (data keys + where keys) |
| `delete()` | Table name, column names (where keys) |
| `paginate()` | Builds or clones `CTGDBQuery`; sort validation occurs in the builder |
| `CTGDBQuery` | Table name, column names, join types, operators, sort direction — **all validated by construction** |
| `run()` | **No identifier validation** — raw SQL is the caller's responsibility |

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
delegates materialized row execution to `CTGDBConn::query()`.
`CTGDB::create()` delegates inserts to `CTGDBConn::insert()`, while update and
delete delegate affected-row execution to `CTGDBConn::execute()`.
`CTGDB::process()` delegates incremental result handling to
`CTGDBConn::process()`. These paths map duplicate and constraint failures and
permanently invalidate the connection when driver codes 2006 or 2013 show that
it was lost.

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

PDO binding and type resolution are private `CTGDBConn` details. No PDO
accessor is exposed to `CTGDB`, subclasses, or application callers.

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
2. **CTGDBConn** — PDO setup, guarded execution, lifecycle, and transaction state
3. **CTGDB composition** — main query API owns and delegates to a connection
4. **CTGDBQuery** — the sole SELECT builder and canonical identifier validator
5. **Connection binding** — private value/type resolution inside `CTGDBConn`
6. **run()** — materialized row execution with an array-only return contract
7. **process()** — incremental row handling
8. **CRUD** — create, read, update, delete (delegates to typed connection operations)
9. **paginate()** — paging with metadata
10. **Integration** — `run()`, `process()`, `read()`, and `paginate()` accept `CTGDBQuery` instances where applicable
