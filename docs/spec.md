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
2. **Data-driven queries** — queries are represented as associative
   arrays or `CTGDBQuery` instances, not raw SQL strings
3. **Process results** — callers control incremental output through a row
   processor and initial state
4. **Explicit types** — parameter types are declared, not inferred,
   mapping directly to PDO param constants
5. **Separation of concerns** — `CTGDB` owns query semantics while a composed
   `CTGDBConn` exclusively owns PDO, execution, and connection lifecycle
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

    // :: STRING|ctgdbQuery, ARRAY -> ARRAY|INT|STRING
    // Execute a query and return its complete materialized result
    public function run(string|CTGDBQuery $query, array $values = []): array|int|string;

    // :: STRING|ctgdbQuery, (ARRAY, MIXED -> MIXED), MIXED, ARRAY -> MIXED
    // Process query results one row at a time and return final state
    public function process(string|CTGDBQuery $query, callable $processor, mixed $initial = null, array $values = []): mixed;

    // ─── CRUD ──────────────────────────────────────────────

    // :: STRING, ARRAY -> INT|STRING
    // Insert a single row, returns last insert ID
    public function create(string $table, array $data): int|string;

    // :: STRING|ARRAY|ctgdbQuery, ARRAY -> ARRAY
    // Read rows from one or more tables
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

    // ─── Composition ───────────────────────────────────────

    // :: [(MIXED, ctgdb -> MIXED)] -> (MIXED -> MIXED)
    // Build a pipeline of functions that thread an accumulator and $this
    public function compose(array $fns): callable;

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
// :: STRING|ctgdbQuery, ARRAY -> ARRAY|INT|STRING
// Execute a query and return its complete materialized result
public function run(string|CTGDBQuery $query, array $values = []): array|int|string;
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
- INSERT returns the last insert ID.
- UPDATE, DELETE, and other non-row statements return affected-row count.

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
returned when all rows have been consumed.

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

The general-purpose read method. Handles single-table queries,
multi-table joins, and everything in between based on what you
pass as `$tables`.

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
        ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id'])
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

### Legacy Usage

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
    ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id'])
    ->columns('guitars.*', 'pickups.type as pickup_type');

$result = $db->paginate($query, [
    'sort' => 'guitars.make',
    'page' => 1,
    'per_page' => 5
]);
```

**With transform:**

```php
$pickupsByGuitar = $db->read(['guitars', 'pickups'], [
    'join' => 'inner',
    'on' => [['guitars.id' => 'pickups.guitar_id']],
    'columns' => ['guitars.model', 'pickups.make as pickup_make']
], function($record, $result) {
    $model = $record['model'];
    $result[$model] ??= [];
    $result[$model][] = $record['pickup_make'];
    return $result;
}, []);
// ['GRX20L' => ['USA Jackson', 'Seymour Duncan'], ...]
```

**Composing join + filter via CTGDBQuery:**

```php
$result = $db->read(
    CTGDBQuery::from('guitars')
        ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id'])
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
    ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id'])
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

Table names, column names, and aliases that are interpolated into SQL
are validated with a regex that allows only safe characters:
alphanumeric, underscores, dots, and backticks. Anything else is
rejected with a CTGDBError.

```php
// :: STRING -> STRING
// Validates and backtick-quotes an identifier
protected function validateIdentifier(string $identifier): string
{
    $clean = trim($identifier, '`');
    if (!preg_match('/^[a-zA-Z_][a-zA-Z0-9_.]*$/', $clean)) {
        throw new CTGDBError('INVALID_IDENTIFIER',
            "Invalid identifier: {$identifier}",
            ['identifier' => $identifier]
        );
    }
    return "`{$clean}`";
}
```

Column references with table prefixes (`guitars.model`), aliases
(`guitars.model as guitar_model`), wildcards (`*`, `guitars.*`), and
aggregate expressions (`COUNT(*)`, `COUNT(pickups.id)`) are handled
by parsing and validating each component.

### Keyword Allowlists

SQL keywords that get interpolated are validated against hardcoded
allowlists:

```php
// Join types: inner, left, right, cross
protected function validateJoinType(string $type): string;

// Sort direction: asc, desc
protected function validateSortDirection(string $dir): string;

// Filter operators: =, >, <, >=, <=, !=, like, not like,
//                   in, not in, is, is not, between
protected function validateOperator(string $op): string;
```

### Where Each Validation Runs

| Method | What's validated |
|--------|-----------------|
| `create()` | Table name, column names (from data keys) |
| `read()` | Table name(s), column names, join types, `on` column references, sort direction |
| `update()` | Table name, column names (data keys + where keys) |
| `delete()` | Table name, column names (where keys) |
| `filter()` | Table name, column names, operators |
| `paginate()` | Sort column, sort direction |
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
delegates materialized execution to `CTGDBConn::execute()`. `CTGDB::process()`
delegates incremental result handling to `CTGDBConn::process()`. Both paths
map duplicate and constraint failures and permanently invalidate the connection
when driver codes 2006 or 2013 show that it was lost.

```php
$result = $this->_connection->execute($sql, $values);
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

## compose() — Function Pipelines

### Signature

```php
// :: [(MIXED, ctgdb -> MIXED)] -> (MIXED -> MIXED)
// Build a pipeline of functions that thread an accumulator and $this
public function compose(array $fns): callable;
```

Each function in the array receives `($accumulator, $db)`. The
returned callable accepts an optional initial value.

### Implementation

```php
public function compose(array $fns): callable
{
    return function(mixed $accumulator = null) use ($fns): mixed {
        $result = $accumulator;
        foreach ($fns as $fn) {
            $result = $fn($result, $this);
        }
        return $result;
    };
}
```

`compose` threads both accumulated data and the DB instance through each step.
Standard PHP callbacks and array functions can perform pure data transforms,
so pipeline composition does not require another library.

### Usage

```php
use CTG\DB\CTGDB;
use CTG\DB\CTGDBQuery;

$db = CTGDB::init([
    'host' => 'localhost',
    'database' => 'myapp',
    'username' => 'user',
    'password' => 'pass',
]);

// DB compose — steps can query the database
$report = $db->compose([
    fn($_, $db) => $db->read(
        CTGDBQuery::from('guitars')
            ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id'])
            ->columns('guitars.model', 'pickups.type')
            ->where('guitars.make', '=', 'Fender')
    ),
    function($rows, $_) {
        $grouped = [];
        foreach ($rows as $row) {
            $grouped[$row['model']][] = $row;
        }
        return $grouped;
    },
]);

$fenderReport = $report();
```

### Using PHP Array Functions in Pipelines

Database steps use the `$db` argument. Pure data transforms can use standard
PHP callbacks and array functions:

```php
$pipeline = $db->compose([
    fn($_, $db) => $db->read(
        CTGDBQuery::from('guitars')
            ->where('year_purchased', '>=', 2020, 'int')
            ->orderBy('year_purchased', 'DESC')
    ),
    fn($guitars, $_) => array_map(fn($guitar) => [
        'make' => $guitar['make'],
        'model' => $guitar['model'],
        'color' => $guitar['color'],
    ], $guitars),
]);

$recentGuitars = $pipeline();
```

### Composing Pipelines from Pipelines

```php
$getGuitars = $db->compose([
    fn($_, $db) => $db->read('guitars')
]);

$formatForApi = $db->compose([
    function($guitars, $_) {
        $formatted = array_map(fn($guitar) => [
            'id' => $guitar['id'],
            'make' => $guitar['make'],
            'model' => $guitar['model'],
            'color' => $guitar['color'],
        ], $guitars);
        usort($formatted, fn($left, $right) => $left['make'] <=> $right['make']);
        return $formatted;
    },
]);

$fullPipeline = $db->compose([
    $getGuitars,
    $formatForApi
]);

$result = $fullPipeline();
```

---

## Protected Internals

The following methods are `protected` to support subclassing:

| Method | Purpose |
|--------|---------|
| `buildWhere()` | WHERE clause generation from associative array |
| `buildPaginationMeta()` | Pagination metadata calculation |
| `validateIdentifier()` | Identifier regex validation |
| `validateJoinType()` | Join type allowlist check |
| `validateSortDirection()` | Sort direction allowlist check |
| `validateOperator()` | Filter operator allowlist check |

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
4. **CTGDBQuery** — structured query builder for read operations
5. **Connection binding** — private value/type resolution inside `CTGDBConn`
6. **Validation methods** — validateIdentifier, validateJoinType,
   validateSortDirection, validateOperator (protected)
7. **run()** — materialized execution
8. **process()** — incremental row handling
9. **CRUD** — create, read, update, delete (delegates to run)
10. **paginate()** — paging with metadata
11. **compose()** — function pipelines with DB injection
12. **Integration** — `run()`, `process()`, `read()`, and `paginate()` accept `CTGDBQuery` instances where applicable
