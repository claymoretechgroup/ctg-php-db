# ctg-php-db — Library Specification

## Overview

A minimal, opinionated PHP database library built on PDO. One class,
one connection, one low-level method (`run`) with CRUD convenience
methods (`create`, `read`, `update`, `delete`) built on top. Filtering
and pagination are separate, composable operations that work on any
result set — including joins, subqueries, and aggregations.

Supports parameterized queries with explicit typing and a
fold/accumulator pattern for flexible result shaping.

`CTGDBQuery` is the structured query builder for read operations —
the default path for all SELECT queries. It validates identifiers,
allowlists operators, and parameterizes all values by construction.
Queries that `CTGDBQuery` cannot express use `run()` directly.

---

## Design Principles

1. **`run` is the primitive** — every database operation flows through
   a single method that handles preparation, binding, and execution
2. **Data-driven queries** — queries are represented as associative
   arrays or `CTGDBQuery` instances, not raw SQL strings
3. **Fold over results** — callers control the shape of output via an
   optional transform function and accumulator (reduce/fold pattern)
4. **Explicit types** — parameter types are declared, not inferred,
   mapping directly to PDO param constants
5. **Separation of concerns** — CRUD, filtering, and pagination are
   independent operations that compose together
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
    // ─── Construction ──────────────────────────────────────

    // CONSTRUCTOR :: STRING, STRING, STRING, STRING, ARRAY -> $this
    // Creates a new database connection via PDO
    public function __construct(
        string $host,
        string $database,
        string $username,
        string $password,
        array  $options = []
    );

    // Static Factory Method :: STRING, STRING, STRING, STRING, ARRAY -> ctgdb
    // Creates and returns a new CTGDB instance
    public static function connect(
        string $host,
        string $database,
        string $username,
        string $password,
        array  $options = []
    ): static;

    // ─── Low-level ─────────────────────────────────────────

    // :: STRING|ARRAY, ?(ARRAY, MIXED -> MIXED), MIXED -> MIXED
    // Execute a query with optional fold/accumulator over results
    public function run(
        string|array $query,
        ?callable    $fn = null,
        mixed        $accumulator = []
    ): mixed;

    // ─── CRUD ──────────────────────────────────────────────

    // :: STRING, ARRAY -> INT|STRING
    // Insert a single row, returns last insert ID
    public function create(string $table, array $data): int|string;

    // :: STRING|ARRAY|ctgdbQuery, ARRAY, ?(ARRAY, MIXED -> MIXED), MIXED -> MIXED
    // Read rows from one or more tables with optional transform
    // When $tables is a CTGDBQuery, $config is ignored
    public function read(
        string|array|CTGDBQuery $tables,
        array                   $config = [],
        ?callable               $fn = null,
        mixed                   $accumulator = []
    ): mixed;

    // :: STRING, ARRAY, ARRAY -> INT
    // Update rows matching WHERE conditions, returns affected count
    public function update(
        string $table,
        array  $data,
        array  $where
    ): int;

    // :: STRING, ARRAY -> INT
    // Delete rows matching WHERE conditions, returns affected count
    public function delete(string $table, array $where): int;

    // ─── Filtering ─────────────────────────────────────────

    // :: STRING, ARRAY -> ARRAY
    // Build a reusable filter with operator support
    // @deprecated Use CTGDBQuery::from()->where() instead
    public function filter(string $table, array $conditions): array;

    // ─── Join Shortcuts ────────────────────────────────────

    // :: STRING|ARRAY, ARRAY, ?(ARRAY, MIXED -> MIXED), MIXED -> MIXED
    // Inner join shortcut — delegates to read() with join => 'inner'
    // @deprecated Use CTGDBQuery::from()->join(..., 'inner', ...) instead
    public function join(
        string|array $tables,
        array        $config = [],
        ?callable    $fn = null,
        mixed        $accumulator = []
    ): mixed;

    // :: STRING|ARRAY, ARRAY, ?(ARRAY, MIXED -> MIXED), MIXED -> MIXED
    // Left join shortcut — delegates to read() with join => 'left'
    // @deprecated Use CTGDBQuery::from()->join(..., 'left', ...) instead
    public function leftJoin(
        string|array $tables,
        array        $config = [],
        ?callable    $fn = null,
        mixed        $accumulator = []
    ): mixed;

    // ─── Pagination ────────────────────────────────────────

    // :: STRING|ARRAY|ctgdbQuery, ARRAY, ?(ARRAY, MIXED -> MIXED), MIXED -> ARRAY
    // Paginate any result set with metadata
    // CTGDBQuery is the preferred source type
    public function paginate(
        string|CTGDBQuery $source,
        array                   $config = [],
        ?callable               $fn = null,
        mixed                   $accumulator = []
    ): array;

    // ─── Composition ───────────────────────────────────────

    // :: [(MIXED, ctgdb -> MIXED)] -> (MIXED -> MIXED)
    // Build a pipeline of functions that thread an accumulator and $this
    public function compose(array $fns): callable;
}
```

---

## Constructor & Factory

```php
// Standard construction
$db = new CTGDB('localhost', 'ctg_staging', 'ctg_dev', 'password');

// Static factory for fluent usage
$db = CTGDB::connect('localhost', 'ctg_staging', 'ctg_dev', 'password');

// With options
$db = CTGDB::connect('localhost', 'ctg_staging', 'ctg_dev', 'password', [
    'charset' => 'utf8mb4',        // default: utf8mb4
    'timeout' => 5,                 // connection timeout in seconds
    'persistent' => false,          // persistent connections
]);
```

Internally creates a PDO instance with:
- `PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION`
- `PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC`
- `PDO::ATTR_EMULATE_PREPARES => false`

The factory method `connect()` returns `new static(...)` (not `self`)
so subclasses inherit the factory correctly.

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

## run() — The Primitive

### Signature

```php
// :: STRING|ARRAY, ?(ARRAY, MIXED -> MIXED), MIXED -> MIXED
// Execute a query with optional fold/accumulator over results
public function run(
    string|array $query,
    ?callable    $fn = null,
    mixed        $accumulator = []
): mixed;
```

### Query Formats

```php
// Plain SQL string (no parameters)
$db->run('SELECT * FROM guitars');

// Parameterized with positional placeholders
$db->run([
    'sql' => 'SELECT * FROM guitars WHERE id = ? AND year_purchased > ?',
    'values' => [
        ['type' => 'int', 'value' => 1],
        ['type' => 'int', 'value' => 2010]
    ]
]);

// Parameterized with named placeholders
$db->run([
    'sql' => 'SELECT * FROM guitars WHERE id = :id AND make = :make',
    'values' => [
        'id' => ['type' => 'int', 'value' => 1],
        'make' => ['type' => 'str', 'value' => 'Fender']
    ]
]);

// Untyped convenience
$db->run([
    'sql' => 'SELECT * FROM guitars WHERE id = ?',
    'values' => [42]
]);
```

### Fold/Accumulator Pattern

The optional `$fn` and `$accumulator` parameters implement a fold
(reduce) over the result set. For each row, `$fn` is called with
the current row and the current accumulator value. It returns the
new accumulator value. After all rows, the final accumulator is
returned.

```
$fn signature: fn(array $record, mixed $accumulator): mixed
```

**Default behavior (no function):**
Each row is appended to an array. This is equivalent to:
`fn($record, $result) => [...$result, $record]`

```php
// Returns array of associative arrays
$guitars = $db->run('SELECT * FROM guitars');
// [['id' => 1, 'make' => 'Ibanez', ...], ['id' => 2, ...]]

// Extract a single column
$makes = $db->run(
    'SELECT make FROM guitars',
    fn($record, $result) => [...$result, $record['make']],
    []
);

// Key by ID
$byId = $db->run(
    'SELECT * FROM guitars',
    fn($record, $result) => $result + [$record['id'] => $record],
    []
);

// Sum a column
$total = $db->run(
    'SELECT year_purchased FROM guitars',
    fn($record, $sum) => $sum + $record['year_purchased'],
    0
);
```

### Non-SELECT Queries

For INSERT, UPDATE, DELETE statements, `run` returns:
- **INSERT**: last insert ID (string|int)
- **UPDATE/DELETE**: affected row count (int)

The fold function is ignored for non-SELECT queries.

Detection: if the PDO statement's `columnCount()` is 0, it's a
write operation. Otherwise it's a read. INSERT detection for
returning `lastInsertId()` vs `rowCount()`: check if the SQL starts
with INSERT (case-insensitive, trimmed).

---

## CRUD Methods

All CRUD methods build query arrays internally and delegate to `run`.

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
// :: STRING|ARRAY|ctgdbQuery, ARRAY, ?(ARRAY, MIXED -> MIXED), MIXED -> MIXED
// Read rows from one or more tables with optional transform
public function read(
    string|array|CTGDBQuery $tables,
    array                   $config = [],
    ?callable               $fn = null,
    mixed                   $accumulator = []
): mixed;
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

// With fold
$byMake = $db->read(
    CTGDBQuery::from('guitars'),
    [],
    fn($row, $acc) => array_merge($acc, [$row['make'] => $row]),
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

## filter() — Building Reusable Conditions

> **Deprecated.** Use `CTGDBQuery::from()->where()` as the replacement.
> `CTGDBQuery` supports the full operator set (`=`, `>`, `LIKE`, `IN`,
> `BETWEEN`, etc.) with validated identifiers and parameterized values.
> This section is retained for reference and backward compatibility.

### Signature

```php
// :: STRING, ARRAY -> ARRAY
// Build a reusable filter with operator support
public function filter(string $table, array $conditions): array;
```

**Removed** — use `CTGDBQuery::from()->where()` instead.

Previously built reusable WHERE conditions. This functionality is now
handled by `CTGDBQuery` which provides the same operator support with
safe-by-default query construction.

### Condition Format

```php
$filter = $db->filter('guitars', [
    'year_purchased' => ['type' => 'int', 'value' => 2020, 'op' => '>='],
    'make' => ['type' => 'str', 'value' => '%Fender%', 'op' => 'LIKE'],
]);
```

**Supported operators:**

| Operator | Example | SQL generated |
|----------|---------|---------------|
| `=` (default) | `['type' => 'int', 'value' => 1]` | `column = ?` |
| `>` `<` `>=` `<=` `!=` | `['type' => 'int', 'value' => 2020, 'op' => '>=']` | `column >= ?` |
| `LIKE` | `['type' => 'str', 'value' => '%term%', 'op' => 'LIKE']` | `column LIKE ?` |
| `NOT LIKE` | `['type' => 'str', 'value' => '%term%', 'op' => 'NOT LIKE']` | `column NOT LIKE ?` |
| `IN` | `['type' => 'str', 'value' => ['a','b'], 'op' => 'IN']` | `column IN (?,?)` |
| `NOT IN` | `['type' => 'int', 'value' => [1,2], 'op' => 'NOT IN']` | `column NOT IN (?,?)` |
| `IS` | `['type' => 'null', 'value' => null, 'op' => 'IS']` | `column IS NULL` |
| `IS NOT` | `['type' => 'null', 'value' => null, 'op' => 'IS NOT']` | `column IS NOT NULL` |
| `BETWEEN` | `['type' => 'int', 'value' => [2020,2025], 'op' => 'BETWEEN']` | `column BETWEEN ? AND ?` |

All conditions are AND-joined. For OR logic or complex expressions,
drop down to `run()` with raw SQL.

### Return Structure

```php
[
    'table' => 'guitars',
    'where' => 'year_purchased >= ? AND make LIKE ?',
    'values' => [
        ['type' => 'int', 'value' => 2020],
        ['type' => 'str', 'value' => '%Fender%']
    ]
]
```

This can be passed directly to `paginate()`, or unpacked manually
for use with `run()`:

```php
$filter = $db->filter('guitars', [
    'make' => ['type' => 'str', 'value' => 'Fender'],
]);

// Pass to paginate
$result = $db->paginate($filter, ['page' => 1, 'sort' => 'model']);

// Or use with run for custom queries
$count = $db->run([
    'sql' => "SELECT COUNT(*) as total FROM {$filter['table']} WHERE {$filter['where']}",
    'values' => $filter['values']
]);

// Reuse for different pages
$page1 = $db->paginate($filter, ['page' => 1]);
$page2 = $db->paginate($filter, ['page' => 2]);
```

---

## join() and leftJoin() — Convenience Shortcuts

> **Deprecated.** Use `CTGDBQuery::from()->join()` instead.
> `CTGDBQuery::from()->join($table, 'inner', $on)` replaces `join()`,
> and `CTGDBQuery::from()->join($table, 'left', $on)` replaces
> `leftJoin()`. This section is retained for reference and backward
> compatibility.

Syntactic sugar over `read()`. They accept the same arguments — they
just preset the join type so you don't have to specify it.

```php
// :: STRING|ARRAY, ARRAY, ?(ARRAY, MIXED -> MIXED), MIXED -> MIXED
public function join(string|array $tables, array $config = [], ...) {
    return $this->read($tables, array_merge($config, ['join' => 'inner']), ...);
}

public function leftJoin(string|array $tables, array $config = [], ...) {
    return $this->read($tables, array_merge($config, ['join' => 'left']), ...);
}
```

```php
// All inner joins
$db->join(['guitars', 'pickups'], [
    'on' => [['guitars.id' => 'pickups.guitar_id']],
    'columns' => ['guitars.model', 'pickups.make as pickup_make']
]);

// Left join via CTGDBQuery (preferred)
$result = $db->paginate(
    CTGDBQuery::from('guitars')
        ->join('pickups', 'left', ['guitars.id' => 'pickups.guitar_id'])
        ->columns('guitars.*', 'pickups.type as pickup_type'),
    ['sort' => 'guitars.make', 'page' => 1]
);
```

**When to use which:**
- All joins are the same type → `join()` or `leftJoin()`
- Mixed join types needed → `read()` with the array form of `join`

---

## paginate() — Paging Any Result Set

### Signature

```php
// :: STRING|ARRAY|ctgdbQuery, ARRAY, ?(ARRAY, MIXED -> MIXED), MIXED -> ARRAY
// Paginate any result set with metadata
public function paginate(
    string|array|CTGDBQuery $source,
    array                   $config = [],
    ?callable               $fn = null,
    mixed                   $accumulator = []
): array;
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

The fold function, when provided, applies to `data`. Pagination
metadata is always separate and unaffected by transforms.

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

> **Preferred path:** Use `CTGDBQuery::from()->where()` for all read
> query WHERE conditions. The string form of `where` described below
> is **deprecated** for new code.

The `where` config in `read()` and other CRUD methods accepts two forms:

**Associative array** — simple `=` conditions, auto-parameterized:

```php
'where' => [
    'make' => ['type' => 'str', 'value' => 'Fender'],
    'year_purchased' => ['type' => 'int', 'value' => 2019]
]
// Generates: WHERE make = ? AND year_purchased = ?
```

**String + values** — raw WHERE clause with explicit parameter binding
(**deprecated** — use `CTGDBQuery::from()->where()`):

```php
'where' => 'make = ? AND year_purchased > ?',
'values' => [
    ['type' => 'str', 'value' => 'Fender'],
    ['type' => 'int', 'value' => 2010]
]
```

When `where` is a string, `values` must be provided. All values are
bound through PDO prepared statements.

`filter()` extends the associative array form with operator support
(`>`, `LIKE`, `IN`, etc.) for more expressive conditions without
resorting to raw strings.

For anything beyond what `filter()` supports — OR conditions, complex
subqueries, HAVING — use the string form or drop down to `run()`.

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

```php
// Connection failures (in constructor)
// Uses errorInfo[1] driver codes and SQLSTATE for classification
try {
    $this->_pdo = new PDO($dsn, $username, $password, $options);
} catch (\PDOException $e) {
    $info = $e->errorInfo ?? [null, null, null];
    $driverCode = $info[1] ?? null;
    $sqlstate = $info[0] ?? $e->getCode();

    $type = match(true) {
        in_array($driverCode, [1045, 1044], true) => 'AUTH_FAILED',
        $sqlstate === '28000'                      => 'AUTH_FAILED',
        $driverCode === 2013                       => 'CONNECTION_TIMEOUT',
        in_array($driverCode, [2002, 2003], true)
            && str_contains($msg, 'timed out')     => 'CONNECTION_TIMEOUT',
        default                                    => 'CONNECTION_FAILED',
    };
    throw new CTGDBError($type, $e->getMessage(), [
        'host' => $host,
        'database' => $database,
        'sqlstate' => $sqlstate,
        'driver_code' => $driverCode,
        'original' => $e
    ]);
}

// Query execution (in run)
// Uses driver codes for precise classification, SQLSTATE as fallback
try {
    $stmt->execute();
} catch (\PDOException $e) {
    $info = $e->errorInfo ?? [null, null, null];
    $driverCode = $info[1] ?? null;
    $sqlstate = $info[0] ?? (string)$e->getCode();

    $type = match(true) {
        in_array($driverCode, [1062, 1586], true)
            => 'DUPLICATE_ENTRY',
        in_array($driverCode, [1451, 1452, 1216, 1217, 1048, 3819, 4025], true)
            => 'CONSTRAINT_VIOLATION',
        $sqlstate === '23000'
            => 'CONSTRAINT_VIOLATION',
        in_array($driverCode, [2006, 2013], true)
            => 'CONNECTION_FAILED',
        default
            => 'QUERY_FAILED',
    };
    throw new CTGDBError($type, $e->getMessage(), [
        'sqlstate' => $sqlstate,
        'driver_code' => $driverCode,
        'query' => $sql,
        'original' => $e
    ]);
}

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

`compose` is a specialized version of `CTGFnprog::pipe` that injects
the DB instance as a second argument. Use `compose` when steps need
database access. Use `CTGFnprog::pipe` for pure data transformation.

### Usage

```php
use CTG\DB\CTGDB;
use CTG\FnProg\CTGFnprog;

$db = CTGDB::connect('localhost', 'myapp', 'user', 'pass');

// DB compose — steps can query the database
$report = $db->compose([
    fn($_, $db) => $db->read('guitars', [
        'where' => ['make' => ['type' => 'str', 'value' => 'Fender']]
    ]),
    fn($guitars, $db) => $db->join(['guitars', 'pickups'], [
        'on' => [['guitars.id' => 'pickups.guitar_id']],
        'columns' => ['guitars.model', 'pickups.type'],
        'where' => ['guitars.make' => ['type' => 'str', 'value' => 'Fender']]
    ]),
    fn($rows, $_) => CTGFnprog::pipe([
        CTGFnprog::groupBy('model'),
    ])($rows)
]);

$fenderReport = $report();
```

### Using CTGFnprog in Pipelines

Pure data transforms use CTGFnprog methods. Database steps use the
`$db` argument. They compose naturally:

```php
$pipeline = $db->compose([
    fn($_, $db) => $db->read('guitars'),
    fn($guitars, $_) => CTGFnprog::pipe([
        CTGFnprog::filter(fn($g) => $g['year_purchased'] >= 2020),
        CTGFnprog::sortBy('year_purchased', 'DESC'),
        CTGFnprog::pick(['make', 'model', 'color']),
    ])($guitars),
]);

$recentGuitars = $pipeline();
```

### Composing Pipelines from Pipelines

```php
$getGuitars = $db->compose([
    fn($_, $db) => $db->read('guitars')
]);

$formatForApi = $db->compose([
    fn($guitars, $_) => CTGFnprog::pipe([
        CTGFnprog::pick(['id', 'make', 'model', 'color']),
        CTGFnprog::sortBy('make'),
    ])($guitars),
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
| `resolveType()` | Value/type resolution — typed form or PHP type inference |
| `buildWhere()` | WHERE clause generation from associative array |
| `buildJoinSql()` | JOIN clause generation |
| `buildPaginationMeta()` | Pagination metadata calculation |
| `validateIdentifier()` | Identifier regex validation |
| `validateJoinType()` | Join type allowlist check |
| `validateSortDirection()` | Sort direction allowlist check |
| `validateOperator()` | Filter operator allowlist check |
| `getPdo()` | Access to the underlying PDO instance |

---

## File Structure

```
ctg-php-db/
├── composer.json
├── docs/
│   └── spec.md
├── src/
│   ├── CTGDB.php
│   ├── CTGDBError.php
│   └── CTGDBQuery.php
├── tests/
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
    "description": "Minimal PDO database library with fold/accumulator pattern",
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
2. **Constructor + connect()** — PDO setup, connection options
3. **resolveType()** — value/type resolution (protected)
4. **Validation methods** — validateIdentifier, validateJoinType,
   validateSortDirection, validateOperator (protected)
5. **run()** — the primitive everything delegates to
6. **CRUD** — create, read, update, delete (delegates to run)
7. **filter()** — reusable conditions with operator support
8. **join() / leftJoin()** — convenience shortcuts (delegate to read)
9. **paginate()** — paging with metadata
10. **compose()** — function pipelines with DB injection
11. **CTGDBQuery** — structured query builder for read operations
    (see `docs/CTGDBQuery.md` for full spec)
12. **CTGDB integration** — update `read()` and `paginate()` to accept
    `CTGDBQuery` instances
