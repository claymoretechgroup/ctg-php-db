# CTGDBQuery — Structured Query Builder

## Overview

A fluent query builder that produces the `['sql' => ..., 'values' => [...]]`
statement array that `run()` already accepts. It exists to eliminate raw SQL
strings from the library's read path.

**Primary motivation: AI safety.** When an LLM generates code against this
library, `read()` currently accepts raw SQL in `where`, `having`, and column
expressions. An LLM will naturally reach for the string path and interpolate
user input. `CTGDBQuery` removes the string path: every query element is
structured, validated, and parameterized by construction.

**Trust boundary:** `CTGDBQuery` handles SELECT, JOINs, WHERE conditions,
ORDER BY, GROUP BY, LIMIT/OFFSET, and pagination. Anything the builder
cannot express uses `run()` directly. Any `run()` call that incorporates
user input into the SQL string should be flagged for code review — that is
the explicit trust boundary.

**Relationship to CTGDB:**
- `CTGDBQuery` is a standalone class in `CTG\DB`
- It produces statement arrays consumable by `run()`
- `read()` and `paginate()` accept `CTGDBQuery` instances
- `CTGDBQuery` does not execute queries — execution is always through `CTGDB`

---

## Class Interface

```php
namespace CTG\DB;

class CTGDBQuery
{
    // ─── Static Factory ────────────────────────────────────

    // :: STRING -> ctgdbQuery
    // Create a query for a single table
    public static function from(string $table): static;

    // ─── Column Selection ──────────────────────────────────

    // :: STRING, STRING, ... -> $this
    // Set columns to select (replaces any previous column list)
    // Accepts: 'col', 'table.col', 'table.*', 'col as alias', '*'
    public function columns(string ...$columns): static;

    // ─── WHERE Conditions ──────────────────────────────────

    // :: STRING, STRING, MIXED, ?STRING -> $this
    // Add a WHERE condition (AND-joined with previous conditions)
    public function where(
        string  $column,
        string  $operator,
        mixed   $value,
        ?string $type = null
    ): static;

    // ─── JOINs ─────────────────────────────────────────────

    // :: STRING, STRING, ARRAY -> $this
    // Add a JOIN clause
    // $on is associative: ['left.col' => 'right.col', ...]
    public function join(
        string $table,
        string $type,
        array  $on
    ): static;

    // ─── ORDER BY ──────────────────────────────────────────

    // :: STRING, STRING -> $this
    // Add an ORDER BY column (appends to existing order)
    public function orderBy(
        string $column,
        string $direction = 'ASC'
    ): static;

    // ─── GROUP BY ──────────────────────────────────────────

    // :: STRING, STRING, ... -> $this
    // Set GROUP BY columns
    public function groupBy(string ...$columns): static;

    // ─── LIMIT / OFFSET ────────────────────────────────────

    // :: INT -> $this
    // Set maximum rows to return
    public function limit(int $limit): static;

    // :: INT -> $this
    // Set row offset
    public function offset(int $offset): static;

    // :: INT, INT -> $this
    // Convenience: set limit and offset from page number and per-page count
    public function page(int $page, int $perPage = 20): static;

    // ─── Output ────────────────────────────────────────────

    // :: VOID -> ARRAY
    // Build the SELECT statement: ['sql' => ..., 'values' => [...]]
    public function toStatement(): array;

    // :: VOID -> ARRAY
    // Build the COUNT(*) version: ['sql' => ..., 'values' => [...]]
    // Strips ORDER BY, LIMIT, OFFSET. Replaces columns with COUNT(*) as total.
    public function toCountStatement(): array;
}
```

---

## Static Factory

```php
$query = CTGDBQuery::from('guitars');
```

The table name is validated with the same identifier rules as CTGDB. Invalid
identifiers throw `CTGDBError` with type `INVALID_IDENTIFIER`.

Multi-table queries are expressed through `join()`, not through the factory:

```php
$query = CTGDBQuery::from('guitars')
    ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id']);
```

---

## WHERE Conditions

### `where(column, operator, value, type)`

Adds an AND-joined condition. Conditions are stored as an indexed list (not
an associative map), so multiple conditions on the same column work:

```php
$query = CTGDBQuery::from('guitars')
    ->where('year_purchased', '>=', 2020, 'int')
    ->where('year_purchased', '<=', 2025, 'int')
    ->where('make', 'LIKE', '%Fender%', 'str');
// WHERE `year_purchased` >= ? AND `year_purchased` <= ? AND `make` LIKE ?
```

### Supported Operators

Same set as `filter()` and `validateOperator()`:

| Operator | Value form | SQL generated |
|----------|-----------|---------------|
| `=` | scalar | `col = ?` |
| `>` `<` `>=` `<=` `!=` | scalar | `col >= ?` |
| `LIKE` `NOT LIKE` | string | `col LIKE ?` |
| `IN` `NOT IN` | array of scalars | `col IN (?, ?, ?)` |
| `IS` `IS NOT` | null (value ignored) | `col IS NULL` |
| `BETWEEN` | `[low, high]` | `col BETWEEN ? AND ?` |

### Type Parameter

- When `$type` is provided, values are stored in typed form:
  `['type' => $type, 'value' => $val]`
- When `$type` is null, values are stored as-is and type is inferred at
  bind time (same as CTGDB's untyped convenience)
- For `IN` / `NOT IN`, the type applies to every element in the array
- For `BETWEEN`, the type applies to both bounds
- For `IS` / `IS NOT`, the type parameter is ignored

### Validation

- `$column` is validated with `validateIdentifier()`
- `$operator` is validated with `validateOperator()`
- Invalid inputs throw `CTGDBError` with the appropriate type

---

## JOINs

### `join(table, type, on)`

Adds a JOIN clause. Multiple calls add multiple joins in order.

```php
// Inner join
$query = CTGDBQuery::from('guitars')
    ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id']);

// Left join
$query = CTGDBQuery::from('guitars')
    ->join('pickups', 'left', ['guitars.id' => 'pickups.guitar_id']);

// Multiple joins
$query = CTGDBQuery::from('articles')
    ->join('categories', 'left', ['articles.category_id' => 'categories.id'])
    ->join('users', 'inner', ['articles.author_id' => 'users.id']);

// Composite ON keys
$query = CTGDBQuery::from('orders')
    ->join('users', 'inner', [
        'orders.user_id'   => 'users.id',
        'orders.tenant_id' => 'users.tenant_id'
    ]);
// INNER JOIN `users` ON `orders`.`user_id` = `users`.`id`
//   AND `orders`.`tenant_id` = `users`.`tenant_id`
```

### Parameters

- `$table` — validated with `validateIdentifier()`
- `$type` — validated with `validateJoinType()`: `'inner'`, `'left'`,
  `'right'`, `'cross'`
- `$on` — associative array of column equality pairs. Both sides validated
  with `validateIdentifier()`. For `cross` joins, `$on` must be empty.

---

## Column Selection

### `columns(...$columns)`

Sets the column list. Replaces any previous list. Defaults to `['*']` if
never called.

```php
// Specific columns
CTGDBQuery::from('guitars')->columns('id', 'make', 'model');
// SELECT `id`, `make`, `model` FROM ...

// Table-qualified
CTGDBQuery::from('guitars')
    ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id'])
    ->columns('guitars.*', 'pickups.type', 'pickups.position');

// Aliases
CTGDBQuery::from('guitars')
    ->columns('guitars.model', 'pickups.make as pickup_make');

// Wildcard (default)
CTGDBQuery::from('guitars')->columns('*');
```

### Accepted Forms

- `'col'` — bare column, validated and quoted
- `'table.col'` — table-qualified, both parts validated
- `'table.*'` — table wildcard, table part validated
- `'*'` — global wildcard
- `'col as alias'` / `'table.col as alias'` — both sides validated

### Not Accepted

Aggregate expressions (`COUNT(*)`, `SUM(price)`) and raw SQL expressions
are not supported in `columns()`. Queries requiring aggregate columns in
the SELECT list should use `run()` directly.

---

## ORDER BY

### `orderBy(column, direction)`

Appends an ORDER BY column. Multiple calls build multi-column sort.

```php
$query = CTGDBQuery::from('guitars')
    ->orderBy('make', 'ASC')
    ->orderBy('year_purchased', 'DESC');
// ORDER BY `make` ASC, `year_purchased` DESC

// Default direction is ASC
CTGDBQuery::from('guitars')->orderBy('make');
// ORDER BY `make` ASC
```

- `$column` — validated with `validateIdentifier()`, supports `table.col`
- `$direction` — validated with `validateSortDirection()`, defaults to `'ASC'`

---

## GROUP BY

### `groupBy(...$columns)`

Sets the GROUP BY columns. Replaces any previous GROUP BY.

```php
CTGDBQuery::from('guitars')->groupBy('make');
// GROUP BY `make`

CTGDBQuery::from('guitars')->groupBy('make', 'color');
// GROUP BY `make`, `color`
```

Each column is validated with `validateIdentifier()`.

---

## LIMIT / OFFSET

### `limit(int $limit)` and `offset(int $offset)`

```php
CTGDBQuery::from('guitars')->limit(10)->offset(20);
// LIMIT 10 OFFSET 20
```

Values must be non-negative integers.

### `page(int $page, int $perPage)`

Convenience method. Calculates LIMIT and OFFSET: `offset = (page - 1) * perPage`.

```php
CTGDBQuery::from('guitars')->page(3, 10);
// LIMIT 10 OFFSET 20

// Default perPage is 20
CTGDBQuery::from('guitars')->page(2);
// LIMIT 20 OFFSET 20
```

`$page` is clamped to minimum 1. `$perPage` is clamped to minimum 1.

`page()` and `limit()`/`offset()` write to the same internal state —
calling one overwrites the other.

---

## Output Methods

### `toStatement()`

Builds the complete SELECT statement and returns the statement array.

```php
$query = CTGDBQuery::from('guitars')
    ->columns('make', 'model')
    ->where('year_purchased', '>=', 2020, 'int')
    ->orderBy('make');

$stmt = $query->toStatement();
// [
//     'sql' => 'SELECT `make`, `model` FROM `guitars`
//               WHERE `year_purchased` >= ? ORDER BY `make` ASC',
//     'values' => [['type' => 'int', 'value' => 2020]]
// ]

$rows = $db->run($stmt);
```

**SQL generation order:**
1. `SELECT` columns
2. `FROM` table
3. `JOIN` clauses (in order added)
4. `WHERE` conditions
5. `GROUP BY` columns
6. `ORDER BY` columns
7. `LIMIT`
8. `OFFSET`

### `toCountStatement()`

Builds a `COUNT(*)` version for pagination counting.

```php
$countStmt = $query->toCountStatement();
// [
//     'sql' => 'SELECT COUNT(*) as total FROM `guitars`
//               WHERE `year_purchased` >= ?',
//     'values' => [['type' => 'int', 'value' => 2020]]
// ]
```

**Differences from `toStatement()`:**
- Columns replaced with `COUNT(*) as total`
- ORDER BY stripped
- LIMIT and OFFSET stripped
- JOIN, WHERE, and GROUP BY preserved

When GROUP BY is present, wraps in a subquery to count groups:

```sql
SELECT COUNT(*) as total FROM (
    SELECT `make` FROM `guitars` GROUP BY `make`
) as _counted
```

---

## Integration with CTGDB

### `read()` accepts CTGDBQuery

```php
// :: STRING|ARRAY|ctgdbQuery, ARRAY, ?(ARRAY, MIXED -> MIXED), MIXED -> MIXED
public function read(
    string|array|CTGDBQuery $tables,
    array                   $config = [],
    ?callable               $fn = null,
    mixed                   $accumulator = []
): mixed;
```

When `$tables` is a `CTGDBQuery`:
- `$config` is ignored (the query object contains all configuration)
- `toStatement()` is called and passed to `run()` with `$fn` and `$accumulator`

```php
$rows = $db->read(
    CTGDBQuery::from('guitars')
        ->where('make', '=', 'Fender', 'str')
        ->orderBy('model')
        ->limit(10)
);

// With fold
$byMake = $db->read(
    CTGDBQuery::from('guitars'),
    [],
    fn($row, $acc) => array_merge($acc, [$row['make'] => $row]),
    []
);
```

### `paginate()` accepts CTGDBQuery

```php
// :: STRING|ARRAY|ctgdbQuery, ARRAY, ?(ARRAY, MIXED -> MIXED), MIXED -> ARRAY
public function paginate(
    string|array|CTGDBQuery $source,
    array                   $config = [],
    ?callable               $fn = null,
    mixed                   $accumulator = []
): array;
```

When `$source` is a `CTGDBQuery`:
- `page` and `per_page` from `$config` override the query's pagination
- `sort` and `order` from `$config` override the query's ORDER BY
- `total` from `$config` skips the count query
- Internally calls `toCountStatement()` for the total and builds a
  modified statement with pagination LIMIT/OFFSET

```php
$query = CTGDBQuery::from('guitars')
    ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id'])
    ->columns('guitars.model', 'pickups.type as pickup_type')
    ->where('guitars.year_purchased', '>=', 2020, 'int');

// Page 1
$result = $db->paginate($query, [
    'sort' => 'guitars.model',
    'page' => 1,
    'per_page' => 10
]);

// Page 2, same query
$result = $db->paginate($query, [
    'sort' => 'guitars.model',
    'page' => 2,
    'per_page' => 10
]);
```

---

## Mutability

`CTGDBQuery` is mutable — each method returns `$this`. Query objects are
built in a single fluent chain and then converted to a statement. They are
not forked into variants.

To reuse a base query with variations, clone explicitly:

```php
$base = CTGDBQuery::from('guitars')
    ->where('year_purchased', '>=', 2020, 'int');

$fenders = (clone $base)->where('make', '=', 'Fender', 'str');
$gibsons = (clone $base)->where('make', '=', 'Gibson', 'str');
```

---

## Usage Examples

### Single table with conditions

```php
// Old way (raw string WHERE)
$db->read('guitars', [
    'where' => "make = ? AND year_purchased >= ?",
    'values' => [
        ['type' => 'str', 'value' => 'Fender'],
        ['type' => 'int', 'value' => 2020]
    ],
    'order' => 'model ASC',
    'limit' => 10
]);

// New way
$db->read(
    CTGDBQuery::from('guitars')
        ->where('make', '=', 'Fender', 'str')
        ->where('year_purchased', '>=', 2020, 'int')
        ->orderBy('model')
        ->limit(10)
);
```

### Join query

```php
// Old way
$db->join(['guitars', 'pickups'], [
    'on' => [['guitars.id' => 'pickups.guitar_id']],
    'columns' => ['guitars.model', 'pickups.type'],
    'where' => 'guitars.year_purchased >= ?',
    'values' => [['type' => 'int', 'value' => 2020]]
]);

// New way
$db->read(
    CTGDBQuery::from('guitars')
        ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id'])
        ->columns('guitars.model', 'pickups.type')
        ->where('guitars.year_purchased', '>=', 2020, 'int')
);
```

### Pagination (replaces filter + paginate)

```php
// Old way
$filter = $db->filter('guitars', [
    'make' => ['type' => 'str', 'value' => 'Fender'],
    'year_purchased' => ['type' => 'int', 'value' => 2020, 'op' => '>=']
]);
$result = $db->paginate($filter, ['sort' => 'model', 'page' => 1]);

// New way
$query = CTGDBQuery::from('guitars')
    ->where('make', '=', 'Fender', 'str')
    ->where('year_purchased', '>=', 2020, 'int');

$result = $db->paginate($query, ['sort' => 'model', 'page' => 1]);
```

### Join + pagination (no more as_query)

```php
// Old way
$query = $db->join(['guitars', 'pickups'], [
    'on' => [['guitars.id' => 'pickups.guitar_id']],
    'columns' => ['guitars.*', 'pickups.type as pickup_type'],
    'as_query' => true
]);
$result = $db->paginate($query, ['sort' => 'guitars.make', 'page' => 1]);

// New way
$query = CTGDBQuery::from('guitars')
    ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id'])
    ->columns('guitars.*', 'pickups.type as pickup_type');

$result = $db->paginate($query, ['sort' => 'guitars.make', 'page' => 1]);
```

### Compose pipeline

```php
$pipeline = $db->compose([
    fn($_, $db) => $db->read(
        CTGDBQuery::from('guitars')
            ->where('make', '=', 'Fender', 'str')
            ->orderBy('year_purchased', 'DESC')
    ),
    fn($guitars, $_) => CTGFnprog::pipe([
        CTGFnprog::pick(['make', 'model', 'color']),
    ])($guitars),
]);

$result = $pipeline();
```

---

## What It Replaces

| Current API | Status | Replacement |
|------------|--------|-------------|
| `filter()` | Deprecated | `CTGDBQuery::from()->where()` with full operator support |
| `join()` shortcut | Deprecated | `CTGDBQuery::from()->join(..., 'inner', ...)` |
| `leftJoin()` shortcut | Deprecated | `CTGDBQuery::from()->join(..., 'left', ...)` |
| `as_query` config | Deprecated | The query object is the query |
| `where` as string in `read()` | Superseded | `CTGDBQuery::from()->where()` |
| `where_raw` config | Superseded | Not needed |

Deprecated methods remain functional for backward compatibility. Raw string
paths on `read()` remain available but are not used with `CTGDBQuery`.

## What It Does NOT Replace

- `run()` — still the execution primitive, still accepts raw SQL for
  queries the builder cannot express
- `create()`, `update()`, `delete()` — write operations are already
  fully parameterized
- `compose()` — pipelines work with `CTGDBQuery` naturally

---

## Safety Policy

### Default Path

`CTGDBQuery` is the default path for all application read queries. All
new application code that reads from the database should use `CTGDBQuery`
unless the query cannot be expressed with the builder.

```
┌─────────────────────────────────────────────────┐
│  CTGDBQuery (default path)                      │
│  - All identifiers validated                    │
│  - All operators allowlisted                    │
│  - All values parameterized                     │
│  - No raw SQL strings accepted                  │
│  → Safe for AI-generated code                   │
└──────────────────────┬──────────────────────────┘
                       │ toStatement()
                       ▼
┌─────────────────────────────────────────────────┐
│  CTGDB::run() (explicit escape hatch)           │
│  - Accepts any SQL                              │
│  - Caller responsible for safety                │
│  - Must be audited before release               │
│  → Requires justification + review              │
└─────────────────────────────────────────────────┘
```

### Deprecation Status

The following are deprecated as of this spec. They remain functional for
backward compatibility but must not be used in new application code:

| Path | Status | Notes |
|------|--------|-------|
| `read()` with string `where` | **Deprecated** | Internal/compat only |
| `read()` with `where_raw` | **Deprecated** | Internal/compat only |
| `read()` with string `having` | **Deprecated** | Internal/compat only |
| `filter()` | **Deprecated** | Use `CTGDBQuery::from()->where()` |
| `join()` / `leftJoin()` shortcuts | **Deprecated** | Use `CTGDBQuery::from()->join()` |
| `as_query` config option | **Deprecated** | The query object is the query |

### Release Gate: `run()` Audit

Before any release of application code built on ctg-php-db:

1. **Inventory all `run()` calls.** Every direct `run()` usage must be
   documented with a justification for why `CTGDBQuery` could not
   express the query.

2. **Classify each `run()` call:**
   - **Static SQL** — no external input touches the SQL string. Safe.
     Example: `$db->run('SELECT COUNT(*) FROM migrations')`
   - **Parameterized** — external input is bound via `values`, never
     interpolated into the SQL string. Safe.
     Example: `$db->run(['sql' => 'SELECT ... WHERE col IN (SELECT ...)', 'values' => [...]])`
   - **Dynamic SQL** — external input influences the SQL string structure
     (table names, column names, operators, fragments). **Blocker.**
     Must be refactored to use parameterized form or `CTGDBQuery`.

3. **Unparameterized user input in a SQL string is a release blocker.**
   No exceptions. If a `run()` call concatenates or interpolates any
   value derived from user input, HTTP request data, or external API
   responses into the SQL string, it must be refactored before release.

### Review Checklist

For manual review (automate when feasible):

- [ ] All read queries use `CTGDBQuery` unless documented exception
- [ ] No deprecated `read()` string paths in new code
- [ ] Every `run()` call has a comment documenting why it exists
- [ ] Every `run()` call is classified as static, parameterized, or dynamic
- [ ] Zero dynamic SQL `run()` calls with unparameterized user input
- [ ] `CTGDBQuery` tests pass (unit + integration)
- [ ] Security tests cover `CTGDBQuery` paths (injection attempts rejected)

### Test Evidence

The following test categories provide evidence for the release gate:

1. **CTGDBQueryTest** — unit tests for SQL generation, validation errors,
   operator handling, column validation, join construction
2. **CTGDBSecurityTest** — injection attempts through `CTGDBQuery` methods
   are rejected (malicious identifiers, operators, values)
3. **CTGDBIntegrationTest** — `CTGDBQuery` statements execute correctly
   against a real database through `run()`

### Scope Boundaries

**In scope (v1):**
- SELECT with validated columns
- WHERE with full operator set (AND-joined)
- JOIN (inner, left, right, cross) with ON conditions
- ORDER BY, GROUP BY, LIMIT/OFFSET
- Pagination integration (toStatement / toCountStatement)

**Out of scope (use `run()`, document justification):**
- OR conditions / grouped WHERE logic
- HAVING clauses
- Aggregate expressions in SELECT (`COUNT(*)`, `SUM()`, etc.)
- Subqueries
- DISTINCT
- UNION

Out-of-scope queries are valid `run()` use cases. They require the
standard `run()` audit classification but are not blockers if properly
parameterized. Repeated patterns in `run()` are candidates for future
`CTGDBQuery` capability additions.

---

## New Error Types

Added to `CTGDBError::TYPES`:

| Type | Code | When |
|------|------|------|
| `INVALID_AGGREGATE` | 3008 | Reserved for future structured aggregate support |
| `INVALID_QUERY_STATE` | 3009 | `toStatement()` called on inconsistent state |

---

## Implementation Order

1. Internal condition/clause data structures
2. Static factory `from()` — table validation
3. `columns()` — column parsing and validation
4. `where()` — condition building with full operator set
5. `join()` — join clause building
6. `orderBy()` — order clause building
7. `groupBy()` — group clause building
8. `limit()` / `offset()` / `page()` — pagination helpers
9. `toStatement()` — SQL generation
10. `toCountStatement()` — count variant
11. CTGDB integration — update `read()` and `paginate()`
12. New error types in `CTGDBError`

---

## File Structure

```
ctg-php-db/
├── src/
│   ├── CTGDB.php
│   ├── CTGDBError.php
│   └── CTGDBQuery.php          # NEW
├── tests/
│   ├── CTGDBErrorTest.php
│   ├── CTGDBTest.php
│   ├── CTGDBIntegrationTest.php
│   ├── CTGDBSecurityTest.php
│   └── CTGDBQueryTest.php      # NEW
```
