# CTGDBQuery — Structured Query Builder

## Overview

A fluent query builder and centralized SQL-construction component. Its instance
API owns SELECT construction and produces internal
`['sql' => ..., 'values' => [...]]` statements for `CTGDB`; its static helpers
provide the shared identifier and equality-WHERE construction used by CRUD.

**Primary motivation: AI safety.** When an LLM generates code against this
library, raw SQL fragments create opportunities to interpolate user input.
`CTGDBQuery` removes that path from structured reads: every query element is
validated and every value is parameterized by construction.

**Trust boundary:** `CTGDBQuery` handles SELECT, JOINs, WHERE conditions,
ORDER BY, GROUP BY, LIMIT/OFFSET, and pagination. A row-producing statement the
builder cannot express uses `run()` directly; a custom non-row statement uses
`execute()`. Any raw SQL call that incorporates user input into the SQL string
should be flagged for code review — that is the explicit trust boundary.

**Relationship to CTGDB:**
- `CTGDBQuery` is a standalone class in `CTG\DB`
- It is consumed directly by `run()`, `process()`, `read()`, and `paginate()`
- `read()` and `paginate()` accept only `CTGDBQuery` instances
- `quoteIdentifier()` is the canonical identifier validator used by CRUD
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

    // :: STRING -> STRING
    // Validate and backtick-quote a bare or table-qualified identifier
    public static function quoteIdentifier(string $identifier): string;

    // :: ARRAY -> [STRING, ARRAY]
    // Build a parameterized equality-only WHERE fragment for CRUD statements
    public static function buildWhere(array $where): array;

    // ─── Column Selection ──────────────────────────────────

    // :: STRING, STRING, ... -> $this
    // Set columns to select (replaces any previous column list)
    // Accepts: 'col', 'table.col', 'table.*', 'col as alias', '*'
    public function columns(string ...$columns): static;

    // ─── WHERE Conditions ──────────────────────────────────

    // :: STRING, STRING, MIXED, ?STRING -> $this
    // Add a WHERE condition (AND-joined with previous conditions)
    public function where(string $column, string $operator, mixed $value, ?string $type = null): static;

    // ─── JOINs ─────────────────────────────────────────────

    // :: STRING, STRING, ARRAY -> $this
    // Add a JOIN clause
    // $on is associative: ['left.col' => 'right.col', ...]
    public function join(string $table, string $type, array $on): static;

    // Convenience methods delegating to join()
    public function innerJoin(string $table, array $on): static;
    public function leftJoin(string $table, array $on): static;
    public function rightJoin(string $table, array $on): static;
    public function crossJoin(string $table): static;

    // ─── ORDER BY ──────────────────────────────────────────

    // :: STRING, STRING -> $this
    // Add an ORDER BY column (appends to existing order)
    public function orderBy(string $column, string $direction = 'ASC'): static;

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

The table name is validated by `CTGDBQuery::quoteIdentifier()`. The same method
is used for identifiers in `CTGDB` write operations, leaving one canonical
implementation. Invalid identifiers throw `CTGDBError` with type
`INVALID_IDENTIFIER`. Wildcards and aliases are column expressions and are
handled through `columns()`, not `quoteIdentifier()`.

### Static CRUD WHERE Construction

`buildWhere()` centralizes the equality-only predicate format used by
`update()` and `delete()`. It validates every column identifier, returns only
`?` placeholders in the SQL fragment, and preserves the values separately for
PDO binding:

```php
[$sql, $values] = CTGDBQuery::buildWhere([
    'tenant_id' => ['type' => 'int', 'value' => 7],
    'active' => true,
]);

// $sql:    ' WHERE `tenant_id` = ? AND `active` = ?'
// $values: [['type' => 'int', 'value' => 7], true]
```

This helper does not accept raw conditions or operators. Rich SELECT
conditions continue to use the instance `where()` method.

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

Supported operators:

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

- `$column` is validated with `quoteIdentifier()`
- `$operator` is checked against the private operator allowlist
- Invalid inputs throw `CTGDBError` with the appropriate type

---

## JOINs

### `join(table, type, on)`

Adds a JOIN clause. Multiple calls add multiple joins in order.

```php
// Canonical join method
$query = CTGDBQuery::from('guitars')
    ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id']);

// Convenience methods
$query = CTGDBQuery::from('guitars')
    ->leftJoin('pickups', ['guitars.id' => 'pickups.guitar_id']);

// Multiple joins
$query = CTGDBQuery::from('articles')
    ->leftJoin('categories', ['articles.category_id' => 'categories.id'])
    ->innerJoin('users', ['articles.author_id' => 'users.id']);

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

- `$table` — validated with `quoteIdentifier()`
- `$type` — checked against the join-type allowlist: `'inner'`, `'left'`,
  `'right'`, `'cross'`
- `$on` — associative array of column equality pairs. Both sides validated
  with `quoteIdentifier()`. For `cross` joins, `$on` must be empty.

### Join Convenience Methods

- `innerJoin($table, $on)` delegates to `join($table, 'inner', $on)`.
- `leftJoin($table, $on)` delegates to `join($table, 'left', $on)`.
- `rightJoin($table, $on)` delegates to `join($table, 'right', $on)`.
- `crossJoin($table)` delegates to `join($table, 'cross', [])`.

There is no generic `outerJoin()` because the direction would be ambiguous.
`LEFT OUTER JOIN` and `RIGHT OUTER JOIN` are represented by `leftJoin()` and
`rightJoin()`. MariaDB does not directly support `FULL OUTER JOIN`.

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

- `$column` — validated with `quoteIdentifier()`, supports `table.col`
- `$direction` — checked against the sort-direction allowlist and defaults to `'ASC'`

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

Each column is validated with `quoteIdentifier()`.

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

$rows = $db->run($query);
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
// :: ctgdbQuery -> ARRAY
public function read(CTGDBQuery $query): array;
```

The query is passed directly to `run()` for materialized execution:

```php
$rows = $db->read(
    CTGDBQuery::from('guitars')
        ->where('make', '=', 'Fender', 'str')
        ->orderBy('model')
        ->limit(10)
);

// Incremental processing uses process()
$byMake = $db->process(
    CTGDBQuery::from('guitars'),
    fn($row, $result) => $result + [$row['make'] => $row],
    []
);
```

### `paginate()` accepts CTGDBQuery

```php
// :: ctgdbQuery, ARRAY -> ARRAY
public function paginate(CTGDBQuery $source, array $config = []): array;
```

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
$db->read(
    CTGDBQuery::from('guitars')
        ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id'])
        ->columns('guitars.model', 'pickups.type')
        ->where('guitars.year_purchased', '>=', 2020, 'int')
);
```

### Pagination (replaces filter + paginate)

```php
$query = CTGDBQuery::from('guitars')
    ->where('make', '=', 'Fender', 'str')
    ->where('year_purchased', '>=', 2020, 'int');

$result = $db->paginate($query, ['sort' => 'model', 'page' => 1]);
```

### Join + pagination (no more as_query)

```php
$query = CTGDBQuery::from('guitars')
    ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id'])
    ->columns('guitars.*', 'pickups.type as pickup_type');

$result = $db->paginate($query, ['sort' => 'guitars.make', 'page' => 1]);
```

---

## What It Replaces

| Current API | Status | Replacement |
|------------|--------|-------------|
| `filter()` | Removed | `CTGDBQuery::from()->where()` with full operator support |
| `CTGDB::join()` shortcut | Removed | `CTGDBQuery::from()->innerJoin(...)` |
| `CTGDB::leftJoin()` shortcut | Removed | `CTGDBQuery::from()->leftJoin(...)` |
| `as_query` config | Removed | The query object is the query |
| `where` as string in `read()` | Removed | `CTGDBQuery::from()->where()` |
| `where_raw` config | Removed | `CTGDBQuery::from()->where()` |
| Table/config `read()` | Removed | Pass a `CTGDBQuery` |
| String-table `paginate()` | Removed | Pass a `CTGDBQuery` |

`read()` no longer accepts table/config forms or raw WHERE/HAVING fragments.

## What It Does NOT Replace

- `run()` — still the execution primitive, still accepts raw SQL for
  queries the builder cannot express
- `execute()` — accepts raw SQL for custom non-row statements
- `create()`, `update()`, `delete()` — write operations are already
  fully parameterized
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

### Removed Interface Status

| Path | Status | Notes |
|------|--------|-------|
| Table/config `read()` | **Removed** | Pass a `CTGDBQuery` |
| String-table `paginate()` | **Removed** | Pass a `CTGDBQuery` |
| `read()` with string `where` | **Removed** | Use `where()` |
| `read()` with `where_raw` | **Removed** | Use `where()` |
| `read()` with string `having` | **Removed** | Not supported |
| `filter()` | **Removed** | Use `CTGDBQuery::from()->where()` |
| `CTGDB::join()` / `leftJoin()` shortcuts | **Removed** | Use the corresponding `CTGDBQuery` join methods |
| `as_query` config option | **Removed** | The query object is the query |

### Release Gate: Raw SQL Audit

Before any release of application code built on ctg-php-db:

1. **Inventory all `run()` and `execute()` calls.** Every raw SQL usage must be
   documented with a justification for why the structured query or CRUD APIs
   could not express the operation.

2. **Classify each raw SQL call:**
   - **Static SQL** — no external input touches the SQL string. Safe.
     Example: `$db->run('SELECT COUNT(*) FROM migrations')`
   - **Parameterized** — external input is bound via `values`, never
     interpolated into the SQL string. Safe.
     Example: `$db->run('SELECT ... WHERE col IN (SELECT ...)', [...])`
   - **Dynamic SQL** — external input influences the SQL string structure
     (table names, column names, operators, fragments). **Blocker.**
     Must be refactored to use parameterized form or `CTGDBQuery`.

3. **Unparameterized user input in a SQL string is a release blocker.**
   No exceptions. If a `run()` or `execute()` call concatenates or interpolates any
   value derived from user input, HTTP request data, or external API
   responses into the SQL string, it must be refactored before release.

### Review Checklist

For manual review (automate when feasible):

- [ ] All read queries use `CTGDBQuery` unless documented exception
- [ ] Every raw SQL call has a comment documenting why it exists
- [ ] Every raw SQL call is classified as static, parameterized, or dynamic
- [ ] Zero dynamic SQL calls with unparameterized user input
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
