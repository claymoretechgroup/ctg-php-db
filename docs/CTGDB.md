# CTGDB

Minimal PDO database class with a single low-level primitive (`run`)
and CRUD convenience methods built on top. All queries use PDO prepared
statements. Identifiers are regex-validated. Keywords are allowlisted.
Read queries should use `CTGDBQuery` (see [CTGDBQuery.md](CTGDBQuery.md))
for safe-by-default, structured query building — it validates all
identifiers, allowlists all operators, and parameterizes all values by
construction.

### Properties

| Property | Type | Description |
|----------|------|-------------|
| _pdo | \PDO | Underlying PDO connection instance |

---

## Construction

### CONSTRUCTOR :: STRING, STRING, STRING, STRING, ARRAY -> ctgdb

Creates a new database connection. Internally builds a PDO instance
with `ERRMODE_EXCEPTION`, `FETCH_ASSOC`, and `EMULATE_PREPARES => false`.
Options support `charset` (default `utf8mb4`), `timeout`, and
`persistent`. Throws CTGDBError on connection failure.

```php
$db = new CTGDB('localhost', 'myapp', 'user', 'pass');

$db = new CTGDB('localhost', 'myapp', 'user', 'pass', [
    'charset' => 'utf8mb4',
    'timeout' => 5,
    'persistent' => false,
]);
```

### CTGDB.connect :: STRING, STRING, STRING, STRING, ARRAY -> ctgdb

Static factory method. Returns `new static(...)` so subclasses inherit
the factory correctly.

```php
$db = CTGDB::connect('localhost', 'myapp', 'user', 'pass');
```

---

## Instance Methods

### ctgdb.run :: STRING|ARRAY, ?(ARRAY, MIXED -> MIXED), MIXED -> MIXED

The low-level primitive. Accepts a plain SQL string or a query array
with `sql` and `values` keys. Values support both typed form
(`['type' => 'int', 'value' => 42]`) and untyped form (PHP type
inference). For SELECT queries, folds over results using the optional
callback and accumulator. For INSERT, returns last insert ID. For
UPDATE/DELETE, returns affected row count. Does not validate
identifiers — callers own the SQL structure.

```php
$users = $db->run('SELECT * FROM users');

$admins = $db->run([
    'sql' => 'SELECT * FROM users WHERE role = ?',
    'values' => [['type' => 'str', 'value' => 'admin']]
]);

$emails = $db->run(
    'SELECT email FROM users',
    fn($record, $acc) => [...$acc, $record['email']],
    []
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

### ctgdb.read :: STRING|ARRAY|ctgdbQuery, ARRAY, ?(ARRAY, MIXED -> MIXED), MIXED -> MIXED

General-purpose read. The preferred usage is to pass a `CTGDBQuery`
instance, which provides validated, parameterized query building with
no raw SQL strings.

**Preferred: CTGDBQuery**

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
        ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id'])
        ->columns('guitars.model', 'pickups.type')
);
```

When `$tables` is a `CTGDBQuery`, `$config` is ignored — the query
object contains all configuration. See [CTGDBQuery.md](CTGDBQuery.md)
for full builder documentation.

**Legacy: string and array forms**

When `tables` is a string, builds a single-table
SELECT with optional `columns`, `where`, `order`, and `limit` config.
When `tables` is an array, builds a multi-table join using `join` and
`on` config. Validates all identifiers, join types, and sort directions.

The following config options are **(Deprecated)** — use `CTGDBQuery` instead:
- `where` (string form) — use `CTGDBQuery::from()->where()`
- `where_raw` — use `CTGDBQuery::from()->where()`
- `as_query` — the `CTGDBQuery` object is the query; no config flag needed

These options remain functional for backward compatibility but must not
be used in new application code.

```php
$guitars = $db->read('guitars');

$fenders = $db->read('guitars', [
    'columns' => ['id', 'model', 'color'],
    'where' => ['make' => ['type' => 'str', 'value' => 'Fender']],
    'order' => 'year_purchased DESC',
    'limit' => 10
]);

$db->read(['guitars', 'pickups'], [
    'join' => 'inner',
    'on' => [['guitars.id' => 'pickups.guitar_id']],
    'columns' => ['guitars.model', 'pickups.type']
]);
```

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

### ctgdb.filter :: STRING, ARRAY -> ARRAY

**(Deprecated)** — use `CTGDBQuery::from()->where()` instead. See
[CTGDBQuery.md](CTGDBQuery.md).

```php
// Preferred replacement:
$query = CTGDBQuery::from('guitars')
    ->where('make', '=', 'Fender', 'str')
    ->where('year_purchased', '>=', 2020, 'int');

$page1 = $db->paginate($query, ['sort' => 'model', 'page' => 1]);
$page2 = $db->paginate($query, ['page' => 2]);
```

Builds a reusable set of WHERE conditions with operator support.
Returns a plain array with `table`, `where`, and `values` keys. The
result can be passed to `paginate()`, composed with `read()` via
`where_raw`, or unpacked for use with `run()`. Validates column names
and operators. All conditions are AND-joined.

Supported operators: `=`, `>`, `<`, `>=`, `<=`, `!=`, `LIKE`,
`NOT LIKE`, `IN`, `NOT IN`, `IS`, `IS NOT`, `BETWEEN`.

```php
$filter = $db->filter('guitars', [
    'make' => ['type' => 'str', 'value' => 'Fender'],
    'year_purchased' => ['type' => 'int', 'value' => 2020, 'op' => '>='],
]);

$page1 = $db->paginate($filter, ['sort' => 'model', 'page' => 1]);
$page2 = $db->paginate($filter, ['page' => 2]);
```

### ctgdb.join :: STRING|ARRAY, ARRAY, ?(ARRAY, MIXED -> MIXED), MIXED -> MIXED

**(Deprecated)** — use `CTGDBQuery::from()->join()` instead. See
[CTGDBQuery.md](CTGDBQuery.md).

Inner join shortcut. Delegates to `read()` with `join => 'inner'`.
Accepts the same arguments and config options as `read()`.

```php
$db->join(['guitars', 'pickups'], [
    'on' => [['guitars.id' => 'pickups.guitar_id']],
    'columns' => ['guitars.model', 'pickups.type']
]);
```

### ctgdb.leftJoin :: STRING|ARRAY, ARRAY, ?(ARRAY, MIXED -> MIXED), MIXED -> MIXED

**(Deprecated)** — use `CTGDBQuery::from()->join()` instead. See
[CTGDBQuery.md](CTGDBQuery.md).

```php
// Preferred replacement:
$db->read(
    CTGDBQuery::from('guitars')
        ->join('pickups', 'left', ['guitars.id' => 'pickups.guitar_id'])
        ->columns('guitars.model', 'pickups.position')
);
```

Left join shortcut. Delegates to `read()` with `join => 'left'`.
Accepts the same arguments and config options as `read()`.

```php
$db->leftJoin(['guitars', 'pickups'], [
    'on' => [['guitars.id' => 'pickups.guitar_id']],
    'columns' => ['guitars.model', 'pickups.position']
]);
```

### ctgdb.paginate :: STRING|ARRAY|ctgdbQuery, ARRAY, ?(ARRAY, MIXED -> MIXED), MIXED -> ARRAY

Paginates any result set. Source can be a table name, a `CTGDBQuery`
instance, a filter result, a join query (from `as_query`), or a raw
query array. Runs a count query and a data query, returning `data` and
`pagination` metadata. The `total` config option skips the count query
when the total is already known. The fold function applies to `data`
only — pagination metadata is always present.

**Preferred: CTGDBQuery**

When `$source` is a `CTGDBQuery`, `page` and `per_page` from `$config`
override the query's pagination, and `sort`/`order` override ORDER BY.

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

**Legacy: string and array forms**

```php
$result = $db->paginate('guitars', [
    'sort' => 'year_purchased',
    'order' => 'DESC',
    'page' => 1,
    'per_page' => 5
]);

// $result['data'] — array of rows
// $result['pagination'] — {page, per_page, total_rows, total_pages, has_previous, has_next}
```

### ctgdb.compose :: [(MIXED, ctgdb -> MIXED)] -> (MIXED -> MIXED)

Builds a function pipeline that threads an accumulator and the DB
instance through each step. Each function receives
`($accumulator, $db)`. The returned callable accepts an optional
initial value. Nothing executes until the returned callable is invoked.
Use `compose()` for steps that need database access. Use
`CTGFnprog::pipe()` for pure data transforms within steps.

```php
$pipeline = $db->compose([
    fn($_, $db) => $db->read('guitars'),
    fn($guitars, $_) => CTGFnprog::pipe([
        CTGFnprog::filter(fn($g) => $g['year_purchased'] >= 2020),
        CTGFnprog::sortBy('year_purchased', 'DESC'),
        CTGFnprog::pluck('make'),
    ])($guitars),
]);

$recentMakes = $pipeline();
```

---

## Protected Methods

These methods are `protected` to support subclassing for
application-specific optimization.

### ctgdb.resolveType :: MIXED -> [MIXED, INT]

Resolves a value to a `[value, PDO_TYPE_CONSTANT]` pair. Accepts
typed form (`['type' => 'int', 'value' => 42]`) or untyped form
(infers type from PHP type). Supported types: `int`, `str`, `bool`,
`null`, `float`.

### ctgdb.buildWhere :: ARRAY -> [STRING, ARRAY]

Builds a WHERE clause from an associative array of equality conditions.
Returns `[' WHERE col = ? AND ...', [values]]`.

### ctgdb.buildPaginationMeta :: INT, INT, INT -> ARRAY

Calculates pagination metadata from page, per_page, and total.
Returns `{page, per_page, total_rows, total_pages, has_previous, has_next}`.

### ctgdb.validateIdentifier :: STRING -> STRING

Validates and backtick-quotes an identifier. Allows alphanumeric
characters, underscores, and dots. Throws `INVALID_IDENTIFIER` on
anything else.

### ctgdb.validateJoinType :: STRING -> STRING

Validates a join type against the allowlist: `inner`, `left`, `right`,
`cross`. Throws `INVALID_JOIN_TYPE`.

### ctgdb.validateSortDirection :: STRING -> STRING

Validates a sort direction against the allowlist: `asc`, `desc`.
Throws `INVALID_SORT`.

### ctgdb.validateOperator :: STRING -> STRING

Validates a filter operator against the allowlist: `=`, `>`, `<`,
`>=`, `<=`, `!=`, `like`, `not like`, `in`, `not in`, `is`, `is not`,
`between`. Throws `INVALID_OPERATOR`. `CTGDBQuery` uses this same
validation for its `where()` method.

### ctgdb.getPdo :: VOID -> \PDO

Returns the underlying PDO instance.
