# CTGDB

Main database API operating over a composed `CTGDBConn`, with safe CRUD
methods, materialized execution through `run()`, and incremental row handling
through `process()`.
`CTGDBConn` owns PDO construction, guarded statement execution, connection
state, transactions, and invalidation. All queries use PDO prepared statements.
Identifiers are regex-validated. Keywords are allowlisted.
Read queries should use `CTGDBQuery` (see [CTGDBQuery.md](CTGDBQuery.md))
for safe-by-default, structured query building — it validates all
identifiers, allowlists all operators, and parameterizes all values by
construction.

## Construction

### CONSTRUCTOR :: ctgdbConn -> ctgdb

Creates the main database API over an existing `CTGDBConn`. This makes
connection ownership explicit and permits a connection to be constructed or
managed separately without making `CTGDB` a connection subtype.

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
passes it to `new static(...)`. This is the concise path for callers that only
need the query and CRUD API. Callers that need transaction or lifecycle control
should construct and retain a `CTGDBConn`, then inject it through the
constructor.

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

### ctgdb.process :: STRING|ctgdbQuery, (ARRAY, MIXED -> MIXED), MIXED, ARRAY -> MIXED

Executes a row-producing raw SQL string or `CTGDBQuery`, passes each row and
the current state to the processor, and returns the final state. This avoids
materializing rows inside `CTGDB`, although memory usage still depends on what
the processor retains and on the connection's PDO buffering mode. Statement
cursors are closed after processing, including when the processor throws.
Use `create()`, `update()`, or `delete()` for statements that do not return
rows.

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

### ctgdb.read :: STRING|ARRAY|ctgdbQuery, ARRAY -> ARRAY

General-purpose read. The preferred usage is to pass a `CTGDBQuery`
instance, which provides validated, parameterized query building with
no raw SQL strings. String and array table forms are translated into a
`CTGDBQuery` before execution, so this method does not maintain separate SELECT
construction logic.

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
        ->innerJoin('pickups', ['guitars.id' => 'pickups.guitar_id'])
        ->columns('guitars.model', 'pickups.type')
);
```

When `$tables` is a `CTGDBQuery`, `$config` is ignored — the query
object contains all configuration. See [CTGDBQuery.md](CTGDBQuery.md)
for full builder documentation.

**Compatibility: string and array forms**

When `tables` is a string, `read()` constructs a `CTGDBQuery` with optional
`columns`, equality-only `where`, `order`, and `limit` configuration. When
`tables` is an array, it constructs the same query object with `join` and `on`
configuration. Identifier and keyword validation therefore use the same code
as direct `CTGDBQuery` usage.

The following config options are removed:
- `where` (string form) — use `CTGDBQuery::from()->where()`
- `where_raw` — use `CTGDBQuery::from()->where()`
- `as_query` — the `CTGDBQuery` object is the query; no config flag needed

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

### ctgdb.paginate :: STRING|ctgdbQuery, ARRAY -> ARRAY

Paginates any result set. Source is a `CTGDBQuery` instance or a table
name string. Runs a count query and a data query, returning materialized `data`
and `pagination` metadata. The `total` config option skips the count query when
the total is already known.

When `$source` is a `CTGDBQuery` and `sort`/`order` are provided in
config, they **replace** (not append to) any existing ORDER BY on the
query.

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
`insert()`, `execute()`, and `process()` boundaries so a raw PDO handle or live
statement cannot escape invalidation.
