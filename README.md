# ctg-php-db

`ctg-php-db` is a minimal PHP database library built on PDO. One class,
one connection, one low-level method (`run`) with CRUD convenience methods
built on top. `CTGDBQuery` is the default read path — a structured query
builder where every column, operator, and value is validated and
parameterized by construction. All values are bound through PDO prepared
statements. All identifiers are validated before interpolation.

**Key Features:**

* **`run` is the primitive** — every database operation flows through
  a single method that handles preparation, binding, and execution
* **Fold over results** — callers control output shape via an optional
  transform function and accumulator
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
* **Subclass-friendly** — protected internals for application-specific
  optimization

## Install

```
composer require ctg/php-db
```

## Examples

### Connecting

```php
use CTG\DB\CTGDB;

$db = CTGDB::connect('localhost', 'myapp', 'user', 'pass');
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

### Raw Queries with Fold

Transform results as they stream from the database:

```php
$emails = $db->run(
    'SELECT email FROM users WHERE active = ?',
    fn($record, $acc) => [...$acc, $record['email']],
    []
);

$byId = $db->run(
    'SELECT * FROM guitars',
    fn($record, $acc) => $acc + [$record['id'] => $record],
    []
);
```

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
        ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id'])
        ->columns('guitars.model', 'pickups.position', 'pickups.type')
);

$db->read(
    CTGDBQuery::from('guitars')
        ->join('pickups', 'left', ['guitars.id' => 'pickups.guitar_id'])
        ->columns('guitars.model', 'pickups.position')
);
```

### Pagination

Paginate any source — table names, query objects, or raw SQL.
`CTGDBQuery` is the preferred source for pagination:

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

### Composing Filter + Join + Paginate

```php
use CTG\DB\CTGDBQuery;

$query = CTGDBQuery::from('guitars')
    ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id'])
    ->columns('guitars.model', 'pickups.type')
    ->where('guitars.year_purchased', '>=', 2020, 'int');

$result = $db->paginate($query, [
    'sort' => 'model',
    'order' => 'ASC',
    'page' => 1,
    'per_page' => 10
]);
```

### Pipelines with compose()

Build multi-step workflows that thread data and the DB instance:

```php
use CTG\DB\CTGDBQuery;
use CTG\FnProg\CTGFnprog;

$pipeline = $db->compose([
    fn($_, $db) => $db->read(
        CTGDBQuery::from('guitars')
            ->where('year_purchased', '>=', 2020, 'int')
            ->orderBy('year_purchased', 'DESC')
    ),
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
use CTG\DB\CTGDBQuery;

$getGuitars = $db->compose([
    fn($_, $db) => $db->read(
        CTGDBQuery::from('guitars')
    ),
]);

$formatForApi = $db->compose([
    fn($guitars, $_) => CTGFnprog::pipe([
        CTGFnprog::omit(['id']),
        CTGFnprog::rename(['year_purchased' => 'year']),
        CTGFnprog::sortBy('make'),
    ])($guitars),
]);

$fullPipeline = $db->compose([$getGuitars, $formatForApi]);
$result = $fullPipeline();
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
