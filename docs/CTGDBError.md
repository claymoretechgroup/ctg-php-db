# CTGDBError

Typed error class for database operations. Extends `\Exception` with
a string type code, structured context data, and a chainable handler
pattern for dispatching on error type.

### Properties

| Property | Type | Description |
|----------|------|-------------|
| type | STRING | Error type name (e.g. `'DUPLICATE_ENTRY'`) |
| msg | STRING | Human-readable error message |
| data | MIXED | Structured context (query, table name, etc.) |
| _handled | BOOL | Whether an `on()` handler has matched |

The `code` and `message` properties are inherited from `\Exception`
and accessible via `getCode()` and `getMessage()`.

---

### Error Codes

| Code | Type | Description |
|------|------|-------------|
| 1000 | CONNECTION_FAILED | PDO connection failed |
| 1001 | CONNECTION_TIMEOUT | Connection timed out |
| 1002 | AUTH_FAILED | Authentication rejected |
| 2000 | QUERY_FAILED | Query execution failed |
| 2001 | DUPLICATE_ENTRY | Unique constraint violation |
| 2002 | CONSTRAINT_VIOLATION | Foreign key or other constraint |
| 3000 | INVALID_TABLE | Table name failed validation |
| 3001 | INVALID_COLUMN | Column name failed validation |
| 3002 | INVALID_OPERATOR | Filter operator not in allowlist |
| 3003 | INVALID_JOIN_TYPE | Join type not in allowlist |
| 3004 | INVALID_SORT | Sort direction not in allowlist |
| 3005 | INVALID_ARGUMENT | General argument validation failure |
| 3006 | EMPTY_WHERE_DELETE | delete() called without WHERE clause |
| 3007 | INVALID_IDENTIFIER | Identifier contains unsafe characters |

---

## Construction

### CONSTRUCTOR :: STRING|INT, ?STRING, MIXED -> ctgdbError

Creates a new error. Accepts either a type name (`'DUPLICATE_ENTRY'`)
or integer code (`2001`). The message defaults to the type name if
not provided. The data argument carries structured context about the
error. Throws `\InvalidArgumentException` if the type or code is
unknown.

```php
$e = new CTGDBError('QUERY_FAILED', 'bad query', ['sql' => 'SELECT ...']);

$e = new CTGDBError(2001, 'duplicate email');
```

---

## Instance Methods

### ctgdbError.on :: STRING|INT, (ctgdbError -> VOID) -> $this

Handles the error if it matches the given type name or code. Chainable.
Short-circuits after the first match — subsequent `on()` calls are
skipped. The handler receives the error instance.

```php
try {
    $db->create('users', $data);
} catch (CTGDBError $e) {
    $e->on('DUPLICATE_ENTRY', fn($e) => respondConflict($e->data))
      ->on('CONSTRAINT_VIOLATION', fn($e) => respondBadRequest($e->msg))
      ->otherwise(fn($e) => respondServerError($e->msg));
}
```

### ctgdbError.otherwise :: (ctgdbError -> VOID) -> VOID

Handles the error if no previous `on()` call matched. Typically the
last call in a handler chain. Not chainable.

```php
$e->on('DUPLICATE_ENTRY', fn($e) => handleDup($e))
  ->otherwise(fn($e) => throw $e);
```

---

## Static Methods

### CTGDBError.lookup :: STRING|INT -> INT|STRING|NULL

Bidirectional lookup between type names and codes. Pass a string to
get the code, pass an integer to get the name. Returns null if not
found.

```php
CTGDBError::lookup('DUPLICATE_ENTRY');  // 2001
CTGDBError::lookup(2001);               // 'DUPLICATE_ENTRY'
CTGDBError::lookup('NONEXISTENT');      // null
```
