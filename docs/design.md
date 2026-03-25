# CTG DB — Language-Agnostic Design Document

## Purpose

This document describes the design of a minimal database library intended
for reimplementation in any language with a SQL database driver. It
captures the behavior, contracts, and safety model without reference to
any specific language runtime or database driver API.

---

## Core Concepts

### 1. One Primitive, Everything Else Delegates

The library has a single execution primitive (`run`) that prepares,
binds, and executes SQL. Every other method builds a query and delegates
to `run`. This means:

- There is exactly one code path that touches the database
- Prepared statements with parameter binding are always used
- The fold/accumulator pattern applies to all read operations

### 2. Structured Query Builder (Safe Path)

A query builder (`Query`) produces parameterized statement descriptors
(a SQL string + an ordered list of typed values) that `run` consumes.
The builder validates all identifiers, operators, and keywords at build
time. No raw SQL strings are accepted through the builder.

`Query` is the **default path** for all read operations. It is designed
to be safe for AI-generated code — an LLM building queries against
this library cannot produce SQL injection through the builder API.

### 3. `run` Is the Escape Hatch

`run` accepts raw SQL for queries the builder cannot express. Any `run`
call that incorporates user input into the SQL string must be flagged
for code review. This is the explicit trust boundary.

### 4. Typed Errors with Chainable Handlers

All errors are instances of a single error class with a string type
label and numeric code. Errors support chainable `on(type, handler)`
matching with short-circuit semantics, plus a catch-all `otherwise`.
Unknown types throw immediately.

---

## Type System

Values bound to queries can be expressed in two forms:

### Typed Form (Explicit)

An associative structure with `type` and `value` fields:

```
{ type: "int", value: 42 }
{ type: "str", value: "Fender" }
{ type: "float", value: 29.99 }
```

Supported type strings and their database binding behavior:

| Type | Binding | Notes |
|------|---------|-------|
| `int` | Integer parameter | |
| `str` | String parameter | |
| `bool` | Boolean parameter | |
| `null` | Null parameter | |
| `float` | String parameter | Floats are cast to string for binding (most database drivers lack a native float parameter type) |

Unknown type strings produce an `INVALID_ARGUMENT` error.

### Untyped Form (Inferred)

When a value is not in typed form, the language's native type determines
the binding:

| Native Type | Binding |
|-------------|---------|
| Integer | Integer parameter |
| Boolean | Boolean parameter |
| Null | Null parameter |
| Float | String parameter (cast to string) |
| Everything else | String parameter |

Boolean detection must occur before integer detection in languages where
booleans are a subtype of integers.

---

## Connection

### Constructor

```
connect(host, database, username, password, options?) -> DB
```

Options:
- `charset` — default `utf8mb4`
- `timeout` — connection timeout in seconds (optional)
- `persistent` — persistent connections, default `false`

Connection settings:
- Error mode: exceptions
- Default fetch mode: associative arrays (maps/dictionaries)
- Emulated prepares: **disabled** (real server-side prepared statements)

Connection failures are classified into error types based on driver
error codes (see Error Classification below).

### Static Factory

A static factory method `connect(...)` creates and returns a new
instance. In languages with inheritance, it should return `new Self`
(not a hardcoded class) so subclasses inherit the factory.

---

## Execution Primitive: `run`

```
run(query, fn?, accumulator?) -> result
```

### Query Formats

1. **Plain string** — raw SQL, no parameters
2. **Statement descriptor** — a structure with `sql` (string) and
   `values` (ordered list of typed or untyped values)

When the query is a plain string, it is prepared with an empty values
list.

### Parameter Binding

Values are bound through the database driver's prepared statement API.
Both positional (`?` placeholders) and named (`:param` placeholders)
are supported. The binding mechanism:

1. Detects whether the values list is positional (indexed) or named
   (keyed by parameter name)
2. Resolves each value through the type system
3. Binds with the appropriate database-driver type constant

### Fold/Accumulator Pattern

For queries that return rows (SELECT):

1. Initialize `result = accumulator` (default: empty list)
2. For each row fetched:
   - **With callback:** `result = fn(row, result)`
   - **Without callback:** append `row` to `result`
3. Return `result`

For queries that do not return rows:
- **INSERT:** return last insert ID
- **UPDATE/DELETE:** return affected row count

INSERT detection: check if the SQL string starts with `INSERT`
(case-insensitive, after trimming).

Row-returning detection: check if the statement's column count is
greater than zero.

---

## CRUD Methods

All CRUD methods build statement descriptors and delegate to `run`.

### create(table, data) -> id

Inserts a single row. `data` is a map of column names to values (typed
or untyped). Returns the auto-increment ID of the inserted row.

- Table name validated as identifier
- Column names (data keys) validated as identifiers
- Values bound through prepared statement

### read(source, config?, fn?, accumulator?) -> result

Reads rows. `source` can be:

1. **Query builder instance** — calls `toStatement()` and passes to
   `run` with the fold arguments. Config is ignored.
2. **Table name string** — single-table query with optional config
3. **Array of table names** — multi-table join query with config

Config keys (for string/array source):
- `columns` — list of column names (default: `["*"]`)
- `where` — associative map of column-to-value equality conditions
  (array form only; string form is rejected)
- `order` — comma-separated ORDER BY clause string
- `limit` — integer row limit

For array source (joins), additional config:
- `join` — join type string (`inner`, `left`, etc.) applied uniformly,
  OR an array of `{type, on}` definitions for per-table join types
- `on` — list of column equality maps, one per joined table
- `group` — comma-separated GROUP BY clause string

### update(table, data, where) -> affected_count

Updates rows matching WHERE conditions. Returns affected row count.

- `data` — map of column names to new values
- `where` — map of column names to match values (equality only)
- All identifiers validated, all values bound

### delete(table, where) -> affected_count

Deletes rows matching WHERE conditions. Returns affected row count.

- `where` must not be empty — throws `EMPTY_WHERE_DELETE` if it is
- All identifiers validated, all values bound

---

## Query Builder

The query builder is a fluent, mutable object that produces statement
descriptors consumable by `run`.

### Factory

```
Query.from(table) -> Query
```

Validates the table name as an identifier. Returns a new builder
instance.

### Column Selection

```
query.columns(col1, col2, ...) -> query
```

Sets the SELECT column list (replaces any previous list). Defaults to
`["*"]` if never called.

Accepted column forms:
- `"col"` — bare column, validated
- `"table.col"` — table-qualified, both parts validated
- `"table.*"` — table wildcard, table part validated
- `"*"` — global wildcard
- `"col as alias"` — aliased, both sides validated
- `"table.col as alias"` — table-qualified alias, all parts validated

Raw expressions and aggregate functions are **not accepted**. Use `run`
directly for queries requiring them.

### WHERE Conditions

```
query.where(column, operator, value, type?) -> query
```

Appends an AND-joined condition. Conditions are stored as an ordered
list (not keyed by column), so multiple conditions on the same column
work naturally.

Supported operators:

| Operator | Value | SQL Output |
|----------|-------|-----------|
| `=`, `>`, `<`, `>=`, `<=`, `!=` | scalar | `col OP ?` |
| `LIKE`, `NOT LIKE` | string | `col LIKE ?` |
| `IN`, `NOT IN` | list of scalars | `col IN (?, ?, ?)` |
| `IS`, `IS NOT` | ignored | `col IS NULL` / `col IS NOT NULL` |
| `BETWEEN` | pair `[low, high]` | `col BETWEEN ? AND ?` |

Type behavior:
- When `type` is provided, values are stored in typed form
- When `type` is omitted, values are stored as-is (untyped)
- For `IN`/`NOT IN`, the type applies to every element
- For `BETWEEN`, the type applies to both bounds
- For `IS`/`IS NOT`, no value is added to the values list

### JOINs

```
query.join(table, type, on) -> query
```

Adds a JOIN clause. Multiple calls add multiple joins in order.

- `table` — validated as identifier
- `type` — validated against allowlist: `inner`, `left`, `right`, `cross`
- `on` — map of column equality pairs (`{leftCol: rightCol}`); both
  sides validated as identifiers. For `cross` joins, `on` must be empty.

### ORDER BY

```
query.orderBy(column, direction?) -> query
```

Appends an ORDER BY column. Default direction is `ASC`.

- `column` — validated as identifier
- `direction` — validated against allowlist: `asc`, `desc`

```
query.resetOrderBy() -> query
```

Clears all ORDER BY columns. Used internally by `paginate` to implement
sort override.

### GROUP BY

```
query.groupBy(col1, col2, ...) -> query
```

Sets GROUP BY columns (replaces previous list). Each column validated
as identifier.

### LIMIT / OFFSET

```
query.limit(n) -> query
query.offset(n) -> query
```

Both must be non-negative integers.

```
query.page(page, perPage?) -> query
```

Convenience: sets limit to `perPage` (default 20) and offset to
`(page - 1) * perPage`. Page is clamped to minimum 1, perPage to
minimum 1. Shares internal state with `limit`/`offset` — calling one
overwrites the other.

### Statement Generation

```
query.toStatement() -> { sql: string, values: list }
```

Builds the complete SELECT statement. SQL generation order:

1. `SELECT {columns}`
2. `FROM {table}`
3. JOIN clauses (in order added)
4. `WHERE {conditions}` (AND-joined)
5. `GROUP BY {columns}`
6. `ORDER BY {columns}`
7. `LIMIT {n}`
8. `OFFSET {n}`

Values are collected in order as the SQL is built.

```
query.toCountStatement() -> { sql: string, values: list }
```

Builds the COUNT version for pagination:

**Without GROUP BY:**
- Replaces columns with `COUNT(*) as total`
- Strips ORDER BY, LIMIT, OFFSET
- Preserves FROM, JOINs, WHERE (with values)

**With GROUP BY:**
- Builds the full inner query (columns, FROM, JOINs, WHERE, GROUP BY)
- Strips ORDER BY, LIMIT, OFFSET from inner query
- Wraps in: `SELECT COUNT(*) as total FROM ({inner}) as _counted`

### Mutability

The builder is mutable — methods return `self`. For branching, callers
clone explicitly:

```
base = Query.from("guitars").where("year", ">=", 2020, "int")
fenders = clone(base).where("make", "=", "Fender", "str")
gibsons = clone(base).where("make", "=", "Gibson", "str")
```

---

## Pagination

```
paginate(source, config?, fn?, accumulator?) -> { data, pagination }
```

Source accepts:
1. **Query builder instance** — the default, safe path
2. **Table name string** — simple "paginate all rows" shorthand

Config keys:
- `page` — 1-indexed, default 1, clamped to minimum 1
- `per_page` — default 20, clamped to minimum 1
- `sort` — optional column name (validated as identifier)
- `order` — optional direction, default `ASC` (validated)
- `total` — optional pre-computed total (skips count query)
- `columns` — only used for string source

### Query Builder Source

1. If `total` not provided: run `source.toCountStatement()` to get count
2. Clone the source query
3. If `sort` provided: call `resetOrderBy().orderBy(sort, order)` on
   the clone — this **replaces** any existing ORDER BY
4. Call `page(page, perPage)` on the clone
5. Run `clone.toStatement()` with fold arguments
6. Return result with pagination metadata

### String Source

1. Validate table name
2. If `total` not provided: run `SELECT COUNT(*) as total FROM {table}`
3. Build SQL with columns, optional ORDER BY, LIMIT, OFFSET
4. Run and return with pagination metadata

### Pagination Metadata

```
{
    page: int,
    per_page: int,
    total_rows: int,
    total_pages: int,       // ceil(total_rows / per_page), or 0 if per_page is 0
    has_previous: bool,     // page > 1
    has_next: bool          // page < total_pages
}
```

---

## Function Pipelines

```
compose(fns) -> callable
```

Accepts a list of functions, each with signature
`fn(accumulator, db) -> new_accumulator`. Returns a callable that
accepts an initial accumulator (default null) and threads it through
all functions in order, injecting the database instance as the second
argument.

---

## Validation Rules

### Identifier Validation

**Regex:** `/^[a-zA-Z_][a-zA-Z0-9_.]*$/`

Before matching, strip any quoting characters (backticks in MySQL,
double quotes in PostgreSQL/SQLite, brackets in SQL Server).

If the clean value contains `.`, split on `.` and quote each part
individually. The `*` component in `table.*` is left unquoted.

Output: the identifier quoted using the target database's quoting
convention.

Error: `INVALID_IDENTIFIER`

### Operator Allowlist

Accepted (case-insensitive): `=`, `>`, `<`, `>=`, `<=`, `!=`, `like`,
`not like`, `in`, `not in`, `is`, `is not`, `between`

Output: uppercased form.
Error: `INVALID_OPERATOR`

### Join Type Allowlist

Accepted (case-insensitive): `inner`, `left`, `right`, `cross`

Output: uppercased form.
Error: `INVALID_JOIN_TYPE`

### Sort Direction Allowlist

Accepted (case-insensitive): `asc`, `desc`

Output: uppercased form.
Error: `INVALID_SORT`

---

## Error System

### Error Structure

Every error has:
- `type` — string label (e.g., `DUPLICATE_ENTRY`)
- `code` — numeric code (e.g., 2001)
- `msg` — human-readable message (defaults to type name)
- `data` — arbitrary context data (e.g., table name, query text,
  original driver exception)

### Error Types

| Type | Code | Category |
|------|------|----------|
| `CONNECTION_FAILED` | 1000 | Connection (1xxx) |
| `CONNECTION_TIMEOUT` | 1001 | Connection |
| `AUTH_FAILED` | 1002 | Connection |
| `QUERY_FAILED` | 2000 | Query execution (2xxx) |
| `DUPLICATE_ENTRY` | 2001 | Query execution |
| `CONSTRAINT_VIOLATION` | 2002 | Query execution |
| `INVALID_TABLE` | 3000 | Validation (3xxx) |
| `INVALID_COLUMN` | 3001 | Validation |
| `INVALID_OPERATOR` | 3002 | Validation |
| `INVALID_JOIN_TYPE` | 3003 | Validation |
| `INVALID_SORT` | 3004 | Validation |
| `INVALID_ARGUMENT` | 3005 | Validation |
| `EMPTY_WHERE_DELETE` | 3006 | Validation |
| `INVALID_IDENTIFIER` | 3007 | Validation |
| `INVALID_AGGREGATE` | 3008 | Validation (reserved) |
| `INVALID_QUERY_STATE` | 3009 | Validation (reserved) |

### Constructor

```
Error(type_or_code, msg?, data?) -> Error
```

Accepts either a type name string or a numeric code. Performs
bidirectional lookup to resolve both. Unknown types or codes throw
immediately (not as an Error — as a native argument error).

### Chainable Handlers

```
error.on(type, handler) -> error
```

If the error matches the given type **and** no previous `on` has
matched, calls `handler(error)` and marks as handled. Returns `self`
for chaining.

Unknown type names or codes in `on()` throw immediately.

```
error.otherwise(handler) -> void
```

Calls `handler(error)` only if no prior `on` matched.

### Bidirectional Lookup

```
Error.lookup(key) -> code_or_name_or_null
```

Static method. Accepts a string (returns numeric code) or integer
(returns string name). Returns null if not found.

---

## Error Classification

Error classification uses driver-specific error codes as the primary
discriminant, standard SQL state codes as a fallback grouping, and
message text only as a last resort.

### Connection Errors

Priority order (first match wins):

| Condition | Error Type |
|-----------|-----------|
| Driver code 1045 or 1044 | `AUTH_FAILED` |
| SQL state `28000` | `AUTH_FAILED` |
| Driver code 2013 | `CONNECTION_TIMEOUT` |
| Driver code 2002 or 2003 AND message contains "timed out" | `CONNECTION_TIMEOUT` |
| Everything else | `CONNECTION_FAILED` |

Error data includes: host, database, SQL state, driver code, original
exception.

### Query Errors

Priority order (first match wins):

| Condition | Error Type |
|-----------|-----------|
| Driver code 1062 or 1586 | `DUPLICATE_ENTRY` |
| Driver code 1451, 1452, 1216, 1217, 1048, 3819, or 4025 | `CONSTRAINT_VIOLATION` |
| SQL state `23000` | `CONSTRAINT_VIOLATION` |
| Driver code 2006 or 2013 | `CONNECTION_FAILED` |
| Everything else | `QUERY_FAILED` |

Error data includes: SQL state, driver code, query text, original
exception.

### Adaptation Notes

The driver codes above are MySQL/MariaDB-specific. When porting to
other databases:

- Map equivalent constraint violation codes from the target database
- PostgreSQL uses SQLSTATE `23505` for unique violations, `23503` for
  FK violations — these are more specific than MySQL's blanket `23000`
- SQLite uses its own error codes (e.g., `SQLITE_CONSTRAINT_UNIQUE`)
- The classification priority (driver code > SQL state > message text)
  should remain the same

---

## Safety Model

### Trust Boundary

```
+---------------------------------------------------+
|  Query Builder (safe by construction)              |
|  - All identifiers validated via regex             |
|  - All operators validated via allowlist            |
|  - All values parameterized via prepared statements|
|  - No raw SQL strings accepted                     |
+---------------------------------------------------+
                    |
                    | toStatement()
                    v
+---------------------------------------------------+
|  run() (explicit escape hatch)                     |
|  - Accepts any SQL                                 |
|  - Caller responsible for safety                   |
|  - Must be audited before release                  |
+---------------------------------------------------+
```

### Blocked Paths

The following are rejected at runtime with `INVALID_ARGUMENT`:
- String WHERE clauses in `read` config
- `where_raw` config key
- Raw `having` config key
- Array sources in `paginate` (filter results, raw query arrays)

### Release Gate

Before any release of application code built on this library:

1. Inventory all `run` calls with justification
2. Classify each as static SQL, parameterized, or dynamic SQL
3. Unparameterized user input in a SQL string is a release blocker

---

## Scope Boundaries

### In Scope

- SELECT with validated columns
- WHERE with full operator set (AND-joined only)
- JOIN (inner, left, right, cross) with ON equality conditions
- ORDER BY, GROUP BY
- LIMIT / OFFSET / page-based pagination
- CRUD (create, read, update, delete)
- Fold/accumulator result shaping
- Function pipelines with database injection
- Typed error system with chainable handlers

### Out of Scope (Use `run` Directly)

- OR conditions / grouped WHERE logic
- HAVING clauses
- Aggregate expressions in SELECT
- Subqueries
- DISTINCT / UNION
- Stored procedures
- Schema DDL (CREATE TABLE, ALTER, etc.)
- Transactions (can be added as a future extension)

---

## Implementation Checklist

1. Error class with type codes, constructor, `on`/`otherwise`, `lookup`
2. Connection with driver setup and error classification
3. Type resolution (typed form + inference)
4. Parameter binding (positional + named)
5. `run` — prepare, bind, execute, fold
6. Identifier/operator/join/sort validation
7. `create`, `update`, `delete`
8. `buildWhere` (equality-only, associative map)
9. `read` (single table + join paths)
10. Query builder — factory, columns, where, join, orderBy, groupBy,
    limit, offset, page, resetOrderBy
11. `toStatement` and `toCountStatement`
12. `read` and `paginate` integration with query builder
13. `compose` pipelines
14. Block legacy raw SQL paths in `read`/`paginate`
