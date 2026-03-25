<?php
declare(strict_types=1);

namespace CTG\DB;

// Minimal PDO database library with fold/accumulator pattern
class CTGDB {

    /* Instance Properties */
    private \PDO $_pdo;

    // CONSTRUCTOR :: STRING, STRING, STRING, STRING, ARRAY -> $this
    // Creates a new database connection via PDO
    public function __construct(
        string $host,
        string $database,
        string $username,
        string $password,
        array  $options = []
    ) {
        $charset = $options['charset'] ?? 'utf8mb4';
        $timeout = $options['timeout'] ?? null;
        $persistent = $options['persistent'] ?? false;

        $dsn = "mysql:host={$host};dbname={$database};charset={$charset}";

        $pdoOptions = [
            \PDO::ATTR_ERRMODE            => \PDO::ERRMODE_EXCEPTION,
            \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
            \PDO::ATTR_EMULATE_PREPARES   => false,
            \PDO::ATTR_PERSISTENT         => $persistent,
        ];

        if ($timeout !== null) {
            $pdoOptions[\PDO::ATTR_TIMEOUT] = $timeout;
        }

        try {
            $this->_pdo = new \PDO($dsn, $username, $password, $pdoOptions);
        } catch (\PDOException $e) {
            $msg = $e->getMessage();
            $info = $e->errorInfo ?? [null, null, null];
            $driverCode = $info[1] ?? null;
            $sqlstate = $info[0] ?? $e->getCode();

            $type = match(true) {
                // Auth: driver codes 1045 (bad password), 1044 (no DB privilege)
                in_array($driverCode, [1045, 1044], true) => 'AUTH_FAILED',
                $sqlstate === '28000'                      => 'AUTH_FAILED',
                // Timeout: driver code 2013 (lost connection), or 2002/2003 with timeout message
                $driverCode === 2013                       => 'CONNECTION_TIMEOUT',
                in_array($driverCode, [2002, 2003], true)
                    && str_contains($msg, 'timed out')     => 'CONNECTION_TIMEOUT',
                // Everything else is a connection failure
                default                                    => 'CONNECTION_FAILED',
            };
            throw new CTGDBError($type, $msg, [
                'host' => $host,
                'database' => $database,
                'sqlstate' => $sqlstate,
                'driver_code' => $driverCode,
                'original' => $e
            ]);
        }
    }

    /**
     *
     * Instance Methods
     *
     */

    // :: STRING|ARRAY, ?(ARRAY, MIXED -> MIXED), MIXED -> MIXED
    // Execute a query with optional fold/accumulator over results
    public function run(
        string|array $query,
        ?callable    $fn = null,
        mixed        $accumulator = []
    ): mixed {
        if (is_string($query)) {
            $sql = $query;
            $values = [];
        } else {
            if (!isset($query['sql'])) {
                throw new CTGDBError('INVALID_ARGUMENT',
                    "Query array must contain an 'sql' key",
                    ['keys' => array_keys($query)]
                );
            }
            $sql = $query['sql'];
            $values = $query['values'] ?? [];
        }

        try {
            $stmt = $this->_pdo->prepare($sql);
            $this->_bindValues($stmt, $values);
            $stmt->execute();
        } catch (\PDOException $e) {
            $info = $e->errorInfo ?? [null, null, null];
            $driverCode = $info[1] ?? null;
            $sqlstate = $info[0] ?? (string)$e->getCode();

            $type = match(true) {
                // Duplicate entry: driver codes 1062, 1586
                in_array($driverCode, [1062, 1586], true)
                    => 'DUPLICATE_ENTRY',
                // Constraint violation: FK (1451/1452/1216/1217), NOT NULL (1048), CHECK (3819/4025)
                in_array($driverCode, [1451, 1452, 1216, 1217, 1048, 3819, 4025], true)
                    => 'CONSTRAINT_VIOLATION',
                // SQLSTATE fallback for any integrity constraint violation
                $sqlstate === '23000'
                    => 'CONSTRAINT_VIOLATION',
                // Connection lost mid-query
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

        if ($stmt->columnCount() > 0) {
            $result = $accumulator;
            while ($row = $stmt->fetch()) {
                if ($fn !== null) {
                    $result = $fn($row, $result);
                } else {
                    $result[] = $row;
                }
            }
            return $result;
        }

        $trimmed = strtoupper(ltrim($sql));
        if (str_starts_with($trimmed, 'INSERT')) {
            return $this->_pdo->lastInsertId();
        }
        return $stmt->rowCount();
    }

    // :: STRING, ARRAY -> INT|STRING
    // Insert a single row, returns last insert ID
    public function create(string $table, array $data): int|string {
        $table = $this->validateIdentifier($table);
        $columns = [];
        $placeholders = [];
        $values = [];

        foreach ($data as $col => $val) {
            $columns[] = $this->validateIdentifier($col);
            $placeholders[] = '?';
            $values[] = $val;
        }

        $colStr = implode(', ', $columns);
        $phStr = implode(', ', $placeholders);

        return $this->run([
            'sql' => "INSERT INTO {$table} ({$colStr}) VALUES ({$phStr})",
            'values' => $values
        ]);
    }

    // :: STRING|ARRAY|ctgdbQuery, ARRAY, ?(ARRAY, MIXED -> MIXED), MIXED -> MIXED
    // Read rows from one or more tables with optional transform
    public function read(
        string|array|CTGDBQuery $tables,
        array                   $config = [],
        ?callable               $fn = null,
        mixed                   $accumulator = []
    ): mixed {
        if ($tables instanceof CTGDBQuery) {
            return $this->run($tables->toStatement(), $fn, $accumulator);
        }

        if (is_array($tables)) {
            return $this->_readJoin($tables, $config, $fn, $accumulator);
        }

        $table = $this->validateIdentifier($tables);
        $columns = $this->_buildColumnList($config['columns'] ?? ['*'], $tables);
        $values = [];
        $whereSql = '';

        if (isset($config['where'])) {
            if (is_string($config['where'])) {
                throw new CTGDBError('INVALID_ARGUMENT',
                    'String where is no longer supported in read(). Use CTGDBQuery instead.',
                    ['where' => $config['where']]
                );
            }
            [$whereSql, $values] = $this->buildWhere($config['where']);
        }

        $sql = "SELECT {$columns} FROM {$table}{$whereSql}";

        if (isset($config['order'])) {
            $sql .= " ORDER BY " . $this->validateOrderClause($config['order']);
        }

        if (isset($config['limit'])) {
            $sql .= " LIMIT " . (int)$config['limit'];
        }

        return $this->run(['sql' => $sql, 'values' => $values], $fn, $accumulator);
    }

    // :: STRING, ARRAY, ARRAY -> INT
    // Update rows matching WHERE conditions, returns affected count
    public function update(string $table, array $data, array $where): int {
        $table = $this->validateIdentifier($table);
        $setParts = [];
        $values = [];

        foreach ($data as $col => $val) {
            $setParts[] = $this->validateIdentifier($col) . ' = ?';
            $values[] = $val;
        }

        [$whereSql, $whereValues] = $this->buildWhere($where);
        $values = array_merge($values, $whereValues);

        $setStr = implode(', ', $setParts);
        return $this->run([
            'sql' => "UPDATE {$table} SET {$setStr}{$whereSql}",
            'values' => $values
        ]);
    }

    // :: STRING, ARRAY -> INT
    // Delete rows matching WHERE conditions, returns affected count
    public function delete(string $table, array $where): int {
        if (empty($where)) {
            throw new CTGDBError('EMPTY_WHERE_DELETE',
                "delete() requires a WHERE clause",
                ['table' => $table]
            );
        }

        $table = $this->validateIdentifier($table);
        [$whereSql, $values] = $this->buildWhere($where);

        return $this->run([
            'sql' => "DELETE FROM {$table}{$whereSql}",
            'values' => $values
        ]);
    }

    // :: STRING|ARRAY|ctgdbQuery, ARRAY, ?(ARRAY, MIXED -> MIXED), MIXED -> ARRAY
    // Paginate any result set with metadata
    public function paginate(
        string|CTGDBQuery $source,
        array                   $config = [],
        ?callable               $fn = null,
        mixed                   $accumulator = []
    ): array {
        $page = max(1, $config['page'] ?? 1);
        $perPage = max(1, $config['per_page'] ?? 20);
        $offset = ($page - 1) * $perPage;
        $sort = isset($config['sort']) ? $this->validateIdentifier($config['sort']) : null;
        $order = isset($config['order']) ? $this->validateSortDirection($config['order']) : 'ASC';

        $total = $config['total'] ?? null;

        if ($source instanceof CTGDBQuery) {
            if ($total === null) {
                $countResult = $this->run($source->toCountStatement());
                $total = (int)$countResult[0]['total'];
            }

            $query = clone $source;
            if ($sort !== null) {
                $query->resetOrderBy()->orderBy($sort, $order);
            }
            $query->page($page, $perPage);

            $data = $this->run($query->toStatement(), $fn, $accumulator);

            return [
                'data' => $data,
                'pagination' => $this->buildPaginationMeta($page, $perPage, $total),
            ];
        }

        if (is_string($source)) {
            $table = $this->validateIdentifier($source);
            $columns = $this->_buildColumnList($config['columns'] ?? ['*'], $source);

            if ($total === null) {
                $countResult = $this->run("SELECT COUNT(*) as total FROM {$table}");
                $total = (int)$countResult[0]['total'];
            }

            $sql = "SELECT {$columns} FROM {$table}";
            if ($sort !== null) {
                $sql .= " ORDER BY {$sort} {$order}";
            }
            $sql .= " LIMIT {$perPage} OFFSET {$offset}";

            $data = $this->run(['sql' => $sql, 'values' => []], $fn, $accumulator);

        } else {
            throw new CTGDBError('INVALID_ARGUMENT',
                'paginate() source must be a CTGDBQuery instance or a table name string',
                ['source' => $source]
            );
        }

        return [
            'data' => $data,
            'pagination' => $this->buildPaginationMeta($page, $perPage, $total),
        ];
    }

    // :: [(MIXED, ctgdb -> MIXED)] -> (MIXED -> MIXED)
    // Build a pipeline of functions that thread an accumulator and $this
    public function compose(array $fns): callable {
        return function(mixed $accumulator = null) use ($fns): mixed {
            $result = $accumulator;
            foreach ($fns as $fn) {
                $result = $fn($result, $this);
            }
            return $result;
        };
    }

    /**
     *
     * Protected Methods
     *
     */

    // :: MIXED -> ARRAY
    // Resolve a value to [value, PDO type constant]
    protected function resolveType(mixed $value): array {
        if (is_array($value) && isset($value['type'], $value['value'])) {
            return [$value['value'], match($value['type']) {
                'int'   => \PDO::PARAM_INT,
                'str'   => \PDO::PARAM_STR,
                'bool'  => \PDO::PARAM_BOOL,
                'null'  => \PDO::PARAM_NULL,
                'float' => \PDO::PARAM_STR,
                default => throw new CTGDBError('INVALID_ARGUMENT',
                    "Unknown type: {$value['type']}",
                    ['type' => $value['type'], 'allowed' => ['int','str','bool','null','float']]
                )
            }];
        }

        return match(true) {
            is_int($value)   => [$value, \PDO::PARAM_INT],
            is_bool($value)  => [$value, \PDO::PARAM_BOOL],
            is_null($value)  => [$value, \PDO::PARAM_NULL],
            is_float($value) => [(string)$value, \PDO::PARAM_STR],
            default          => [$value, \PDO::PARAM_STR],
        };
    }

    // :: ARRAY -> [STRING, ARRAY]
    // Build WHERE clause from associative array of conditions
    protected function buildWhere(array $where): array {
        $parts = [];
        $values = [];
        foreach ($where as $col => $val) {
            $quotedCol = $this->validateIdentifier($col);
            $parts[] = "{$quotedCol} = ?";
            $values[] = $val;
        }
        $sql = !empty($parts) ? ' WHERE ' . implode(' AND ', $parts) : '';
        return [$sql, $values];
    }

    // :: INT, INT, INT -> ARRAY
    // Calculate pagination metadata
    protected function buildPaginationMeta(int $page, int $perPage, int $total): array {
        $totalPages = $perPage > 0 ? (int)ceil($total / $perPage) : 0;
        return [
            'page' => $page,
            'per_page' => $perPage,
            'total_rows' => $total,
            'total_pages' => $totalPages,
            'has_previous' => $page > 1,
            'has_next' => $page < $totalPages,
        ];
    }

    // :: STRING -> STRING
    // Validate and backtick-quote an identifier
    protected function validateIdentifier(string $identifier): string {
        $clean = trim($identifier, '`');
        if (!preg_match('/^[a-zA-Z_][a-zA-Z0-9_.]*$/', $clean)) {
            throw new CTGDBError('INVALID_IDENTIFIER',
                "Invalid identifier: {$identifier}",
                ['identifier' => $identifier]
            );
        }
        if (str_contains($clean, '.')) {
            $parts = explode('.', $clean);
            return implode('.', array_map(fn($p) => $p === '*' ? '*' : "`{$p}`", $parts));
        }
        return "`{$clean}`";
    }

    // :: STRING -> STRING
    // Validate join type against allowlist
    protected function validateJoinType(string $type): string {
        $allowed = ['inner', 'left', 'right', 'cross'];
        $clean = strtolower(trim($type));
        if (!in_array($clean, $allowed, true)) {
            throw new CTGDBError('INVALID_JOIN_TYPE',
                "Invalid join type: {$type}. Allowed: " . implode(', ', $allowed),
                ['type' => $type, 'allowed' => $allowed]
            );
        }
        return strtoupper($clean);
    }

    // :: STRING -> STRING
    // Validate sort direction against allowlist
    protected function validateSortDirection(string $dir): string {
        $allowed = ['asc', 'desc'];
        $clean = strtolower(trim($dir));
        if (!in_array($clean, $allowed, true)) {
            throw new CTGDBError('INVALID_SORT',
                "Invalid sort direction: {$dir}. Allowed: ASC, DESC",
                ['direction' => $dir, 'allowed' => ['ASC', 'DESC']]
            );
        }
        return strtoupper($clean);
    }

    // :: STRING -> STRING
    // Validate and sanitize an ORDER BY clause (e.g., 'col ASC, col2 DESC')
    protected function validateOrderClause(string $order): string {
        $parts = array_map('trim', explode(',', $order));
        $validated = [];
        foreach ($parts as $part) {
            $tokens = preg_split('/\s+/', $part);
            $col = $this->validateIdentifier($tokens[0]);
            if (isset($tokens[1])) {
                $dir = $this->validateSortDirection($tokens[1]);
                $validated[] = "{$col} {$dir}";
            } else {
                $validated[] = $col;
            }
        }
        return implode(', ', $validated);
    }

    // :: STRING -> STRING
    // Validate and sanitize a GROUP BY clause (e.g., 'col1, col2')
    protected function validateGroupClause(string $group): string {
        $parts = array_map('trim', explode(',', $group));
        $validated = [];
        foreach ($parts as $part) {
            $validated[] = $this->validateIdentifier($part);
        }
        return implode(', ', $validated);
    }

    // :: STRING -> STRING
    // Validate filter operator against allowlist
    protected function validateOperator(string $op): string {
        $allowed = [
            '=', '>', '<', '>=', '<=', '!=',
            'like', 'not like',
            'in', 'not in',
            'is', 'is not',
            'between'
        ];
        $clean = strtolower(trim($op));
        if (!in_array($clean, $allowed, true)) {
            throw new CTGDBError('INVALID_OPERATOR',
                "Invalid operator: {$op}",
                ['operator' => $op, 'allowed' => $allowed]
            );
        }
        return strtoupper($clean);
    }

    // :: VOID -> \PDO
    // Access the underlying PDO instance
    protected function getPdo(): \PDO {
        return $this->_pdo;
    }

    /**
     *
     * Static Methods
     *
     */

    // Static Factory Method :: STRING, STRING, STRING, STRING, ARRAY -> ctgdb
    // Creates and returns a new CTGDB instance
    public static function connect(
        string $host,
        string $database,
        string $username,
        string $password,
        array  $options = []
    ): static {
        return new static($host, $database, $username, $password, $options);
    }

    /**
     *
     * Private Methods
     *
     */

    // :: \PDOStatement, ARRAY -> VOID
    // Bind values to a prepared statement
    private function _bindValues(\PDOStatement $stmt, array $values): void {
        $isAssoc = !array_is_list($values);
        $index = 1;
        foreach ($values as $key => $val) {
            [$resolved, $pdoType] = $this->resolveType($val);
            if ($isAssoc && is_string($key)) {
                $param = str_starts_with($key, ':') ? $key : ":{$key}";
                $stmt->bindValue($param, $resolved, $pdoType);
            } else {
                $stmt->bindValue($index, $resolved, $pdoType);
                $index++;
            }
        }
    }

    // :: ARRAY, ARRAY, ?(ARRAY, MIXED -> MIXED), MIXED -> MIXED
    // Handle multi-table join reads
    private function _readJoin(
        array     $tables,
        array     $config,
        ?callable $fn,
        mixed     $accumulator
    ): mixed {
        $baseTable = array_shift($tables);
        $validatedBase = $this->validateIdentifier($baseTable);
        $joinType = $config['join'] ?? 'inner';
        $columns = $this->_buildColumnList($config['columns'] ?? ['*'], $baseTable);
        $values = [];

        $joinClauses = [];

        if (is_array($joinType) && isset($joinType[0]['type'])) {
            if (count($joinType) !== count($tables)) {
                throw new CTGDBError('INVALID_ARGUMENT',
                    'Join definitions count must match joined tables count',
                    ['join_count' => count($joinType), 'table_count' => count($tables)]
                );
            }
            foreach ($joinType as $i => $joinDef) {
                $jType = $this->validateJoinType($joinDef['type']);
                $jTable = $this->validateIdentifier($tables[$i]);
                $onParts = [];
                foreach ($joinDef['on'] as $left => $right) {
                    $onParts[] = $this->validateIdentifier($left) . " = " . $this->validateIdentifier($right);
                }
                $joinClauses[] = "{$jType} JOIN {$jTable} ON " . implode(' AND ', $onParts);
            }
        } else {
            $jType = $this->validateJoinType(is_string($joinType) ? $joinType : 'inner');
            $onArr = $config['on'] ?? [];
            foreach ($tables as $i => $tbl) {
                $jTable = $this->validateIdentifier($tbl);
                if (!isset($onArr[$i])) {
                    throw new CTGDBError('INVALID_ARGUMENT',
                        "Missing 'on' condition for join table: {$tbl}",
                        ['table' => $tbl, 'index' => $i]
                    );
                }
                $onParts = [];
                foreach ($onArr[$i] as $left => $right) {
                    $onParts[] = $this->validateIdentifier($left) . " = " . $this->validateIdentifier($right);
                }
                $joinClauses[] = "{$jType} JOIN {$jTable} ON " . implode(' AND ', $onParts);
            }
        }

        $sql = "SELECT {$columns} FROM {$validatedBase} " . implode(' ', $joinClauses);

        if (isset($config['where_raw'])) {
            throw new CTGDBError('INVALID_ARGUMENT',
                'where_raw is no longer supported. Use CTGDBQuery instead.',
                ['where_raw' => $config['where_raw']]
            );
        }
        if (isset($config['where'])) {
            if (is_string($config['where'])) {
                throw new CTGDBError('INVALID_ARGUMENT',
                    'String where is no longer supported in read(). Use CTGDBQuery instead.',
                    ['where' => $config['where']]
                );
            }
            [$whereSql, $values] = $this->buildWhere($config['where']);
            $sql .= $whereSql;
        }

        if (isset($config['group'])) {
            $sql .= " GROUP BY " . $this->validateGroupClause($config['group']);
        }
        if (isset($config['having'])) {
            throw new CTGDBError('INVALID_ARGUMENT',
                'Raw having is no longer supported. Use CTGDBQuery instead.',
                ['having' => $config['having']]
            );
        }
        if (isset($config['order'])) {
            $sql .= " ORDER BY " . $this->validateOrderClause($config['order']);
        }
        if (isset($config['limit'])) {
            $sql .= " LIMIT " . (int)$config['limit'];
        }

        return $this->run(['sql' => $sql, 'values' => $values], $fn, $accumulator);
    }

    // :: ARRAY, STRING -> STRING
    // Build comma-separated column list, handling * and table.* patterns
    private function _buildColumnList(array $columns, string $context): string {
        if ($columns === ['*']) {
            return '*';
        }
        return implode(', ', array_map(function($col) {
            if ($col === '*') {
                return '*';
            }
            // table.* — validate table part
            if (preg_match('/^([a-zA-Z_][a-zA-Z0-9_]*)\.\*$/', $col, $m)) {
                return $this->validateIdentifier($m[1]) . '.*';
            }
            // col as alias or table.col as alias
            if (preg_match('/^(.+)\s+as\s+(.+)$/i', $col, $m)) {
                return $this->validateIdentifier(trim($m[1])) . ' as ' . $this->validateIdentifier(trim($m[2]));
            }
            // Reject raw expressions — use run() for aggregates
            if (str_contains($col, '(') || str_contains($col, '*')) {
                throw new CTGDBError('INVALID_IDENTIFIER',
                    "Raw expressions not allowed in columns. Use run() for aggregates: {$col}",
                    ['column' => $col]
                );
            }
            return $this->validateIdentifier($col);
        }, $columns));
    }
}
