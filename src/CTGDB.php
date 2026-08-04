<?php
declare(strict_types=1);

namespace CTG\DB;

// Minimal PDO database library with safe CRUD and incremental result processing
class CTGDB {

    /* Instance Properties */
    private CTGDBConn $_connection; // Connection responsible for PDO access, transactions, and invalidation

    // CONSTRUCTOR :: ctgdbConn -> $this
    // Creates the main database API over an existing connection instance
    public function __construct(CTGDBConn $connection) {
        $this->_connection = $connection;
    }

    /**
     *
     * Instance Methods
     *
     */

    // :: STRING|ctgdbQuery, ARRAY -> ARRAY|INT|STRING
    // Executes a query and returns its complete materialized result
    public function run(string|CTGDBQuery $query, array $values = []): array|int|string {
        [$sql, $resolvedValues] = $this->_resolveQuery($query, $values);
        return $this->_connection->execute($sql, $resolvedValues);
    }

    // :: STRING|ctgdbQuery, (ARRAY, MIXED -> MIXED), MIXED, ARRAY -> MIXED
    // Processes query results one row at a time and returns the final state
    public function process(string|CTGDBQuery $query, callable $processor, mixed $initial = null, array $values = []): mixed {
        [$sql, $resolvedValues] = $this->_resolveQuery($query, $values);
        return $this->_connection->process($sql, $processor, $initial, $resolvedValues);
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

        return $this->run("INSERT INTO {$table} ({$colStr}) VALUES ({$phStr})", $values);
    }

    // :: STRING|ARRAY|ctgdbQuery, ARRAY -> ARRAY
    // Reads rows from one or more tables
    public function read(string|array|CTGDBQuery $tables, array $config = []): array {
        if ($tables instanceof CTGDBQuery) {
            return $this->run($tables);
        }

        if (is_array($tables)) {
            return $this->_readJoin($tables, $config);
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

        return $this->run($sql, $values);
    }

    // :: STRING, ARRAY, ARRAY -> INT
    // Update rows matching WHERE conditions, returns affected count.
    // Empty $where is rejected to prevent accidental full-table updates;
    // the caller must pass at least one predicate.
    public function update(string $table, array $data, array $where): int {
        if (empty($where)) {
            throw new CTGDBError('EMPTY_WHERE_UPDATE',
                "update() requires a WHERE clause",
                ['table' => $table]
            );
        }

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
        return $this->run("UPDATE {$table} SET {$setStr}{$whereSql}", $values);
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

        return $this->run("DELETE FROM {$table}{$whereSql}", $values);
    }

    // :: STRING|ctgdbQuery, ARRAY -> ARRAY
    // Paginate any result set with metadata
    public function paginate(string|CTGDBQuery $source, array $config = []): array {
        $page = max(1, $config['page'] ?? 1);
        $perPage = max(1, $config['per_page'] ?? 20);
        $offset = ($page - 1) * $perPage;
        $sort = isset($config['sort']) ? $this->validateIdentifier($config['sort']) : null;
        $order = isset($config['order']) ? $this->validateSortDirection($config['order']) : 'ASC';

        $total = $config['total'] ?? null;

        if ($source instanceof CTGDBQuery) {
            if ($total === null) {
                $countStatement = $source->toCountStatement();
                $countResult = $this->run($countStatement['sql'], $countStatement['values']);
                $total = (int)$countResult[0]['total'];
            }

            $query = clone $source;
            if ($sort !== null) {
                $query->resetOrderBy()->orderBy($sort, $order);
            }
            $query->page($page, $perPage);

            $data = $this->run($query);

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

            $data = $this->run($sql);

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

    /**
     *
     * Private Methods
     *
     */

    // :: ARRAY, ARRAY -> ARRAY
    // Handle multi-table join reads
    private function _readJoin(array $tables, array $config): array {
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

        return $this->run($sql, $values);
    }

    // :: STRING|ctgdbQuery, ARRAY -> [STRING, ARRAY]
    // Resolves raw SQL or a structured query into SQL and bound values
    private function _resolveQuery(string|CTGDBQuery $query, array $values): array {
        if (is_string($query)) {
            return [$query, $values];
        }
        if ($values !== []) {
            throw new CTGDBError(
                'INVALID_ARGUMENT',
                'Values must be defined by CTGDBQuery when a structured query is supplied',
                ['value_count' => count($values)]
            );
        }
        $statement = $query->toStatement();
        return [$statement['sql'], $statement['values']];
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

    /**
     *
     * Static Methods
     *
     */

    // Static Factory Method :: ["host" => STRING, "database" => STRING, "username" => STRING, "password" => STRING, "charset"? => STRING, "timeout"? => ?INT, "persistent"? => BOOL] -> ctgdb
    // Creates the main database API over a new validated connection
    public static function init(array $config): static {
        return new static(CTGDBConn::init($config));
    }
}
