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

    // :: STRING|ctgdbQuery, ARRAY -> ARRAY
    // Executes a row-producing query and returns all rows as a materialized array
    public function run(string|CTGDBQuery $query, array $values = []): array {
        [$sql, $resolvedValues] = $this->_resolveQuery($query, $values);
        return $this->_connection->query($sql, $resolvedValues);
    }

    // :: STRING|ctgdbQuery, (ARRAY, MIXED -> MIXED), MIXED, ARRAY -> MIXED
    // Processes row-producing query results one row at a time and returns the final state
    public function process(string|CTGDBQuery $query, callable $processor, mixed $initial = null, array $values = []): mixed {
        [$sql, $resolvedValues] = $this->_resolveQuery($query, $values);
        return $this->_connection->process($sql, $processor, $initial, $resolvedValues);
    }

    // :: STRING, ARRAY -> INT|STRING
    // Insert a single row, returns last insert ID
    public function create(string $table, array $data): int|string {
        $table = CTGDBQuery::quoteIdentifier($table);
        $columns = [];
        $placeholders = [];
        $values = [];

        foreach ($data as $col => $val) {
            $columns[] = CTGDBQuery::quoteIdentifier($col);
            $placeholders[] = '?';
            $values[] = $val;
        }

        $colStr = implode(', ', $columns);
        $phStr = implode(', ', $placeholders);

        // Only validated identifiers and placeholders enter SQL; values remain bound parameters
        return $this->_connection->insert("INSERT INTO {$table} ({$colStr}) VALUES ({$phStr})", $values);
    }

    // :: STRING|ARRAY|ctgdbQuery, ARRAY -> ARRAY
    // Builds or accepts a structured SELECT query and returns its materialized rows
    public function read(string|array|CTGDBQuery $tables, array $config = []): array {
        if ($tables instanceof CTGDBQuery) {
            return $this->run($tables);
        }
        return $this->run($this->_createReadQuery($tables, $config));
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

        $table = CTGDBQuery::quoteIdentifier($table);
        $setParts = [];
        $values = [];

        foreach ($data as $col => $val) {
            $setParts[] = CTGDBQuery::quoteIdentifier($col) . ' = ?';
            $values[] = $val;
        }

        [$whereSql, $whereValues] = CTGDBQuery::buildWhere($where);
        $values = array_merge($values, $whereValues);

        $setStr = implode(', ', $setParts);
        return $this->_connection->execute("UPDATE {$table} SET {$setStr}{$whereSql}", $values);
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

        $table = CTGDBQuery::quoteIdentifier($table);
        [$whereSql, $values] = CTGDBQuery::buildWhere($where);

        return $this->_connection->execute("DELETE FROM {$table}{$whereSql}", $values);
    }

    // :: STRING|ctgdbQuery, ARRAY -> ARRAY
    // Paginate any result set with metadata
    public function paginate(string|CTGDBQuery $source, array $config = []): array {
        $page = max(1, $config['page'] ?? 1);
        $perPage = max(1, $config['per_page'] ?? 20);
        $total = $config['total'] ?? null;
        $query = $source instanceof CTGDBQuery ? clone $source : CTGDBQuery::from($source);
        if (is_string($source) && isset($config['columns'])) {
            $query->columns(...$config['columns']);
        }
        if (isset($config['sort'])) {
            $query->resetOrderBy()->orderBy($config['sort'], $config['order'] ?? 'ASC');
        }
        if ($total === null) {
            $countStatement = $query->toCountStatement();
            $countResult = $this->run($countStatement['sql'], $countStatement['values']);
            $total = (int)$countResult[0]['total'];
        }
        $query->page($page, $perPage);
        $data = $this->run($query);

        return [
            'data' => $data,
            'pagination' => $this->calcPaginationInfo($page, $perPage, $total),
        ];
    }

    /**
     *
     * Protected Methods
     *
     */

    // :: INT, INT, INT -> ARRAY
    // Calculate pagination information from page size and total row count
    protected function calcPaginationInfo(int $page, int $perPage, int $total): array {
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

    /**
     *
     * Private Methods
     *
     */

    // :: STRING|ARRAY, ARRAY -> ctgdbQuery
    // Translates the legacy read interface into the canonical SELECT builder
    private function _createReadQuery(string|array $tables, array $config): CTGDBQuery {
        if (is_string($tables)) {
            $query = CTGDBQuery::from($tables);
        } else {
            if ($tables === []) {
                throw new CTGDBError('INVALID_ARGUMENT', 'read() requires at least one table');
            }
            $baseTable = array_shift($tables);
            $query = CTGDBQuery::from($baseTable);
            $this->_configureReadJoins($query, $tables, $config);
        }
        $this->_configureReadQuery($query, $config);
        return $query;
    }

    // :: ctgdbQuery, ARRAY, ARRAY -> VOID
    // Translates legacy uniform or per-table join configuration into builder calls
    private function _configureReadJoins(CTGDBQuery $query, array $tables, array $config): void {
        $joinType = $config['join'] ?? 'inner';
        if (is_array($joinType) && isset($joinType[0]['type'])) {
            if (count($joinType) !== count($tables)) {
                throw new CTGDBError(
                    'INVALID_ARGUMENT',
                    'Join definitions count must match joined tables count',
                    ['join_count' => count($joinType), 'table_count' => count($tables)]
                );
            }
            foreach ($joinType as $index => $joinDefinition) {
                $query->join($tables[$index], $joinDefinition['type'], $joinDefinition['on'] ?? []);
            }
            return;
        }

        $resolvedType = is_string($joinType) ? $joinType : 'inner';
        $on = $config['on'] ?? [];
        foreach ($tables as $index => $table) {
            if (!isset($on[$index])) {
                throw new CTGDBError(
                    'INVALID_ARGUMENT',
                    "Missing 'on' condition for join table: {$table}",
                    ['table' => $table, 'index' => $index]
                );
            }
            $query->join($table, $resolvedType, $on[$index]);
        }
    }

    // :: ctgdbQuery, ARRAY -> VOID
    // Translates supported legacy SELECT configuration into builder calls
    private function _configureReadQuery(CTGDBQuery $query, array $config): void {
        if (isset($config['columns'])) {
            $query->columns(...$config['columns']);
        }
        if (array_key_exists('where_raw', $config)) {
            throw new CTGDBError(
                'INVALID_ARGUMENT',
                'where_raw is no longer supported. Use CTGDBQuery instead.',
                ['where_raw' => $config['where_raw']]
            );
        }
        if (isset($config['where'])) {
            if (is_string($config['where'])) {
                throw new CTGDBError(
                    'INVALID_ARGUMENT',
                    'String where is no longer supported in read(). Use CTGDBQuery instead.',
                    ['where' => $config['where']]
                );
            }
            $this->_configureReadWhere($query, $config['where']);
        }
        if (isset($config['group'])) {
            $groups = array_map('trim', explode(',', $config['group']));
            $query->groupBy(...$groups);
        }
        if (array_key_exists('having', $config)) {
            throw new CTGDBError(
                'INVALID_ARGUMENT',
                'Raw having is no longer supported. Use CTGDBQuery instead.',
                ['having' => $config['having']]
            );
        }
        if (isset($config['order'])) {
            $this->_configureReadOrder($query, $config['order']);
        }
        if (isset($config['limit'])) {
            $query->limit((int)$config['limit']);
        }
    }

    // :: ctgdbQuery, ARRAY -> VOID
    // Converts equality-only legacy WHERE entries into structured conditions
    private function _configureReadWhere(CTGDBQuery $query, array $where): void {
        foreach ($where as $column => $condition) {
            $value = $condition;
            $type = null;
            if (is_array($condition) && array_key_exists('type', $condition) && array_key_exists('value', $condition)) {
                $value = $condition['value'];
                $type = $condition['type'];
            }
            $query->where($column, '=', $value, $type);
        }
    }

    // :: ctgdbQuery, STRING -> VOID
    // Converts a comma-separated legacy order clause into structured ordering
    private function _configureReadOrder(CTGDBQuery $query, string $order): void {
        foreach (array_map('trim', explode(',', $order)) as $part) {
            $tokens = preg_split('/\s+/', $part) ?: [];
            $query->orderBy($tokens[0] ?? '', $tokens[1] ?? 'ASC');
        }
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
