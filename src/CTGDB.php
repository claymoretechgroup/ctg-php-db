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

    // :: STRING, ARRAY -> INT
    // Executes a custom non-row prepared statement and returns its affected-row count
    public function execute(string $sql, array $values = []): int {
        return $this->_connection->execute($sql, $values);
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

    // :: ctgdbQuery -> ARRAY
    // Executes a structured SELECT query and returns its materialized rows
    public function read(CTGDBQuery $query): array {
        return $this->run($query);
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

    // :: ctgdbQuery, ARRAY -> ARRAY
    // Paginate any result set with metadata
    public function paginate(CTGDBQuery $source, array $config = []): array {
        $page = max(1, $config['page'] ?? 1);
        $perPage = max(1, $config['per_page'] ?? 20);
        $total = $config['total'] ?? null;
        $query = clone $source;
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

    // :: VOID -> VOID
    // Starts a transaction for subsequent operations on this database instance
    public function beginTransaction(): void {
        $this->_connection->beginTransaction();
    }

    // :: VOID -> VOID
    // Commits the active transaction
    public function commit(): void {
        $this->_connection->commit();
    }

    // :: VOID -> VOID
    // Rolls back the active transaction
    public function rollBack(): void {
        $this->_connection->rollBack();
    }

    // :: VOID -> BOOL
    // Returns whether this database instance has an active transaction
    public function inTransaction(): bool {
        return $this->_connection->inTransaction();
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
