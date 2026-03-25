<?php
declare(strict_types=1);

namespace CTG\DB;

// Structured query builder — produces statement arrays for CTGDB::run()
class CTGDBQuery {

    /* Instance Properties */
    private string $_table;
    private array $_columns = ['*'];
    private array $_where = [];
    private array $_joins = [];
    private array $_orderBy = [];
    private array $_groupBy = [];
    private ?int $_limit = null;
    private ?int $_offset = null;

    // CONSTRUCTOR :: STRING -> $this
    private function __construct(string $table) {
        $this->_table = self::validateIdentifier($table);
    }

    /**
     *
     * Instance Methods
     *
     */

    // :: STRING, STRING, ... -> $this
    // Set columns to select (replaces any previous column list)
    public function columns(string ...$columns): static {
        $this->_columns = array_map(
            fn($col) => self::parseColumn($col),
            $columns
        );
        return $this;
    }

    // :: STRING, STRING, MIXED, ?STRING -> $this
    // Add a WHERE condition (AND-joined with previous conditions)
    public function where(
        string  $column,
        string  $operator,
        mixed   $value,
        ?string $type = null
    ): static {
        $quotedCol = self::validateIdentifier($column);
        $op = self::validateOperator($operator);

        if ($op === 'IN' || $op === 'NOT IN') {
            $placeholders = implode(', ', array_fill(0, count($value), '?'));
            $sql = "{$quotedCol} {$op} ({$placeholders})";
            $values = [];
            foreach ($value as $v) {
                $values[] = $type !== null ? ['type' => $type, 'value' => $v] : $v;
            }
            $this->_where[] = ['sql' => $sql, 'values' => $values];
        } elseif ($op === 'IS' || $op === 'IS NOT') {
            $this->_where[] = ['sql' => "{$quotedCol} {$op} NULL", 'values' => []];
        } elseif ($op === 'BETWEEN') {
            $sql = "{$quotedCol} BETWEEN ? AND ?";
            $values = [];
            $values[] = $type !== null ? ['type' => $type, 'value' => $value[0]] : $value[0];
            $values[] = $type !== null ? ['type' => $type, 'value' => $value[1]] : $value[1];
            $this->_where[] = ['sql' => $sql, 'values' => $values];
        } else {
            $sql = "{$quotedCol} {$op} ?";
            $val = $type !== null ? ['type' => $type, 'value' => $value] : $value;
            $this->_where[] = ['sql' => $sql, 'values' => [$val]];
        }

        return $this;
    }

    // :: STRING, STRING, ARRAY -> $this
    // Add a JOIN clause
    public function join(
        string $table,
        string $type,
        array  $on
    ): static {
        $validatedTable = self::validateIdentifier($table);
        $validatedType = self::validateJoinType($type);

        // Non-cross joins require at least one ON condition
        if ($validatedType !== 'CROSS' && empty($on)) {
            throw new CTGDBError('INVALID_ARGUMENT',
                "Non-cross joins require at least one ON condition",
                ['table' => $table, 'type' => $type]
            );
        }

        $onParts = [];
        foreach ($on as $left => $right) {
            $onParts[] = [
                'left' => self::validateIdentifier($left),
                'right' => self::validateIdentifier($right),
            ];
        }

        $this->_joins[] = [
            'table' => $validatedTable,
            'type' => $validatedType,
            'on' => $onParts,
        ];

        return $this;
    }

    // :: VOID -> $this
    // Clear all ORDER BY columns
    public function resetOrderBy(): static {
        $this->_orderBy = [];
        return $this;
    }

    // :: STRING, STRING -> $this
    // Add an ORDER BY column (appends to existing order)
    public function orderBy(
        string $column,
        string $direction = 'ASC'
    ): static {
        $quotedCol = self::validateIdentifier($column);
        $dir = self::validateSortDirection($direction);
        $this->_orderBy[] = "{$quotedCol} {$dir}";
        return $this;
    }

    // :: STRING, STRING, ... -> $this
    // Set GROUP BY columns
    public function groupBy(string ...$columns): static {
        $this->_groupBy = array_map(
            fn($col) => self::validateIdentifier($col),
            $columns
        );
        return $this;
    }

    // :: INT -> $this
    // Set maximum rows to return
    public function limit(int $limit): static {
        if ($limit < 0) {
            throw new CTGDBError('INVALID_ARGUMENT',
                "Limit must be non-negative, got: {$limit}",
                ['limit' => $limit]
            );
        }
        $this->_limit = $limit;
        return $this;
    }

    // :: INT -> $this
    // Set row offset
    public function offset(int $offset): static {
        if ($offset < 0) {
            throw new CTGDBError('INVALID_ARGUMENT',
                "Offset must be non-negative, got: {$offset}",
                ['offset' => $offset]
            );
        }
        $this->_offset = $offset;
        return $this;
    }

    // :: INT, INT -> $this
    // Convenience: set limit and offset from page number and per-page count
    public function page(int $page, int $perPage = 20): static {
        $page = max(1, $page);
        $perPage = max(1, $perPage);
        $this->_limit = $perPage;
        $this->_offset = ($page - 1) * $perPage;
        return $this;
    }

    // :: VOID -> ARRAY
    // Build the SELECT statement: ['sql' => ..., 'values' => [...]]
    public function toStatement(): array {
        $values = [];

        // SELECT columns
        $columnStr = implode(', ', $this->_columns);
        $sql = "SELECT {$columnStr} FROM {$this->_table}";

        // JOINs
        foreach ($this->_joins as $join) {
            $sql .= " {$join['type']} JOIN {$join['table']}";
            if (!empty($join['on'])) {
                $onParts = array_map(
                    fn($pair) => "{$pair['left']} = {$pair['right']}",
                    $join['on']
                );
                $sql .= " ON " . implode(' AND ', $onParts);
            }
        }

        // WHERE
        if (!empty($this->_where)) {
            $whereParts = [];
            foreach ($this->_where as $condition) {
                $whereParts[] = $condition['sql'];
                foreach ($condition['values'] as $v) {
                    $values[] = $v;
                }
            }
            $sql .= " WHERE " . implode(' AND ', $whereParts);
        }

        // GROUP BY
        if (!empty($this->_groupBy)) {
            $sql .= " GROUP BY " . implode(', ', $this->_groupBy);
        }

        // ORDER BY
        if (!empty($this->_orderBy)) {
            $sql .= " ORDER BY " . implode(', ', $this->_orderBy);
        }

        // LIMIT
        if ($this->_limit !== null) {
            $sql .= " LIMIT {$this->_limit}";
        }

        // OFFSET
        if ($this->_offset !== null) {
            $sql .= " OFFSET {$this->_offset}";
        }

        return ['sql' => $sql, 'values' => $values];
    }

    // :: VOID -> ARRAY
    // Build the COUNT(*) version: ['sql' => ..., 'values' => [...]]
    public function toCountStatement(): array {
        $values = [];

        // When GROUP BY is present, wrap inner query in subquery
        if (!empty($this->_groupBy)) {
            $columnStr = implode(', ', $this->_columns);
            $innerSql = "SELECT {$columnStr} FROM {$this->_table}";

            // JOINs
            foreach ($this->_joins as $join) {
                $innerSql .= " {$join['type']} JOIN {$join['table']}";
                if (!empty($join['on'])) {
                    $onParts = array_map(
                        fn($pair) => "{$pair['left']} = {$pair['right']}",
                        $join['on']
                    );
                    $innerSql .= " ON " . implode(' AND ', $onParts);
                }
            }

            // WHERE
            if (!empty($this->_where)) {
                $whereParts = [];
                foreach ($this->_where as $condition) {
                    $whereParts[] = $condition['sql'];
                    foreach ($condition['values'] as $v) {
                        $values[] = $v;
                    }
                }
                $innerSql .= " WHERE " . implode(' AND ', $whereParts);
            }

            // GROUP BY
            $innerSql .= " GROUP BY " . implode(', ', $this->_groupBy);

            return [
                'sql' => "SELECT COUNT(*) as total FROM ({$innerSql}) as _counted",
                'values' => $values,
            ];
        }

        // No GROUP BY — simple count
        $sql = "SELECT COUNT(*) as total FROM {$this->_table}";

        // JOINs
        foreach ($this->_joins as $join) {
            $sql .= " {$join['type']} JOIN {$join['table']}";
            if (!empty($join['on'])) {
                $onParts = array_map(
                    fn($pair) => "{$pair['left']} = {$pair['right']}",
                    $join['on']
                );
                $sql .= " ON " . implode(' AND ', $onParts);
            }
        }

        // WHERE
        if (!empty($this->_where)) {
            $whereParts = [];
            foreach ($this->_where as $condition) {
                $whereParts[] = $condition['sql'];
                foreach ($condition['values'] as $v) {
                    $values[] = $v;
                }
            }
            $sql .= " WHERE " . implode(' AND ', $whereParts);
        }

        return ['sql' => $sql, 'values' => $values];
    }

    /**
     *
     * Static Methods
     *
     */

    // :: STRING -> ctgdbQuery
    // Create a query for a single table
    public static function from(string $table): static {
        return new static($table);
    }

    /**
     *
     * Private Static Methods — Validation
     * Duplicated from CTGDB to keep CTGDBQuery self-contained
     *
     */

    // :: STRING -> STRING
    // Validate and backtick-quote an identifier
    private static function validateIdentifier(string $identifier): string {
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
    // Validate filter operator against allowlist
    private static function validateOperator(string $op): string {
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

    // :: STRING -> STRING
    // Validate join type against allowlist
    private static function validateJoinType(string $type): string {
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
    private static function validateSortDirection(string $dir): string {
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
    // Parse a column expression into its quoted SQL form
    private static function parseColumn(string $col): string {
        // Global wildcard
        if ($col === '*') {
            return '*';
        }

        // Handle 'col as alias' or 'table.col as alias'
        if (preg_match('/^(.+)\s+as\s+(.+)$/i', $col, $m)) {
            $left = trim($m[1]);
            $alias = trim($m[2]);
            return self::validateIdentifier($left) . ' as ' . self::validateIdentifier($alias);
        }

        // Table wildcard: 'table.*'
        if (preg_match('/^([a-zA-Z_][a-zA-Z0-9_]*)\.\*$/', $col, $m)) {
            return self::validateIdentifier($m[1]) . '.*';
        }

        // Table-qualified or bare column
        return self::validateIdentifier($col);
    }
}
