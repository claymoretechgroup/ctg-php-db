<?php
declare(strict_types=1);

namespace CTG\DB;

// PDO connection lifecycle manager
class CTGDBConn {

    /* Constants */
    public const DEFAULT_OPTIONS = [
        'charset' => 'utf8mb4', // Character set negotiated for the PDO MySQL connection
        'timeout' => null,      // Connection timeout in seconds; null uses the PDO driver default
        'persistent' => false,  // Whether PDO persistent connection pooling is requested
    ];
    private const REQUIRED_CONFIG_FIELDS = ['host', 'database', 'username', 'password'];

    /* Instance Properties */
    private ?\PDO $_PDO = null;         // Active PDO handle; null after permanent invalidation
    private bool $_persistent;          // Whether persistent PDO connection pooling was requested
    private bool $_invalidated = false; // Whether this connection object has been permanently poisoned

    // CONSTRUCTOR :: ["host" => STRING, "database" => STRING, "username" => STRING, "password" => STRING, "charset"? => STRING, "timeout"? => ?INT, "persistent"? => BOOL] -> $this
    // Creates and owns one PDO database connection
    public function __construct(array $config) {
        $config = $this->_normalizeConfig($config);
        $DSN = $this->_createDSN($config);
        $PDOOptions = $this->_createPDOOptions($config);
        $this->_persistent = $config['persistent'];

        try {
            $this->_PDO = new \PDO($DSN, $config['username'], $config['password'], $PDOOptions);
        } catch (\PDOException $error) {
            throw $this->_connectionError($error, $config['host'], $config['database']);
        }
    }

    /**
     *
     * Instance Methods
     *
     */

    // :: VOID -> VOID
    // Starts a transaction on the active connection
    public function beginTransaction(): void {
        $PDO = $this->_requirePDO();
        if ($this->inTransaction()) {
            throw new CTGDBError(
                'INVALID_QUERY_STATE',
                'A database transaction is already active'
            );
        }
        $this->_transactionOperation('begin', fn(): bool => $PDO->beginTransaction());
    }

    // :: VOID -> VOID
    // Commits the active transaction
    public function commit(): void {
        $PDO = $this->_requirePDO();
        if (!$this->inTransaction()) {
            throw new CTGDBError(
                'INVALID_QUERY_STATE',
                'No active database transaction can be committed'
            );
        }
        $this->_transactionOperation('commit', fn(): bool => $PDO->commit());
    }

    // :: VOID -> VOID
    // Rolls back the active transaction
    public function rollBack(): void {
        $PDO = $this->_requirePDO();
        if (!$this->inTransaction()) {
            throw new CTGDBError(
                'INVALID_QUERY_STATE',
                'No active database transaction can be rolled back'
            );
        }
        $this->_transactionOperation('rollback', fn(): bool => $PDO->rollBack());
    }

    // :: VOID -> BOOL
    // Returns whether the connection currently has an active transaction
    public function inTransaction(): bool {
        $PDO = $this->_requirePDO();
        try {
            return $PDO->inTransaction();
        } catch (\PDOException $error) {
            $this->invalidate();
            throw new CTGDBError(
                'CONNECTION_FAILED',
                'Unable to determine the database transaction state',
                [
                    'connection_invalidated' => true,
                    'original' => $error,
                ]
            );
        }
    }

    // :: VOID -> VOID
    // Releases the PDO handle and permanently poisons this connection object
    public function invalidate(): void {
        $this->_invalidated = true;
        $this->_PDO = null;
    }

    // :: VOID -> BOOL
    // Returns whether this connection object has been permanently invalidated
    public function isInvalidated(): bool {
        return $this->_invalidated;
    }

    // :: VOID -> BOOL
    // Returns whether PDO persistent-connection pooling was requested
    public function isPersistent(): bool {
        return $this->_persistent;
    }

    // :: STRING, ARRAY -> ARRAY|INT|STRING
    // Executes one prepared statement and returns its complete materialized result
    public function execute(string $sql, array $values = []): array|int|string {
        $PDO = $this->_requirePDO();
        $statement = $this->_executeStatement($PDO, $sql, $values);
        $columnCount = $this->_statementColumnCount($statement, $sql);
        if ($columnCount > 0) {
            $result = [];
            while (true) {
                $this->_requirePDO();
                $row = $this->_fetchRow($statement, $sql);
                if ($row === false) {
                    return $result;
                }
                $result[] = $row;
            }
        }
        return $this->_statementResult($PDO, $statement, $sql);
    }

    // :: STRING, (ARRAY, MIXED -> MIXED), MIXED, ARRAY -> MIXED
    // Processes one prepared statement result row at a time and returns final state
    public function process(string $sql, callable $processor, mixed $initial = null, array $values = []): mixed {
        $PDO = $this->_requirePDO();
        $statement = $this->_executeStatement($PDO, $sql, $values);
        if ($this->_statementColumnCount($statement, $sql) === 0) {
            return $initial;
        }

        $result = $initial;
        while (true) {
            $this->_requirePDO();
            $row = $this->_fetchRow($statement, $sql);
            if ($row === false) {
                return $result;
            }
            $result = $processor($row, $result);
        }
    }

    /**
     *
     * Private Methods
     *
     */

    // :: \PDO, STRING, ARRAY -> \PDOStatement
    // Prepares, binds, and executes one statement behind the connection boundary
    private function _executeStatement(\PDO $PDO, string $sql, array $values): \PDOStatement {
        try {
            $statement = $PDO->prepare($sql);
            $this->_bindValues($statement, $values);
            $statement->execute();
            return $statement;
        } catch (\PDOException $error) {
            throw $this->_queryError($error, $sql);
        }
    }

    // :: \PDOStatement, STRING -> INT
    // Returns statement column count through public query-error mapping
    private function _statementColumnCount(\PDOStatement $statement, string $sql): int {
        try {
            return $statement->columnCount();
        } catch (\PDOException $error) {
            throw $this->_queryError($error, $sql);
        }
    }

    // :: \PDOStatement, STRING -> ARRAY|FALSE
    // Fetches the next associative result row through public query-error mapping
    private function _fetchRow(\PDOStatement $statement, string $sql): array|false {
        try {
            return $statement->fetch();
        } catch (\PDOException $error) {
            throw $this->_queryError($error, $sql);
        }
    }

    // :: \PDO, \PDOStatement, STRING -> INT|STRING
    // Returns an insert identifier or affected-row count for a completed statement
    private function _statementResult(\PDO $PDO, \PDOStatement $statement, string $sql): int|string {
        if (str_starts_with(strtoupper(ltrim($sql)), 'INSERT')) {
            try {
                $insertId = $PDO->lastInsertId();
            } catch (\PDOException $error) {
                throw $this->_queryError($error, $sql);
            }
            if ($insertId === false) {
                throw new CTGDBError('QUERY_FAILED', 'PDO did not return a last insert identifier', ['query' => $sql]);
            }
            return $insertId;
        }
        try {
            return $statement->rowCount();
        } catch (\PDOException $error) {
            throw $this->_queryError($error, $sql);
        }
    }

    // :: VOID -> \PDO
    // Returns the active PDO handle or fails closed after invalidation
    private function _requirePDO(): \PDO {
        if ($this->_invalidated || !$this->_PDO instanceof \PDO) {
            throw new CTGDBError(
                'CONNECTION_FAILED',
                'Database connection has been invalidated'
            );
        }
        return $this->_PDO;
    }

    // :: STRING, (VOID -> BOOL) -> VOID
    // Runs a transaction boundary and poisons indeterminate connections on failure
    private function _transactionOperation(string $operation, callable $fn): void {
        try {
            if ($fn() !== true) {
                throw new \PDOException("Database transaction {$operation} returned false");
            }
        } catch (\PDOException $error) {
            $this->invalidate();
            throw new CTGDBError(
                'QUERY_FAILED',
                "Database transaction {$operation} failed",
                [
                    'transaction_operation' => $operation,
                    'connection_invalidated' => true,
                    'original' => $error,
                ]
            );
        }
    }

    // :: \PDOStatement, ARRAY -> VOID
    // Binds positional or named values to a prepared statement
    private function _bindValues(\PDOStatement $statement, array $values): void {
        $isAssociative = !array_is_list($values);
        $index = 1;
        foreach ($values as $key => $value) {
            [$resolved, $PDOType] = $this->_resolveType($value);
            if ($isAssociative && is_string($key)) {
                $parameter = str_starts_with($key, ':') ? $key : ":{$key}";
                $statement->bindValue($parameter, $resolved, $PDOType);
                continue;
            }
            $statement->bindValue($index, $resolved, $PDOType);
            $index++;
        }
    }

    // :: MIXED -> ARRAY
    // Resolves an explicit or inferred value to its PDO binding value and type
    private function _resolveType(mixed $value): array {
        if (is_array($value) && array_key_exists('type', $value) && array_key_exists('value', $value)) {
            return [$value['value'], match($value['type']) {
                'int'   => \PDO::PARAM_INT,
                'str'   => \PDO::PARAM_STR,
                'bool'  => \PDO::PARAM_BOOL,
                'null'  => \PDO::PARAM_NULL,
                'float' => \PDO::PARAM_STR,
                default => throw new CTGDBError(
                    'INVALID_ARGUMENT',
                    "Unknown type: {$value['type']}",
                    ['type' => $value['type'], 'allowed' => ['int', 'str', 'bool', 'null', 'float']]
                ),
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

    // :: PDOEXCEPTION, STRING -> CTGDBERROR
    // Maps statement failures and invalidates connections lost during execution
    private function _queryError(\PDOException $error, string $sql): CTGDBError {
        $info = $error->errorInfo ?? [null, null, null];
        $driverCode = $info[1] ?? null;
        $sqlstate = $info[0] ?? (string)$error->getCode();
        $connectionFailed = in_array($driverCode, [2006, 2013], true);
        if ($connectionFailed) {
            $this->invalidate();
        }

        $type = match(true) {
            in_array($driverCode, [1062, 1586], true) => 'DUPLICATE_ENTRY',
            in_array($driverCode, [1451, 1452, 1216, 1217, 1048, 3819, 4025], true) => 'CONSTRAINT_VIOLATION',
            $sqlstate === '23000' => 'CONSTRAINT_VIOLATION',
            $connectionFailed => 'CONNECTION_FAILED',
            default => 'QUERY_FAILED',
        };

        return new CTGDBError($type, $error->getMessage(), [
            'sqlstate' => $sqlstate,
            'driver_code' => $driverCode,
            'query' => $sql,
            'connection_invalidated' => $connectionFailed,
            'original' => $error,
        ]);
    }

    // :: ARRAY -> ARRAY
    // Applies connection defaults and validates every supported configuration field
    private function _normalizeConfig(array $config): array {
        $allowedFields = [...self::REQUIRED_CONFIG_FIELDS, ...array_keys(self::DEFAULT_OPTIONS)];
        $unknownFields = array_values(array_diff(array_keys($config), $allowedFields));
        if ($unknownFields !== []) {
            throw new CTGDBError(
                'INVALID_ARGUMENT',
                'Unknown CTGDBConn configuration field',
                ['unknown_fields' => $unknownFields, 'allowed_fields' => $allowedFields]
            );
        }

        $missingFields = array_values(array_filter(
            self::REQUIRED_CONFIG_FIELDS,
            fn(string $field): bool => !array_key_exists($field, $config)
        ));
        if ($missingFields !== []) {
            throw new CTGDBError(
                'INVALID_ARGUMENT',
                'Missing required CTGDBConn configuration field',
                ['missing_fields' => $missingFields]
            );
        }

        $config = array_replace(self::DEFAULT_OPTIONS, $config);
        foreach (self::REQUIRED_CONFIG_FIELDS as $field) {
            if (!is_string($config[$field])) {
                throw $this->_invalidConfig($field, 'string', $config[$field]);
            }
        }
        foreach (['host', 'database', 'username'] as $field) {
            if ($config[$field] === '') {
                throw $this->_invalidConfig($field, 'non-empty string', $config[$field]);
            }
        }
        foreach (['host', 'database'] as $field) {
            if (str_contains($config[$field], ';') || preg_match('/[\x00-\x1F\x7F]/', $config[$field]) === 1) {
                throw $this->_invalidConfig($field, 'DSN-safe string', $config[$field]);
            }
        }
        if (!is_string($config['charset']) || preg_match('/\A[A-Za-z0-9_]+\z/', $config['charset']) !== 1) {
            throw $this->_invalidConfig('charset', 'non-empty database charset name', $config['charset']);
        }
        if ($config['timeout'] !== null && (!is_int($config['timeout']) || $config['timeout'] < 0)) {
            throw $this->_invalidConfig('timeout', 'null or a non-negative integer', $config['timeout']);
        }
        if (!is_bool($config['persistent'])) {
            throw $this->_invalidConfig('persistent', 'boolean', $config['persistent']);
        }
        return $config;
    }

    // :: ARRAY -> STRING
    // Transforms validated connection configuration into a PDO MySQL DSN
    private function _createDSN(array $config): string {
        return "mysql:host={$config['host']};dbname={$config['database']};charset={$config['charset']}";
    }

    // :: ARRAY -> ARRAY
    // Transforms validated connection configuration into PDO driver options
    private function _createPDOOptions(array $config): array {
        $options = [
            \PDO::ATTR_ERRMODE            => \PDO::ERRMODE_EXCEPTION,
            \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
            \PDO::ATTR_EMULATE_PREPARES   => false,
            \PDO::ATTR_PERSISTENT         => $config['persistent'],
        ];
        if ($config['timeout'] !== null) {
            $options[\PDO::ATTR_TIMEOUT] = $config['timeout'];
        }
        return $options;
    }

    // :: STRING, STRING, MIXED -> CTGDBERROR
    // Creates a configuration error without disclosing the configured value
    private function _invalidConfig(string $field, string $expected, mixed $value): CTGDBError {
        return new CTGDBError(
            'INVALID_ARGUMENT',
            "Invalid CTGDBConn configuration field: {$field}",
            ['field' => $field, 'expected' => $expected, 'actual_type' => get_debug_type($value)]
        );
    }

    // :: PDOEXCEPTION, STRING, STRING -> CTGDBERROR
    // Maps PDO connection failures to the public CTGDB error contract
    private function _connectionError(\PDOException $error, string $host, string $database): CTGDBError {
        $message = $error->getMessage();
        $info = $error->errorInfo ?? [null, null, null];
        $driverCode = $info[1] ?? null;
        $sqlstate = $info[0] ?? $error->getCode();

        $type = match(true) {
            in_array($driverCode, [1045, 1044], true) => 'AUTH_FAILED',
            $sqlstate === '28000' => 'AUTH_FAILED',
            $driverCode === 2013 => 'CONNECTION_TIMEOUT',
            in_array($driverCode, [2002, 2003], true)
                && str_contains($message, 'timed out') => 'CONNECTION_TIMEOUT',
            default => 'CONNECTION_FAILED',
        };

        return new CTGDBError($type, $message, [
            'host' => $host,
            'database' => $database,
            'sqlstate' => $sqlstate,
            'driver_code' => $driverCode,
            'original' => $error,
        ]);
    }

    /**
     *
     * Static Methods
     *
     */

    // Static Factory Method :: ["host" => STRING, "database" => STRING, "username" => STRING, "password" => STRING, "charset"? => STRING, "timeout"? => ?INT, "persistent"? => BOOL] -> ctgdbConn
    // Creates and returns a new CTGDBConn instance
    public static function init(array $config): static {
        return new static($config);
    }
}
