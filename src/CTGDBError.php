<?php
declare(strict_types=1);

namespace CTG\DB;

// Typed error class with chainable handler for database operations
class CTGDBError extends \Exception {

    /* Constants */
    public const TYPES = [
        // 1xxx — Connection
        'CONNECTION_FAILED'    => 1000, // General database connection failure
        'CONNECTION_TIMEOUT'   => 1001, // Database connection attempt timed out
        'AUTH_FAILED'          => 1002, // Database authentication was rejected
        // 2xxx — Query execution
        'QUERY_FAILED'         => 2000, // General prepared-statement execution failure
        'DUPLICATE_ENTRY'      => 2001, // Unique or duplicate-key constraint failure
        'CONSTRAINT_VIOLATION' => 2002, // Referential, null, check, or related constraint failure
        // 3xxx — Validation
        'INVALID_TABLE'        => 3000, // Invalid table reference
        'INVALID_COLUMN'       => 3001, // Invalid column reference
        'INVALID_OPERATOR'     => 3002, // Unsupported SQL comparison operator
        'INVALID_JOIN_TYPE'    => 3003, // Unsupported SQL join type
        'INVALID_SORT'         => 3004, // Invalid sort direction or expression
        'INVALID_ARGUMENT'     => 3005, // Invalid public API argument
        'EMPTY_WHERE_DELETE'   => 3006, // Unsafe delete without predicates
        'INVALID_IDENTIFIER'   => 3007, // Identifier failed structural validation
        'INVALID_AGGREGATE'    => 3008, // Unsupported SQL aggregate
        'INVALID_QUERY_STATE'  => 3009, // Statement or transaction state violates the requested operation
        'EMPTY_WHERE_UPDATE'   => 3010, // Unsafe update without predicates
    ];

    /* Instance Properties */
    public readonly string $type;    // Stable symbolic error type
    public readonly string $msg;     // Public application-facing error message
    public readonly mixed $data;     // Optional structured error context
    private bool $_handled = false;  // Whether a chainable handler has matched this error

    // CONSTRUCTOR :: STRING|INT, ?STRING, MIXED -> $this
    // Creates a new error — accepts type name or integer code
    public function __construct(string|int $type, ?string $msg = null, mixed $data = null) {
        if (is_string($type)) {
            $this->type = $type;
            $code = self::TYPES[$type]
                ?? throw new \InvalidArgumentException("Unknown CTGDBError type: {$type}");
        } else {
            $code = $type;
            $this->type = self::lookup($type)
                ?? throw new \InvalidArgumentException("Unknown CTGDBError code: {$type}");
        }

        $this->msg = $msg ?? $this->type;
        $this->data = $data;
        parent::__construct($this->msg, $code);
    }

    /**
     *
     * Instance Methods
     *
     */

    // :: STRING|INT, (ctgdbError -> VOID) -> $this
    // Handle error if it matches the given type. Chainable. Short-circuits after first match.
    public function on(string|int $type, callable $handler): static {
        if (is_string($type)) {
            $code = self::TYPES[$type]
                ?? throw new \InvalidArgumentException("Unknown CTGDBError type for on(): {$type}");
        } else {
            $code = $type;
            if (self::lookup($type) === null) {
                throw new \InvalidArgumentException("Unknown CTGDBError code for on(): {$type}");
            }
        }

        if (!$this->_handled && $this->getCode() === $code) {
            $handler($this);
            $this->_handled = true;
        }
        return $this;
    }

    // :: (ctgdbError -> VOID) -> VOID
    // Handle error if no previous on() matched
    public function otherwise(callable $handler): void {
        if (!$this->_handled) {
            $handler($this);
        }
    }

    /**
     *
     * Static Methods
     *
     */

    // :: STRING|INT -> INT|STRING|NULL
    // Bidirectional lookup — name to code or code to name
    public static function lookup(string|int $key): int|string|null {
        if (is_string($key)) {
            return self::TYPES[$key] ?? null;
        }
        $result = array_search($key, self::TYPES, true);
        return $result !== false ? $result : null;
    }
}
