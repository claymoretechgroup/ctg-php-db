<?php
declare(strict_types=1);

namespace CTG\DB;

// Typed error class with chainable handler for database operations
class CTGDBError extends \Exception {

    /* Constants */
    const TYPES = [
        // 1xxx — Connection
        'CONNECTION_FAILED'    => 1000,
        'CONNECTION_TIMEOUT'   => 1001,
        'AUTH_FAILED'          => 1002,
        // 2xxx — Query execution
        'QUERY_FAILED'         => 2000,
        'DUPLICATE_ENTRY'      => 2001,
        'CONSTRAINT_VIOLATION' => 2002,
        // 3xxx — Validation
        'INVALID_TABLE'        => 3000,
        'INVALID_COLUMN'       => 3001,
        'INVALID_OPERATOR'     => 3002,
        'INVALID_JOIN_TYPE'    => 3003,
        'INVALID_SORT'         => 3004,
        'INVALID_ARGUMENT'     => 3005,
        'EMPTY_WHERE_DELETE'   => 3006,
        'INVALID_IDENTIFIER'   => 3007,
    ];

    /* Instance Properties */
    public readonly string $type;
    public readonly string $msg;
    public readonly mixed  $data;
    private bool $_handled = false;

    // CONSTRUCTOR :: STRING|INT, ?STRING, MIXED -> $this
    // Creates a new error — accepts type name or integer code
    public function __construct(
        string|int $type,
        ?string    $msg = null,
        mixed      $data = null
    ) {
        if (is_string($type)) {
            $this->type = $type;
            $code = self::TYPES[$type]
                ?? throw new \InvalidArgumentException("Unknown CTGDBError type: {$type}");
        } else {
            $code = $type;
            $this->type = self::lookup($type)
                ?? throw new \InvalidArgumentException("Unknown CTGDBError code: {$type}");
        }

        $this->msg  = $msg ?? $this->type;
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
        $code = is_string($type) ? (self::TYPES[$type] ?? null) : $type;

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
        return array_search($key, self::TYPES, true) ?: null;
    }
}
