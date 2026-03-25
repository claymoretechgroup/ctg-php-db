<?php
declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use CTG\Test\CTGTest;
use CTG\DB\CTGDBError;

// Tests for CTGDBError — construction, lookup, chainable handler

$config = ['output' => 'console'];

// ── Construction by type name ───────────────────────────────────

CTGTest::init('construct with type name')
    ->stage('create', fn($_) => new CTGDBError('QUERY_FAILED', 'bad query', ['sql' => 'SELECT']))
    ->assert('code is 2000', fn($e) => $e->getCode(), 2000)
    ->assert('type is QUERY_FAILED', fn($e) => $e->type, 'QUERY_FAILED')
    ->assert('msg is set', fn($e) => $e->msg, 'bad query')
    ->assert('data is set', fn($e) => $e->data, ['sql' => 'SELECT'])
    ->start(null, $config);

CTGTest::init('construct with type name — default msg')
    ->stage('create', fn($_) => new CTGDBError('DUPLICATE_ENTRY'))
    ->assert('msg defaults to type', fn($e) => $e->msg, 'DUPLICATE_ENTRY')
    ->assert('code is 2001', fn($e) => $e->getCode(), 2001)
    ->start(null, $config);

CTGTest::init('construct with type name — null data')
    ->stage('create', fn($_) => new CTGDBError('CONNECTION_FAILED', 'timeout'))
    ->assert('data is null', fn($e) => $e->data, null)
    ->start(null, $config);

// ── Construction by code ────────────────────────────────────────

CTGTest::init('construct with integer code')
    ->stage('create', fn($_) => new CTGDBError(3000, 'bad table', ['table' => 'foo']))
    ->assert('type is INVALID_TABLE', fn($e) => $e->type, 'INVALID_TABLE')
    ->assert('code is 3000', fn($e) => $e->getCode(), 3000)
    ->assert('msg is set', fn($e) => $e->msg, 'bad table')
    ->start(null, $config);

// ── Construction with unknown type throws ───────────────────────

CTGTest::init('construct with unknown type name throws')
    ->stage('create', function($_) {
        try {
            new CTGDBError('NONEXISTENT_TYPE');
            return 'no exception';
        } catch (\InvalidArgumentException $e) {
            return 'threw InvalidArgumentException';
        }
    })
    ->assert('throws', fn($r) => $r, 'threw InvalidArgumentException')
    ->start(null, $config);

CTGTest::init('construct with unknown code throws')
    ->stage('create', function($_) {
        try {
            new CTGDBError(9999);
            return 'no exception';
        } catch (\InvalidArgumentException $e) {
            return 'threw InvalidArgumentException';
        }
    })
    ->assert('throws', fn($r) => $r, 'threw InvalidArgumentException')
    ->start(null, $config);

// ── Extends Exception ───────────────────────────────────────────

CTGTest::init('extends Exception')
    ->stage('create', fn($_) => new CTGDBError('QUERY_FAILED', 'test'))
    ->assert('is Exception', fn($e) => $e instanceof \Exception, true)
    ->assert('getMessage works', fn($e) => $e->getMessage(), 'test')
    ->assert('getCode works', fn($e) => $e->getCode(), 2000)
    ->start(null, $config);

// ── Bidirectional lookup ────────────────────────────────────────

CTGTest::init('lookup — name to code')
    ->stage('execute', fn($_) => CTGDBError::lookup('DUPLICATE_ENTRY'))
    ->assert('returns 2001', fn($r) => $r, 2001)
    ->start(null, $config);

CTGTest::init('lookup — code to name')
    ->stage('execute', fn($_) => CTGDBError::lookup(2001))
    ->assert('returns DUPLICATE_ENTRY', fn($r) => $r, 'DUPLICATE_ENTRY')
    ->start(null, $config);

CTGTest::init('lookup — unknown name returns null')
    ->stage('execute', fn($_) => CTGDBError::lookup('NONEXISTENT'))
    ->assert('returns null', fn($r) => $r, null)
    ->start(null, $config);

CTGTest::init('lookup — unknown code returns null')
    ->stage('execute', fn($_) => CTGDBError::lookup(9999))
    ->assert('returns null', fn($r) => $r, null)
    ->start(null, $config);

// ── All type codes covered ──────────────────────────────────────

CTGTest::init('all connection codes in 1xxx range')
    ->stage('execute', fn($_) => [
        CTGDBError::lookup('CONNECTION_FAILED'),
        CTGDBError::lookup('CONNECTION_TIMEOUT'),
        CTGDBError::lookup('AUTH_FAILED'),
    ])
    ->assert('CONNECTION_FAILED', fn($r) => $r[0], 1000)
    ->assert('CONNECTION_TIMEOUT', fn($r) => $r[1], 1001)
    ->assert('AUTH_FAILED', fn($r) => $r[2], 1002)
    ->start(null, $config);

CTGTest::init('all query codes in 2xxx range')
    ->stage('execute', fn($_) => [
        CTGDBError::lookup('QUERY_FAILED'),
        CTGDBError::lookup('DUPLICATE_ENTRY'),
        CTGDBError::lookup('CONSTRAINT_VIOLATION'),
    ])
    ->assert('QUERY_FAILED', fn($r) => $r[0], 2000)
    ->assert('DUPLICATE_ENTRY', fn($r) => $r[1], 2001)
    ->assert('CONSTRAINT_VIOLATION', fn($r) => $r[2], 2002)
    ->start(null, $config);

CTGTest::init('all validation codes in 3xxx range')
    ->stage('execute', fn($_) => [
        CTGDBError::lookup('INVALID_TABLE'),
        CTGDBError::lookup('INVALID_COLUMN'),
        CTGDBError::lookup('INVALID_OPERATOR'),
        CTGDBError::lookup('INVALID_JOIN_TYPE'),
        CTGDBError::lookup('INVALID_SORT'),
        CTGDBError::lookup('INVALID_ARGUMENT'),
        CTGDBError::lookup('EMPTY_WHERE_DELETE'),
        CTGDBError::lookup('INVALID_IDENTIFIER'),
    ])
    ->assert('INVALID_TABLE', fn($r) => $r[0], 3000)
    ->assert('INVALID_COLUMN', fn($r) => $r[1], 3001)
    ->assert('INVALID_OPERATOR', fn($r) => $r[2], 3002)
    ->assert('INVALID_JOIN_TYPE', fn($r) => $r[3], 3003)
    ->assert('INVALID_SORT', fn($r) => $r[4], 3004)
    ->assert('INVALID_ARGUMENT', fn($r) => $r[5], 3005)
    ->assert('EMPTY_WHERE_DELETE', fn($r) => $r[6], 3006)
    ->assert('INVALID_IDENTIFIER', fn($r) => $r[7], 3007)
    ->start(null, $config);

// ── Chainable on() handler ──────────────────────────────────────

CTGTest::init('on — matches by type name')
    ->stage('execute', function($_) {
        $e = new CTGDBError('DUPLICATE_ENTRY', 'dup');
        $handled = null;
        $e->on('DUPLICATE_ENTRY', function($err) use (&$handled) {
            $handled = $err->type;
        });
        return $handled;
    })
    ->assert('handler called', fn($r) => $r, 'DUPLICATE_ENTRY')
    ->start(null, $config);

CTGTest::init('on — matches by integer code')
    ->stage('execute', function($_) {
        $e = new CTGDBError('DUPLICATE_ENTRY', 'dup');
        $handled = null;
        $e->on(2001, function($err) use (&$handled) {
            $handled = $err->getCode();
        });
        return $handled;
    })
    ->assert('handler called', fn($r) => $r, 2001)
    ->start(null, $config);

CTGTest::init('on — does not match wrong type')
    ->stage('execute', function($_) {
        $e = new CTGDBError('DUPLICATE_ENTRY', 'dup');
        $handled = false;
        $e->on('QUERY_FAILED', function($err) use (&$handled) {
            $handled = true;
        });
        return $handled;
    })
    ->assert('handler not called', fn($r) => $r, false)
    ->start(null, $config);

CTGTest::init('on — short circuits after first match')
    ->stage('execute', function($_) {
        $e = new CTGDBError('DUPLICATE_ENTRY', 'dup');
        $calls = [];
        $e->on('DUPLICATE_ENTRY', function($err) use (&$calls) {
            $calls[] = 'first';
        })->on('DUPLICATE_ENTRY', function($err) use (&$calls) {
            $calls[] = 'second';
        });
        return $calls;
    })
    ->assert('only first handler called', fn($r) => $r, ['first'])
    ->start(null, $config);

CTGTest::init('on — chaining multiple types')
    ->stage('execute', function($_) {
        $e = new CTGDBError('CONSTRAINT_VIOLATION', 'fk');
        $matched = null;
        $e->on('DUPLICATE_ENTRY', function($err) use (&$matched) {
            $matched = 'dup';
        })->on('CONSTRAINT_VIOLATION', function($err) use (&$matched) {
            $matched = 'constraint';
        })->on('QUERY_FAILED', function($err) use (&$matched) {
            $matched = 'query';
        });
        return $matched;
    })
    ->assert('matched second handler', fn($r) => $r, 'constraint')
    ->start(null, $config);

// ── otherwise() handler ─────────────────────────────────────────

CTGTest::init('otherwise — called when no on() matches')
    ->stage('execute', function($_) {
        $e = new CTGDBError('QUERY_FAILED', 'bad');
        $matched = null;
        $e->on('DUPLICATE_ENTRY', function($err) use (&$matched) {
            $matched = 'dup';
        })->otherwise(function($err) use (&$matched) {
            $matched = 'otherwise';
        });
        return $matched;
    })
    ->assert('otherwise called', fn($r) => $r, 'otherwise')
    ->start(null, $config);

CTGTest::init('otherwise — not called when on() matches')
    ->stage('execute', function($_) {
        $e = new CTGDBError('DUPLICATE_ENTRY', 'dup');
        $matched = null;
        $e->on('DUPLICATE_ENTRY', function($err) use (&$matched) {
            $matched = 'on';
        })->otherwise(function($err) use (&$matched) {
            $matched = 'otherwise';
        });
        return $matched;
    })
    ->assert('on() handler called, not otherwise', fn($r) => $r, 'on')
    ->start(null, $config);

// ── on() — unknown type throws ─────────────────────────────────

CTGTest::init('on — unknown string type throws InvalidArgumentException')
    ->stage('execute', function($_) {
        $e = new CTGDBError('DUPLICATE_ENTRY', 'dup');
        try {
            $e->on('NONEXISTENT_TYPE', fn($err) => null);
            return 'no exception';
        } catch (\InvalidArgumentException $ex) {
            return 'thrown';
        }
    })
    ->assert('throws', fn($r) => $r, 'thrown')
    ->start(null, $config);

CTGTest::init('on — unknown integer code throws InvalidArgumentException')
    ->stage('execute', function($_) {
        $e = new CTGDBError('DUPLICATE_ENTRY', 'dup');
        try {
            $e->on(99999, fn($err) => null);
            return 'no exception';
        } catch (\InvalidArgumentException $ex) {
            return 'thrown';
        }
    })
    ->assert('throws', fn($r) => $r, 'thrown')
    ->start(null, $config);

// ── Catchable as Exception ──────────────────────────────────────

CTGTest::init('catchable in try/catch')
    ->stage('execute', function($_) {
        try {
            throw new CTGDBError('QUERY_FAILED', 'bad query');
        } catch (CTGDBError $e) {
            return $e->type;
        }
    })
    ->assert('caught by type', fn($r) => $r, 'QUERY_FAILED')
    ->start(null, $config);

CTGTest::init('catchable as generic Exception')
    ->stage('execute', function($_) {
        try {
            throw new CTGDBError('QUERY_FAILED', 'bad query');
        } catch (\Exception $e) {
            return $e instanceof CTGDBError;
        }
    })
    ->assert('caught as Exception', fn($r) => $r, true)
    ->start(null, $config);
