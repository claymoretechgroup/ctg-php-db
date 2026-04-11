<?php
declare(strict_types=1);

use CTG\Test\CTGTest;
use CTG\Test\CTGTestState;
use CTG\Test\Predicates\CTGTestPredicates;
use CTG\DB\CTGDBError;

// Tests for CTGDBError — construction, lookup, chainable handler

return [

    // ── Construction by type name ───────────────────────────────────

    CTGTest::init('construct with type name')
        ->stage('create', fn(CTGTestState $s) => new CTGDBError('QUERY_FAILED', 'bad query', ['sql' => 'SELECT']))
        ->assert('code is 2000', fn(CTGTestState $s) => $s->getSubject()->getCode(), CTGTestPredicates::equals(2000))
        ->assert('type is QUERY_FAILED', fn(CTGTestState $s) => $s->getSubject()->type, CTGTestPredicates::equals('QUERY_FAILED'))
        ->assert('msg is set', fn(CTGTestState $s) => $s->getSubject()->msg, CTGTestPredicates::equals('bad query'))
        ->assert('data is set', fn(CTGTestState $s) => $s->getSubject()->data, CTGTestPredicates::equals(['sql' => 'SELECT'])),

    CTGTest::init('construct with type name — default msg')
        ->stage('create', fn(CTGTestState $s) => new CTGDBError('DUPLICATE_ENTRY'))
        ->assert('msg defaults to type', fn(CTGTestState $s) => $s->getSubject()->msg, CTGTestPredicates::equals('DUPLICATE_ENTRY'))
        ->assert('code is 2001', fn(CTGTestState $s) => $s->getSubject()->getCode(), CTGTestPredicates::equals(2001)),

    CTGTest::init('construct with type name — null data')
        ->stage('create', fn(CTGTestState $s) => new CTGDBError('CONNECTION_FAILED', 'timeout'))
        ->assert('data is null', fn(CTGTestState $s) => $s->getSubject()->data, CTGTestPredicates::isNull()),

    // ── Construction by code ────────────────────────────────────────

    CTGTest::init('construct with integer code')
        ->stage('create', fn(CTGTestState $s) => new CTGDBError(3000, 'bad table', ['table' => 'foo']))
        ->assert('type is INVALID_TABLE', fn(CTGTestState $s) => $s->getSubject()->type, CTGTestPredicates::equals('INVALID_TABLE'))
        ->assert('code is 3000', fn(CTGTestState $s) => $s->getSubject()->getCode(), CTGTestPredicates::equals(3000))
        ->assert('msg is set', fn(CTGTestState $s) => $s->getSubject()->msg, CTGTestPredicates::equals('bad table')),

    // ── Construction with unknown type throws ───────────────────────

    CTGTest::init('construct with unknown type name throws')
        ->stage('create', function(CTGTestState $s) {
            try {
                new CTGDBError('NONEXISTENT_TYPE');
                return 'no exception';
            } catch (\InvalidArgumentException $e) {
                return 'threw InvalidArgumentException';
            }
        })
        ->assert('throws', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::equals('threw InvalidArgumentException')),

    CTGTest::init('construct with unknown code throws')
        ->stage('create', function(CTGTestState $s) {
            try {
                new CTGDBError(9999);
                return 'no exception';
            } catch (\InvalidArgumentException $e) {
                return 'threw InvalidArgumentException';
            }
        })
        ->assert('throws', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::equals('threw InvalidArgumentException')),

    // ── Extends Exception ───────────────────────────────────────────

    CTGTest::init('extends Exception')
        ->stage('create', fn(CTGTestState $s) => new CTGDBError('QUERY_FAILED', 'test'))
        ->assert('is Exception', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::isInstanceOf(\Exception::class))
        ->assert('getMessage works', fn(CTGTestState $s) => $s->getSubject()->getMessage(), CTGTestPredicates::equals('test'))
        ->assert('getCode works', fn(CTGTestState $s) => $s->getSubject()->getCode(), CTGTestPredicates::equals(2000)),

    // ── Bidirectional lookup ────────────────────────────────────────

    CTGTest::init('lookup — name to code')
        ->stage('execute', fn(CTGTestState $s) => CTGDBError::lookup('DUPLICATE_ENTRY'))
        ->assert('returns 2001', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::equals(2001)),

    CTGTest::init('lookup — code to name')
        ->stage('execute', fn(CTGTestState $s) => CTGDBError::lookup(2001))
        ->assert('returns DUPLICATE_ENTRY', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::equals('DUPLICATE_ENTRY')),

    CTGTest::init('lookup — unknown name returns null')
        ->stage('execute', fn(CTGTestState $s) => CTGDBError::lookup('NONEXISTENT'))
        ->assert('returns null', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::isNull()),

    CTGTest::init('lookup — unknown code returns null')
        ->stage('execute', fn(CTGTestState $s) => CTGDBError::lookup(9999))
        ->assert('returns null', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::isNull()),

    // ── All type codes covered ──────────────────────────────────────

    CTGTest::init('all connection codes in 1xxx range')
        ->stage('execute', fn(CTGTestState $s) => [
            CTGDBError::lookup('CONNECTION_FAILED'),
            CTGDBError::lookup('CONNECTION_TIMEOUT'),
            CTGDBError::lookup('AUTH_FAILED'),
        ])
        ->assert('CONNECTION_FAILED', fn(CTGTestState $s) => $s->getSubject()[0], CTGTestPredicates::equals(1000))
        ->assert('CONNECTION_TIMEOUT', fn(CTGTestState $s) => $s->getSubject()[1], CTGTestPredicates::equals(1001))
        ->assert('AUTH_FAILED', fn(CTGTestState $s) => $s->getSubject()[2], CTGTestPredicates::equals(1002)),

    CTGTest::init('all query codes in 2xxx range')
        ->stage('execute', fn(CTGTestState $s) => [
            CTGDBError::lookup('QUERY_FAILED'),
            CTGDBError::lookup('DUPLICATE_ENTRY'),
            CTGDBError::lookup('CONSTRAINT_VIOLATION'),
        ])
        ->assert('QUERY_FAILED', fn(CTGTestState $s) => $s->getSubject()[0], CTGTestPredicates::equals(2000))
        ->assert('DUPLICATE_ENTRY', fn(CTGTestState $s) => $s->getSubject()[1], CTGTestPredicates::equals(2001))
        ->assert('CONSTRAINT_VIOLATION', fn(CTGTestState $s) => $s->getSubject()[2], CTGTestPredicates::equals(2002)),

    CTGTest::init('all validation codes in 3xxx range')
        ->stage('execute', fn(CTGTestState $s) => [
            CTGDBError::lookup('INVALID_TABLE'),
            CTGDBError::lookup('INVALID_COLUMN'),
            CTGDBError::lookup('INVALID_OPERATOR'),
            CTGDBError::lookup('INVALID_JOIN_TYPE'),
            CTGDBError::lookup('INVALID_SORT'),
            CTGDBError::lookup('INVALID_ARGUMENT'),
            CTGDBError::lookup('EMPTY_WHERE_DELETE'),
            CTGDBError::lookup('INVALID_IDENTIFIER'),
            CTGDBError::lookup('INVALID_AGGREGATE'),
            CTGDBError::lookup('INVALID_QUERY_STATE'),
        ])
        ->assert('INVALID_TABLE', fn(CTGTestState $s) => $s->getSubject()[0], CTGTestPredicates::equals(3000))
        ->assert('INVALID_COLUMN', fn(CTGTestState $s) => $s->getSubject()[1], CTGTestPredicates::equals(3001))
        ->assert('INVALID_OPERATOR', fn(CTGTestState $s) => $s->getSubject()[2], CTGTestPredicates::equals(3002))
        ->assert('INVALID_JOIN_TYPE', fn(CTGTestState $s) => $s->getSubject()[3], CTGTestPredicates::equals(3003))
        ->assert('INVALID_SORT', fn(CTGTestState $s) => $s->getSubject()[4], CTGTestPredicates::equals(3004))
        ->assert('INVALID_ARGUMENT', fn(CTGTestState $s) => $s->getSubject()[5], CTGTestPredicates::equals(3005))
        ->assert('EMPTY_WHERE_DELETE', fn(CTGTestState $s) => $s->getSubject()[6], CTGTestPredicates::equals(3006))
        ->assert('INVALID_IDENTIFIER', fn(CTGTestState $s) => $s->getSubject()[7], CTGTestPredicates::equals(3007))
        ->assert('INVALID_AGGREGATE', fn(CTGTestState $s) => $s->getSubject()[8], CTGTestPredicates::equals(3008))
        ->assert('INVALID_QUERY_STATE', fn(CTGTestState $s) => $s->getSubject()[9], CTGTestPredicates::equals(3009)),

    // ── Chainable on() handler ──────────────────────────────────────

    CTGTest::init('on — matches by type name')
        ->stage('execute', function(CTGTestState $s) {
            $e = new CTGDBError('DUPLICATE_ENTRY', 'dup');
            $handled = null;
            $e->on('DUPLICATE_ENTRY', function($err) use (&$handled) {
                $handled = $err->type;
            });
            return $handled;
        })
        ->assert('handler called', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::equals('DUPLICATE_ENTRY')),

    CTGTest::init('on — matches by integer code')
        ->stage('execute', function(CTGTestState $s) {
            $e = new CTGDBError('DUPLICATE_ENTRY', 'dup');
            $handled = null;
            $e->on(2001, function($err) use (&$handled) {
                $handled = $err->getCode();
            });
            return $handled;
        })
        ->assert('handler called', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::equals(2001)),

    CTGTest::init('on — does not match wrong type')
        ->stage('execute', function(CTGTestState $s) {
            $e = new CTGDBError('DUPLICATE_ENTRY', 'dup');
            $handled = false;
            $e->on('QUERY_FAILED', function($err) use (&$handled) {
                $handled = true;
            });
            return $handled;
        })
        ->assert('handler not called', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::isFalse()),

    CTGTest::init('on — short circuits after first match')
        ->stage('execute', function(CTGTestState $s) {
            $e = new CTGDBError('DUPLICATE_ENTRY', 'dup');
            $calls = [];
            $e->on('DUPLICATE_ENTRY', function($err) use (&$calls) {
                $calls[] = 'first';
            })->on('DUPLICATE_ENTRY', function($err) use (&$calls) {
                $calls[] = 'second';
            });
            return $calls;
        })
        ->assert('only first handler called', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::equals(['first'])),

    CTGTest::init('on — chaining multiple types')
        ->stage('execute', function(CTGTestState $s) {
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
        ->assert('matched second handler', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::equals('constraint')),

    // ── otherwise() handler ─────────────────────────────────────────

    CTGTest::init('otherwise — called when no on() matches')
        ->stage('execute', function(CTGTestState $s) {
            $e = new CTGDBError('QUERY_FAILED', 'bad');
            $matched = null;
            $e->on('DUPLICATE_ENTRY', function($err) use (&$matched) {
                $matched = 'dup';
            })->otherwise(function($err) use (&$matched) {
                $matched = 'otherwise';
            });
            return $matched;
        })
        ->assert('otherwise called', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::equals('otherwise')),

    CTGTest::init('otherwise — not called when on() matches')
        ->stage('execute', function(CTGTestState $s) {
            $e = new CTGDBError('DUPLICATE_ENTRY', 'dup');
            $matched = null;
            $e->on('DUPLICATE_ENTRY', function($err) use (&$matched) {
                $matched = 'on';
            })->otherwise(function($err) use (&$matched) {
                $matched = 'otherwise';
            });
            return $matched;
        })
        ->assert('on() handler called, not otherwise', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::equals('on')),

    // ── on() — unknown type throws ─────────────────────────────────

    CTGTest::init('on — unknown string type throws InvalidArgumentException')
        ->stage('execute', function(CTGTestState $s) {
            $e = new CTGDBError('DUPLICATE_ENTRY', 'dup');
            try {
                $e->on('NONEXISTENT_TYPE', fn($err) => null);
                return 'no exception';
            } catch (\InvalidArgumentException $ex) {
                return 'thrown';
            }
        })
        ->assert('throws', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::equals('thrown')),

    CTGTest::init('on — unknown integer code throws InvalidArgumentException')
        ->stage('execute', function(CTGTestState $s) {
            $e = new CTGDBError('DUPLICATE_ENTRY', 'dup');
            try {
                $e->on(99999, fn($err) => null);
                return 'no exception';
            } catch (\InvalidArgumentException $ex) {
                return 'thrown';
            }
        })
        ->assert('throws', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::equals('thrown')),

    // ── Catchable as Exception ──────────────────────────────────────

    CTGTest::init('catchable in try/catch')
        ->stage('execute', function(CTGTestState $s) {
            try {
                throw new CTGDBError('QUERY_FAILED', 'bad query');
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('caught by type', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::equals('QUERY_FAILED')),

    CTGTest::init('catchable as generic Exception')
        ->stage('execute', function(CTGTestState $s) {
            try {
                throw new CTGDBError('QUERY_FAILED', 'bad query');
            } catch (\Exception $e) {
                return $e instanceof CTGDBError;
            }
        })
        ->assert('caught as Exception', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::isTrue()),

];
