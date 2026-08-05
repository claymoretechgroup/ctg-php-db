<?php
declare(strict_types=1);

use CTG\Test\CTGTest;
use CTG\Test\CTGTestState;
use CTG\Test\Predicates\CTGTestPredicates;
use CTG\DB\CTGDBConn;
use CTG\DB\CTGDBError;

// Tests for CTGDBConn — connection construction, transactions, and invalidation

$dbHost = getenv('DB_HOST') ?: 'db';
$dbName = getenv('DB_NAME') ?: 'ctg_staging';
$dbUser = getenv('DB_USER') ?: 'ctg_dev';
$dbPass = getenv('DB_PASSWORD') ?: 'devpass_change_me';
$connectionConfig = fn(array $config = []): array => array_replace([
    'host' => $dbHost,
    'database' => $dbName,
    'username' => $dbUser,
    'password' => $dbPass,
], $config);

return [

    // ── Construction ────────────────────────────────────────────────

    CTGTest::init('connection — static init factory')
        ->stage('connect', fn(CTGTestState $s) => CTGDBConn::init($connectionConfig()))
        ->assert('returns CTGDBConn instance', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::isInstanceOf(CTGDBConn::class))
        ->assert('starts valid', fn(CTGTestState $s) => $s->getSubject()->isInvalidated(), CTGTestPredicates::isFalse())
        ->assert('defaults to nonpersistent', fn(CTGTestState $s) => $s->getSubject()->isPersistent(), CTGTestPredicates::isFalse()),

    CTGTest::init('connection — constructor')
        ->stage('connect', fn(CTGTestState $s) => new CTGDBConn($connectionConfig()))
        ->assert('returns CTGDBConn instance', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::isInstanceOf(CTGDBConn::class)),

    CTGTest::init('connection — optional configuration defaults are explicit')
        ->stage('read', fn(CTGTestState $s) => CTGDBConn::DEFAULT_OPTIONS)
        ->assert('charset defaults to utf8mb4', fn(CTGTestState $s) => $s->getSubject()['charset'], CTGTestPredicates::equals('utf8mb4'))
        ->assert('timeout defaults to null', fn(CTGTestState $s) => $s->getSubject()['timeout'], CTGTestPredicates::isNull())
        ->assert('persistent defaults to false', fn(CTGTestState $s) => $s->getSubject()['persistent'], CTGTestPredicates::isFalse()),

    CTGTest::init('connection — explicit PDO options are accepted')
        ->stage('connect', fn(CTGTestState $s) => CTGDBConn::init($connectionConfig([
            'charset' => 'utf8mb4',
            'timeout' => 5,
            'persistent' => true,
        ])))
        ->assert('returns CTGDBConn instance', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::isInstanceOf(CTGDBConn::class))
        ->assert('reports persistent option', fn(CTGTestState $s) => $s->getSubject()->isPersistent(), CTGTestPredicates::isTrue()),

    CTGTest::init('connection — execution boundary does not expose PDO')
        ->stage('execute', function(CTGTestState $s) use ($connectionConfig) {
            $connection = CTGDBConn::init($connectionConfig());
            return [
                'rows' => $connection->query(
                    'SELECT make FROM guitars WHERE id = ?',
                    [['type' => 'int', 'value' => 1]]
                ),
                'typed_null' => $connection->query(
                    'SELECT ? IS NULL AS is_null',
                    [['type' => 'null', 'value' => null]]
                ),
                'processed' => $connection->process(
                    'SELECT make FROM guitars ORDER BY id',
                    fn($row, $result) => [...$result, $row['make']],
                    []
                ),
                'raw_pdo_callable' => is_callable([$connection, 'getPDO']),
                'raw_statement_callable' => is_callable([$connection, 'getStatement']) || is_callable([$connection, 'statement']),
            ];
        })
        ->assert('executes a typed prepared statement', fn(CTGTestState $s) => $s->getSubject()['rows'][0]['make'], CTGTestPredicates::equals('Ibanez'))
        ->assert('binds an explicitly typed null', fn(CTGTestState $s) => (int)$s->getSubject()['typed_null'][0]['is_null'], CTGTestPredicates::equals(1))
        ->assert('processes rows incrementally', fn(CTGTestState $s) => $s->getSubject()['processed'][0], CTGTestPredicates::equals('Ibanez'))
        ->assert('raw PDO accessor is not callable', fn(CTGTestState $s) => $s->getSubject()['raw_pdo_callable'], CTGTestPredicates::isFalse())
        ->assert('statement accessor is not callable', fn(CTGTestState $s) => $s->getSubject()['raw_statement_callable'], CTGTestPredicates::isFalse()),

    CTGTest::init('connection — processor failure closes cursor and preserves connection')
        ->stage('exercise', function(CTGTestState $s) use ($connectionConfig) {
            $connection = CTGDBConn::init($connectionConfig());
            try {
                $connection->process(
                    'SELECT id FROM guitars ORDER BY id',
                    fn($row, $result) => throw new \RuntimeException('processor failed'),
                    null
                );
                $message = 'no exception';
            } catch (\RuntimeException $error) {
                $message = $error->getMessage();
            }
            return [
                'message' => $message,
                'follow_up' => $connection->query('SELECT 1 AS available'),
                'invalidated' => $connection->isInvalidated(),
            ];
        })
        ->assert('preserves processor exception', fn(CTGTestState $s) => $s->getSubject()['message'], CTGTestPredicates::equals('processor failed'))
        ->assert('permits a subsequent query', fn(CTGTestState $s) => (int)$s->getSubject()['follow_up'][0]['available'], CTGTestPredicates::equals(1))
        ->assert('connection remains valid', fn(CTGTestState $s) => $s->getSubject()['invalidated'], CTGTestPredicates::isFalse()),

    CTGTest::init('connection — query rejects non-row statements')
        ->stage('attempt', function(CTGTestState $s) use ($connectionConfig) {
            try {
                CTGDBConn::init($connectionConfig())->query('DO 1');
                return 'no exception';
            } catch (CTGDBError $error) {
                return $error->type;
            }
        })
        ->assert('throws INVALID_QUERY_STATE', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::equals('INVALID_QUERY_STATE')),

    CTGTest::init('connection — execute rejects row-producing statements')
        ->stage('attempt', function(CTGTestState $s) use ($connectionConfig) {
            try {
                CTGDBConn::init($connectionConfig())->execute('SELECT 1');
                return 'no exception';
            } catch (CTGDBError $error) {
                return $error->type;
            }
        })
        ->assert('throws INVALID_QUERY_STATE', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::equals('INVALID_QUERY_STATE')),

    CTGTest::init('connection — process rejects non-row statements')
        ->stage('attempt', function(CTGTestState $s) use ($connectionConfig) {
            try {
                CTGDBConn::init($connectionConfig())->process('DO 1', fn($row, $state) => $state, null);
                return 'no exception';
            } catch (CTGDBError $error) {
                return $error->type;
            }
        })
        ->assert('throws INVALID_QUERY_STATE', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::equals('INVALID_QUERY_STATE')),

    CTGTest::init('connection — bad credentials map to CTGDBError')
        ->stage('attempt', function(CTGTestState $s) use ($connectionConfig) {
            try {
                CTGDBConn::init($connectionConfig(['username' => 'bad_user', 'password' => 'bad_pass']));
                return 'no exception';
            } catch (CTGDBError $error) {
                return $error->type;
            }
        })
        ->assert(
            'throws connection error',
            fn(CTGTestState $s) => $s->getSubject(),
            CTGTestPredicates::satisfies(fn($value) => in_array($value, ['CONNECTION_FAILED', 'AUTH_FAILED'], true))
        ),

    CTGTest::init('connection — missing required configuration is rejected')
        ->stage('attempt', function(CTGTestState $s) use ($connectionConfig) {
            $config = $connectionConfig();
            unset($config['database']);
            try {
                CTGDBConn::init($config);
                return 'no exception';
            } catch (CTGDBError $error) {
                return ['type' => $error->type, 'fields' => $error->data['missing_fields'] ?? []];
            }
        })
        ->assert('throws INVALID_ARGUMENT', fn(CTGTestState $s) => $s->getSubject()['type'], CTGTestPredicates::equals('INVALID_ARGUMENT'))
        ->assert('identifies database field', fn(CTGTestState $s) => $s->getSubject()['fields'], CTGTestPredicates::equals(['database'])),

    CTGTest::init('connection — unknown configuration is rejected')
        ->stage('attempt', function(CTGTestState $s) use ($connectionConfig) {
            try {
                CTGDBConn::init($connectionConfig(['unknown' => true]));
                return 'no exception';
            } catch (CTGDBError $error) {
                return ['type' => $error->type, 'fields' => $error->data['unknown_fields'] ?? []];
            }
        })
        ->assert('throws INVALID_ARGUMENT', fn(CTGTestState $s) => $s->getSubject()['type'], CTGTestPredicates::equals('INVALID_ARGUMENT'))
        ->assert('identifies unknown field', fn(CTGTestState $s) => $s->getSubject()['fields'], CTGTestPredicates::equals(['unknown'])),

    // ── Transactions ────────────────────────────────────────────────

    CTGTest::init('connection — transaction rollback lifecycle')
        ->stage('connect', fn(CTGTestState $s) => CTGDBConn::init($connectionConfig()))
        ->stage('exercise', function(CTGTestState $s) {
            $connection = $s->getSubject();
            $before = $connection->inTransaction();
            $connection->beginTransaction();
            $during = $connection->inTransaction();
            $connection->rollBack();
            return [
                'before' => $before,
                'during' => $during,
                'after' => $connection->inTransaction(),
                'valid' => !$connection->isInvalidated(),
            ];
        })
        ->assert('starts outside a transaction', fn(CTGTestState $s) => $s->getSubject()['before'], CTGTestPredicates::isFalse())
        ->assert('reports active transaction', fn(CTGTestState $s) => $s->getSubject()['during'], CTGTestPredicates::isTrue())
        ->assert('rollback ends transaction', fn(CTGTestState $s) => $s->getSubject()['after'], CTGTestPredicates::isFalse())
        ->assert('connection remains valid', fn(CTGTestState $s) => $s->getSubject()['valid'], CTGTestPredicates::isTrue()),

    CTGTest::init('connection — transaction commit lifecycle')
        ->stage('connect', fn(CTGTestState $s) => CTGDBConn::init($connectionConfig()))
        ->stage('exercise', function(CTGTestState $s) {
            $connection = $s->getSubject();
            $connection->beginTransaction();
            $connection->commit();
            return [
                'active' => $connection->inTransaction(),
                'valid' => !$connection->isInvalidated(),
            ];
        })
        ->assert('commit ends transaction', fn(CTGTestState $s) => $s->getSubject()['active'], CTGTestPredicates::isFalse())
        ->assert('connection remains valid', fn(CTGTestState $s) => $s->getSubject()['valid'], CTGTestPredicates::isTrue()),

    CTGTest::init('connection — nested transaction is rejected without poisoning connection')
        ->stage('connect', fn(CTGTestState $s) => CTGDBConn::init($connectionConfig()))
        ->stage('exercise', function(CTGTestState $s) {
            $connection = $s->getSubject();
            $connection->beginTransaction();
            try {
                $connection->beginTransaction();
                $type = 'no exception';
            } catch (CTGDBError $error) {
                $type = $error->type;
            }
            $active = $connection->inTransaction();
            $connection->rollBack();
            return [
                'type' => $type,
                'active' => $active,
                'valid' => !$connection->isInvalidated(),
            ];
        })
        ->assert('throws INVALID_QUERY_STATE', fn(CTGTestState $s) => $s->getSubject()['type'], CTGTestPredicates::equals('INVALID_QUERY_STATE'))
        ->assert('original transaction remains active', fn(CTGTestState $s) => $s->getSubject()['active'], CTGTestPredicates::isTrue())
        ->assert('connection remains valid', fn(CTGTestState $s) => $s->getSubject()['valid'], CTGTestPredicates::isTrue()),

    CTGTest::init('connection — low-level transaction primitives are explicit')
        ->stage('inspect', fn(CTGTestState $s) => CTGDBConn::init($connectionConfig()))
        ->assert('beginTransaction is callable', fn(CTGTestState $s) => is_callable([$s->getSubject(), 'beginTransaction']), CTGTestPredicates::isTrue())
        ->assert('commit is callable', fn(CTGTestState $s) => is_callable([$s->getSubject(), 'commit']), CTGTestPredicates::isTrue())
        ->assert('rollBack is callable', fn(CTGTestState $s) => is_callable([$s->getSubject(), 'rollBack']), CTGTestPredicates::isTrue())
        ->assert('inTransaction is callable', fn(CTGTestState $s) => is_callable([$s->getSubject(), 'inTransaction']), CTGTestPredicates::isTrue()),

    CTGTest::init('connection — commit without transaction is rejected')
        ->stage('attempt', function(CTGTestState $s) use ($connectionConfig) {
            $connection = CTGDBConn::init($connectionConfig());
            try {
                $connection->commit();
                return ['type' => 'no exception', 'valid' => true];
            } catch (CTGDBError $error) {
                return ['type' => $error->type, 'valid' => !$connection->isInvalidated()];
            }
        })
        ->assert('throws INVALID_QUERY_STATE', fn(CTGTestState $s) => $s->getSubject()['type'], CTGTestPredicates::equals('INVALID_QUERY_STATE'))
        ->assert('connection remains valid', fn(CTGTestState $s) => $s->getSubject()['valid'], CTGTestPredicates::isTrue()),

    CTGTest::init('connection — rollback without transaction is rejected')
        ->stage('attempt', function(CTGTestState $s) use ($connectionConfig) {
            $connection = CTGDBConn::init($connectionConfig());
            try {
                $connection->rollBack();
                return ['type' => 'no exception', 'valid' => true];
            } catch (CTGDBError $error) {
                return ['type' => $error->type, 'valid' => !$connection->isInvalidated()];
            }
        })
        ->assert('throws INVALID_QUERY_STATE', fn(CTGTestState $s) => $s->getSubject()['type'], CTGTestPredicates::equals('INVALID_QUERY_STATE'))
        ->assert('connection remains valid', fn(CTGTestState $s) => $s->getSubject()['valid'], CTGTestPredicates::isTrue()),

    CTGTest::init('connection — begin failure invalidates connection')
        ->stage('exercise', function(CTGTestState $s) use ($connectionConfig) {
            $connection = CTGDBConn::init($connectionConfig());
            $killer = CTGDBConn::init($connectionConfig());
            $connectionId = (int)$connection->query('SELECT CONNECTION_ID() AS id')[0]['id'];
            $killer->execute("KILL CONNECTION {$connectionId}");
            try {
                $connection->beginTransaction();
                return ['type' => 'no exception', 'invalidated' => false];
            } catch (CTGDBError $error) {
                return [
                    'type' => $error->type,
                    'invalidated' => $connection->isInvalidated(),
                ];
            }
        })
        ->assert('throws database failure', fn(CTGTestState $s) => in_array($s->getSubject()['type'], ['QUERY_FAILED', 'CONNECTION_FAILED'], true), CTGTestPredicates::isTrue())
        ->assert('connection is invalidated', fn(CTGTestState $s) => $s->getSubject()['invalidated'], CTGTestPredicates::isTrue()),

    CTGTest::init('connection — commit failure invalidates connection')
        ->stage('exercise', function(CTGTestState $s) use ($connectionConfig) {
            $connection = CTGDBConn::init($connectionConfig());
            $killer = CTGDBConn::init($connectionConfig());
            $connection->beginTransaction();
            $connectionId = (int)$connection->query('SELECT CONNECTION_ID() AS id')[0]['id'];
            $killer->execute("KILL CONNECTION {$connectionId}");
            try {
                $connection->commit();
                return ['type' => 'no exception', 'invalidated' => false];
            } catch (CTGDBError $error) {
                return [
                    'type' => $error->type,
                    'invalidated' => $connection->isInvalidated(),
                ];
            }
        })
        ->assert('throws database failure', fn(CTGTestState $s) => in_array($s->getSubject()['type'], ['QUERY_FAILED', 'CONNECTION_FAILED'], true), CTGTestPredicates::isTrue())
        ->assert('connection is invalidated', fn(CTGTestState $s) => $s->getSubject()['invalidated'], CTGTestPredicates::isTrue()),

    CTGTest::init('connection — rollback failure invalidates connection')
        ->stage('exercise', function(CTGTestState $s) use ($connectionConfig) {
            $connection = CTGDBConn::init($connectionConfig());
            $killer = CTGDBConn::init($connectionConfig());
            $connection->beginTransaction();
            $connectionId = (int)$connection->query('SELECT CONNECTION_ID() AS id')[0]['id'];
            $killer->execute("KILL CONNECTION {$connectionId}");
            try {
                $connection->rollBack();
                return ['type' => 'no exception', 'invalidated' => false];
            } catch (CTGDBError $error) {
                return [
                    'type' => $error->type,
                    'invalidated' => $connection->isInvalidated(),
                ];
            }
        })
        ->assert('throws database failure', fn(CTGTestState $s) => in_array($s->getSubject()['type'], ['QUERY_FAILED', 'CONNECTION_FAILED'], true), CTGTestPredicates::isTrue())
        ->assert('connection is invalidated', fn(CTGTestState $s) => $s->getSubject()['invalidated'], CTGTestPredicates::isTrue()),

    // ── Invalidation ────────────────────────────────────────────────

    CTGTest::init('connection — invalidation is permanent and idempotent')
        ->stage('connect', fn(CTGTestState $s) => CTGDBConn::init($connectionConfig()))
        ->stage('invalidate', function(CTGTestState $s) {
            $connection = $s->getSubject();
            $connection->invalidate();
            $connection->invalidate();
            try {
                $connection->query('SELECT 1');
                $type = 'no exception';
            } catch (CTGDBError $error) {
                $type = $error->type;
            }
            return [
                'invalidated' => $connection->isInvalidated(),
                'type' => $type,
            ];
        })
        ->assert('reports invalidated', fn(CTGTestState $s) => $s->getSubject()['invalidated'], CTGTestPredicates::isTrue())
        ->assert('later access fails closed', fn(CTGTestState $s) => $s->getSubject()['type'], CTGTestPredicates::equals('CONNECTION_FAILED')),
];
