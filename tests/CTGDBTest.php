<?php
declare(strict_types=1);

use CTG\Test\CTGTest;
use CTG\Test\CTGTestState;
use CTG\Test\Predicates\CTGTestPredicates;
use CTG\DB\CTGDB;
use CTG\DB\CTGDBConn;
use CTG\DB\CTGDBError;
use CTG\DB\CTGDBQuery;

// Tests for CTGDB — connection composition, run, CRUD, read via CTGDBQuery, join, paginate
// Requires a running MariaDB with guitars/pickups test data

$dbHost = getenv('DB_HOST') ?: 'db';
$dbName = getenv('DB_NAME') ?: 'ctg_staging';
$dbUser = getenv('DB_USER') ?: 'ctg_dev';
$dbPass = getenv('DB_PASSWORD') ?: 'devpass_change_me';
$dbConfig = fn(array $config = []): array => array_replace([
    'host' => $dbHost,
    'database' => $dbName,
    'username' => $dbUser,
    'password' => $dbPass,
], $config);

return [

    // ── Connection ──────────────────────────────────────────────────

    CTGTest::init('init — static factory')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->assert('returns CTGDB instance', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::isInstanceOf(CTGDB::class))
        ->assert('is not a connection subtype', fn(CTGTestState $s) => $s->getSubject() instanceof CTGDBConn, CTGTestPredicates::isFalse())
        ->stage('execute', fn(CTGTestState $s) => $s->getSubject()->run('SELECT 1 AS connected'))
        ->assert('internally initialized connection executes queries', fn(CTGTestState $s) => (int)$s->getSubject()[0]['connected'], CTGTestPredicates::equals(1)),

    CTGTest::init('connection — constructor retains supplied connection')
        ->stage('exercise', function(CTGTestState $s) use ($dbConfig) {
            $connection = CTGDBConn::init($dbConfig());
            $db = new CTGDB($connection);
            $connection->invalidate();
            try {
                $db->run('SELECT 1');
                $rejected = false;
            } catch (CTGDBError $error) {
                $rejected = $error->type === 'CONNECTION_FAILED';
            }
            return ['db' => $db, 'rejected' => $rejected];
        })
        ->assert('returns CTGDB instance', fn(CTGTestState $s) => $s->getSubject()['db'], CTGTestPredicates::isInstanceOf(CTGDB::class))
        ->assert('uses supplied connection instance', fn(CTGTestState $s) => $s->getSubject()['rejected'], CTGTestPredicates::isTrue()),

    CTGTest::init('connection — explicit lifecycle remains on supplied connection')
        ->stage('exercise', function(CTGTestState $s) use ($dbConfig) {
            $connection = CTGDBConn::init($dbConfig());
            $db = new CTGDB($connection);
            $connection->beginTransaction();
            $activeBeforeQuery = $connection->inTransaction();
            $queryResult = $db->run('SELECT 1 AS inside_transaction');
            $connection->rollBack();
            return [
                'active_before_query' => $activeBeforeQuery,
                'query_result' => (int)$queryResult[0]['inside_transaction'],
                'active_after_rollback' => $connection->inTransaction(),
            ];
        })
        ->assert('connection owns transaction state', fn(CTGTestState $s) => $s->getSubject()['active_before_query'], CTGTestPredicates::isTrue())
        ->assert('CTGDB executes within supplied transaction', fn(CTGTestState $s) => $s->getSubject()['query_result'], CTGTestPredicates::equals(1))
        ->assert('connection rollback ends transaction', fn(CTGTestState $s) => $s->getSubject()['active_after_rollback'], CTGTestPredicates::isFalse()),

    CTGTest::init('init — bad credentials throws CTGDBError')
        ->stage('attempt', function(CTGTestState $s) use ($dbConfig) {
            try {
                CTGDB::init($dbConfig(['username' => 'bad_user', 'password' => 'bad_pass']));
                return 'no exception';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('throws connection error', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::satisfies(fn($v) => in_array($v, ['CONNECTION_FAILED', 'AUTH_FAILED'], true))),

    CTGTest::init('connection invalidation closes and permanently poisons the object')
        ->stage('invalidate', function(CTGTestState $s) use ($dbConfig) {
            $connection = CTGDBConn::init($dbConfig());
            $db = new CTGDB($connection);
            $connection->invalidate();
            try {
                $db->run('SELECT 1');
                $rejected = false;
            } catch (CTGDBError $error) {
                $rejected = $error->type === 'CONNECTION_FAILED';
            }
            return [
                'invalidated' => $connection->isInvalidated(),
                'persistent' => $connection->isPersistent(),
                'rejected' => $rejected,
            ];
        })
        ->assert('reports invalidated', fn(CTGTestState $s) => $s->getSubject()['invalidated'], CTGTestPredicates::isTrue())
        ->assert('default connection is nonpersistent', fn(CTGTestState $s) => $s->getSubject()['persistent'], CTGTestPredicates::isFalse())
        ->assert('later queries fail closed', fn(CTGTestState $s) => $s->getSubject()['rejected'], CTGTestPredicates::isTrue()),

    CTGTest::init('lost connection invalidates CTGDBConn and later queries fail closed')
        ->stage('exercise', function(CTGTestState $s) use ($dbConfig) {
            $victimConnection = CTGDBConn::init($dbConfig());
            $victim = new CTGDB($victimConnection);
            $killer = CTGDB::init($dbConfig());
            $connectionId = (int)$victim->run('SELECT CONNECTION_ID() AS connection_id')[0]['connection_id'];
            $killer->run("KILL CONNECTION {$connectionId}");
            try {
                $victim->run('SELECT 1');
                $type = 'no exception';
                $invalidatedInError = false;
            } catch (CTGDBError $error) {
                $type = $error->type;
                $invalidatedInError = ($error->data['connection_invalidated'] ?? false) === true;
            }
            try {
                $victim->run('SELECT 1');
                $retryRejected = false;
            } catch (CTGDBError $error) {
                $retryRejected = $error->type === 'CONNECTION_FAILED';
            }
            return [
                'type' => $type,
                'invalidated' => $victimConnection->isInvalidated(),
                'invalidated_in_error' => $invalidatedInError,
                'retry_rejected' => $retryRejected,
            ];
        })
        ->assert('throws CONNECTION_FAILED', fn(CTGTestState $s) => $s->getSubject()['type'], CTGTestPredicates::equals('CONNECTION_FAILED'))
        ->assert('reports invalidated', fn(CTGTestState $s) => $s->getSubject()['invalidated'], CTGTestPredicates::isTrue())
        ->assert('error reports invalidation', fn(CTGTestState $s) => $s->getSubject()['invalidated_in_error'], CTGTestPredicates::isTrue())
        ->assert('retry is rejected', fn(CTGTestState $s) => $s->getSubject()['retry_rejected'], CTGTestPredicates::isTrue()),

    // ── run() — raw queries ─────────────────────────────────────────

    CTGTest::init('run — plain SQL string')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('execute', fn(CTGTestState $s) => $s->getSubject()->run('SELECT * FROM guitars'))
        ->assert('returns array', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::isType('array'))
        ->assert('returned rows', fn(CTGTestState $s) => count($s->getSubject()), CTGTestPredicates::greaterThan(0))
        ->assert('first has make', fn(CTGTestState $s) => isset($s->getSubject()[0]['make']), CTGTestPredicates::isTrue()),

    CTGTest::init('run — parameterized positional')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('execute', fn(CTGTestState $s) => $s->getSubject()->run(
            'SELECT * FROM guitars WHERE make = ?',
            [['type' => 'str', 'value' => 'Fender']]
        ))
        ->assert('returns 3 Fenders', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::hasCount(3))
        ->assert('all are Fender', fn(CTGTestState $s) => $s->getSubject()[0]['make'], CTGTestPredicates::equals('Fender')),

    CTGTest::init('run — parameterized named')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('execute', fn(CTGTestState $s) => $s->getSubject()->run(
            'SELECT * FROM guitars WHERE make = :make AND year_purchased > :year',
            [
                'make' => ['type' => 'str', 'value' => 'Fender'],
                'year' => ['type' => 'int', 'value' => 2010]
            ]
        ))
        ->assert('returns post-2010 Fenders', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::hasCount(2)),

    CTGTest::init('run — untyped convenience values')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('execute', fn(CTGTestState $s) => $s->getSubject()->run('SELECT * FROM guitars WHERE id = ?', [1]))
        ->assert('returns 1 row', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::hasCount(1))
        ->assert('is Ibanez', fn(CTGTestState $s) => $s->getSubject()[0]['make'], CTGTestPredicates::equals('Ibanez')),

    // ── process() — incremental result handling ────────────────────

    CTGTest::init('process — extracts column')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('execute', fn(CTGTestState $s) => $s->getSubject()->process(
            'SELECT make FROM guitars ORDER BY id',
            fn($record, $result) => [...$result, $record['make']],
            []
        ))
        ->assert('first is Ibanez', fn(CTGTestState $s) => $s->getSubject()[0], CTGTestPredicates::equals('Ibanez'))
        ->assert('accumulated rows', fn(CTGTestState $s) => count($s->getSubject()), CTGTestPredicates::greaterThan(0)),

    CTGTest::init('process — keys rows by id')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('execute', fn(CTGTestState $s) => $s->getSubject()->process(
            'SELECT * FROM guitars',
            fn($record, $result) => $result + [$record['id'] => $record],
            []
        ))
        ->assert('key 1 is Ibanez', fn(CTGTestState $s) => $s->getSubject()[1]['make'], CTGTestPredicates::equals('Ibanez'))
        ->assert('key 5 is Gibson', fn(CTGTestState $s) => $s->getSubject()[5]['make'], CTGTestPredicates::equals('Gibson')),

    CTGTest::init('process — counts rows')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('process and baseline', fn(CTGTestState $s) => [
            'processed' => $s->getSubject()->process(
                'SELECT * FROM guitars',
                fn($record, $count) => $count + 1,
                0
            ),
            'baseline' => count($s->getSubject()->run('SELECT * FROM guitars')),
        ])
        ->assert('processed count equals row count', fn(CTGTestState $s) => $s->getSubject()['processed'] === $s->getSubject()['baseline'], CTGTestPredicates::isTrue())
        ->assert('processed count is positive', fn(CTGTestState $s) => $s->getSubject()['processed'], CTGTestPredicates::greaterThan(0)),

    // ── create() ────────────────────────────────────────────────────

    CTGTest::init('create — typed values')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('insert', fn(CTGTestState $s) => [
            'db' => $s->getSubject(),
            'id' => $s->getSubject()->create('guitars', [
                'make' => ['type' => 'str', 'value' => 'PRS'],
                'model' => ['type' => 'str', 'value' => 'Custom 24'],
                'color' => ['type' => 'str', 'value' => 'Violet'],
                'year_purchased' => ['type' => 'int', 'value' => 2025]
            ])
        ])
        ->assert('returns insert id', fn(CTGTestState $s) => is_numeric($s->getSubject()['id']), CTGTestPredicates::isTrue())
        ->assert('id is positive', fn(CTGTestState $s) => (int)$s->getSubject()['id'], CTGTestPredicates::greaterThan(0))
        ->stage('verify', fn(CTGTestState $s) => $s->getSubject()['db']->run(
            'SELECT * FROM guitars WHERE id = ?',
            [['type' => 'int', 'value' => (int)$s->getSubject()['id']]]
        ))
        ->assert('make is PRS', fn(CTGTestState $s) => $s->getSubject()[0]['make'], CTGTestPredicates::equals('PRS'))
        ->assert('model is Custom 24', fn(CTGTestState $s) => $s->getSubject()[0]['model'], CTGTestPredicates::equals('Custom 24'))
        ->stage('cleanup', fn(CTGTestState $s) => CTGDB::init($dbConfig())
            ->run("DELETE FROM guitars WHERE make = 'PRS'")),

    CTGTest::init('create — untyped values')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('insert', fn(CTGTestState $s) => [
            'db' => $s->getSubject(),
            'id' => $s->getSubject()->create('guitars', [
                'make' => 'Jackson',
                'model' => 'Soloist',
                'color' => 'White',
                'year_purchased' => 2025
            ])
        ])
        ->assert('returns insert id', fn(CTGTestState $s) => is_numeric($s->getSubject()['id']), CTGTestPredicates::isTrue())
        ->stage('cleanup', fn(CTGTestState $s) => $s->getSubject()['db']->run("DELETE FROM guitars WHERE make = 'Jackson'")),

    // ── read() via CTGDBQuery — single table ────────────────────────

    CTGTest::init('read via CTGDBQuery — all rows')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('execute', fn(CTGTestState $s) => $s->getSubject()->read(CTGDBQuery::from('guitars')))
        ->assert('returned rows', fn(CTGTestState $s) => count($s->getSubject()), CTGTestPredicates::greaterThan(0)),

    CTGTest::init('read via CTGDBQuery — with columns')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('execute', fn(CTGTestState $s) => $s->getSubject()->read(
            CTGDBQuery::from('guitars')->columns('make', 'model')
        ))
        ->assert('has make', fn(CTGTestState $s) => isset($s->getSubject()[0]['make']), CTGTestPredicates::isTrue())
        ->assert('has model', fn(CTGTestState $s) => isset($s->getSubject()[0]['model']), CTGTestPredicates::isTrue())
        ->assert('no color', fn(CTGTestState $s) => isset($s->getSubject()[0]['color']), CTGTestPredicates::isFalse()),

    CTGTest::init('read via CTGDBQuery — with where')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('execute', fn(CTGTestState $s) => $s->getSubject()->read(
            CTGDBQuery::from('guitars')->where('make', '=', 'Fender', 'str')
        ))
        ->assert('returns 3 Fenders', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::hasCount(3)),

    CTGTest::init('read via CTGDBQuery — with order')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('execute', fn(CTGTestState $s) => $s->getSubject()->read(
            CTGDBQuery::from('guitars')->orderBy('year_purchased', 'DESC')
        ))
        ->assert('most recent first', fn(CTGTestState $s) => $s->getSubject()[0]['make'], CTGTestPredicates::equals('Schecter')),

    CTGTest::init('read via CTGDBQuery — with limit')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('execute', fn(CTGTestState $s) => $s->getSubject()->read(
            CTGDBQuery::from('guitars')->limit(3)
        ))
        ->assert('returns 3 rows', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::hasCount(3)),

    CTGTest::init('process CTGDBQuery — transforms rows')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('execute', fn(CTGTestState $s) => $s->getSubject()->process(
            CTGDBQuery::from('guitars')
                ->columns('make')
                ->where('make', '=', 'Fender', 'str'),
            fn($record, $result) => [...$result, $record['make']],
            []
        ))
        ->assert('returns array of makes', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::equals(['Fender', 'Fender', 'Fender'])),

    // ── read() via CTGDBQuery — multi-table join ────────────────────

    CTGTest::init('read via CTGDBQuery — inner join two tables')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('execute', fn(CTGTestState $s) => $s->getSubject()->read(
            CTGDBQuery::from('guitars')
                ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id'])
                ->columns('guitars.model', 'pickups.position', 'pickups.make as pickup_make')
        ))
        ->assert('returns rows', fn(CTGTestState $s) => count($s->getSubject()), CTGTestPredicates::greaterThan(0))
        ->assert('has model', fn(CTGTestState $s) => isset($s->getSubject()[0]['model']), CTGTestPredicates::isTrue())
        ->assert('has position', fn(CTGTestState $s) => isset($s->getSubject()[0]['position']), CTGTestPredicates::isTrue())
        ->assert('has pickup_make', fn(CTGTestState $s) => isset($s->getSubject()[0]['pickup_make']), CTGTestPredicates::isTrue()),

    CTGTest::init('read via CTGDBQuery — inner join with where')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('execute', fn(CTGTestState $s) => $s->getSubject()->read(
            CTGDBQuery::from('guitars')
                ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id'])
                ->columns('guitars.model', 'pickups.type')
                ->where('pickups.type', '=', 'active', 'str')
        ))
        ->assert('only active pickups', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::hasCount(2)),

    // ── update() ────────────────────────────────────────────────────

    CTGTest::init('update — single row')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('update', fn(CTGTestState $s) => [
            'db' => $s->getSubject(),
            'affected' => $s->getSubject()->update('guitars',
                ['color' => ['type' => 'str', 'value' => 'TestColor']],
                ['id' => ['type' => 'int', 'value' => 1]]
            )
        ])
        ->assert('affected 1 row', fn(CTGTestState $s) => $s->getSubject()['affected'], CTGTestPredicates::equals(1))
        ->stage('verify', fn(CTGTestState $s) => $s->getSubject()['db']->run(
            'SELECT color FROM guitars WHERE id = ?',
            [['type' => 'int', 'value' => 1]]
        ))
        ->assert('color updated', fn(CTGTestState $s) => $s->getSubject()[0]['color'], CTGTestPredicates::equals('TestColor'))
        ->stage('revert', fn(CTGTestState $s) => CTGDB::init($dbConfig())
            ->update('guitars',
                ['color' => ['type' => 'str', 'value' => 'Black']],
                ['id' => ['type' => 'int', 'value' => 1]]
            )),

    CTGTest::init('update — no matching rows')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('update', fn(CTGTestState $s) => $s->getSubject()->update('guitars',
            ['color' => ['type' => 'str', 'value' => 'Nope']],
            ['id' => ['type' => 'int', 'value' => 99999]]
        ))
        ->assert('affected 0 rows', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::equals(0)),

    CTGTest::init('update — empty where throws')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('attempt', function(CTGTestState $s) {
            try {
                $s->getSubject()->update('guitars',
                    ['color' => ['type' => 'str', 'value' => 'AccidentalBlast']],
                    []
                );
                return 'no exception';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('throws EMPTY_WHERE_UPDATE', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::equals('EMPTY_WHERE_UPDATE')),

    // ── delete() ────────────────────────────────────────────────────

    CTGTest::init('delete — removes row')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('insert', fn(CTGTestState $s) => [
            'db' => $s->getSubject(),
            'id' => $s->getSubject()->create('guitars', [
                'make' => 'DeleteMe',
                'model' => 'Test',
                'color' => 'Red',
                'year_purchased' => 2025
            ])
        ])
        ->stage('delete', fn(CTGTestState $s) => [
            'db' => $s->getSubject()['db'],
            'affected' => $s->getSubject()['db']->delete('guitars', [
                'id' => ['type' => 'int', 'value' => (int)$s->getSubject()['id']]
            ])
        ])
        ->assert('affected 1 row', fn(CTGTestState $s) => $s->getSubject()['affected'], CTGTestPredicates::equals(1))
        ->stage('verify gone', fn(CTGTestState $s) => $s->getSubject()['db']->run(
            'SELECT * FROM guitars WHERE make = ?',
            [['type' => 'str', 'value' => 'DeleteMe']]
        ))
        ->assert('row is gone', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::hasCount(0)),

    CTGTest::init('delete — empty where throws')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('attempt', function(CTGTestState $s) {
            try {
                $s->getSubject()->delete('guitars', []);
                return 'no exception';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('throws EMPTY_WHERE_DELETE', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::equals('EMPTY_WHERE_DELETE')),

    // ── left join via CTGDBQuery ────────────────────────────────────

    CTGTest::init('read via CTGDBQuery — left join includes guitars without pickups')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('insert orphan', fn(CTGTestState $s) => [
            'db' => $s->getSubject(),
            'id' => $s->getSubject()->create('guitars', [
                'make' => 'Orphan',
                'model' => 'NoPickups',
                'color' => 'None',
                'year_purchased' => 2025
            ])
        ])
        ->stage('left join', fn(CTGTestState $s) => [
            'db' => $s->getSubject()['db'],
            'id' => $s->getSubject()['id'],
            'rows' => $s->getSubject()['db']->read(
                CTGDBQuery::from('guitars')
                    ->join('pickups', 'left', ['guitars.id' => 'pickups.guitar_id'])
                    ->columns('guitars.model', 'pickups.position')
                    ->where('guitars.make', '=', 'Orphan', 'str')
            )
        ])
        ->assert('returns orphan', fn(CTGTestState $s) => count($s->getSubject()['rows']), CTGTestPredicates::equals(1))
        ->assert('position is null', fn(CTGTestState $s) => $s->getSubject()['rows'][0]['position'], CTGTestPredicates::isNull())
        ->stage('cleanup', fn(CTGTestState $s) => $s->getSubject()['db']->delete('guitars', [
            'id' => ['type' => 'int', 'value' => (int)$s->getSubject()['id']]
        ])),

    // ── paginate() ──────────────────────────────────────────────────

    CTGTest::init('paginate — table name source')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('execute', fn(CTGTestState $s) => $s->getSubject()->paginate('guitars', [
            'sort' => 'year_purchased',
            'order' => 'DESC',
            'page' => 1,
            'per_page' => 3
        ]))
        ->assert('has data key', fn(CTGTestState $s) => isset($s->getSubject()['data']), CTGTestPredicates::isTrue())
        ->assert('has pagination key', fn(CTGTestState $s) => isset($s->getSubject()['pagination']), CTGTestPredicates::isTrue())
        ->assert('data has 3 rows', fn(CTGTestState $s) => count($s->getSubject()['data']), CTGTestPredicates::equals(3))
        ->assert('page is 1', fn(CTGTestState $s) => $s->getSubject()['pagination']['page'], CTGTestPredicates::equals(1))
        ->assert('per_page is 3', fn(CTGTestState $s) => $s->getSubject()['pagination']['per_page'], CTGTestPredicates::equals(3))
        ->assert('total_rows is 9', fn(CTGTestState $s) => $s->getSubject()['pagination']['total_rows'], CTGTestPredicates::equals(9))
        ->assert('total_pages is 3', fn(CTGTestState $s) => $s->getSubject()['pagination']['total_pages'], CTGTestPredicates::equals(3))
        ->assert('has_previous is false', fn(CTGTestState $s) => $s->getSubject()['pagination']['has_previous'], CTGTestPredicates::isFalse())
        ->assert('has_next is true', fn(CTGTestState $s) => $s->getSubject()['pagination']['has_next'], CTGTestPredicates::isTrue()),

    CTGTest::init('paginate — page 2')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('execute', fn(CTGTestState $s) => $s->getSubject()->paginate('guitars', [
            'sort' => 'id',
            'page' => 2,
            'per_page' => 3
        ]))
        ->assert('data has 3 rows', fn(CTGTestState $s) => count($s->getSubject()['data']), CTGTestPredicates::equals(3))
        ->assert('page is 2', fn(CTGTestState $s) => $s->getSubject()['pagination']['page'], CTGTestPredicates::equals(2))
        ->assert('has_previous is true', fn(CTGTestState $s) => $s->getSubject()['pagination']['has_previous'], CTGTestPredicates::isTrue())
        ->assert('has_next is true', fn(CTGTestState $s) => $s->getSubject()['pagination']['has_next'], CTGTestPredicates::isTrue()),

    CTGTest::init('paginate — last page')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('execute', fn(CTGTestState $s) => $s->getSubject()->paginate('guitars', [
            'sort' => 'id',
            'page' => 3,
            'per_page' => 3
        ]))
        ->assert('data has 3 rows', fn(CTGTestState $s) => count($s->getSubject()['data']), CTGTestPredicates::equals(3))
        ->assert('has_next is false', fn(CTGTestState $s) => $s->getSubject()['pagination']['has_next'], CTGTestPredicates::isFalse()),

    CTGTest::init('paginate — CTGDBQuery source')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('execute', fn(CTGTestState $s) => $s->getSubject()->paginate(
            CTGDBQuery::from('guitars')
                ->where('make', '=', 'Fender', 'str'),
            ['sort' => 'model', 'page' => 1, 'per_page' => 10]
        ))
        ->assert('total_rows is 3', fn(CTGTestState $s) => $s->getSubject()['pagination']['total_rows'], CTGTestPredicates::equals(3))
        ->assert('data has 3 rows', fn(CTGTestState $s) => count($s->getSubject()['data']), CTGTestPredicates::equals(3)),

    CTGTest::init('paginate — config sort overrides query orderBy')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('execute', function(CTGTestState $s) {
            $db = $s->getSubject();
            $query = CTGDBQuery::from('guitars')
                ->orderBy('year_purchased', 'DESC');
            return $db->paginate($query, [
                'sort' => 'make',
                'order' => 'ASC',
                'page' => 1,
                'per_page' => 9
            ]);
        })
        ->assert('sorted by make ASC not year DESC', fn(CTGTestState $s) => $s->getSubject()['data'][0]['make'] <= $s->getSubject()['data'][1]['make'], CTGTestPredicates::isTrue()),

    CTGTest::init('paginate — CTGDBQuery join source')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('paginate', fn(CTGTestState $s) => $s->getSubject()->paginate(
            CTGDBQuery::from('guitars')
                ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id'])
                ->columns('guitars.model', 'pickups.type'),
            ['sort' => 'model', 'page' => 1, 'per_page' => 5]
        ))
        ->assert('has data', fn(CTGTestState $s) => count($s->getSubject()['data']), CTGTestPredicates::greaterThan(0))
        ->assert('has pagination', fn(CTGTestState $s) => $s->getSubject()['pagination']['page'], CTGTestPredicates::equals(1)),

    CTGTest::init('paginate — returns materialized rows')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('execute', fn(CTGTestState $s) => $s->getSubject()->paginate('guitars', [
            'columns' => ['id', 'make'],
            'sort' => 'id',
            'page' => 1,
            'per_page' => 3
        ]))
        ->assert('data contains three rows', fn(CTGTestState $s) => $s->getSubject()['data'], CTGTestPredicates::hasCount(3))
        ->assert('first row has id', fn(CTGTestState $s) => isset($s->getSubject()['data'][0]['id']), CTGTestPredicates::isTrue())
        ->assert('pagination still present', fn(CTGTestState $s) => $s->getSubject()['pagination']['total_rows'], CTGTestPredicates::equals(9)),

    CTGTest::init('paginate — pre-computed total skips count')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('execute', fn(CTGTestState $s) => $s->getSubject()->paginate('guitars', [
            'sort' => 'id',
            'page' => 1,
            'per_page' => 3,
            'total' => 100
        ]))
        ->assert('uses provided total', fn(CTGTestState $s) => $s->getSubject()['pagination']['total_rows'], CTGTestPredicates::equals(100))
        ->assert('total_pages from provided total', fn(CTGTestState $s) => $s->getSubject()['pagination']['total_pages'], CTGTestPredicates::equals(34)),

    // ── Validation ──────────────────────────────────────────────────

    CTGTest::init('validate — bad join type throws via CTGDBQuery')
        ->stage('attempt', function(CTGTestState $s) {
            try {
                CTGDBQuery::from('guitars')
                    ->join('pickups', 'EVIL', ['guitars.id' => 'pickups.guitar_id']);
                return 'no exception';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('throws INVALID_JOIN_TYPE', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::equals('INVALID_JOIN_TYPE')),

    CTGTest::init('validate — bad sort direction throws')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('attempt', function(CTGTestState $s) {
            try {
                $s->getSubject()->paginate('guitars', [
                    'sort' => 'id',
                    'order' => 'SIDEWAYS'
                ]);
                return 'no exception';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('throws INVALID_SORT', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::equals('INVALID_SORT')),

    CTGTest::init('validate — bad identifier throws via CTGDBQuery')
        ->stage('attempt', function(CTGTestState $s) {
            try {
                CTGDBQuery::from('guitars; DROP TABLE guitars;--');
                return 'no exception';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('throws identifier error', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::satisfies(fn($v) => in_array($v, ['INVALID_IDENTIFIER', 'INVALID_TABLE'], true))),

    CTGTest::init('validate — CTGDBQuery rejects separate bound values')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('attempt', function(CTGTestState $s) {
            try {
                $s->getSubject()->run(CTGDBQuery::from('guitars'), [1, 2, 3]);
                return 'no exception';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('throws INVALID_ARGUMENT', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::equals('INVALID_ARGUMENT')),

    CTGTest::init('validate — missing on condition for join via CTGDBQuery')
        ->stage('attempt', function(CTGTestState $s) {
            try {
                CTGDBQuery::from('guitars')
                    ->join('pickups', 'inner', []);
                return 'no exception';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('throws INVALID_ARGUMENT', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::equals('INVALID_ARGUMENT')),

    // ── Legacy raw SQL paths rejected ─────────────────────────────────

    CTGTest::init('validate — string where in read() throws INVALID_ARGUMENT')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('attempt', function(CTGTestState $s) {
            try {
                $s->getSubject()->read('guitars', ['where' => 'make = ?', 'values' => [['type' => 'str', 'value' => 'Fender']]]);
                return 'no exception';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('throws INVALID_ARGUMENT', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::equals('INVALID_ARGUMENT')),

    CTGTest::init('validate — where_raw in join throws INVALID_ARGUMENT')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('attempt', function(CTGTestState $s) {
            try {
                $s->getSubject()->read(['guitars', 'pickups'], [
                    'join' => 'inner',
                    'on' => [['guitars.id' => 'pickups.guitar_id']],
                    'where_raw' => ['where' => 'guitars.make = ?', 'values' => [['type' => 'str', 'value' => 'Fender']]]
                ]);
                return 'no exception';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('throws INVALID_ARGUMENT', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::equals('INVALID_ARGUMENT')),

    CTGTest::init('validate — string where in join throws INVALID_ARGUMENT')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('attempt', function(CTGTestState $s) {
            try {
                $s->getSubject()->read(['guitars', 'pickups'], [
                    'join' => 'inner',
                    'on' => [['guitars.id' => 'pickups.guitar_id']],
                    'where' => 'guitars.make = ?',
                    'values' => [['type' => 'str', 'value' => 'Fender']]
                ]);
                return 'no exception';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('throws INVALID_ARGUMENT', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::equals('INVALID_ARGUMENT')),

    CTGTest::init('validate — raw having in join throws INVALID_ARGUMENT')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('attempt', function(CTGTestState $s) {
            try {
                $s->getSubject()->read(['guitars', 'pickups'], [
                    'join' => 'inner',
                    'on' => [['guitars.id' => 'pickups.guitar_id']],
                    'where' => ['guitars.make' => ['type' => 'str', 'value' => 'Fender']],
                    'group' => 'guitars.id',
                    'having' => 'COUNT(*) > 1'
                ]);
                return 'no exception';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('throws INVALID_ARGUMENT', fn(CTGTestState $s) => $s->getSubject(), CTGTestPredicates::equals('INVALID_ARGUMENT')),

    CTGTest::init('validate — paginate clamps negative page to 1')
        ->stage('connect', fn(CTGTestState $s) => CTGDB::init($dbConfig()))
        ->stage('execute', fn(CTGTestState $s) => $s->getSubject()->paginate('guitars', [
            'sort' => 'id',
            'page' => -1,
            'per_page' => 3
        ]))
        ->assert('page clamped to 1', fn(CTGTestState $s) => $s->getSubject()['pagination']['page'], CTGTestPredicates::equals(1))
        ->assert('returns data', fn(CTGTestState $s) => count($s->getSubject()['data']), CTGTestPredicates::equals(3)),

];
