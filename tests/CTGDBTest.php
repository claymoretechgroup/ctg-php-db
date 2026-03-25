<?php
declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use CTG\Test\CTGTest;
use CTG\DB\CTGDB;
use CTG\DB\CTGDBError;
use CTG\DB\CTGDBQuery;

// Tests for CTGDB — connection, run, CRUD, read via CTGDBQuery, join, paginate
// Requires a running MariaDB with guitars/pickups test data

$config = ['output' => 'console'];

// Connection config from Docker environment
$dbHost = getenv('DB_HOST') ?: 'db';
$dbName = getenv('DB_NAME') ?: 'ctg_staging';
$dbUser = getenv('DB_USER') ?: 'ctg_dev';
$dbPass = getenv('DB_PASSWORD') ?: 'devpass_change_me';

// ── Connection ──────────────────────────────────────────────────

CTGTest::init('connect — static factory')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->assert('returns CTGDB instance', fn($db) => $db instanceof CTGDB, true)
    ->start(null, $config);

CTGTest::init('connect — constructor')
    ->stage('connect', fn($_) => new CTGDB($dbHost, $dbName, $dbUser, $dbPass))
    ->assert('returns CTGDB instance', fn($db) => $db instanceof CTGDB, true)
    ->start(null, $config);

CTGTest::init('connect — bad credentials throws CTGDBError')
    ->stage('attempt', function($_) {
        try {
            CTGDB::connect('db', 'ctg_staging', 'bad_user', 'bad_pass');
            return 'no exception';
        } catch (CTGDBError $e) {
            return $e->type;
        }
    })
    ->assert('throws connection error', fn($r) => in_array($r, ['CONNECTION_FAILED', 'AUTH_FAILED']), true)
    ->start(null, $config);

// ── run() — raw queries ─────────────────────────────────────────

CTGTest::init('run — plain SQL string')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn($db) => $db->run('SELECT * FROM guitars'))
    ->assert('returns array', fn($r) => is_array($r), true)
    ->assert('has 9 guitars', fn($r) => count($r), 9)
    ->assert('first has make', fn($r) => isset($r[0]['make']), true)
    ->start(null, $config);

CTGTest::init('run — parameterized positional')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn($db) => $db->run([
        'sql' => 'SELECT * FROM guitars WHERE make = ?',
        'values' => [['type' => 'str', 'value' => 'Fender']]
    ]))
    ->assert('returns 3 Fenders', fn($r) => count($r), 3)
    ->assert('all are Fender', fn($r) => $r[0]['make'], 'Fender')
    ->start(null, $config);

CTGTest::init('run — parameterized named')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn($db) => $db->run([
        'sql' => 'SELECT * FROM guitars WHERE make = :make AND year_purchased > :year',
        'values' => [
            'make' => ['type' => 'str', 'value' => 'Fender'],
            'year' => ['type' => 'int', 'value' => 2010]
        ]
    ]))
    ->assert('returns post-2010 Fenders', fn($r) => count($r), 2)
    ->start(null, $config);

CTGTest::init('run — untyped convenience values')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn($db) => $db->run([
        'sql' => 'SELECT * FROM guitars WHERE id = ?',
        'values' => [1]
    ]))
    ->assert('returns 1 row', fn($r) => count($r), 1)
    ->assert('is Ibanez', fn($r) => $r[0]['make'], 'Ibanez')
    ->start(null, $config);

// ── run() — fold/accumulator ────────────────────────────────────

CTGTest::init('run — fold extracts column')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn($db) => $db->run(
        'SELECT make FROM guitars ORDER BY id',
        fn($record, $result) => [...$result, $record['make']],
        []
    ))
    ->assert('first is Ibanez', fn($r) => $r[0], 'Ibanez')
    ->assert('returns 9 makes', fn($r) => count($r), 9)
    ->start(null, $config);

CTGTest::init('run — fold keys by id')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn($db) => $db->run(
        'SELECT * FROM guitars',
        fn($record, $result) => $result + [$record['id'] => $record],
        []
    ))
    ->assert('key 1 is Ibanez', fn($r) => $r[1]['make'], 'Ibanez')
    ->assert('key 5 is Gibson', fn($r) => $r[5]['make'], 'Gibson')
    ->start(null, $config);

CTGTest::init('run — fold counts')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn($db) => $db->run(
        'SELECT * FROM guitars',
        fn($record, $count) => $count + 1,
        0
    ))
    ->assert('count is 9', fn($r) => $r, 9)
    ->start(null, $config);

// ── create() ────────────────────────────────────────────────────

CTGTest::init('create — typed values')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('insert', fn($db) => [
        'db' => $db,
        'id' => $db->create('guitars', [
            'make' => ['type' => 'str', 'value' => 'PRS'],
            'model' => ['type' => 'str', 'value' => 'Custom 24'],
            'color' => ['type' => 'str', 'value' => 'Violet'],
            'year_purchased' => ['type' => 'int', 'value' => 2025]
        ])
    ])
    ->assert('returns insert id', fn($r) => is_numeric($r['id']), true)
    ->assert('id is positive', fn($r) => (int)$r['id'] > 0, true)
    ->stage('verify', fn($r) => $r['db']->run([
        'sql' => 'SELECT * FROM guitars WHERE id = ?',
        'values' => [['type' => 'int', 'value' => (int)$r['id']]]
    ]))
    ->assert('make is PRS', fn($r) => $r[0]['make'], 'PRS')
    ->assert('model is Custom 24', fn($r) => $r[0]['model'], 'Custom 24')
    ->stage('cleanup', fn($r) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass)
        ->run("DELETE FROM guitars WHERE make = 'PRS'"))
    ->start(null, $config);

CTGTest::init('create — untyped values')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('insert', fn($db) => [
        'db' => $db,
        'id' => $db->create('guitars', [
            'make' => 'Jackson',
            'model' => 'Soloist',
            'color' => 'White',
            'year_purchased' => 2025
        ])
    ])
    ->assert('returns insert id', fn($r) => is_numeric($r['id']), true)
    ->stage('cleanup', fn($r) => $r['db']->run("DELETE FROM guitars WHERE make = 'Jackson'"))
    ->start(null, $config);

// ── read() via CTGDBQuery — single table ────────────────────────

CTGTest::init('read via CTGDBQuery — all rows')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn($db) => $db->read(CTGDBQuery::from('guitars')))
    ->assert('returns 9 guitars', fn($r) => count($r), 9)
    ->start(null, $config);

CTGTest::init('read via CTGDBQuery — with columns')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn($db) => $db->read(
        CTGDBQuery::from('guitars')->columns('make', 'model')
    ))
    ->assert('has make', fn($r) => isset($r[0]['make']), true)
    ->assert('has model', fn($r) => isset($r[0]['model']), true)
    ->assert('no color', fn($r) => isset($r[0]['color']), false)
    ->start(null, $config);

CTGTest::init('read via CTGDBQuery — with where')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn($db) => $db->read(
        CTGDBQuery::from('guitars')->where('make', '=', 'Fender', 'str')
    ))
    ->assert('returns 3 Fenders', fn($r) => count($r), 3)
    ->start(null, $config);

CTGTest::init('read via CTGDBQuery — with order')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn($db) => $db->read(
        CTGDBQuery::from('guitars')->orderBy('year_purchased', 'DESC')
    ))
    ->assert('most recent first', fn($r) => $r[0]['make'], 'Schecter')
    ->start(null, $config);

CTGTest::init('read via CTGDBQuery — with limit')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn($db) => $db->read(
        CTGDBQuery::from('guitars')->limit(3)
    ))
    ->assert('returns 3 rows', fn($r) => count($r), 3)
    ->start(null, $config);

CTGTest::init('read via CTGDBQuery — with transform')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn($db) => $db->read(
        CTGDBQuery::from('guitars')
            ->columns('make')
            ->where('make', '=', 'Fender', 'str'),
        [],
        fn($record, $acc) => [...$acc, $record['make']]
    ))
    ->assert('returns array of makes', fn($r) => $r, ['Fender', 'Fender', 'Fender'])
    ->start(null, $config);

// ── read() via CTGDBQuery — multi-table join ────────────────────

CTGTest::init('read via CTGDBQuery — inner join two tables')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn($db) => $db->read(
        CTGDBQuery::from('guitars')
            ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id'])
            ->columns('guitars.model', 'pickups.position', 'pickups.make as pickup_make')
    ))
    ->assert('returns rows', fn($r) => count($r) > 0, true)
    ->assert('has model', fn($r) => isset($r[0]['model']), true)
    ->assert('has position', fn($r) => isset($r[0]['position']), true)
    ->assert('has pickup_make', fn($r) => isset($r[0]['pickup_make']), true)
    ->start(null, $config);

CTGTest::init('read via CTGDBQuery — inner join with where')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn($db) => $db->read(
        CTGDBQuery::from('guitars')
            ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id'])
            ->columns('guitars.model', 'pickups.type')
            ->where('pickups.type', '=', 'active', 'str')
    ))
    ->assert('only active pickups', fn($r) => count($r), 2)
    ->start(null, $config);

// ── update() ────────────────────────────────────────────────────

CTGTest::init('update — single row')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('update', fn($db) => [
        'db' => $db,
        'affected' => $db->update('guitars',
            ['color' => ['type' => 'str', 'value' => 'TestColor']],
            ['id' => ['type' => 'int', 'value' => 1]]
        )
    ])
    ->assert('affected 1 row', fn($r) => $r['affected'], 1)
    ->stage('verify', fn($r) => $r['db']->run([
        'sql' => 'SELECT color FROM guitars WHERE id = ?',
        'values' => [['type' => 'int', 'value' => 1]]
    ]))
    ->assert('color updated', fn($r) => $r[0]['color'], 'TestColor')
    ->stage('revert', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass)
        ->update('guitars',
            ['color' => ['type' => 'str', 'value' => 'Black']],
            ['id' => ['type' => 'int', 'value' => 1]]
        ))
    ->start(null, $config);

CTGTest::init('update — no matching rows')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('update', fn($db) => $db->update('guitars',
        ['color' => ['type' => 'str', 'value' => 'Nope']],
        ['id' => ['type' => 'int', 'value' => 99999]]
    ))
    ->assert('affected 0 rows', fn($r) => $r, 0)
    ->start(null, $config);

// ── delete() ────────────────────────────────────────────────────

CTGTest::init('delete — removes row')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('insert', fn($db) => [
        'db' => $db,
        'id' => $db->create('guitars', [
            'make' => 'DeleteMe',
            'model' => 'Test',
            'color' => 'Red',
            'year_purchased' => 2025
        ])
    ])
    ->stage('delete', fn($r) => [
        'db' => $r['db'],
        'affected' => $r['db']->delete('guitars', [
            'id' => ['type' => 'int', 'value' => (int)$r['id']]
        ])
    ])
    ->assert('affected 1 row', fn($r) => $r['affected'], 1)
    ->stage('verify gone', fn($r) => $r['db']->run([
        'sql' => 'SELECT * FROM guitars WHERE make = ?',
        'values' => [['type' => 'str', 'value' => 'DeleteMe']]
    ]))
    ->assert('row is gone', fn($r) => count($r), 0)
    ->start(null, $config);

CTGTest::init('delete — empty where throws')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('attempt', function($db) {
        try {
            $db->delete('guitars', []);
            return 'no exception';
        } catch (CTGDBError $e) {
            return $e->type;
        }
    })
    ->assert('throws EMPTY_WHERE_DELETE', fn($r) => $r, 'EMPTY_WHERE_DELETE')
    ->start(null, $config);

// ── left join via CTGDBQuery ────────────────────────────────────

CTGTest::init('read via CTGDBQuery — left join includes guitars without pickups')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('insert orphan', fn($db) => [
        'db' => $db,
        'id' => $db->create('guitars', [
            'make' => 'Orphan',
            'model' => 'NoPickups',
            'color' => 'None',
            'year_purchased' => 2025
        ])
    ])
    ->stage('left join', fn($ctx) => [
        'db' => $ctx['db'],
        'id' => $ctx['id'],
        'rows' => $ctx['db']->read(
            CTGDBQuery::from('guitars')
                ->join('pickups', 'left', ['guitars.id' => 'pickups.guitar_id'])
                ->columns('guitars.model', 'pickups.position')
                ->where('guitars.make', '=', 'Orphan', 'str')
        )
    ])
    ->assert('returns orphan', fn($r) => count($r['rows']), 1)
    ->assert('position is null', fn($r) => $r['rows'][0]['position'], null)
    ->stage('cleanup', fn($r) => $r['db']->delete('guitars', [
        'id' => ['type' => 'int', 'value' => (int)$r['id']]
    ]))
    ->start(null, $config);

// ── paginate() ──────────────────────────────────────────────────

CTGTest::init('paginate — table name source')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn($db) => $db->paginate('guitars', [
        'sort' => 'year_purchased',
        'order' => 'DESC',
        'page' => 1,
        'per_page' => 3
    ]))
    ->assert('has data key', fn($r) => isset($r['data']), true)
    ->assert('has pagination key', fn($r) => isset($r['pagination']), true)
    ->assert('data has 3 rows', fn($r) => count($r['data']), 3)
    ->assert('page is 1', fn($r) => $r['pagination']['page'], 1)
    ->assert('per_page is 3', fn($r) => $r['pagination']['per_page'], 3)
    ->assert('total_rows is 9', fn($r) => $r['pagination']['total_rows'], 9)
    ->assert('total_pages is 3', fn($r) => $r['pagination']['total_pages'], 3)
    ->assert('has_previous is false', fn($r) => $r['pagination']['has_previous'], false)
    ->assert('has_next is true', fn($r) => $r['pagination']['has_next'], true)
    ->start(null, $config);

CTGTest::init('paginate — page 2')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn($db) => $db->paginate('guitars', [
        'sort' => 'id',
        'page' => 2,
        'per_page' => 3
    ]))
    ->assert('data has 3 rows', fn($r) => count($r['data']), 3)
    ->assert('page is 2', fn($r) => $r['pagination']['page'], 2)
    ->assert('has_previous is true', fn($r) => $r['pagination']['has_previous'], true)
    ->assert('has_next is true', fn($r) => $r['pagination']['has_next'], true)
    ->start(null, $config);

CTGTest::init('paginate — last page')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn($db) => $db->paginate('guitars', [
        'sort' => 'id',
        'page' => 3,
        'per_page' => 3
    ]))
    ->assert('data has 3 rows', fn($r) => count($r['data']), 3)
    ->assert('has_next is false', fn($r) => $r['pagination']['has_next'], false)
    ->start(null, $config);

CTGTest::init('paginate — CTGDBQuery source')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn($db) => $db->paginate(
        CTGDBQuery::from('guitars')
            ->where('make', '=', 'Fender', 'str'),
        ['sort' => 'model', 'page' => 1, 'per_page' => 10]
    ))
    ->assert('total_rows is 3', fn($r) => $r['pagination']['total_rows'], 3)
    ->assert('data has 3 rows', fn($r) => count($r['data']), 3)
    ->start(null, $config);

CTGTest::init('paginate — raw query array source throws INVALID_ARGUMENT')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('attempt', function($db) {
        try {
            $db->paginate([
                'sql' => 'SELECT * FROM guitars',
                'values' => []
            ], ['sort' => 'id', 'page' => 1]);
            return 'no exception';
        } catch (CTGDBError $e) {
            return $e->type;
        }
    })
    ->assert('throws INVALID_ARGUMENT', fn($r) => $r, 'INVALID_ARGUMENT')
    ->start(null, $config);

CTGTest::init('paginate — filter array source throws INVALID_ARGUMENT')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('attempt', function($db) {
        try {
            $db->paginate([
                'table' => 'guitars',
                'where' => 'make = ?',
                'values' => [['type' => 'str', 'value' => 'Fender']]
            ], ['sort' => 'id', 'page' => 1]);
            return 'no exception';
        } catch (CTGDBError $e) {
            return $e->type;
        }
    })
    ->assert('throws INVALID_ARGUMENT', fn($r) => $r, 'INVALID_ARGUMENT')
    ->start(null, $config);

CTGTest::init('paginate — CTGDBQuery join source')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('paginate', fn($db) => $db->paginate(
        CTGDBQuery::from('guitars')
            ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id'])
            ->columns('guitars.model', 'pickups.type'),
        ['sort' => 'model', 'page' => 1, 'per_page' => 5]
    ))
    ->assert('has data', fn($r) => count($r['data']) > 0, true)
    ->assert('has pagination', fn($r) => $r['pagination']['page'], 1)
    ->start(null, $config);

CTGTest::init('paginate — with transform')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn($db) => $db->paginate('guitars', [
        'columns' => ['id', 'make'],
        'sort' => 'id',
        'page' => 1,
        'per_page' => 3
    ], fn($record, $acc) => $acc + [$record['id'] => $record['make']]))
    ->assert('data is keyed by id', fn($r) => isset($r['data'][1]), true)
    ->assert('pagination still present', fn($r) => $r['pagination']['total_rows'], 9)
    ->start(null, $config);

CTGTest::init('paginate — pre-computed total skips count')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn($db) => $db->paginate('guitars', [
        'sort' => 'id',
        'page' => 1,
        'per_page' => 3,
        'total' => 100
    ]))
    ->assert('uses provided total', fn($r) => $r['pagination']['total_rows'], 100)
    ->assert('total_pages from provided total', fn($r) => $r['pagination']['total_pages'], 34)
    ->start(null, $config);

// ── Validation ──────────────────────────────────────────────────

CTGTest::init('validate — bad join type throws via CTGDBQuery')
    ->stage('attempt', function($_) {
        try {
            CTGDBQuery::from('guitars')
                ->join('pickups', 'EVIL', ['guitars.id' => 'pickups.guitar_id']);
            return 'no exception';
        } catch (CTGDBError $e) {
            return $e->type;
        }
    })
    ->assert('throws INVALID_JOIN_TYPE', fn($r) => $r, 'INVALID_JOIN_TYPE')
    ->start(null, $config);

CTGTest::init('validate — bad sort direction throws')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('attempt', function($db) {
        try {
            $db->paginate('guitars', [
                'sort' => 'id',
                'order' => 'SIDEWAYS'
            ]);
            return 'no exception';
        } catch (CTGDBError $e) {
            return $e->type;
        }
    })
    ->assert('throws INVALID_SORT', fn($r) => $r, 'INVALID_SORT')
    ->start(null, $config);

CTGTest::init('validate — bad identifier throws via CTGDBQuery')
    ->stage('attempt', function($_) {
        try {
            CTGDBQuery::from('guitars; DROP TABLE guitars;--');
            return 'no exception';
        } catch (CTGDBError $e) {
            return $e->type;
        }
    })
    ->assert('throws identifier error', fn($r) => in_array($r, ['INVALID_IDENTIFIER', 'INVALID_TABLE']), true)
    ->start(null, $config);

CTGTest::init('validate — run() with missing sql key throws')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('attempt', function($db) {
        try {
            $db->run(['values' => [1, 2, 3]]);
            return 'no exception';
        } catch (CTGDBError $e) {
            return $e->type;
        }
    })
    ->assert('throws INVALID_ARGUMENT', fn($r) => $r, 'INVALID_ARGUMENT')
    ->start(null, $config);

CTGTest::init('validate — missing on condition for join via CTGDBQuery')
    ->stage('attempt', function($_) {
        try {
            CTGDBQuery::from('guitars')
                ->join('pickups', 'inner', []);
            return 'no exception';
        } catch (CTGDBError $e) {
            return $e->type;
        }
    })
    ->assert('throws INVALID_ARGUMENT', fn($r) => $r, 'INVALID_ARGUMENT')
    ->start(null, $config);

// ── Legacy raw SQL paths rejected ─────────────────────────────────

CTGTest::init('validate — string where in read() throws INVALID_ARGUMENT')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('attempt', function($db) {
        try {
            $db->read('guitars', ['where' => 'make = ?', 'values' => [['type' => 'str', 'value' => 'Fender']]]);
            return 'no exception';
        } catch (CTGDBError $e) {
            return $e->type;
        }
    })
    ->assert('throws INVALID_ARGUMENT', fn($r) => $r, 'INVALID_ARGUMENT')
    ->start(null, $config);

CTGTest::init('validate — where_raw in join throws INVALID_ARGUMENT')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('attempt', function($db) {
        try {
            $db->read(['guitars', 'pickups'], [
                'join' => 'inner',
                'on' => [['guitars.id' => 'pickups.guitar_id']],
                'where_raw' => ['where' => 'guitars.make = ?', 'values' => [['type' => 'str', 'value' => 'Fender']]]
            ]);
            return 'no exception';
        } catch (CTGDBError $e) {
            return $e->type;
        }
    })
    ->assert('throws INVALID_ARGUMENT', fn($r) => $r, 'INVALID_ARGUMENT')
    ->start(null, $config);

CTGTest::init('validate — string where in join throws INVALID_ARGUMENT')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('attempt', function($db) {
        try {
            $db->read(['guitars', 'pickups'], [
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
    ->assert('throws INVALID_ARGUMENT', fn($r) => $r, 'INVALID_ARGUMENT')
    ->start(null, $config);

CTGTest::init('validate — raw having in join throws INVALID_ARGUMENT')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('attempt', function($db) {
        try {
            $db->read(['guitars', 'pickups'], [
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
    ->assert('throws INVALID_ARGUMENT', fn($r) => $r, 'INVALID_ARGUMENT')
    ->start(null, $config);

CTGTest::init('validate — paginate clamps negative page to 1')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn($db) => $db->paginate('guitars', [
        'sort' => 'id',
        'page' => -1,
        'per_page' => 3
    ]))
    ->assert('page clamped to 1', fn($r) => $r['pagination']['page'], 1)
    ->assert('returns data', fn($r) => count($r['data']), 3)
    ->start(null, $config);
