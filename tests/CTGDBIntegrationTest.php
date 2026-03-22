<?php
declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use CTG\Test\CTGTest;
use CTG\DB\CTGDB;
use CTG\DB\CTGDBError;
use CTG\FnProg\CTGFnprog;

// Integration tests — compose pipelines, CTGFnprog interop, end-to-end workflows
// Requires a running MariaDB with guitars/pickups test data

$config = ['output' => 'console'];

$dbHost = getenv('DB_HOST') ?: 'db';
$dbName = getenv('DB_NAME') ?: 'ctg_staging';
$dbUser = getenv('DB_USER') ?: 'ctg_dev';
$dbPass = getenv('DB_PASSWORD') ?: 'devpass_change_me';

// ── compose() — basic pipeline ──────────────────────────────────

CTGTest::init('compose — basic read pipeline')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('build', fn($db) => [
        'db' => $db,
        'pipeline' => $db->compose([
            fn($_, $db) => $db->read('guitars'),
        ])
    ])
    ->stage('execute', fn($ctx) => $ctx['pipeline']())
    ->assert('returns 9 guitars', fn($r) => count($r), 9)
    ->start(null, $config);

CTGTest::init('compose — multi-step pipeline')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('build and execute', fn($db) => $db->compose([
        fn($_, $db) => $db->read('guitars'),
        fn($guitars, $_) => array_filter($guitars, fn($g) => $g['make'] === 'Fender'),
        fn($fenders, $_) => count($fenders),
    ])())
    ->assert('counted 3 Fenders', fn($r) => $r, 3)
    ->start(null, $config);

CTGTest::init('compose — pipeline with initial value')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('build and execute', fn($db) => $db->compose([
        fn($make, $db) => $db->read('guitars', [
            'where' => ['make' => ['type' => 'str', 'value' => $make]]
        ]),
        fn($guitars, $_) => count($guitars),
    ])('Fender'))
    ->assert('counted 3 Fenders', fn($r) => $r, 3)
    ->start(null, $config);

CTGTest::init('compose — pipeline with DB at multiple steps')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('build and execute', fn($db) => $db->compose([
        fn($_, $db) => $db->read('guitars', [
            'columns' => ['id', 'make', 'model']
        ]),
        fn($guitars, $db) => [
            'guitars' => $guitars,
            'pickups' => $db->join(['guitars', 'pickups'], [
                'on' => [['guitars.id' => 'pickups.guitar_id']],
                'columns' => ['guitars.id as guitar_id', 'pickups.make as pickup_make']
            ])
        ],
        fn($data, $_) => [
            'guitar_count' => count($data['guitars']),
            'pickup_count' => count($data['pickups']),
        ],
    ])())
    ->assert('guitar_count is 9', fn($r) => $r['guitar_count'], 9)
    ->assert('pickup_count > 0', fn($r) => $r['pickup_count'] > 0, true)
    ->start(null, $config);

// ── compose() with CTGFnprog ────────────────────────────────────

CTGTest::init('compose + CTGFnprog::pipe — pure transform')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('build and execute', fn($db) => $db->compose([
        fn($_, $db) => $db->read('guitars'),
        fn($guitars, $_) => CTGFnprog::pipe([
            CTGFnprog::filter(fn($g) => (int)$g['year_purchased'] >= 2020),
            CTGFnprog::sortBy('year_purchased', 'DESC'),
            CTGFnprog::pluck('make'),
        ])($guitars),
    ])())
    ->assert('returns array', fn($r) => is_array($r), true)
    ->assert('first is most recent', fn($r) => $r[0], 'Schecter')
    ->start(null, $config);

CTGTest::init('compose + CTGFnprog::pick — select fields')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('build and execute', fn($db) => $db->compose([
        fn($_, $db) => $db->read('guitars'),
        fn($guitars, $_) => CTGFnprog::pipe([
            CTGFnprog::pick(['make', 'model']),
            CTGFnprog::sortBy('make'),
            CTGFnprog::take(3),
        ])($guitars),
    ])())
    ->assert('returns 3 rows', fn($r) => count($r), 3)
    ->assert('has only make and model', fn($r) => array_keys($r[0]), ['make', 'model'])
    ->start(null, $config);

CTGTest::init('compose + CTGFnprog::groupBy — group results')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('build and execute', fn($db) => $db->compose([
        fn($_, $db) => $db->read('guitars'),
        fn($guitars, $_) => CTGFnprog::groupBy('make')($guitars),
    ])())
    ->assert('has Fender group', fn($r) => count($r['Fender']), 3)
    ->assert('has Ibanez group', fn($r) => count($r['Ibanez']), 2)
    ->assert('has Gibson group', fn($r) => count($r['Gibson']), 1)
    ->start(null, $config);

CTGTest::init('compose + CTGFnprog::withField — computed fields')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('build and execute', fn($db) => $db->compose([
        fn($_, $db) => $db->read('guitars', [
            'where' => ['id' => ['type' => 'int', 'value' => 4]]
        ]),
        fn($guitars, $_) => CTGFnprog::pipe([
            CTGFnprog::withField('display', fn($g) => $g['make'] . ' ' . $g['model']),
        ])($guitars),
    ])())
    ->assert('computed display field', fn($r) => $r[0]['display'], 'Fender Telecaster MOD Shop')
    ->start(null, $config);

CTGTest::init('compose + CTGFnprog predicate composition')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('build and execute', fn($db) => $db->compose([
        fn($_, $db) => $db->read('guitars'),
        fn($guitars, $_) => CTGFnprog::pipe([
            CTGFnprog::filter(CTGFnprog::all(
                fn($g) => (int)$g['year_purchased'] >= 2020,
                CTGFnprog::not(fn($g) => $g['make'] === 'Schecter')
            )),
            CTGFnprog::pluck('make'),
        ])($guitars),
    ])())
    ->assert('excludes Schecter', fn($r) => !in_array('Schecter', $r), true)
    ->assert('only post-2020', fn($r) => count($r) > 0, true)
    ->start(null, $config);

// ── compose + CTGFnprog aggregation ─────────────────────────────

CTGTest::init('compose + CTGFnprog aggregation')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('build and execute', fn($db) => $db->compose([
        fn($_, $db) => $db->join(['guitars', 'pickups'], [
            'on' => [['guitars.id' => 'pickups.guitar_id']],
            'columns' => ['guitars.make', 'pickups.type']
        ]),
        fn($rows, $_) => [
            'total_pickups' => CTGFnprog::count()($rows),
            'by_type' => CTGFnprog::pipe([
                CTGFnprog::groupBy('type'),
                fn($groups) => array_map(fn($g) => count($g), $groups),
            ])($rows),
        ],
    ])())
    ->assert('counted pickups', fn($r) => $r['total_pickups'] > 0, true)
    ->assert('has humbucker count', fn($r) => isset($r['by_type']['humbucker']), true)
    ->assert('has single_coil count', fn($r) => isset($r['by_type']['single_coil']), true)
    ->start(null, $config);

// ── Composing pipelines from pipelines ──────────────────────────

CTGTest::init('compose — pipelines from pipelines')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('build and execute', function($db) {
        $getGuitars = $db->compose([
            fn($_, $db) => $db->read('guitars'),
        ]);
        $formatForApi = $db->compose([
            fn($guitars, $_) => CTGFnprog::pipe([
                CTGFnprog::pick(['make', 'model', 'color']),
                CTGFnprog::sortBy('make'),
            ])($guitars),
        ]);
        $fullPipeline = $db->compose([
            $getGuitars,
            $formatForApi,
        ]);
        return $fullPipeline();
    })
    ->assert('returns sorted guitars', fn($r) => $r[0]['make'], 'ESP')
    ->assert('has only picked fields', fn($r) => array_keys($r[0]), ['make', 'model', 'color'])
    ->start(null, $config);

// ── End-to-end: filter + paginate + transform ───────────────────

CTGTest::init('end-to-end — filter + paginate')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', function($db) {
        $filter = $db->filter('guitars', [
            'year_purchased' => ['type' => 'int', 'value' => 2015, 'op' => '>=']
        ]);
        return $db->paginate($filter, [
            'sort' => 'year_purchased',
            'order' => 'DESC',
            'page' => 1,
            'per_page' => 3
        ]);
    })
    ->assert('has data', fn($r) => count($r['data']) > 0, true)
    ->assert('has pagination', fn($r) => isset($r['pagination']['total_rows']), true)
    ->assert('sorted DESC', fn($r) => (int)$r['data'][0]['year_purchased'] >= (int)$r['data'][1]['year_purchased'], true)
    ->start(null, $config);

CTGTest::init('end-to-end — join + as_query + paginate')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', function($db) {
        $query = $db->join(['guitars', 'pickups'], [
            'on' => [['guitars.id' => 'pickups.guitar_id']],
            'columns' => ['guitars.make', 'guitars.model', 'pickups.type'],
            'as_query' => true
        ]);
        return $db->paginate($query, [
            'sort' => 'make',
            'page' => 1,
            'per_page' => 5
        ]);
    })
    ->assert('data has up to 5 rows', fn($r) => count($r['data']) <= 5, true)
    ->assert('has pagination metadata', fn($r) => $r['pagination']['per_page'], 5)
    ->start(null, $config);

CTGTest::init('end-to-end — filter + join via where_raw')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', function($db) {
        $filter = $db->filter('guitars', [
            'year_purchased' => ['type' => 'int', 'value' => 2020, 'op' => '>=']
        ]);
        return $db->read(['guitars', 'pickups'], [
            'join' => 'inner',
            'on' => [['guitars.id' => 'pickups.guitar_id']],
            'columns' => ['guitars.model', 'pickups.type'],
            'where_raw' => $filter
        ]);
    })
    ->assert('returns rows', fn($r) => count($r) > 0, true)
    ->assert('has model', fn($r) => isset($r[0]['model']), true)
    ->start(null, $config);

// ── CRUD lifecycle ──────────────────────────────────────────────

CTGTest::init('end-to-end — full CRUD lifecycle')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('create', fn($db) => [
        'db' => $db,
        'id' => $db->create('guitars', [
            'make' => ['type' => 'str', 'value' => 'TestBrand'],
            'model' => ['type' => 'str', 'value' => 'TestModel'],
            'color' => ['type' => 'str', 'value' => 'TestColor'],
            'year_purchased' => ['type' => 'int', 'value' => 2025]
        ])
    ])
    ->assert('created with id', fn($r) => is_numeric($r['id']), true)
    ->stage('read back', fn($r) => [
        'db' => $r['db'],
        'id' => $r['id'],
        'row' => $r['db']->read('guitars', [
            'where' => ['id' => ['type' => 'int', 'value' => (int)$r['id']]]
        ])[0]
    ])
    ->assert('read make', fn($r) => $r['row']['make'], 'TestBrand')
    ->assert('read model', fn($r) => $r['row']['model'], 'TestModel')
    ->stage('update', fn($r) => [
        'db' => $r['db'],
        'id' => $r['id'],
        'affected' => $r['db']->update('guitars',
            ['color' => ['type' => 'str', 'value' => 'UpdatedColor']],
            ['id' => ['type' => 'int', 'value' => (int)$r['id']]]
        )
    ])
    ->assert('updated 1 row', fn($r) => $r['affected'], 1)
    ->stage('verify update', fn($r) => [
        'db' => $r['db'],
        'id' => $r['id'],
        'row' => $r['db']->read('guitars', [
            'where' => ['id' => ['type' => 'int', 'value' => (int)$r['id']]]
        ])[0]
    ])
    ->assert('color updated', fn($r) => $r['row']['color'], 'UpdatedColor')
    ->stage('delete', fn($r) => [
        'db' => $r['db'],
        'affected' => $r['db']->delete('guitars', [
            'id' => ['type' => 'int', 'value' => (int)$r['id']]
        ])
    ])
    ->assert('deleted 1 row', fn($r) => $r['affected'], 1)
    ->stage('verify delete', fn($r) => $r['db']->read('guitars', [
        'where' => ['make' => ['type' => 'str', 'value' => 'TestBrand']]
    ]))
    ->assert('row is gone', fn($r) => count($r), 0)
    ->start(null, $config);

// ── Error handling in pipelines ─────────────────────────────────

CTGTest::init('compose — error propagates from pipeline')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', function($db) {
        try {
            $pipeline = $db->compose([
                fn($_, $db) => $db->read('nonexistent_table_xyz'),
            ]);
            $pipeline();
            return 'no exception';
        } catch (CTGDBError $e) {
            return $e->type;
        } catch (\Exception $e) {
            return 'other: ' . get_class($e);
        }
    })
    ->assert('threw CTGDBError', fn($r) => is_string($r) && $r !== 'no exception', true)
    ->start(null, $config);
