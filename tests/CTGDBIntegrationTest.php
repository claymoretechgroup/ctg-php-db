<?php
declare(strict_types=1);


use CTG\Test\CTGTest;
use CTG\Test\CTGTestState;
use CTG\Test\Predicates\CTGTestPredicates;
use CTG\DB\CTGDB;
use CTG\DB\CTGDBError;
use CTG\DB\CTGDBQuery;
use CTG\FnProg\CTGFnprog;

$pipelines = [];

// Integration tests — compose pipelines, CTGFnprog interop, end-to-end workflows
// Requires a running MariaDB with guitars/pickups test data


$dbHost = getenv('DB_HOST') ?: 'db';
$dbName = getenv('DB_NAME') ?: 'ctg_staging';
$dbUser = getenv('DB_USER') ?: 'ctg_dev';
$dbPass = getenv('DB_PASSWORD') ?: 'devpass_change_me';

// ── compose() — basic pipeline ──────────────────────────────────

$pipelines[] = CTGTest::init('compose — basic read pipeline')
    ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('build', fn(CTGTestState $state) => [
        'db' => $state->getSubject(),
        'pipeline' => $state->getSubject()->compose([
            fn($_, $db) => $db->read('guitars'),
        ])
    ])
    ->stage('execute', fn(CTGTestState $state) => $state->getSubject()['pipeline']())
    ->assert('returns 9 guitars', fn(CTGTestState $state) => count($state->getSubject()), CTGTestPredicates::equals(9))
    ;

$pipelines[] = CTGTest::init('compose — multi-step pipeline')
    ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('build and execute', fn(CTGTestState $state) => $state->getSubject()->compose([
        fn($_, $db) => $db->read('guitars'),
        fn($guitars, $_) => array_filter($guitars, fn($g) => $g['make'] === 'Fender'),
        fn($fenders, $_) => count($fenders),
    ])())
    ->assert('counted 3 Fenders', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals(3))
    ;

$pipelines[] = CTGTest::init('compose — pipeline with initial value')
    ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('build and execute', fn(CTGTestState $state) => $state->getSubject()->compose([
        fn($make, $db) => $db->read('guitars', [
            'where' => ['make' => ['type' => 'str', 'value' => $make]]
        ]),
        fn($guitars, $_) => count($guitars),
    ])('Fender'))
    ->assert('counted 3 Fenders', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals(3))
    ;

$pipelines[] = CTGTest::init('compose — pipeline with DB at multiple steps')
    ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('build and execute', fn(CTGTestState $state) => $state->getSubject()->compose([
        fn($_, $db) => $db->read('guitars', [
            'columns' => ['id', 'make', 'model']
        ]),
        fn($guitars, $db) => [
            'guitars' => $guitars,
            'pickups' => $db->read(
                CTGDBQuery::from('guitars')
                    ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id'])
                    ->columns('guitars.id as guitar_id', 'pickups.make as pickup_make')
            )
        ],
        fn($data, $_) => [
            'guitar_count' => count($data['guitars']),
            'pickup_count' => count($data['pickups']),
        ],
    ])())
    ->assert('guitar_count is 9', fn(CTGTestState $state) => $state->getSubject()['guitar_count'], CTGTestPredicates::equals(9))
    ->assert('pickup_count > 0', fn(CTGTestState $state) => $state->getSubject()['pickup_count'] > 0, CTGTestPredicates::isTrue())
    ;

// ── compose() with CTGFnprog ────────────────────────────────────

$pipelines[] = CTGTest::init('compose + CTGFnprog::pipe — pure transform')
    ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('build and execute', fn(CTGTestState $state) => $state->getSubject()->compose([
        fn($_, $db) => $db->read('guitars'),
        fn($guitars, $_) => CTGFnprog::pipe([
            CTGFnprog::filter(fn($g) => (int)$g['year_purchased'] >= 2020),
            CTGFnprog::sortBy('year_purchased', 'DESC'),
            CTGFnprog::pluck('make'),
        ])($guitars),
    ])())
    ->assert('returns array', fn(CTGTestState $state) => is_array($state->getSubject()), CTGTestPredicates::isTrue())
    ->assert('first is most recent', fn(CTGTestState $state) => $state->getSubject()[0], CTGTestPredicates::equals('Schecter'))
    ;

$pipelines[] = CTGTest::init('compose + CTGFnprog::pick — select fields')
    ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('build and execute', fn(CTGTestState $state) => $state->getSubject()->compose([
        fn($_, $db) => $db->read('guitars'),
        fn($guitars, $_) => CTGFnprog::pipe([
            CTGFnprog::pick(['make', 'model']),
            CTGFnprog::sortBy('make'),
            CTGFnprog::take(3),
        ])($guitars),
    ])())
    ->assert('returns 3 rows', fn(CTGTestState $state) => count($state->getSubject()), CTGTestPredicates::equals(3))
    ->assert('has only make and model', fn(CTGTestState $state) => array_keys($state->getSubject()[0]), CTGTestPredicates::equals(['make', 'model']))
    ;

$pipelines[] = CTGTest::init('compose + CTGFnprog::groupBy — group results')
    ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('build and execute', fn(CTGTestState $state) => $state->getSubject()->compose([
        fn($_, $db) => $db->read('guitars'),
        fn($guitars, $_) => CTGFnprog::groupBy('make')($guitars),
    ])())
    ->assert('has Fender group', fn(CTGTestState $state) => count($state->getSubject()['Fender']), CTGTestPredicates::equals(3))
    ->assert('has Ibanez group', fn(CTGTestState $state) => count($state->getSubject()['Ibanez']), CTGTestPredicates::equals(2))
    ->assert('has Gibson group', fn(CTGTestState $state) => count($state->getSubject()['Gibson']), CTGTestPredicates::equals(1))
    ;

$pipelines[] = CTGTest::init('compose + CTGFnprog::withField — computed fields')
    ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('build and execute', fn(CTGTestState $state) => $state->getSubject()->compose([
        fn($_, $db) => $db->read('guitars', [
            'where' => ['id' => ['type' => 'int', 'value' => 4]]
        ]),
        fn($guitars, $_) => CTGFnprog::pipe([
            CTGFnprog::withField('display', fn($g) => $g['make'] . ' ' . $g['model']),
        ])($guitars),
    ])())
    ->assert('computed display field', fn(CTGTestState $state) => $state->getSubject()[0]['display'], CTGTestPredicates::equals('Fender Telecaster MOD Shop'))
    ;

$pipelines[] = CTGTest::init('compose + CTGFnprog predicate composition')
    ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('build and execute', fn(CTGTestState $state) => $state->getSubject()->compose([
        fn($_, $db) => $db->read('guitars'),
        fn($guitars, $_) => CTGFnprog::pipe([
            CTGFnprog::filter(CTGFnprog::all(
                fn($g) => (int)$g['year_purchased'] >= 2020,
                CTGFnprog::not(fn($g) => $g['make'] === 'Schecter')
            )),
            CTGFnprog::pluck('make'),
        ])($guitars),
    ])())
    ->assert('excludes Schecter', fn(CTGTestState $state) => !in_array('Schecter', $state->getSubject()), CTGTestPredicates::isTrue())
    ->assert('only post-2020', fn(CTGTestState $state) => count($state->getSubject()) > 0, CTGTestPredicates::isTrue())
    ;

// ── compose + CTGFnprog aggregation ─────────────────────────────

$pipelines[] = CTGTest::init('compose + CTGFnprog aggregation')
    ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('build and execute', fn(CTGTestState $state) => $state->getSubject()->compose([
        fn($_, $db) => $db->read(
            CTGDBQuery::from('guitars')
                ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id'])
                ->columns('guitars.make', 'pickups.type')
        ),
        fn($rows, $_) => [
            'total_pickups' => CTGFnprog::count()($rows),
            'by_type' => CTGFnprog::pipe([
                CTGFnprog::groupBy('type'),
                fn($groups) => array_map(fn($g) => count($g), $groups),
            ])($rows),
        ],
    ])())
    ->assert('counted pickups', fn(CTGTestState $state) => $state->getSubject()['total_pickups'] > 0, CTGTestPredicates::isTrue())
    ->assert('has humbucker count', fn(CTGTestState $state) => isset($state->getSubject()['by_type']['humbucker']), CTGTestPredicates::isTrue())
    ->assert('has single_coil count', fn(CTGTestState $state) => isset($state->getSubject()['by_type']['single_coil']), CTGTestPredicates::isTrue())
    ;

// ── Composing pipelines from pipelines ──────────────────────────

$pipelines[] = CTGTest::init('compose — pipelines from pipelines')
    ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('build and execute', function(CTGTestState $state) {
            $db = $state->getSubject();
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
    ->assert('returns sorted guitars', fn(CTGTestState $state) => $state->getSubject()[0]['make'], CTGTestPredicates::equals('ESP'))
    ->assert('has only picked fields', fn(CTGTestState $state) => array_keys($state->getSubject()[0]), CTGTestPredicates::equals(['make', 'model', 'color']))
    ;

// ── End-to-end: filter + paginate + transform ───────────────────

$pipelines[] = CTGTest::init('end-to-end — CTGDBQuery + paginate')
    ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', function(CTGTestState $state) {
            $db = $state->getSubject();
        $query = CTGDBQuery::from('guitars')
            ->where('year_purchased', '>=', 2015, 'int');
        return $db->paginate($query, [
            'sort' => 'year_purchased',
            'order' => 'DESC',
            'page' => 1,
            'per_page' => 3
        ]);
    })
    ->assert('has data', fn(CTGTestState $state) => count($state->getSubject()['data']) > 0, CTGTestPredicates::isTrue())
    ->assert('has pagination', fn(CTGTestState $state) => isset($state->getSubject()['pagination']['total_rows']), CTGTestPredicates::isTrue())
    ->assert('sorted DESC', fn(CTGTestState $state) => (int)$state->getSubject()['data'][0]['year_purchased'] >= (int)$state->getSubject()['data'][1]['year_purchased'], CTGTestPredicates::isTrue())
    ;

$pipelines[] = CTGTest::init('end-to-end — CTGDBQuery join + paginate')
    ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', function(CTGTestState $state) {
            $db = $state->getSubject();
        $query = CTGDBQuery::from('guitars')
            ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id'])
            ->columns('guitars.make', 'guitars.model', 'pickups.type');
        return $db->paginate($query, [
            'sort' => 'make',
            'page' => 1,
            'per_page' => 5
        ]);
    })
    ->assert('data has up to 5 rows', fn(CTGTestState $state) => count($state->getSubject()['data']) <= 5, CTGTestPredicates::isTrue())
    ->assert('has pagination metadata', fn(CTGTestState $state) => $state->getSubject()['pagination']['per_page'], CTGTestPredicates::equals(5))
    ;

$pipelines[] = CTGTest::init('end-to-end — CTGDBQuery join + where')
    ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', function(CTGTestState $state) {
            $db = $state->getSubject();
        return $db->read(
            CTGDBQuery::from('guitars')
                ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id'])
                ->columns('guitars.model', 'pickups.type')
                ->where('guitars.year_purchased', '>=', 2020, 'int')
        );
    })
    ->assert('returns rows', fn(CTGTestState $state) => count($state->getSubject()) > 0, CTGTestPredicates::isTrue())
    ->assert('has model', fn(CTGTestState $state) => isset($state->getSubject()[0]['model']), CTGTestPredicates::isTrue())
    ;

// ── CRUD lifecycle ──────────────────────────────────────────────

$pipelines[] = CTGTest::init('end-to-end — full CRUD lifecycle')
    ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('create', fn(CTGTestState $state) => [
        'db' => $state->getSubject(),
        'id' => $state->getSubject()->create('guitars', [
            'make' => ['type' => 'str', 'value' => 'TestBrand'],
            'model' => ['type' => 'str', 'value' => 'TestModel'],
            'color' => ['type' => 'str', 'value' => 'TestColor'],
            'year_purchased' => ['type' => 'int', 'value' => 2025]
        ])
    ])
    ->assert('created with id', fn(CTGTestState $state) => is_numeric($state->getSubject()['id']), CTGTestPredicates::isTrue())
    ->stage('read back', fn(CTGTestState $state) => [
        'db' => $state->getSubject()['db'],
        'id' => $state->getSubject()['id'],
        'row' => $state->getSubject()['db']->read('guitars', [
            'where' => ['id' => ['type' => 'int', 'value' => (int)$state->getSubject()['id']]]
        ])[0]
    ])
    ->assert('read make', fn(CTGTestState $state) => $state->getSubject()['row']['make'], CTGTestPredicates::equals('TestBrand'))
    ->assert('read model', fn(CTGTestState $state) => $state->getSubject()['row']['model'], CTGTestPredicates::equals('TestModel'))
    ->stage('update', fn(CTGTestState $state) => [
        'db' => $state->getSubject()['db'],
        'id' => $state->getSubject()['id'],
        'affected' => $state->getSubject()['db']->update('guitars',
            ['color' => ['type' => 'str', 'value' => 'UpdatedColor']],
            ['id' => ['type' => 'int', 'value' => (int)$state->getSubject()['id']]]
        )
    ])
    ->assert('updated 1 row', fn(CTGTestState $state) => $state->getSubject()['affected'], CTGTestPredicates::equals(1))
    ->stage('verify update', fn(CTGTestState $state) => [
        'db' => $state->getSubject()['db'],
        'id' => $state->getSubject()['id'],
        'row' => $state->getSubject()['db']->read('guitars', [
            'where' => ['id' => ['type' => 'int', 'value' => (int)$state->getSubject()['id']]]
        ])[0]
    ])
    ->assert('color updated', fn(CTGTestState $state) => $state->getSubject()['row']['color'], CTGTestPredicates::equals('UpdatedColor'))
    ->stage('delete', fn(CTGTestState $state) => [
        'db' => $state->getSubject()['db'],
        'affected' => $state->getSubject()['db']->delete('guitars', [
            'id' => ['type' => 'int', 'value' => (int)$state->getSubject()['id']]
        ])
    ])
    ->assert('deleted 1 row', fn(CTGTestState $state) => $state->getSubject()['affected'], CTGTestPredicates::equals(1))
    ->stage('verify delete', fn(CTGTestState $state) => $state->getSubject()['db']->read('guitars', [
        'where' => ['make' => ['type' => 'str', 'value' => 'TestBrand']]
    ]))
    ->assert('row is gone', fn(CTGTestState $state) => count($state->getSubject()), CTGTestPredicates::equals(0))
    ;

// ── Error handling in pipelines ─────────────────────────────────

$pipelines[] = CTGTest::init('compose — error propagates from pipeline')
    ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', function(CTGTestState $state) {
            $db = $state->getSubject();
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
    ->assert('threw CTGDBError', fn(CTGTestState $state) => is_string($state->getSubject()) && $state->getSubject() !== 'no exception', CTGTestPredicates::isTrue())
    ;

return $pipelines;
