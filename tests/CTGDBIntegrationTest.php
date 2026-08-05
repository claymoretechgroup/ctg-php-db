<?php
declare(strict_types=1);


use CTG\Test\CTGTest;
use CTG\Test\CTGTestState;
use CTG\Test\Predicates\CTGTestPredicates;
use CTG\DB\CTGDB;
use CTG\DB\CTGDBQuery;

$pipelines = [];

// Integration tests — end-to-end query and CRUD workflows
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

// ── End-to-end: filter + paginate + transform ───────────────────

$pipelines[] = CTGTest::init('end-to-end — CTGDBQuery + paginate')
    ->stage('connect', fn(CTGTestState $state) => CTGDB::init($dbConfig()))
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
    ->stage('connect', fn(CTGTestState $state) => CTGDB::init($dbConfig()))
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
    ->stage('connect', fn(CTGTestState $state) => CTGDB::init($dbConfig()))
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
    ->stage('connect', fn(CTGTestState $state) => CTGDB::init($dbConfig()))
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
        'row' => $state->getSubject()['db']->read(
            CTGDBQuery::from('guitars')->where('id', '=', (int)$state->getSubject()['id'], 'int')
        )[0]
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
        'row' => $state->getSubject()['db']->read(
            CTGDBQuery::from('guitars')->where('id', '=', (int)$state->getSubject()['id'], 'int')
        )[0]
    ])
    ->assert('color updated', fn(CTGTestState $state) => $state->getSubject()['row']['color'], CTGTestPredicates::equals('UpdatedColor'))
    ->stage('delete', fn(CTGTestState $state) => [
        'db' => $state->getSubject()['db'],
        'affected' => $state->getSubject()['db']->delete('guitars', [
            'id' => ['type' => 'int', 'value' => (int)$state->getSubject()['id']]
        ])
    ])
    ->assert('deleted 1 row', fn(CTGTestState $state) => $state->getSubject()['affected'], CTGTestPredicates::equals(1))
    ->stage('verify delete', fn(CTGTestState $state) => $state->getSubject()['db']->read(
        CTGDBQuery::from('guitars')->where('make', '=', 'TestBrand', 'str')
    ))
    ->assert('row is gone', fn(CTGTestState $state) => count($state->getSubject()), CTGTestPredicates::equals(0))
    ;

return $pipelines;
