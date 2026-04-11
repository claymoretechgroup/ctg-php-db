<?php
declare(strict_types=1);


use CTG\Test\CTGTest;
use CTG\Test\CTGTestState;
use CTG\Test\Predicates\CTGTestPredicates;
use CTG\DB\CTGDB;
use CTG\DB\CTGDBError;
use CTG\DB\CTGDBQuery;

$pipelines = [];

// Security tests — SQL injection mitigation across all attack surfaces
// These tests verify that no user-controlled input can alter SQL structure.
// Requires a running MariaDB with guitars/pickups test data.


$dbHost = getenv('DB_HOST') ?: 'db';
$dbName = getenv('DB_NAME') ?: 'ctg_staging';
$dbUser = getenv('DB_USER') ?: 'ctg_dev';
$dbPass = getenv('DB_PASSWORD') ?: 'devpass_change_me';

// ═══════════════════════════════════════════════════════════════
// TABLE NAME INJECTION
// Table names are interpolated into SQL — validateIdentifier must
// reject anything that could alter query structure
// ═══════════════════════════════════════════════════════════════

$tablePayloads = [
    'DROP TABLE'            => 'guitars; DROP TABLE guitars;--',
    'UNION SELECT'          => 'guitars UNION SELECT * FROM information_schema.tables;--',
    'subquery'              => '(SELECT * FROM guitars)',
    'comment escape'        => 'guitars/**/UNION/**/SELECT/**/1,2,3--',
    'semicolon'             => 'guitars; SELECT 1;',
    'single quote'          => "guitars' OR '1'='1",
    'double quote'          => 'guitars" OR "1"="1',
    'backslash escape'      => 'guitars\\',
    'null byte'             => "guitars\x00",
    'newline injection'     => "guitars\nUNION SELECT 1,2,3,4",
    'tab injection'         => "guitars\tUNION SELECT 1",
    'hex encoded'           => '0x676974617273',
    'space injection'       => 'guitars --',
    'backtick escape'       => 'guitars` UNION SELECT 1,2,3,4;--`',
    'equals sign'           => 'guitars WHERE 1=1',
    'parentheses'           => 'guitars()',
    'curly braces'          => 'guitars{}',
    'pipe operator'         => 'guitars||1',
    'ampersand'             => 'guitars&&1',
    'at sign'               => '@guitars',
    'hash comment'          => 'guitars#',
    'dash comment'          => 'guitars--',
    'comma injection'       => 'guitars,pickups',
    'star injection'        => 'guitars*',
    'percent'               => 'guitars%',
    'exclamation'           => 'guitars!',
    'question mark'         => 'guitars?',
    'dollar sign'           => '$guitars',
    'plus sign'             => 'guitars+1',
    'less than'             => 'guitars<1',
    'greater than'          => 'guitars>1',
    'bracket injection'     => 'guitars[0]',
    'tilde'                 => '~guitars',
    'caret'                 => 'guitars^1',
];

foreach ($tablePayloads as $label => $payload) {
    $pipelines[] = CTGTest::init("table injection — {$label}")
        ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
        ->stage('attempt read', function(CTGTestState $state) use ($payload){
            $db = $state->getSubject();
            try {
                $db->read($payload);
                return 'NOT BLOCKED';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('blocked', fn(CTGTestState $state) => $state->getSubject() !== 'NOT BLOCKED', CTGTestPredicates::isTrue())
        ;
}

foreach (['create', 'update', 'delete'] as $method) {
    $pipelines[] = CTGTest::init("table injection — {$method} with DROP TABLE")
        ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
        ->stage('attempt', function(CTGTestState $state) use ($method){
            $db = $state->getSubject();
            try {
                match($method) {
                    'create' => $db->create('guitars; DROP TABLE guitars;--', ['make' => 'x']),
                    'update' => $db->update('guitars; DROP TABLE guitars;--', ['make' => 'x'], ['id' => 1]),
                    'delete' => $db->delete('guitars; DROP TABLE guitars;--', ['id' => 1]),
                };
                return 'NOT BLOCKED';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('blocked', fn(CTGTestState $state) => $state->getSubject() !== 'NOT BLOCKED', CTGTestPredicates::isTrue())
        ;
}

// Verify table still exists after all injection attempts
$pipelines[] = CTGTest::init('table injection — guitars table survived')
    ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn(CTGTestState $state) => $state->getSubject()->run('SELECT COUNT(*) as cnt FROM guitars'))
    ->assert('table exists and has data', fn(CTGTestState $state) => (int)$state->getSubject()[0]['cnt'] >= 9, CTGTestPredicates::isTrue())
    ;

// ═══════════════════════════════════════════════════════════════
// COLUMN NAME INJECTION
// Column names in create/update/where are interpolated via
// validateIdentifier
// ═══════════════════════════════════════════════════════════════

$columnPayloads = [
    'DROP TABLE'        => 'make; DROP TABLE guitars;--',
    'subquery'          => '(SELECT password FROM users)',
    'UNION'             => 'make UNION SELECT 1',
    'comment'           => 'make/**/OR/**/1=1',
    'single quote'      => "make' OR '1'='1",
    'semicolon'         => 'make; SELECT 1;',
    'newline'           => "make\nOR 1=1",
    'equals injection'  => 'make=1 OR 1=1',
    'space injection'   => 'make --',
    'backtick escape'   => 'make` FROM guitars;--`',
    'parentheses'       => 'make()',
];

foreach ($columnPayloads as $label => $payload) {
    $pipelines[] = CTGTest::init("column injection — create with {$label}")
        ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
        ->stage('attempt', function(CTGTestState $state) use ($payload){
            $db = $state->getSubject();
            try {
                $db->create('guitars', [$payload => 'injected']);
                return 'NOT BLOCKED';
            } catch (\Exception $e) {
                return 'BLOCKED';
            }
        })
        ->assert('blocked', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals('BLOCKED'))
        ;
}

foreach ($columnPayloads as $label => $payload) {
    $pipelines[] = CTGTest::init("column injection — update SET with {$label}")
        ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
        ->stage('attempt', function(CTGTestState $state) use ($payload){
            $db = $state->getSubject();
            try {
                $db->update('guitars', [$payload => 'injected'], ['id' => ['type' => 'int', 'value' => 1]]);
                return 'NOT BLOCKED';
            } catch (\Exception $e) {
                return 'BLOCKED';
            }
        })
        ->assert('blocked', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals('BLOCKED'))
        ;
}

foreach ($columnPayloads as $label => $payload) {
    $pipelines[] = CTGTest::init("column injection — WHERE key with {$label}")
        ->stage('attempt', function(CTGTestState $state) use ($payload){
            try {
                CTGDBQuery::from('guitars')->where($payload, '=', 'Fender', 'str');
                return 'NOT BLOCKED';
            } catch (\Exception $e) {
                return 'BLOCKED';
            }
        })
        ->assert('blocked', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals('BLOCKED'))
        ;
}

// ═══════════════════════════════════════════════════════════════
// VALUE INJECTION — PDO PREPARED STATEMENTS
// Values go through PDO binding — these should never affect SQL
// structure. The test verifies the value is treated as data, not
// SQL, by confirming no rows match and no errors occur.
// ═══════════════════════════════════════════════════════════════

$valuePayloads = [
    'classic OR 1=1'        => "' OR '1'='1",
    'classic OR true'       => "' OR TRUE--",
    'UNION SELECT'          => "' UNION SELECT * FROM information_schema.tables--",
    'stacked query'         => "'; DROP TABLE guitars;--",
    'double quote escape'   => '" OR "1"="1',
    'comment bypass'        => "'/**/OR/**/1=1/**/--",
    'hex injection'         => "0x27204f522027313d2731",
    'null byte'             => "Fender\x00' OR '1'='1",
    'backslash escape'      => "Fender\\' OR '1'='1",
    'nested quotes'         => "Fender''' OR '''1'''='''1",
    'unicode escape'        => "Fender\u0027 OR 1=1",
    'concat injection'      => "Fender' || (SELECT password FROM users) || '",
    'sleep injection'       => "' OR SLEEP(5)--",
    'benchmark injection'   => "' OR BENCHMARK(1000000,SHA1('test'))--",
    'extractvalue'          => "' OR EXTRACTVALUE(1,CONCAT(0x7e,VERSION()))--",
    'updatexml'             => "' OR UPDATEXML(1,CONCAT(0x7e,VERSION()),1)--",
    'into outfile'          => "' INTO OUTFILE '/tmp/pwned'--",
    'load file'             => "' UNION SELECT LOAD_FILE('/etc/passwd')--",
    'information_schema'    => "' UNION SELECT table_name FROM information_schema.tables--",
    'very long string'      => str_repeat("' OR '1'='1", 1000),
];

foreach ($valuePayloads as $label => $payload) {
    $pipelines[] = CTGTest::init("value injection — read WHERE with {$label}")
        ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
        ->stage('execute', function(CTGTestState $state) use ($payload){
            $db = $state->getSubject();
            $result = $db->read('guitars', [
                'where' => ['make' => ['type' => 'str', 'value' => $payload]]
            ]);
            return $result;
        })
        ->assert('returns empty — payload treated as literal data', fn(CTGTestState $state) => count($state->getSubject()), CTGTestPredicates::equals(0))
        ;
}

// Verify values with injection payloads can round-trip through create/read/delete
$pipelines[] = CTGTest::init('value injection — payload stored and retrieved as literal data')
    ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('create', fn(CTGTestState $state) => [
        'db' => $state->getSubject(),
        'id' => $state->getSubject()->create('guitars', [
            'make' => ['type' => 'str', 'value' => "'; DROP TABLE guitars;--"],
            'model' => ['type' => 'str', 'value' => "\" OR \"1\"=\"1"],
            'color' => ['type' => 'str', 'value' => "' UNION SELECT * FROM information_schema.tables--"],
            'year_purchased' => ['type' => 'int', 'value' => 2025]
        ])
    ])
    ->stage('read back', fn(CTGTestState $state) => [
        'db' => $state->getSubject()['db'],
        'id' => $state->getSubject()['id'],
        'row' => $state->getSubject()['db']->read('guitars', [
            'where' => ['id' => ['type' => 'int', 'value' => (int)$state->getSubject()['id']]]
        ])[0]
    ])
    ->assert('make stored literally', fn(CTGTestState $state) => $state->getSubject()['row']['make'], CTGTestPredicates::equals("'; DROP TABLE guitars;--"))
    ->assert('model stored literally', fn(CTGTestState $state) => $state->getSubject()['row']['model'], CTGTestPredicates::equals("\" OR \"1\"=\"1"))
    ->stage('cleanup', fn(CTGTestState $state) => $state->getSubject()['db']->delete('guitars', [
        'id' => ['type' => 'int', 'value' => (int)$state->getSubject()['id']]
    ]))
    ;

// Verify table survived all value injection attempts
$pipelines[] = CTGTest::init('value injection — guitars table survived')
    ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn(CTGTestState $state) => $state->getSubject()->run('SELECT COUNT(*) as cnt FROM guitars'))
    ->assert('table exists and has data', fn(CTGTestState $state) => (int)$state->getSubject()[0]['cnt'] >= 9, CTGTestPredicates::isTrue())
    ;

// ═══════════════════════════════════════════════════════════════
// JOIN TYPE INJECTION
// Join type is interpolated directly — must be from allowlist
// ═══════════════════════════════════════════════════════════════

$joinPayloads = [
    'DROP TABLE'        => 'inner; DROP TABLE guitars;--',
    'UNION'             => 'UNION',
    'subquery'          => '(SELECT 1)',
    'natural'           => 'natural',
    'full outer'        => 'full outer',
    'comma join'        => ',',
    'semicolon'         => ';',
    'straight_join'     => 'straight_join',
    'empty string'      => '',
];

foreach ($joinPayloads as $label => $payload) {
    $pipelines[] = CTGTest::init("join type injection — {$label}")
        ->stage('attempt', function(CTGTestState $state) use ($payload){
            try {
                CTGDBQuery::from('guitars')->join('pickups', $payload, ['guitars.id' => 'pickups.guitar_id']);
                return 'NOT BLOCKED';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('blocked as INVALID_JOIN_TYPE', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals('INVALID_JOIN_TYPE'))
        ;
}

// ═══════════════════════════════════════════════════════════════
// SORT DIRECTION INJECTION
// Sort direction is interpolated — must be ASC or DESC
// ═══════════════════════════════════════════════════════════════

$sortDirPayloads = [
    'DROP TABLE'        => 'ASC; DROP TABLE guitars;--',
    'stacked query'     => 'ASC; SELECT 1;',
    'subquery'          => '(SELECT 1)',
    'comment'           => 'ASC --',
    'sleep'             => 'ASC, SLEEP(5)',
    'random'            => 'RANDOM',
    'comma injection'   => 'ASC, make',
    'empty string'      => '',
];

foreach ($sortDirPayloads as $label => $payload) {
    $pipelines[] = CTGTest::init("sort direction injection — {$label}")
        ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
        ->stage('attempt', function(CTGTestState $state) use ($payload){
            $db = $state->getSubject();
            try {
                $db->paginate('guitars', [
                    'sort' => 'id',
                    'order' => $payload
                ]);
                return 'NOT BLOCKED';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('blocked as INVALID_SORT', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals('INVALID_SORT'))
        ;
}

// ═══════════════════════════════════════════════════════════════
// SECOND-ORDER INJECTION
// Data stored via create(), then used in a WHERE to ensure the
// stored payload doesn't become SQL when read back
// ═══════════════════════════════════════════════════════════════

$pipelines[] = CTGTest::init('second-order injection — stored payload does not execute on read')
    ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('store payload', fn(CTGTestState $state) => [
        'db' => $state->getSubject(),
        'id' => $state->getSubject()->create('guitars', [
            'make' => ['type' => 'str', 'value' => "' OR '1'='1"],
            'model' => ['type' => 'str', 'value' => 'Injection Test'],
            'color' => ['type' => 'str', 'value' => 'Red'],
            'year_purchased' => ['type' => 'int', 'value' => 2025]
        ])
    ])
    ->stage('read with stored value as filter', fn(CTGTestState $state) => [
        'db' => $state->getSubject()['db'],
        'id' => $state->getSubject()['id'],
        'rows' => $state->getSubject()['db']->read('guitars', [
            'where' => ['make' => ['type' => 'str', 'value' => "' OR '1'='1"]]
        ])
    ])
    ->assert('returns exactly 1 row (the one we stored)', fn(CTGTestState $state) => count($state->getSubject()['rows']), CTGTestPredicates::equals(1))
    ->assert('not all rows', fn(CTGTestState $state) => count($state->getSubject()['rows']) < 9, CTGTestPredicates::isTrue())
    ->stage('cleanup', fn(CTGTestState $state) => $state->getSubject()['db']->delete('guitars', [
        'id' => ['type' => 'int', 'value' => (int)$state->getSubject()['id']]
    ]))
    ;

// ═══════════════════════════════════════════════════════════════
// INTEGER TYPE COERCION
// Ensure integer parameters can't be abused with string payloads
// ═══════════════════════════════════════════════════════════════

$pipelines[] = CTGTest::init('type coercion — int type enforced on bind')
    ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', function(CTGTestState $state) {
            $db = $state->getSubject();
        // PDO with emulated prepares off will enforce int type
        $result = $db->read('guitars', [
            'where' => ['id' => ['type' => 'int', 'value' => 1]]
        ]);
        return count($result);
    })
    ->assert('returns exactly 1', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals(1))
    ;

// ═══════════════════════════════════════════════════════════════
// BATCH INJECTION — Multiple vectors in one request
// ═══════════════════════════════════════════════════════════════

$pipelines[] = CTGTest::init('batch injection — multiple injection vectors at once')
    ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('attempt', function(CTGTestState $state) {
            $db = $state->getSubject();
        $blocked = 0;
        $attacks = [
            fn() => $db->read("guitars; DROP TABLE guitars;--"),
            fn() => $db->create("guitars; DROP TABLE guitars;--", ['make' => 'x']),
            fn() => CTGDBQuery::from('guitars')->where('make', 'DROP', 'x', 'str'),
            fn() => CTGDBQuery::from('guitars')->join('pickups', 'EVIL', ['guitars.id' => 'pickups.guitar_id']),
            fn() => $db->paginate('guitars', ['sort' => 'id', 'order' => 'EVIL']),
        ];
        foreach ($attacks as $attack) {
            try {
                $attack();
            } catch (\Exception $e) {
                $blocked++;
            }
        }
        return $blocked;
    })
    ->assert('all 5 attacks blocked', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals(5))
    ;

// ═══════════════════════════════════════════════════════════════
// SORT COLUMN INJECTION (paginate)
// Sort column is now validated through validateIdentifier
// ═══════════════════════════════════════════════════════════════

$sortColPayloads = [
    'DROP TABLE'        => 'id; DROP TABLE guitars;--',
    'stacked query'     => 'id; SELECT 1;',
    'UNION'             => 'id UNION SELECT 1,2,3,4,5',
    'subquery'          => '(SELECT 1)',
    'comment'           => 'id/**/UNION/**/SELECT/**/1',
    'OR bypass'         => 'id OR 1=1',
    'space injection'   => 'id --',
    'backtick escape'   => 'id` FROM guitars;--`',
    'comma injection'   => 'id, (SELECT password FROM users)',
];

foreach ($sortColPayloads as $label => $payload) {
    $pipelines[] = CTGTest::init("sort column injection — {$label}")
        ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
        ->stage('attempt', function(CTGTestState $state) use ($payload){
            $db = $state->getSubject();
            try {
                $db->paginate('guitars', [
                    'sort' => $payload,
                    'page' => 1,
                    'per_page' => 3
                ]);
                return 'NOT BLOCKED';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('blocked', fn(CTGTestState $state) => $state->getSubject() !== 'NOT BLOCKED', CTGTestPredicates::isTrue())
        ;
}

// ═══════════════════════════════════════════════════════════════
// JOIN ON COLUMN INJECTION
// ON clause column references are now validated
// ═══════════════════════════════════════════════════════════════

$onPayloads = [
    'DROP TABLE'    => 'guitars.id; DROP TABLE guitars;--',
    'OR bypass'     => 'guitars.id OR 1=1',
    'subquery'      => '(SELECT 1)',
    'UNION'         => 'guitars.id UNION SELECT 1',
    'comment'       => 'guitars.id/**/OR/**/1=1',
    'single quote'  => "guitars.id' OR '1'='1",
];

foreach ($onPayloads as $label => $payload) {
    $pipelines[] = CTGTest::init("join on column injection — {$label}")
        ->stage('attempt', function(CTGTestState $state) use ($payload){
            try {
                CTGDBQuery::from('guitars')->join('pickups', 'inner', [$payload => 'pickups.guitar_id']);
                return 'NOT BLOCKED';
            } catch (\Exception $e) {
                return 'BLOCKED';
            }
        })
        ->assert('blocked', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals('BLOCKED'))
        ;
}

// ═══════════════════════════════════════════════════════════════
// ORDER CLAUSE INJECTION (CTGDBQuery)
// ORDER BY column is validated through validateIdentifier
// ═══════════════════════════════════════════════════════════════

$orderPayloads = [
    'DROP TABLE'        => 'id; DROP TABLE guitars;--',
    'subquery'          => '(SELECT 1) DESC',
    'UNION'             => 'id UNION SELECT 1,2,3,4,5',
    'sleep'             => 'SLEEP(5)',
    'comment'           => 'id/**/UNION/**/SELECT/**/1',
];

foreach ($orderPayloads as $label => $payload) {
    $pipelines[] = CTGTest::init("order clause injection — {$label}")
        ->stage('attempt', function(CTGTestState $state) use ($payload){
            try {
                CTGDBQuery::from('guitars')->orderBy($payload);
                return 'NOT BLOCKED';
            } catch (\Exception $e) {
                return 'BLOCKED';
            }
        })
        ->assert('blocked', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals('BLOCKED'))
        ;
}

// Verify valid order clauses still work
$pipelines[] = CTGTest::init('order clause — valid single column ASC')
    ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn(CTGTestState $state) => $state->getSubject()->read(
        CTGDBQuery::from('guitars')->orderBy('year_purchased', 'DESC')
    ))
    ->assert('most recent first', fn(CTGTestState $state) => $state->getSubject()[0]['make'], CTGTestPredicates::equals('Schecter'))
    ;

$pipelines[] = CTGTest::init('order clause — valid column without direction')
    ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn(CTGTestState $state) => $state->getSubject()->read(
        CTGDBQuery::from('guitars')->orderBy('make')
    ))
    ->assert('returns rows', fn(CTGTestState $state) => count($state->getSubject()), CTGTestPredicates::equals(9))
    ;

// Final integrity check — the database is intact after all attacks
$pipelines[] = CTGTest::init('final integrity — guitars table intact after all attacks')
    ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn(CTGTestState $state) => [
        'count' => (int)$state->getSubject()->run('SELECT COUNT(*) as cnt FROM guitars')[0]['cnt'],
        'columns' => $state->getSubject()->run("SHOW COLUMNS FROM guitars"),
    ])
    ->assert('still has 9 rows', fn(CTGTestState $state) => $state->getSubject()['count'], CTGTestPredicates::equals(9))
    ->assert('still has 5 columns', fn(CTGTestState $state) => count($state->getSubject()['columns']), CTGTestPredicates::equals(5))
    ;

$pipelines[] = CTGTest::init('final integrity — pickups table intact after all attacks')
    ->stage('connect', fn(CTGTestState $state) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn(CTGTestState $state) => [
        'count' => (int)$state->getSubject()->run('SELECT COUNT(*) as cnt FROM pickups')[0]['cnt'],
    ])
    ->assert('still has pickup data', fn(CTGTestState $state) => $state->getSubject()['count'] > 0, CTGTestPredicates::isTrue())
    ;

return $pipelines;
