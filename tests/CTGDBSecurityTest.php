<?php
declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

use CTG\Test\CTGTest;
use CTG\DB\CTGDB;
use CTG\DB\CTGDBError;

// Security tests — SQL injection mitigation across all attack surfaces
// These tests verify that no user-controlled input can alter SQL structure.
// Requires a running MariaDB with guitars/pickups test data.

$config = ['output' => 'console'];

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
    CTGTest::init("table injection — {$label}")
        ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
        ->stage('attempt read', function($db) use ($payload) {
            try {
                $db->read($payload);
                return 'NOT BLOCKED';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('blocked', fn($r) => $r !== 'NOT BLOCKED', true)
        ->start(null, $config);
}

foreach (['create', 'update', 'delete'] as $method) {
    CTGTest::init("table injection — {$method} with DROP TABLE")
        ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
        ->stage('attempt', function($db) use ($method) {
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
        ->assert('blocked', fn($r) => $r !== 'NOT BLOCKED', true)
        ->start(null, $config);
}

// Verify table still exists after all injection attempts
CTGTest::init('table injection — guitars table survived')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn($db) => $db->run('SELECT COUNT(*) as cnt FROM guitars'))
    ->assert('table exists and has data', fn($r) => (int)$r[0]['cnt'] >= 9, true)
    ->start(null, $config);

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
    CTGTest::init("column injection — create with {$label}")
        ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
        ->stage('attempt', function($db) use ($payload) {
            try {
                $db->create('guitars', [$payload => 'injected']);
                return 'NOT BLOCKED';
            } catch (\Exception $e) {
                return 'BLOCKED';
            }
        })
        ->assert('blocked', fn($r) => $r, 'BLOCKED')
        ->start(null, $config);
}

foreach ($columnPayloads as $label => $payload) {
    CTGTest::init("column injection — update SET with {$label}")
        ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
        ->stage('attempt', function($db) use ($payload) {
            try {
                $db->update('guitars', [$payload => 'injected'], ['id' => ['type' => 'int', 'value' => 1]]);
                return 'NOT BLOCKED';
            } catch (\Exception $e) {
                return 'BLOCKED';
            }
        })
        ->assert('blocked', fn($r) => $r, 'BLOCKED')
        ->start(null, $config);
}

foreach ($columnPayloads as $label => $payload) {
    CTGTest::init("column injection — WHERE key with {$label}")
        ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
        ->stage('attempt', function($db) use ($payload) {
            try {
                $db->read('guitars', [
                    'where' => [$payload => ['type' => 'str', 'value' => 'Fender']]
                ]);
                return 'NOT BLOCKED';
            } catch (\Exception $e) {
                return 'BLOCKED';
            }
        })
        ->assert('blocked', fn($r) => $r, 'BLOCKED')
        ->start(null, $config);
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
    CTGTest::init("value injection — read WHERE with {$label}")
        ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
        ->stage('execute', function($db) use ($payload) {
            $result = $db->read('guitars', [
                'where' => ['make' => ['type' => 'str', 'value' => $payload]]
            ]);
            return $result;
        })
        ->assert('returns empty — payload treated as literal data', fn($r) => count($r), 0)
        ->start(null, $config);
}

foreach ($valuePayloads as $label => $payload) {
    CTGTest::init("value injection — filter with {$label}")
        ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
        ->stage('execute', function($db) use ($payload) {
            $filter = $db->filter('guitars', [
                'make' => ['type' => 'str', 'value' => $payload]
            ]);
            return $db->run([
                'sql' => "SELECT * FROM {$filter['table']} WHERE {$filter['where']}",
                'values' => $filter['values']
            ]);
        })
        ->assert('returns empty — payload treated as literal data', fn($r) => count($r), 0)
        ->start(null, $config);
}

// Verify values with injection payloads can round-trip through create/read/delete
CTGTest::init('value injection — payload stored and retrieved as literal data')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('create', fn($db) => [
        'db' => $db,
        'id' => $db->create('guitars', [
            'make' => ['type' => 'str', 'value' => "'; DROP TABLE guitars;--"],
            'model' => ['type' => 'str', 'value' => "\" OR \"1\"=\"1"],
            'color' => ['type' => 'str', 'value' => "' UNION SELECT * FROM information_schema.tables--"],
            'year_purchased' => ['type' => 'int', 'value' => 2025]
        ])
    ])
    ->stage('read back', fn($r) => [
        'db' => $r['db'],
        'id' => $r['id'],
        'row' => $r['db']->read('guitars', [
            'where' => ['id' => ['type' => 'int', 'value' => (int)$r['id']]]
        ])[0]
    ])
    ->assert('make stored literally', fn($r) => $r['row']['make'], "'; DROP TABLE guitars;--")
    ->assert('model stored literally', fn($r) => $r['row']['model'], "\" OR \"1\"=\"1")
    ->stage('cleanup', fn($r) => $r['db']->delete('guitars', [
        'id' => ['type' => 'int', 'value' => (int)$r['id']]
    ]))
    ->start(null, $config);

// Verify table survived all value injection attempts
CTGTest::init('value injection — guitars table survived')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn($db) => $db->run('SELECT COUNT(*) as cnt FROM guitars'))
    ->assert('table exists and has data', fn($r) => (int)$r[0]['cnt'] >= 9, true)
    ->start(null, $config);

// ═══════════════════════════════════════════════════════════════
// OPERATOR INJECTION
// filter() operators are validated against a hardcoded allowlist
// ═══════════════════════════════════════════════════════════════

$operatorPayloads = [
    'DROP TABLE'        => 'DROP TABLE guitars;--',
    'stacked SQL'       => '= 1; DROP TABLE guitars;--',
    'UNION'             => 'UNION',
    'OR bypass'         => 'OR 1=1',
    'comment'           => '= 1 --',
    'semicolon'         => ';',
    'nested operator'   => '>= 1 OR 1=1',
    'double operator'   => '= = =',
    'empty string'      => '',
    'null byte'         => "\x00; DROP TABLE guitars;--",
    'sleep'             => '= SLEEP(5)',
    'into outfile'      => "INTO OUTFILE '/tmp/pwned'",
    'case bypass'       => 'lIkE',  // valid operator in different case — should be allowed
];

foreach ($operatorPayloads as $label => $payload) {
    // Skip 'lIkE' — that's a valid operator
    if ($label === 'case bypass') continue;

    CTGTest::init("operator injection — {$label}")
        ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
        ->stage('attempt', function($db) use ($payload) {
            try {
                $db->filter('guitars', [
                    'make' => ['type' => 'str', 'value' => 'x', 'op' => $payload]
                ]);
                return 'NOT BLOCKED';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('blocked as INVALID_OPERATOR', fn($r) => $r, 'INVALID_OPERATOR')
        ->start(null, $config);
}

// Verify valid operators still work
$validOperators = ['=', '>', '<', '>=', '<=', '!=', 'LIKE', 'NOT LIKE', 'IN', 'NOT IN', 'IS', 'IS NOT', 'BETWEEN'];

foreach ($validOperators as $op) {
    CTGTest::init("operator allowlist — {$op} is valid")
        ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
        ->stage('attempt', function($db) use ($op) {
            try {
                $condition = match(strtoupper($op)) {
                    'IN', 'NOT IN'   => ['type' => 'str', 'value' => ['Fender'], 'op' => $op],
                    'IS', 'IS NOT'   => ['type' => 'null', 'value' => null, 'op' => $op],
                    'BETWEEN'        => ['type' => 'int', 'value' => [2000, 2025], 'op' => $op],
                    default          => ['type' => 'str', 'value' => 'Fender', 'op' => $op],
                };
                $db->filter('guitars', ['make' => $condition]);
                return 'ALLOWED';
            } catch (CTGDBError $e) {
                return 'BLOCKED: ' . $e->type;
            }
        })
        ->assert('allowed', fn($r) => $r, 'ALLOWED')
        ->start(null, $config);
}

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
    CTGTest::init("join type injection — {$label}")
        ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
        ->stage('attempt', function($db) use ($payload) {
            try {
                $db->read(['guitars', 'pickups'], [
                    'join' => $payload,
                    'on' => [['guitars.id' => 'pickups.guitar_id']]
                ]);
                return 'NOT BLOCKED';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('blocked as INVALID_JOIN_TYPE', fn($r) => $r, 'INVALID_JOIN_TYPE')
        ->start(null, $config);
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
    CTGTest::init("sort direction injection — {$label}")
        ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
        ->stage('attempt', function($db) use ($payload) {
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
        ->assert('blocked as INVALID_SORT', fn($r) => $r, 'INVALID_SORT')
        ->start(null, $config);
}

// ═══════════════════════════════════════════════════════════════
// FILTER COLUMN NAME INJECTION
// Column names inside filter() conditions
// ═══════════════════════════════════════════════════════════════

$filterColPayloads = [
    'stacked query'     => "make; DROP TABLE guitars;--",
    'OR bypass'         => "make OR 1=1",
    'subquery column'   => "(SELECT 1)",
    'comment escape'    => "make/**/OR/**/1=1",
    'backtick escape'   => "make` OR `1`=`1",
    'single quote'      => "make' OR '1'='1",
];

foreach ($filterColPayloads as $label => $payload) {
    CTGTest::init("filter column injection — {$label}")
        ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
        ->stage('attempt', function($db) use ($payload) {
            try {
                $db->filter('guitars', [
                    $payload => ['type' => 'str', 'value' => 'Fender']
                ]);
                return 'NOT BLOCKED';
            } catch (\Exception $e) {
                return 'BLOCKED';
            }
        })
        ->assert('blocked', fn($r) => $r, 'BLOCKED')
        ->start(null, $config);
}

// ═══════════════════════════════════════════════════════════════
// SECOND-ORDER INJECTION
// Data stored via create(), then used in a WHERE to ensure the
// stored payload doesn't become SQL when read back
// ═══════════════════════════════════════════════════════════════

CTGTest::init('second-order injection — stored payload does not execute on read')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('store payload', fn($db) => [
        'db' => $db,
        'id' => $db->create('guitars', [
            'make' => ['type' => 'str', 'value' => "' OR '1'='1"],
            'model' => ['type' => 'str', 'value' => 'Injection Test'],
            'color' => ['type' => 'str', 'value' => 'Red'],
            'year_purchased' => ['type' => 'int', 'value' => 2025]
        ])
    ])
    ->stage('read with stored value as filter', fn($ctx) => [
        'db' => $ctx['db'],
        'id' => $ctx['id'],
        'rows' => $ctx['db']->read('guitars', [
            'where' => ['make' => ['type' => 'str', 'value' => "' OR '1'='1"]]
        ])
    ])
    ->assert('returns exactly 1 row (the one we stored)', fn($r) => count($r['rows']), 1)
    ->assert('not all rows', fn($r) => count($r['rows']) < 9, true)
    ->stage('cleanup', fn($r) => $r['db']->delete('guitars', [
        'id' => ['type' => 'int', 'value' => (int)$r['id']]
    ]))
    ->start(null, $config);

// ═══════════════════════════════════════════════════════════════
// INTEGER TYPE COERCION
// Ensure integer parameters can't be abused with string payloads
// ═══════════════════════════════════════════════════════════════

CTGTest::init('type coercion — int type enforced on bind')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', function($db) {
        // PDO with emulated prepares off will enforce int type
        $result = $db->read('guitars', [
            'where' => ['id' => ['type' => 'int', 'value' => 1]]
        ]);
        return count($result);
    })
    ->assert('returns exactly 1', fn($r) => $r, 1)
    ->start(null, $config);

// ═══════════════════════════════════════════════════════════════
// BATCH INJECTION — Multiple vectors in one request
// ═══════════════════════════════════════════════════════════════

CTGTest::init('batch injection — multiple injection vectors at once')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('attempt', function($db) {
        $blocked = 0;
        $attacks = [
            fn() => $db->read("guitars; DROP TABLE guitars;--"),
            fn() => $db->create("guitars; DROP TABLE guitars;--", ['make' => 'x']),
            fn() => $db->read('guitars', ['where' => ["id; DROP TABLE guitars;--" => 1]]),
            fn() => $db->filter('guitars', ['make' => ['type' => 'str', 'value' => 'x', 'op' => 'DROP']]),
            fn() => $db->read(['guitars', 'pickups'], ['join' => 'EVIL', 'on' => [['guitars.id' => 'pickups.guitar_id']]]),
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
    ->assert('all 6 attacks blocked', fn($r) => $r, 6)
    ->start(null, $config);

// Final integrity check — the database is intact after all attacks
CTGTest::init('final integrity — guitars table intact after all attacks')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn($db) => [
        'count' => (int)$db->run('SELECT COUNT(*) as cnt FROM guitars')[0]['cnt'],
        'columns' => $db->run("SHOW COLUMNS FROM guitars"),
    ])
    ->assert('still has 9 rows', fn($r) => $r['count'], 9)
    ->assert('still has 5 columns', fn($r) => count($r['columns']), 5)
    ->start(null, $config);

CTGTest::init('final integrity — pickups table intact after all attacks')
    ->stage('connect', fn($_) => CTGDB::connect($dbHost, $dbName, $dbUser, $dbPass))
    ->stage('execute', fn($db) => [
        'count' => (int)$db->run('SELECT COUNT(*) as cnt FROM pickups')[0]['cnt'],
    ])
    ->assert('still has pickup data', fn($r) => $r['count'] > 0, true)
    ->start(null, $config);
