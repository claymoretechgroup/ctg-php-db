<?php
declare(strict_types=1);


use CTG\Test\CTGTest;
use CTG\Test\CTGTestState;
use CTG\Test\Predicates\CTGTestPredicates;
use CTG\DB\CTGDBQuery;
use CTG\DB\CTGDBError;

$pipelines = [];

// Unit tests for CTGDBQuery — SQL generation, validation, operator handling
// These are pure unit tests: no database connection required.


// ═══════════════════════════════════════════════════════════════
// STATIC FACTORY
// ═══════════════════════════════════════════════════════════════

$pipelines[] = CTGTest::init('from — returns CTGDBQuery instance')
    ->stage('create', fn(CTGTestState $state) => CTGDBQuery::from('guitars'))
    ->assert('is CTGDBQuery', fn(CTGTestState $state) => $state->getSubject() instanceof CTGDBQuery, CTGTestPredicates::isTrue())
    ;

$pipelines[] = CTGTest::init('from — invalid table name throws INVALID_IDENTIFIER')
    ->stage('attempt', function(CTGTestState $state) {
        try {
            CTGDBQuery::from('guitars; DROP TABLE guitars;--');
            return 'no exception';
        } catch (CTGDBError $e) {
            return $e->type;
        }
    })
    ->assert('throws INVALID_IDENTIFIER', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals('INVALID_IDENTIFIER'))
    ;

// ═══════════════════════════════════════════════════════════════
// BASIC SELECT (toStatement)
// ═══════════════════════════════════════════════════════════════

$pipelines[] = CTGTest::init('toStatement — default SELECT *')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars`'))
    ->assert('values empty', fn(CTGTestState $state) => $state->getSubject()['values'], CTGTestPredicates::equals([]))
    ;

$pipelines[] = CTGTest::init('toStatement — specific columns')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')->columns('id', 'make', 'model')->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT `id`, `make`, `model` FROM `guitars`'))
    ->assert('values empty', fn(CTGTestState $state) => $state->getSubject()['values'], CTGTestPredicates::equals([]))
    ;

$pipelines[] = CTGTest::init('toStatement — table-qualified columns')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')->columns('guitars.id', 'guitars.make')->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT `guitars`.`id`, `guitars`.`make` FROM `guitars`'))
    ;

$pipelines[] = CTGTest::init('toStatement — aliased column')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')->columns('guitars.make as guitar_make')->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT `guitars`.`make` as `guitar_make` FROM `guitars`'))
    ;

$pipelines[] = CTGTest::init('toStatement — table wildcard')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')->columns('guitars.*')->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT `guitars`.* FROM `guitars`'))
    ;

$pipelines[] = CTGTest::init('toStatement — global wildcard')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')->columns('*')->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars`'))
    ;

$pipelines[] = CTGTest::init('columns — invalid column throws INVALID_IDENTIFIER')
    ->stage('attempt', function(CTGTestState $state) {
        try {
            CTGDBQuery::from('guitars')->columns('make; DROP TABLE guitars;--');
            return 'no exception';
        } catch (CTGDBError $e) {
            return $e->type;
        }
    })
    ->assert('throws INVALID_IDENTIFIER', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals('INVALID_IDENTIFIER'))
    ;

// ═══════════════════════════════════════════════════════════════
// WHERE CONDITIONS
// ═══════════════════════════════════════════════════════════════

$pipelines[] = CTGTest::init('where — single equality')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')
        ->where('make', '=', 'Fender', 'str')
        ->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` WHERE `make` = ?'))
    ->assert('values', fn(CTGTestState $state) => $state->getSubject()['values'], CTGTestPredicates::equals([['type' => 'str', 'value' => 'Fender']]))
    ;

$pipelines[] = CTGTest::init('where — multiple AND conditions')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')
        ->where('make', '=', 'Fender', 'str')
        ->where('color', '=', 'Sunburst', 'str')
        ->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` WHERE `make` = ? AND `color` = ?'))
    ->assert('values count', fn(CTGTestState $state) => count($state->getSubject()['values']), CTGTestPredicates::equals(2))
    ->assert('first value', fn(CTGTestState $state) => $state->getSubject()['values'][0], CTGTestPredicates::equals(['type' => 'str', 'value' => 'Fender']))
    ->assert('second value', fn(CTGTestState $state) => $state->getSubject()['values'][1], CTGTestPredicates::equals(['type' => 'str', 'value' => 'Sunburst']))
    ;

$pipelines[] = CTGTest::init('where — same column range')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')
        ->where('year_purchased', '>=', 2020, 'int')
        ->where('year_purchased', '<=', 2025, 'int')
        ->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` WHERE `year_purchased` >= ? AND `year_purchased` <= ?'))
    ->assert('first value', fn(CTGTestState $state) => $state->getSubject()['values'][0], CTGTestPredicates::equals(['type' => 'int', 'value' => 2020]))
    ->assert('second value', fn(CTGTestState $state) => $state->getSubject()['values'][1], CTGTestPredicates::equals(['type' => 'int', 'value' => 2025]))
    ;

$pipelines[] = CTGTest::init('where — operator >')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')->where('year_purchased', '>', 2020, 'int')->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` WHERE `year_purchased` > ?'))
    ;

$pipelines[] = CTGTest::init('where — operator <')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')->where('year_purchased', '<', 2020, 'int')->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` WHERE `year_purchased` < ?'))
    ;

$pipelines[] = CTGTest::init('where — operator >=')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')->where('year_purchased', '>=', 2020, 'int')->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` WHERE `year_purchased` >= ?'))
    ;

$pipelines[] = CTGTest::init('where — operator <=')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')->where('year_purchased', '<=', 2025, 'int')->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` WHERE `year_purchased` <= ?'))
    ;

$pipelines[] = CTGTest::init('where — operator !=')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')->where('make', '!=', 'Fender', 'str')->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` WHERE `make` != ?'))
    ;

$pipelines[] = CTGTest::init('where — operator LIKE')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')->where('make', 'LIKE', '%Fender%', 'str')->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` WHERE `make` LIKE ?'))
    ->assert('value', fn(CTGTestState $state) => $state->getSubject()['values'][0], CTGTestPredicates::equals(['type' => 'str', 'value' => '%Fender%']))
    ;

$pipelines[] = CTGTest::init('where — operator NOT LIKE')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')->where('make', 'NOT LIKE', '%Gibson%', 'str')->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` WHERE `make` NOT LIKE ?'))
    ;

$pipelines[] = CTGTest::init('where — operator IN')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')
        ->where('make', 'IN', ['Fender', 'Gibson', 'Ibanez'], 'str')
        ->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` WHERE `make` IN (?, ?, ?)'))
    ->assert('values count', fn(CTGTestState $state) => count($state->getSubject()['values']), CTGTestPredicates::equals(3))
    ->assert('first value', fn(CTGTestState $state) => $state->getSubject()['values'][0], CTGTestPredicates::equals(['type' => 'str', 'value' => 'Fender']))
    ->assert('second value', fn(CTGTestState $state) => $state->getSubject()['values'][1], CTGTestPredicates::equals(['type' => 'str', 'value' => 'Gibson']))
    ->assert('third value', fn(CTGTestState $state) => $state->getSubject()['values'][2], CTGTestPredicates::equals(['type' => 'str', 'value' => 'Ibanez']))
    ;

$pipelines[] = CTGTest::init('where — operator NOT IN')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')
        ->where('make', 'NOT IN', ['Fender', 'Gibson'], 'str')
        ->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` WHERE `make` NOT IN (?, ?)'))
    ->assert('values count', fn(CTGTestState $state) => count($state->getSubject()['values']), CTGTestPredicates::equals(2))
    ;

$pipelines[] = CTGTest::init('where — operator IS (NULL)')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')
        ->where('color', 'IS', null)
        ->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` WHERE `color` IS NULL'))
    ->assert('no values', fn(CTGTestState $state) => $state->getSubject()['values'], CTGTestPredicates::equals([]))
    ;

$pipelines[] = CTGTest::init('where — operator IS NOT (NULL)')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')
        ->where('color', 'IS NOT', null)
        ->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` WHERE `color` IS NOT NULL'))
    ->assert('no values', fn(CTGTestState $state) => $state->getSubject()['values'], CTGTestPredicates::equals([]))
    ;

$pipelines[] = CTGTest::init('where — operator BETWEEN')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')
        ->where('year_purchased', 'BETWEEN', [2020, 2025], 'int')
        ->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` WHERE `year_purchased` BETWEEN ? AND ?'))
    ->assert('values count', fn(CTGTestState $state) => count($state->getSubject()['values']), CTGTestPredicates::equals(2))
    ->assert('low bound', fn(CTGTestState $state) => $state->getSubject()['values'][0], CTGTestPredicates::equals(['type' => 'int', 'value' => 2020]))
    ->assert('high bound', fn(CTGTestState $state) => $state->getSubject()['values'][1], CTGTestPredicates::equals(['type' => 'int', 'value' => 2025]))
    ;

$pipelines[] = CTGTest::init('where — untyped value stored as-is')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')
        ->where('make', '=', 'Fender')
        ->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` WHERE `make` = ?'))
    ->assert('value stored as-is', fn(CTGTestState $state) => $state->getSubject()['values'][0], CTGTestPredicates::equals('Fender'))
    ;

$pipelines[] = CTGTest::init('where — invalid column throws INVALID_IDENTIFIER')
    ->stage('attempt', function(CTGTestState $state) {
        try {
            CTGDBQuery::from('guitars')->where('make; DROP TABLE', '=', 'x', 'str');
            return 'no exception';
        } catch (CTGDBError $e) {
            return $e->type;
        }
    })
    ->assert('throws INVALID_IDENTIFIER', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals('INVALID_IDENTIFIER'))
    ;

$pipelines[] = CTGTest::init('where — invalid operator throws INVALID_OPERATOR')
    ->stage('attempt', function(CTGTestState $state) {
        try {
            CTGDBQuery::from('guitars')->where('make', 'EVIL', 'x', 'str');
            return 'no exception';
        } catch (CTGDBError $e) {
            return $e->type;
        }
    })
    ->assert('throws INVALID_OPERATOR', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals('INVALID_OPERATOR'))
    ;

// ═══════════════════════════════════════════════════════════════
// JOINs
// ═══════════════════════════════════════════════════════════════

$pipelines[] = CTGTest::init('join — inner join')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')
        ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id'])
        ->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` INNER JOIN `pickups` ON `guitars`.`id` = `pickups`.`guitar_id`'))
    ->assert('values empty', fn(CTGTestState $state) => $state->getSubject()['values'], CTGTestPredicates::equals([]))
    ;

$pipelines[] = CTGTest::init('join — left join')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')
        ->join('pickups', 'left', ['guitars.id' => 'pickups.guitar_id'])
        ->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` LEFT JOIN `pickups` ON `guitars`.`id` = `pickups`.`guitar_id`'))
    ;

$pipelines[] = CTGTest::init('join — right join')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')
        ->join('pickups', 'right', ['guitars.id' => 'pickups.guitar_id'])
        ->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` RIGHT JOIN `pickups` ON `guitars`.`id` = `pickups`.`guitar_id`'))
    ;

$pipelines[] = CTGTest::init('join — cross join')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')
        ->join('pickups', 'cross', [])
        ->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` CROSS JOIN `pickups`'))
    ;

$pipelines[] = CTGTest::init('join — multiple joins in order')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('articles')
        ->join('categories', 'left', ['articles.category_id' => 'categories.id'])
        ->join('users', 'inner', ['articles.author_id' => 'users.id'])
        ->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `articles` LEFT JOIN `categories` ON `articles`.`category_id` = `categories`.`id` INNER JOIN `users` ON `articles`.`author_id` = `users`.`id`'))
    ;

$pipelines[] = CTGTest::init('join — composite ON (multiple column pairs)')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('orders')
        ->join('users', 'inner', [
            'orders.user_id'   => 'users.id',
            'orders.tenant_id' => 'users.tenant_id'
        ])
        ->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `orders` INNER JOIN `users` ON `orders`.`user_id` = `users`.`id` AND `orders`.`tenant_id` = `users`.`tenant_id`'))
    ;

$pipelines[] = CTGTest::init('join — invalid table throws INVALID_IDENTIFIER')
    ->stage('attempt', function(CTGTestState $state) {
        try {
            CTGDBQuery::from('guitars')->join('pickups; DROP TABLE', 'inner', ['guitars.id' => 'pickups.guitar_id']);
            return 'no exception';
        } catch (CTGDBError $e) {
            return $e->type;
        }
    })
    ->assert('throws INVALID_IDENTIFIER', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals('INVALID_IDENTIFIER'))
    ;

$pipelines[] = CTGTest::init('join — invalid join type throws INVALID_JOIN_TYPE')
    ->stage('attempt', function(CTGTestState $state) {
        try {
            CTGDBQuery::from('guitars')->join('pickups', 'EVIL', ['guitars.id' => 'pickups.guitar_id']);
            return 'no exception';
        } catch (CTGDBError $e) {
            return $e->type;
        }
    })
    ->assert('throws INVALID_JOIN_TYPE', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals('INVALID_JOIN_TYPE'))
    ;

$pipelines[] = CTGTest::init('join — invalid ON column throws INVALID_IDENTIFIER')
    ->stage('attempt', function(CTGTestState $state) {
        try {
            CTGDBQuery::from('guitars')->join('pickups', 'inner', ['guitars.id; DROP' => 'pickups.guitar_id']);
            return 'no exception';
        } catch (CTGDBError $e) {
            return $e->type;
        }
    })
    ->assert('throws INVALID_IDENTIFIER', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals('INVALID_IDENTIFIER'))
    ;

// ═══════════════════════════════════════════════════════════════
// ORDER BY
// ═══════════════════════════════════════════════════════════════

$pipelines[] = CTGTest::init('orderBy — single column default ASC')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')->orderBy('make')->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` ORDER BY `make` ASC'))
    ;

$pipelines[] = CTGTest::init('orderBy — explicit DESC')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')->orderBy('make', 'DESC')->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` ORDER BY `make` DESC'))
    ;

$pipelines[] = CTGTest::init('orderBy — multiple columns')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')
        ->orderBy('make', 'ASC')
        ->orderBy('year_purchased', 'DESC')
        ->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` ORDER BY `make` ASC, `year_purchased` DESC'))
    ;

$pipelines[] = CTGTest::init('orderBy — table-qualified')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')->orderBy('guitars.make')->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` ORDER BY `guitars`.`make` ASC'))
    ;

$pipelines[] = CTGTest::init('orderBy — invalid column throws INVALID_IDENTIFIER')
    ->stage('attempt', function(CTGTestState $state) {
        try {
            CTGDBQuery::from('guitars')->orderBy('make; DROP TABLE');
            return 'no exception';
        } catch (CTGDBError $e) {
            return $e->type;
        }
    })
    ->assert('throws INVALID_IDENTIFIER', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals('INVALID_IDENTIFIER'))
    ;

$pipelines[] = CTGTest::init('orderBy — invalid direction throws INVALID_SORT')
    ->stage('attempt', function(CTGTestState $state) {
        try {
            CTGDBQuery::from('guitars')->orderBy('make', 'RANDOM');
            return 'no exception';
        } catch (CTGDBError $e) {
            return $e->type;
        }
    })
    ->assert('throws INVALID_SORT', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals('INVALID_SORT'))
    ;

// ═══════════════════════════════════════════════════════════════
// GROUP BY
// ═══════════════════════════════════════════════════════════════

$pipelines[] = CTGTest::init('groupBy — single column')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')->groupBy('make')->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` GROUP BY `make`'))
    ;

$pipelines[] = CTGTest::init('groupBy — multiple columns')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')->groupBy('make', 'color')->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` GROUP BY `make`, `color`'))
    ;

$pipelines[] = CTGTest::init('groupBy — invalid column throws INVALID_IDENTIFIER')
    ->stage('attempt', function(CTGTestState $state) {
        try {
            CTGDBQuery::from('guitars')->groupBy('make; DROP TABLE');
            return 'no exception';
        } catch (CTGDBError $e) {
            return $e->type;
        }
    })
    ->assert('throws INVALID_IDENTIFIER', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals('INVALID_IDENTIFIER'))
    ;

// ═══════════════════════════════════════════════════════════════
// LIMIT / OFFSET / PAGE
// ═══════════════════════════════════════════════════════════════

$pipelines[] = CTGTest::init('limit — produces LIMIT clause')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')->limit(10)->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` LIMIT 10'))
    ;

$pipelines[] = CTGTest::init('offset — produces OFFSET clause')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')->offset(20)->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` OFFSET 20'))
    ;

$pipelines[] = CTGTest::init('limit + offset — both present')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')->limit(10)->offset(20)->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` LIMIT 10 OFFSET 20'))
    ;

$pipelines[] = CTGTest::init('page — page 1 default perPage')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')->page(1)->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` LIMIT 20 OFFSET 0'))
    ;

$pipelines[] = CTGTest::init('page — page 3 with perPage 10')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')->page(3, 10)->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` LIMIT 10 OFFSET 20'))
    ;

$pipelines[] = CTGTest::init('page — page 2 default perPage 20')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')->page(2)->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` LIMIT 20 OFFSET 20'))
    ;

$pipelines[] = CTGTest::init('page — overrides limit/offset')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')
        ->limit(5)
        ->offset(10)
        ->page(2, 15)
        ->toStatement())
    ->assert('sql uses page values', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` LIMIT 15 OFFSET 15'))
    ;

$pipelines[] = CTGTest::init('limit/offset — overrides page')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')
        ->page(3, 10)
        ->limit(5)
        ->offset(0)
        ->toStatement())
    ->assert('sql uses limit/offset values', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT * FROM `guitars` LIMIT 5 OFFSET 0'))
    ;

// ═══════════════════════════════════════════════════════════════
// toCountStatement
// ═══════════════════════════════════════════════════════════════

$pipelines[] = CTGTest::init('toCountStatement — basic count')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')->toCountStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT COUNT(*) as total FROM `guitars`'))
    ->assert('values empty', fn(CTGTestState $state) => $state->getSubject()['values'], CTGTestPredicates::equals([]))
    ;

$pipelines[] = CTGTest::init('toCountStatement — strips ORDER BY')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')
        ->orderBy('make')
        ->toCountStatement())
    ->assert('sql has no ORDER BY', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT COUNT(*) as total FROM `guitars`'))
    ;

$pipelines[] = CTGTest::init('toCountStatement — strips LIMIT/OFFSET')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')
        ->limit(10)
        ->offset(20)
        ->toCountStatement())
    ->assert('sql has no LIMIT/OFFSET', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT COUNT(*) as total FROM `guitars`'))
    ;

$pipelines[] = CTGTest::init('toCountStatement — preserves WHERE')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')
        ->where('make', '=', 'Fender', 'str')
        ->orderBy('make')
        ->limit(10)
        ->toCountStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT COUNT(*) as total FROM `guitars` WHERE `make` = ?'))
    ->assert('values preserved', fn(CTGTestState $state) => $state->getSubject()['values'], CTGTestPredicates::equals([['type' => 'str', 'value' => 'Fender']]))
    ;

$pipelines[] = CTGTest::init('toCountStatement — preserves JOINs')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')
        ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id'])
        ->where('guitars.make', '=', 'Fender', 'str')
        ->toCountStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT COUNT(*) as total FROM `guitars` INNER JOIN `pickups` ON `guitars`.`id` = `pickups`.`guitar_id` WHERE `guitars`.`make` = ?'))
    ->assert('values preserved', fn(CTGTestState $state) => $state->getSubject()['values'], CTGTestPredicates::equals([['type' => 'str', 'value' => 'Fender']]))
    ;

$pipelines[] = CTGTest::init('toCountStatement — preserves GROUP BY (wraps in subquery)')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')
        ->columns('make')
        ->groupBy('make')
        ->toCountStatement())
    ->assert('sql wraps in subquery', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT COUNT(*) as total FROM (SELECT `make` FROM `guitars` GROUP BY `make`) as _counted'))
    ;

// ═══════════════════════════════════════════════════════════════
// FULL QUERY COMPOSITION
// ═══════════════════════════════════════════════════════════════

$pipelines[] = CTGTest::init('full query — columns, where, join, orderBy, groupBy, limit')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')
        ->join('pickups', 'inner', ['guitars.id' => 'pickups.guitar_id'])
        ->columns('guitars.make', 'pickups.type')
        ->where('guitars.year_purchased', '>=', 2020, 'int')
        ->groupBy('guitars.make')
        ->orderBy('guitars.make', 'ASC')
        ->limit(10)
        ->offset(5)
        ->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT `guitars`.`make`, `pickups`.`type` FROM `guitars` INNER JOIN `pickups` ON `guitars`.`id` = `pickups`.`guitar_id` WHERE `guitars`.`year_purchased` >= ? GROUP BY `guitars`.`make` ORDER BY `guitars`.`make` ASC LIMIT 10 OFFSET 5'))
    ->assert('values', fn(CTGTestState $state) => $state->getSubject()['values'], CTGTestPredicates::equals([['type' => 'int', 'value' => 2020]]))
    ;

$pipelines[] = CTGTest::init('full query — join + where + pagination')
    ->stage('build', fn(CTGTestState $state) => CTGDBQuery::from('guitars')
        ->join('pickups', 'left', ['guitars.id' => 'pickups.guitar_id'])
        ->columns('guitars.*', 'pickups.type as pickup_type')
        ->where('guitars.make', '=', 'Fender', 'str')
        ->where('guitars.year_purchased', '>=', 2020, 'int')
        ->page(2, 10)
        ->toStatement())
    ->assert('sql', fn(CTGTestState $state) => $state->getSubject()['sql'], CTGTestPredicates::equals('SELECT `guitars`.*, `pickups`.`type` as `pickup_type` FROM `guitars` LEFT JOIN `pickups` ON `guitars`.`id` = `pickups`.`guitar_id` WHERE `guitars`.`make` = ? AND `guitars`.`year_purchased` >= ? LIMIT 10 OFFSET 10'))
    ->assert('values count', fn(CTGTestState $state) => count($state->getSubject()['values']), CTGTestPredicates::equals(2))
    ->assert('first value', fn(CTGTestState $state) => $state->getSubject()['values'][0], CTGTestPredicates::equals(['type' => 'str', 'value' => 'Fender']))
    ->assert('second value', fn(CTGTestState $state) => $state->getSubject()['values'][1], CTGTestPredicates::equals(['type' => 'int', 'value' => 2020]))
    ;

// ═══════════════════════════════════════════════════════════════
// SECURITY — INJECTION VIA CTGDBQuery
// All user-controlled entry points must reject malicious input.
// No database connection is needed — validation happens at build time.
// ═══════════════════════════════════════════════════════════════

$identifierPayloads = [
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

// ── Malicious table name in from() ──────────────────────────────

foreach ($identifierPayloads as $label => $payload) {
    $pipelines[] = CTGTest::init("injection — from() with {$label}")
        ->stage('attempt', function(CTGTestState $state) use ($payload){
            try {
                CTGDBQuery::from($payload);
                return 'NOT BLOCKED';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('blocked as INVALID_IDENTIFIER', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals('INVALID_IDENTIFIER'))
        ;
}

// ── Malicious column in columns() ───────────────────────────────

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
    $pipelines[] = CTGTest::init("injection — columns() with {$label}")
        ->stage('attempt', function(CTGTestState $state) use ($payload){
            try {
                CTGDBQuery::from('guitars')->columns($payload);
                return 'NOT BLOCKED';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('blocked as INVALID_IDENTIFIER', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals('INVALID_IDENTIFIER'))
        ;
}

// ── Malicious column in where() ─────────────────────────────────

foreach ($columnPayloads as $label => $payload) {
    $pipelines[] = CTGTest::init("injection — where() column with {$label}")
        ->stage('attempt', function(CTGTestState $state) use ($payload){
            try {
                CTGDBQuery::from('guitars')->where($payload, '=', 'x', 'str');
                return 'NOT BLOCKED';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('blocked as INVALID_IDENTIFIER', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals('INVALID_IDENTIFIER'))
        ;
}

// ── Malicious operator in where() ───────────────────────────────

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
];

foreach ($operatorPayloads as $label => $payload) {
    $pipelines[] = CTGTest::init("injection — where() operator with {$label}")
        ->stage('attempt', function(CTGTestState $state) use ($payload){
            try {
                CTGDBQuery::from('guitars')->where('make', $payload, 'x', 'str');
                return 'NOT BLOCKED';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('blocked as INVALID_OPERATOR', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals('INVALID_OPERATOR'))
        ;
}

// ── Malicious column in orderBy() ───────────────────────────────

foreach ($columnPayloads as $label => $payload) {
    $pipelines[] = CTGTest::init("injection — orderBy() with {$label}")
        ->stage('attempt', function(CTGTestState $state) use ($payload){
            try {
                CTGDBQuery::from('guitars')->orderBy($payload);
                return 'NOT BLOCKED';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('blocked as INVALID_IDENTIFIER', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals('INVALID_IDENTIFIER'))
        ;
}

// ── Malicious direction in orderBy() ────────────────────────────

$sortDirPayloads = [
    'DROP TABLE'        => 'ASC; DROP TABLE guitars;--',
    'stacked query'     => 'ASC; SELECT 1;',
    'subquery'          => '(SELECT 1)',
    'comment'           => 'ASC --',
    'sleep'             => 'ASC, SLEEP(5)',
    'RANDOM'            => 'RANDOM',
    'comma injection'   => 'ASC, make',
    'empty string'      => '',
];

foreach ($sortDirPayloads as $label => $payload) {
    $pipelines[] = CTGTest::init("injection — orderBy() direction with {$label}")
        ->stage('attempt', function(CTGTestState $state) use ($payload){
            try {
                CTGDBQuery::from('guitars')->orderBy('make', $payload);
                return 'NOT BLOCKED';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('blocked as INVALID_SORT', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals('INVALID_SORT'))
        ;
}

// ── Malicious column in groupBy() ───────────────────────────────

foreach ($columnPayloads as $label => $payload) {
    $pipelines[] = CTGTest::init("injection — groupBy() with {$label}")
        ->stage('attempt', function(CTGTestState $state) use ($payload){
            try {
                CTGDBQuery::from('guitars')->groupBy($payload);
                return 'NOT BLOCKED';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('blocked as INVALID_IDENTIFIER', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals('INVALID_IDENTIFIER'))
        ;
}

// ── Malicious join table ────────────────────────────────────────

foreach ($identifierPayloads as $label => $payload) {
    $pipelines[] = CTGTest::init("injection — join() table with {$label}")
        ->stage('attempt', function(CTGTestState $state) use ($payload){
            try {
                CTGDBQuery::from('guitars')->join($payload, 'inner', ['guitars.id' => 'pickups.guitar_id']);
                return 'NOT BLOCKED';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('blocked as INVALID_IDENTIFIER', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals('INVALID_IDENTIFIER'))
        ;
}

// ── Malicious join type ─────────────────────────────────────────

$joinTypePayloads = [
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

foreach ($joinTypePayloads as $label => $payload) {
    $pipelines[] = CTGTest::init("injection — join() type with {$label}")
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

// ── Malicious ON column in join() ───────────────────────────────

$onPayloads = [
    'DROP TABLE'    => 'guitars.id; DROP TABLE guitars;--',
    'OR bypass'     => 'guitars.id OR 1=1',
    'subquery'      => '(SELECT 1)',
    'UNION'         => 'guitars.id UNION SELECT 1',
    'comment'       => 'guitars.id/**/OR/**/1=1',
    'single quote'  => "guitars.id' OR '1'='1",
];

foreach ($onPayloads as $label => $payload) {
    $pipelines[] = CTGTest::init("injection — join() ON left column with {$label}")
        ->stage('attempt', function(CTGTestState $state) use ($payload){
            try {
                CTGDBQuery::from('guitars')->join('pickups', 'inner', [$payload => 'pickups.guitar_id']);
                return 'NOT BLOCKED';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('blocked as INVALID_IDENTIFIER', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals('INVALID_IDENTIFIER'))
        ;
}

foreach ($onPayloads as $label => $payload) {
    $pipelines[] = CTGTest::init("injection — join() ON right column with {$label}")
        ->stage('attempt', function(CTGTestState $state) use ($payload){
            try {
                CTGDBQuery::from('guitars')->join('pickups', 'inner', ['guitars.id' => $payload]);
                return 'NOT BLOCKED';
            } catch (CTGDBError $e) {
                return $e->type;
            }
        })
        ->assert('blocked as INVALID_IDENTIFIER', fn(CTGTestState $state) => $state->getSubject(), CTGTestPredicates::equals('INVALID_IDENTIFIER'))
        ;
}

return $pipelines;
