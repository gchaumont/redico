<?php

namespace Redico\Query;

use Enum;
use stdClass;
use Exception;
use BackedEnum;
use Illuminate\Support\Arr;
use Redico\Query\Term\Term;
use Redico\Query\Term\Range;
use Redico\Query\Term\Terms;
use Redico\Scripting\Script;
use Redico\Query\Term\Exists;
use Redico\Query\Term\Prefix;
use Redico\Index\Fields\Field;
use Redico\Query\Term\Wildcard;
use Redico\Query\Compound\Boolean;
use Redico\Query\Compound\FunctionScore;
use Redico\Query\Specialized\RankFeature;
use Illuminate\Database\Query\Builder as BaseBuilder;
use Illuminate\Database\Query\Grammars\Grammar as BaseGrammar;
use Illuminate\Support\Collection;
use Redico\Index\Fields\NumericField;
use Redico\Index\Fields\TagField;
use Redico\Index\Fields\TextField;

/*
 *  Elasticsearch Query Builder
 *  Extension of Larvel Database Query Builder.
 */

class Grammar extends BaseGrammar
{
    const NULL = '_null_';

    public function compileSelect(BaseBuilder $query)
    {
        $payload['index'] = $query->from;

        $payload['query'] =   $this->compileWhereComponents($query);


        if (isset($query->orders)) {
            $payload = array_merge($payload, $this->compileOrderComponents($query));
        }

        if (isset($query->offset) || isset($query->limit)) {
            $payload['LIMIT'] = 'LIMIT';
            $payload['LIMIT_OFFSET'] = $query->offset ?? 0;
            $payload['LIMIT_COUNT'] = $query->limit ?? 'inf';
        }


        if ($query->columns && '*' != $query->columns[0]) {
            $payload['RETURN'] = 'RETURN';
            $payload['RETURN_COUNT'] = count($query->columns);
            $payload['RETURN_FIELDS'] = implode(' ', $query->columns);
        }


        return array_values($payload);
    }

    public function compileCount(BaseBuilder $query)
    {
        $payload['index'] = $query->from;

        $payload['query'] = $this->compileWhereComponents($query);

        $payload['LIMIT'] = 'LIMIT';
        $payload['LIMIT_OFFSET'] = 0;
        $payload['LIMIT_COUNT'] = 0;

        $payload['NOCONTENT'] = 'NOCONTENT';

        return array_values($payload);
    }

    public function compileDelete(BaseBuilder $query)
    {
        return $this->compileSelect($query);
    }

    public function compileDeleteMany(BaseBuilder $query, iterable $ids)
    {
        /** @var Builder $query */

        return [
            'options' => [
                'ignore_conflicts' => $query->ignore_conflicts,
            ],
            'body' => collect($ids)
                ->flatMap(static fn ($val): array => [
                    [
                        'delete' => [
                            '_id' => $val,
                            '_index' => $query->from,
                        ],
                    ],
                ])
                ->all(),

        ];
    }



    /**
     * Compile the random statement into SQL.
     *
     * @param string $seed
     *
     * @return string
     */
    public function compileRandom($seed)
    {
        return (new FunctionScore())->randomScore();
    }

    public function compileGet($from, $id, $columns)
    {
        return [$from . '::' . $id];
    }

    public function compileFindMany($from, $ids, $columns)
    {
        return [
            'body' => [
                'docs' => collect($ids)
                    ->map(fn (string|int $id) => array_filter([
                        '_index' => $from,
                        '_id' => $id,
                        '_source' => $columns,
                    ]))
                    ->values()
                    ->all(),
            ],
        ];
    }

    public function compileSelectMany($queries)
    {
        return [
            'body' => collect($queries)
                ->flatMap(fn ($query) => [
                    ['index' => $query->from ?? $query->getQuery()->from],
                    $query->toSql()['body'],
                ])
                ->all(),
        ];
    }

    /**
     * Compile an update statement into SQL.
     *
     * @return string
     */
    public function compileUpdate(BaseBuilder $query, array|Script $values)
    {

        $key = Arr::pull($values, '_key');

        $attrs = collect($values)
            ->map(fn ($value) => is_null($value) ? static::NULL : $value)
            ->map(fn ($value, $key) => [$key, $value])
            ->flatten()
            ->all();





        return [$key, ...$attrs];
    }



    /**
     * Compile an insert statement into SQL.
     *
     * @return string
     */
    public function compileInsert(BaseBuilder $query, array $values)
    {
        // Essentially we will force every insert to be treated as a batch insert which
        // simply makes creating the SQL easier for us since we can utilize the same
        // basic routine regardless of an amount of records given to us to insert.

        /**  @var Builder $query*/

        foreach ($values  as $i => $attributes) {
            $key = Arr::pull($attributes, '_key');
            # get standard array into => [key, val, key, val, key, val]
            $attrs = collect($attributes)
                ->map(fn ($value) => is_null($value) ? 'null' : $value)
                ->map(fn ($value) => (string) $value)
                ->map(fn ($value, $key) => [$key, $value])
                ->flatten()
                ->all();

            $values[$i] = [$key, ...$attrs];
        }

        return $values;
    }

    /**
     * Compile an exists statement into SQL.
     *
     * @return string
     */
    public function compileExists(BaseBuilder $query)
    {
        $payload = $this->compileSelect($query->take(0));
        $payload['terminate_after'] = 1;

        return $payload;
    }

    public function compileSuggestComponents(Builder $query): array
    {
        $suggest = [];
        if (!empty($query->suggest)) {
            foreach ($query->suggest as $suggestion) {
                $suggest[$suggestion['name']] = [
                    'text' => $suggestion['text'],
                    $suggestion['type'] => array_filter([
                        'field' => $suggestion['field'],
                        'size' => $suggestion['size'],
                        'sort' => $suggestion['sort'],
                        'suggest_mode' => $suggestion['mode'],
                        'min_doc_freq' => $suggestion['min_doc_freq'],
                    ]),
                ];
            }
        }

        return $suggest;
    }

    public function compileOrderComponents(Builder $query): array
    {
        $sorts = [];
        if (!empty($query->orders)) {
            foreach ($query->orders as $order) {
                if (!empty($order['type']) && 'Raw' == $order['type']) {
                    throw new \Exception('TODO: allow raw arrays');
                } else {
                    $sorts['SORTBY'] = 'SORTBY';
                    $sorts['SORTBY_FIELD'] = $order['column'];
                    $sorts['SORTBY_ORDER'] = strtoupper($order['direction']);

                    return $sorts;
                    // $sorts[] = [
                    //     (string) $order['column'] => array_filter([
                    //         'order' => $order['direction'],
                    //         'missing' => $order['missing'] ?? null,
                    //         'mode' => $order['mode'] ?? null,
                    //         'nested' => $order['nested'] ?? null,
                    //     ]),
                    // ];
                }
            }
        }

        return $sorts;
    }

    public function compileWhereComponents(Builder $query): string
    {

        if (empty($query->wheres)) {
            return '*';
        }

        return collect($query->wheres)
            ->map(function ($where) use ($query): string {

                if (str_starts_with($where['column'], $query->from . '.')) {
                    $where['column'] = substr($where['column'], strlen($query->from . '.'));
                }

                $field = $query
                    ->indexConfig
                    ->getFields()
                    ->first(fn (Field $f) => $f->getName() === $where['column']);


                if ($field instanceof TextField) {
                    if ($where['type'] == 'NotNull') {
                        return  '-@' . $where['column'] . ':' . static::NULL;
                    }
                    if ($where['type'] == 'Null') {
                        return  '@' . $where['column'] . ':' . static::NULL;
                    }
                    return  '@' . $where['column'] . ':"' . $where['value'] . '"';
                }

                if ($field instanceof NumericField) {
                    $range = match ($where['operator']) {
                        '>' => "({$where['value']} +inf",
                        '>=' => "{$where['value']} +inf",
                        '<' => "-inf {$where['value']})",
                        '<=' => "-inf {$where['value']}",
                    };

                    return "@{$where['column']}:[$range]";
                }

                if ($field instanceof TagField) {

                    $tags = Collection::wrap($where['value'] ?? $where['values'])
                        ->map(function (mixed $value): string {
                            if (is_object($value)) {
                                if ($value instanceof BackedEnum) {
                                    return  $value->value;
                                } elseif (enum_exists($value)) {
                                    return $value->name;
                                }
                            }
                            return $value;
                        })
                        // ->map(fn (string $value) =>  '"' . $value . '"')
                        ->implode(' | ');

                    return "@{$where['column']}:{ $tags }";
                }



                throw new Exception("Unknown field: {$where['column']}");
            })
            ->implode(' ');

        // WHERE Types
        // - Basic
        // - In
        // - NotIn
        // - Null
        // - NotNull
        // - Between
        // - Date
        // - Month
        // - Day
        // - Year
        // - Time
        // - Raw
        // - Nested

        $orWheres = collect($query->wheres)
            ->chunkWhile(fn ($where) => 'or' != $where['boolean']);

        foreach ($orWheres as $whereGroup) {
            $bool->should($groupBool = Boolean::make());
            foreach ($whereGroup as $where) {
                if ('raw' == $where['type']) {
                    if ($where['sql'] instanceof Query) {
                        $groupBool->must($where['sql']);
                    } elseif (is_array($where['sql'])) {
                        throw new \Exception('TODO: allow raw arrays');
                    }

                    continue;
                }

                if ('Nested' == $where['type']) {
                    $groupBool->must($where['query']->getGrammar()->compileWhereComponents($where['query']));

                    continue;
                }

                if (in_array($where['type'], ['Null', 'NotNull'])) {
                    $notNull = (new Exists(field: $where['column']));
                    if ('NotNull' == $where['type']) {
                        $groupBool->filter($notNull);
                    } else {
                        $groupBool->filter((new Boolean())->mustNot($notNull));
                    }

                    continue;
                }

                if (in_array($where['type'], ['Exists', 'NotExists'])) {
                    // dump($where, $query);

                    throw new \Exception('Elasticsearch does not support Exists/NotExists');
                }

                if (in_array($where['type'], ['FullText'])) {
                    throw new \Exception('TODO');
                }

                $field = $where['column'];

                if ('between' == $where['type']) {
                    $act = $where['not'] ? 'mustNot' : 'filter';

                    $groupBool->{$act}(
                        Boolean::make()
                            ->filter((new Range(field: $field))->gt($where['values'][0]))
                            ->filter((new Range(field: $field))->lt($where['values'][1]))
                    );

                    continue;
                }
                if ('In' == $where['type']) {
                    if (!empty($where['values'])) {
                        $groupBool->filter(
                            new Terms(
                                field: $where['column'],
                                values: array_values($where['values'])
                            )
                        );
                    }

                    continue;
                }
                if ($where['type'] == 'NotIn') {
                    if (!empty($where['values'])) {
                        $groupBool->mustNot(
                            new Terms(
                                field: $where['column'],
                                values: array_values($where['values'])
                            )
                        );
                    }

                    continue;
                }
                // if (empty($where['operator'])) {
                //     dd($where);
                // }

                $operator = $where['operator'];
                $value = $where['value'];


                if (is_object($value)) {
                    if ($value instanceof BackedEnum) {
                        $value = $value->value;
                    } elseif (enum_exists($value)) {
                        $value = $value->name;
                    }
                }


                match ($where['operator']) {
                    '>' => $groupBool->filter((new Range(field: $field))->gt($value)),
                    '>=' => $groupBool->filter((new Range(field: $field))->gte($value)),
                    '<' => $groupBool->filter((new Range(field: $field))->lt($value)),
                    '<=' => $groupBool->filter((new Range(field: $field))->lte($value)),
                    '<>' => $groupBool->filter(
                        Boolean::make()->mustNot(new Term(field: $field, value: $value))
                    ),
                    '=' => match (is_array($value)) {
                        true => $groupBool->must(new Terms(field: $field, values: $value)),
                        false => $groupBool->must(new Term(field: $field, value: $value)),
                    },
                    // 'like' => $groupBool->must(( Term::make())->field($field)->value(trim(strtolower($value), '%'))),
                    // 'like' => $groupBool->must(( Wildcard::make())->field($field)->value(trim(strtolower($value), '%'))),
                    'like' => $groupBool->must(new Prefix(field: $field, value: trim($value, '%'))),
                    // 'like' => $groupBool->must(Term::make()->field($field)->value(trim($value, '%'))),
                    'rank' => $groupBool->should(new RankFeature(field: $field, boost: $value)),
                };
            }
        }

        // dump($bool);

        return $bool;
    }

    public function compileUpsert(BaseBuilder $query, array $values, array $uniqueBy, array|null $update)
    {

        /** @var Builder $query */

        return [
            'options' => [
                'ignore_conflicts' => $query->ignore_conflicts,
            ],
            'body' => collect($values)
                ->flatMap(static function (array $val, int $i) use ($update): array {
                    $id = Arr::pull($val, '_id');
                    if (empty($id)) {
                        throw new \Exception('All upserts must have an _id');
                    }

                    $header = [
                        'update' => [
                            '_id' => $id,
                            '_index' => Arr::pull($val, '_index'),
                        ],
                    ];

                    $body = match (true) {
                        empty($update) => [
                            'doc' => empty($val) ? new stdClass() : $val,
                            'doc_as_upsert' => true,
                        ],
                        $update[$i] instanceof Script => [
                            'scripted_upsert' => true,
                            'script' => $update[$i]->compile(),
                            'upsert' => $val,
                        ],
                        default => throw new \Exception('TODO'),
                    };

                    return [$header, $body];
                })
                ->all(),
        ];
    }


    public function compileBulkOperation(
        BaseBuilder $query,
        iterable $models,
        string $operation,
        null|array $scripts = null,
        bool $doc_as_upsert = false,
        bool $scripted_upsert = false
    ): array {
        /** @var Builder $query */

        if (!in_array($operation, ['create', 'index', 'update', 'delete'])) {
            throw new Exception("Invalid Elastic operation [$operation]");
        }
        if ($scripts) {
            if (($operation !== 'update')) {
                throw new Exception('Script can only be used with update operation');
            }
            if (count($scripts) !== count($models)) {
                throw new Exception('There must be a script for each model');
            }
        }


        return [
            'options' => [
                'ignore_conflicts' => $query->ignore_conflicts,
            ],
            'body' => collect($models)
                ->flatMap(static function (object $model, $i) use ($query, $operation, $scripts, $doc_as_upsert, $scripted_upsert): array {
                    $id = is_string($model) ? $model : $model['_id'];

                    $document = Arr::except($model, ['_id', '_index']) ?: new stdClass();

                    $header = [
                        $operation => [
                            '_id' => $id,
                            '_index' => $model['_index'] ?? $query->from,
                        ],
                    ];
                    if ($operation === 'delete') {
                        return [$header];
                    }

                    if (!empty($scripts)) {
                        return [$header, [
                            'scripted_upsert' => $scripted_upsert,
                            'script' => $scripts[$i]->compile(),
                            'upsert' => $document,
                        ]];
                    }

                    return [$header, [
                        'doc_as_upsert' => $doc_as_upsert,
                        'doc' => $document,
                    ]];
                })
                ->all()
        ];
    }
}
