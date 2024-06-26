<?php

namespace Redico;

use Exception;
use Redico\Query\Builder;
use Redico\Eloquent\Model;
use Illuminate\Support\Arr;
use GuzzleHttp\Promise\Promise;
use Elastic\Elasticsearch\Client;
use Illuminate\Support\Facades\Redis;
use Redico\Exceptions\BulkException;
use Illuminate\Support\LazyCollection;
use Redico\Query\Response\Collection;
use Illuminate\Database\QueryException;
use Elastic\Elasticsearch\ClientBuilder;
use Illuminate\Database\ConnectionInterface;
use Illuminate\Database\Events\QueryExecuted;
use Redico\Exceptions\IndexNotFoundException;
use Elastic\Elasticsearch\Response\Elasticsearch;
use Http\Adapter\Guzzle7\Client as GuzzleAdapter;
use Illuminate\Database\Connection as BaseConnection;
use Illuminate\Database\Eloquent\ModelNotFoundException;
use Illuminate\Contracts\Redis\Connection as RedisConnection;
use Redico\Client as RedicoClient;

/**
 * Keeps the Client and performs the queries
 * Takes care of monitoring all queries.
 */
class Connection extends BaseConnection implements ConnectionInterface
{
    const DEFAULT_CURSOR_SIZE = 1000;

    protected $client;

    public function __construct($config)
    {
        $this->config = $config;

        $this->database = $config['database'] ?? null;

        $this->useDefaultPostProcessor();

        $this->useDefaultQueryGrammar();
    }


    /**
     * Get a new query builder instance.
     *
     * @return \Illuminate\Database\Query\Builder
     */
    public function query()
    {
        return new Builder(
            $this,
            $this->getQueryGrammar(),
            $this->getPostProcessor()
        );
    }

    /**
     * {@inheritdoc}
     */
    public function getDriverName()
    {
        return 'redico';
    }

    /**
     * Custom Function.
     *
     * @param mixed $method
     * @param mixed $payload
     */
    public function performQuery(string $method, array $payload)
    {

        return $this->getClient()->command($method, $payload);


        $identifier = $method . '.' . rand(0, 10000000);

        $this->startingQuery(endpoint: $method, identifier: $identifier);

        $response = $this->getClient()->{$method}($payload);

        if ($response instanceof Promise) {
            $handlePromise = fn ($response) => $this->endingQuery(
                method: $method,
                payload: $payload,
                response: json_decode((string) $response->getBody(), true),
                identifier: $identifier
            );
            $response->then($handlePromise, $handlePromise);
        } elseif ($response instanceof Elasticsearch) {
            $response = json_decode((string) $response->getBody(), true);

            $this->endingQuery(
                method: $method,
                payload: $payload,
                response: $response,
                identifier: $identifier
            );
        } else {
            throw new \RuntimeException('Unsupported Elasticsearch Response');
        }

        return $response;
    }

    public function find($query)
    {
        $query = [
            'method' => 'hgetall',
            'payload' => $query,
        ];

        return $this->run($query, [], function ($query, $bindings) {

            $response = $this->performQuery($query['method'], $query['payload']);
            if (empty($response)) {
                return null;
            }
            return $response;
        });
    }

    public function findMany($query)
    {
        if (collect($query['body']['docs'])->isEmpty()) {
            return new LazyCollection();
        }

        $query = [
            'method' => 'mget',
            'payload' => $query,
        ];

        return $this->run($query, [], function ($query, $bindings) {
            return $this->performQuery($query['method'], $query['payload']);
        });
    }

    public function count($query)
    {
        $query = [
            'method' => 'FT.SEARCH',
            'payload' => $query,
        ];

        return $this->run($query, [], function ($query, $bindings) {

            $response = $this->performQuery($query['method'], $query['payload']);

            return $response;
        });
    }

    public function bulk($query)
    {
        $query = [
            'method' => 'bulk',
            'payload' => $query,
        ];


        return $this->run($query, [], function ($query, $bindings) {
            $response = $this->performQuery($query['method'], $query['payload']);

            if ($response['errors'] ?? false) {

                throw new BulkException(json_encode($response->asArray()));
            }

            return $response;
        });
    }

    public function termsEnum(string $index, string $field)
    {
        $query = [
            'method' => 'FT.TAGVALS',
            'payload' => [
                $index, $field
            ],
        ];

        return $this->run($query, [], function ($query, $bindings) {
            return $this->performQuery($query['method'], $query['payload']);
        });
    }

    /**
     * Run a select statement against the database.
     *
     * @param string $query
     * @param array  $bindings
     * @param bool   $useReadPdo
     *
     * @return array
     */
    public function select($query, $bindings = [], $useReadPdo = true)
    {
        $query = [
            'method' => 'FT.SEARCH',
            'payload' => $query,
        ];

        return $this->run($query, $bindings, function ($query, $bindings) {
            if ($this->pretending()) {
                return [0, []];
            }

            $response = $this->getClient()->command($query['method'], $query['payload']);

            return $response;
        });
    }

    public function selectMany($queries)
    {
        $query = [
            'method' => 'msearch',
            'payload' => $queries,
        ];

        return $this->run($query, [], function ($query, $bindings) {
            if ($this->pretending()) {
                return [];
            }

            $responses = $this->performQuery($query['method'], $query['payload']);
            $responses = $this->getPostProcessor()->resolvePromise($responses);

            foreach ($responses['responses'] as $response) {

                if ($response['error'] ?? false) {
                    throw new QueryException(
                        $this->getDriverName(),
                        mb_substr(json_encode($query), 0, 500),
                        $this->prepareBindings($bindings),
                        new Exception(mb_substr(json_encode($response['error']), 0, 500)) // new Exception(($response['error']['reason'] ?? substr(json_encode($response['error']), 0, 100)) . ': ' . ($response['error']['root_cause'][0]['reason'] ?? ''))
                    );
                }
            }
            return $responses;
        });
    }

    /**
     * Run a select statement against the database and returns a generator.
     *
     * @param string $query
     * @param array  $bindings
     * @param bool   $useReadPdo
     * @param mixed  $keepAlive
     */
    public function cursor($query, $bindings = [], $useReadPdo = true, $keepAlive = '1m'): \Generator
    {
        $total = null;
        $payload = null;

        $response = $this->run($query, $bindings, function ($query, $bindings) use (&$total, $keepAlive, &$payload) {
            if ($this->pretending()) {
                return [];
            }

            $payload = $query;
            // $payload['scroll'] = $seconds.'s';
            $payload['body']['size'] ??= static::DEFAULT_CURSOR_SIZE;
            $payload['body']['sort'] ??= '_shard_doc';

            $pit = $this->performQuery('openPointInTime', [
                'index' => $payload['index'],
                'keep_alive' => $keepAlive,
            ]);

            if ($pit instanceof Promise) {
                $pit = $pit->wait()->asArray();
            }
            if ($pit instanceof Elasticsearch) {
                $pit = $pit->asArray();
            }

            $pit['keep_alive'] = $keepAlive;

            $payload['body']['pit'] = $pit;
            unset($payload['index']);

            return $this->performQuery('search', $payload);
        });

        yield from $response['hits']['hits'];

        $total = $response['hits']['total']['value'];

        while ($total) {
            // if (!empty($query['body']['query'])) {
            //     $payload['body']['query'] = $query['body']['query'];
            // }
            $payload['body']['pit']['id'] = $response['pit_id'];
            $payload['body']['search_after'] = $response['hits']['hits'][count($response['hits']['hits']) - 1]['sort'];
            // $payload['body']['size'] ??= 1000;
            // $payload['body']['sort'] ??= '_shard_doc';

            $response = $this->performQuery('search', $payload);

            if ($response instanceof Promise) {
                $response = $response->wait()->asArray();
            }
            if ($response instanceof Elasticsearch) {
                $response = $response->asArray();
            }

            yield from (new Collection(
                items: $response['hits']['hits'],
                total: count($response['hits']['hits']),
                aggregations: [],
                response: $response,
                // query: $query,
            ))
                ->tap(function ($hits) use (&$total) {
                    $total = $hits->count();
                })
                ->keyBy(fn ($hit) => $hit instanceof Model ? $hit->getKey() : $hit['_id'])
                ->all();

            // yield from (new PromiseResponse(
            //     source: fn ($r): array => $r['hits']['hits'],
            //     total: fn ($r): int => count($r['hits']['total']),
            //     aggregations: fn ($r): array => [],
            //     response: $response,
            //     // query: $query
            // ))
            // ->tap(function ($hits) use (&$total) {
            //     $total = $hits->count();
            // })
            // ->keyBy(fn ($hit) => $hit instanceof Model ? $hit->getKey() : $hit['_id'])
            //     ->all();
        }

        if (isset($response['pit_id'])) {
            // $this->getConnection()->performQuery('clearScroll', ['scroll_id' => $response['_scroll_id']]);

            $this->performQuery('closePointInTime', [
                'body' => ['id' => $response['pit_id']],
            ]);
        }
    }

    /**
     * Run an insert statement against the database.
     *
     * @param array $query
     * @param array  $bindings
     *
     * @return bool
     */
    public function insert($query, $bindings = [])
    {
        $query = [
            'method' => 'hset',
            'payload' => $query,
        ];

        return $this->run($query, $bindings, function ($query, $bindings) {
            if ($this->pretending()) {
                return true;
            }
            // dump($query);
            $r = $this->getClient()
                ->pipeline(static function ($pipe) use ($query, $bindings) {
                    foreach ($query['payload'] as $arguments) {
                        $pipe->rawCommand($query['method'],  ...$arguments);
                    }
                });


            // if (collect($r)->contains(0)) {
            //     throw new \Exception('Failed to insert');
            // }


            return count($r) === count($query['payload']);
        });
    }

    /**
     * Run an update statement against the database.
     *
     * @param string $query
     * @param array  $bindings
     *
     * @return int
     */
    public function update($query, $bindings = [])
    {
        $query = [
            'method' => 'hset',
            'payload' => $query,
        ];

        return $this->run($query, $bindings, function ($query, $bindings) {
            if ($this->pretending()) {
                return 0;
            }

            if ($this->pretending()) {
                return true;
            }



            $r = $this->getClient()->command($query['method'], $query['payload']);

            return true;

            return 1;
        });
    }

    public function script(array $query)
    {
        $query = [
            'method' => 'eval',
            'payload' => $query,
        ];

        return $this->run($query, [], function ($query, $bindings) {
            if ($this->pretending()) {
                return true;
            }

            return $this->getClient()->command($query['method'], $query['payload']);
        });
    }

    public function updateByQuery($query)
    {
        $query = [
            'method' => 'updateByQuery',
            'payload' => $query,
        ];

        return $this->run($query, [], function ($query, $bindings) {
            if ($this->pretending()) {
                return 0;
            }

            $response = $this->performQuery($query['method'], $query['payload']);

            if ($response instanceof Promise) {
                $response = $response->wait()->asArray();
            }

            $this->recordsHaveBeenModified(
                'updated' == $response['updated']
            );

            return $response['updated'];
        });
    }

    /**
     * Run a delete statement against the database.
     *
     * @param string $query
     * @param array  $bindings
     *
     * @return int
     */
    public function delete($query, $bindings = [])
    {
        $query['slices'] = 'auto';

        $query = [
            'method' => 'deleteByQuery',
            'payload' => $query,
        ];

        return $this->run($query, [], function ($query, $bindings) {
            return $this->performQuery($query['method'], $query['payload']);
        });
    }

    public function deleteDocument(string|array $id, string $table)
    {
        $id = Arr::wrap($id);

        $query = [
            'method' => 'del',
            'payload' => array_map(static fn (string $id): string => $table . '::' . $id, $id),
        ];

        return $this->run($query, [], function ($query, $bindings) {
            return $this->performQuery($query['method'], $query['payload']);
        });
    }

    public function hincrby(string $key, string $field, int $amount)
    {
        $query = [
            'method' => 'hincrby',
            'payload' => [$key, $field, $amount],
        ];

        return $this->run($query, [], function ($query, $bindings) {
            return $this->performQuery($query['method'], $query['payload']);
        });
    }

    /**
     * Execute an SQL statement and return the boolean result.
     *
     * @param string $query
     * @param array  $bindings
     *
     * @return bool
     */
    public function statement($query, $bindings = [])
    {
        return $this->run($query, $bindings, function ($query, $bindings) {
            if ($this->pretending()) {
                return true;
            }

            $statement = $this->getPdo()->prepare($query);

            $this->bindValues($statement, $this->prepareBindings($bindings));

            $this->recordsHaveBeenModified();

            return $statement->execute();
        });
    }

    /**
     * Run an SQL statement and get the number of rows affected.
     *
     * @param string $query
     * @param array  $bindings
     *
     * @return int
     */
    public function affectingStatement($query, $bindings = [])
    {
        return $this->run($query, $bindings, function ($query, $bindings) {
            if ($this->pretending()) {
                return 0;
            }

            // For update or delete statements, we want to get the number of rows affected
            // by the statement and return that back to the developer. We'll first need
            // to execute the statement and then we'll use PDO to fetch the affected.
            $statement = $this->getPdo()->prepare($query);

            $this->bindValues($statement, $this->prepareBindings($bindings));

            $statement->execute();

            $this->recordsHaveBeenModified(
                ($count = $statement->rowCount()) > 0
            );

            return $count;
        });
    }

    /**
     * Run a raw, unprepared query against the PDO connection.
     *
     * @param string $query
     *
     * @return bool
     */
    public function unprepared($query)
    {
        return $this->run($query, [], function ($query) {
            if ($this->pretending()) {
                return true;
            }

            $this->recordsHaveBeenModified(
                $change = false !== $this->getPdo()->exec($query)
            );

            return $change;
        });
    }

    /**
     * Log a query in the connection's query log.
     *
     * @param string|array     $query
     * @param array      $bindings
     * @param null|float $time
     */
    public function logQuery($query, $bindings, $time = null)
    {
        $this->totalQueryDuration += $time ?? 0.0;
        $bindings = [];


        $cleanedQuery = static::cleanQuery($query);

        $queryString = json_encode($cleanedQuery);

        if (!app()->isLocal()) {
            $queryString = mb_substr($queryString, 0, 1000);
        }

        $this->event(new QueryExecuted(
            $queryString,
            $bindings,
            $time,
            $this
        ));

        if ($this->loggingQueries) {
            $this->queryLog[] = compact('query', 'bindings', 'time');
        }
    }

    private static function cleanQuery(array $query): array
    {
        # cleanup query by recursively reducing all arrays to max 20 items and add ... 
        # and all strings to max 1000 chars
        $cleanedQuery = [];
        foreach ($query as $key => $value) {
            if (is_array($value)) {
                $cleanedQuery[$key] = static::cleanQuery($value);
                if (count($cleanedQuery[$key]) > 20) {
                    $cleanedQuery[$key] = array_slice($cleanedQuery[$key], 0, 20);
                    $cleanedQuery[$key][] = '...';
                }
            } elseif (is_string($value)) {
                $cleanedQuery[$key] = mb_substr($value, 0, 1000);
            } else {
                $cleanedQuery[$key] = $value;
            }
        }

        return $cleanedQuery;
    }

    public function getClient(): RedicoClient
    {
        return $this->client ??= new RedicoClient(
            connection: Redis::connection($this->getConfig('name'))
        );
    }

    /**
     * Reconnect to the database if a PDO connection is missing.
     */
    public function reconnectIfMissingConnection()
    {
    }

    /**
     * Run a SQL statement.
     *
     * @param string $query
     * @param array  $bindings
     *
     * @return mixed
     *
     * @throws \Illuminate\Database\QueryException
     */
    protected function runQueryCallback($query, $bindings, \Closure $callback)
    {

        // To execute the statement, we'll simply call the callback, which will actually
        // run the SQL against the PDO connection. Then we can calculate the time it
        // took to execute and log the query SQL, bindings and time in our memory.
        try {
            return $callback($query, $bindings);
        }

        // If an exception occurs when attempting to run a query, we'll format the error
        // message to include the bindings with SQL, which will make this exception a
        // lot more helpful to the developer instead of just the database's errors.
        catch (Exception $e) {
            // if (str_contains($e->getMessage(), 'index_not_found_exception')) {
            //     if (preg_match('/no such index \[(.*?)\]/', $e->getMessage(), $matches)) {
            //         $index_name = $matches[1];
            //     } else {
            //         $index_name = 'unknown';
            //     }

            //     throw new IndexNotFoundException(index: $index_name);
            // }
            // if (str_starts_with($e->getMessage(), '404 Not Found')) {

            //     throw new ModelNotFoundException();
            // }


            throw new QueryException(
                $this->getConfig('name'),
                $query['method'] . ' ' . collect($query['payload'])->flatten()->implode(' '),
                [],
                $e
            );
        }
    }

    /**
     * {@inheritdoc}
     */
    protected function getDefaultPostProcessor()
    {
        return new Query\Processor();
    }

    /**
     * {@inheritdoc}
     */
    protected function getDefaultQueryGrammar()
    {
        return new Query\Grammar();
    }
}
