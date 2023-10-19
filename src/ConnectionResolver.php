<?php

namespace Redico;

use Elastic\Elasticsearch\Client;
use Elastic\Elasticsearch\ClientBuilder;
use Elastic\Elasticsearch\Response\Elasticsearch;
use Redico\Eloquent\Model;
use Redico\Exceptions\BulkException;
use Redico\Exceptions\IndexNotFoundException;
use Redico\Query\Builder;
use Redico\Query\Response\Collection;
use Exception;
use GuzzleHttp\Promise\Promise;
use Http\Adapter\Guzzle7\Client as GuzzleAdapter;
use Illuminate\Database\Connection as BaseConnection;
use Illuminate\Database\ConnectionInterface;
use Illuminate\Database\ConnectionResolverInterface;
use Illuminate\Database\Eloquent\ModelNotFoundException;
use Illuminate\Database\Events\QueryExecuted;
use Illuminate\Database\QueryException;
use Illuminate\Support\Facades\Redis;
use Illuminate\Support\LazyCollection;

/**
 * Keeps the Client and performs the queries
 * Takes care of monitoring all queries.
 */
class ConnectionResolver implements ConnectionResolverInterface
{
    /**
     * Get a database connection instance.
     *
     * @param  string|null  $name
     * @return \Illuminate\Database\ConnectionInterface
     */
    public function connection($name = null)
    {
        return new Connection(
            config: [
                'name' => $name,
            ]
            // Redis::connection($name), 
        );
    }

    /**
     * Get the default connection name.
     *
     * @return string
     */
    public function getDefaultConnection()
    {
        return 'default';
    }

    /**
     * Set the default connection name.
     *
     * @param  string  $name
     * @return void
     */
    public function setDefaultConnection($name)
    {
        return;
    }
}
