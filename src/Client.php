<?php

namespace Redico;

use Exception;
use Illuminate\Support\Arr;
use Illuminate\Support\Str;
use Illuminate\Redis\Connections\Connection;
use Redico\Exceptions\UnknownIndexNameException;
use Redico\Exceptions\AliasDoesNotExistException;
use Redico\Exceptions\DocumentAlreadyInIndexException;
use Redico\Exceptions\UnknownRediSearchCommandException;
use Redico\Exceptions\UnsupportedRedisDatabaseException;
use Redico\Exceptions\UnsupportedRediSearchLanguageException;
use Redico\Exceptions\UnknownIndexNameOrNameIsAnAliasItselfException;

/**
 * Keeps the Client and performs the queries
 * Takes care of monitoring all queries.
 */
class Client
{
    public function __construct(public Connection $connection)
    {
        // $connection->client()->setOption(\Redis::OPT_THROW_EXCEPTIONS, true);
    }


    public function pipeline(callable $callback): array
    {
        $responses = $this->connection->pipeline($callback);

        foreach ($responses as $response) {
            $this->validateResponse($response);
        }

        return $responses;
    }

    public function command(string $command, string|array $arguments): mixed
    {
        $arguments = Arr::wrap($arguments);
        $arguments = array_map(fn ($argument) => (string) $argument, $arguments);

        try {
            if ($command == 'hgetall') {
                $response = $this->connection->hgetall($arguments[0]);
            } else {
                $response = $this->connection->rawCommand($command, ...$arguments);
            }

            $this->validateResponse($response);
        } catch (Exception $e) {
            $this->validateResponse($e);
        }


        return $response;
    }


    public function validateResponse(mixed $result): mixed
    {




        if ($result instanceof Exception) {
            $message = $result->getMessage();
        } else if ($result === false && $this->connection->getLastError() !== null) {
            $message = $this->connection->getLastError();
        } else {
            $message = $result;
        }
        if (!is_string($message)) {
            return $result;
        }

        $message = strtolower($message);

        if ($message === 'cannot create index on db != 0') {
            throw new UnsupportedRedisDatabaseException();
        }

        if (str_contains($message, ': no such index')) {
            throw new UnknownIndexNameException($message);
        }


        if ($message === 'unknown index name') {
            throw new UnknownIndexNameException();
        }

        if (in_array($message, ['no such language', 'unsupported language', 'unsupported stemmer language', 'bad argument for `language`'])) {
            throw new UnsupportedRediSearchLanguageException();
        }

        if ($message === 'unknown index name (or name is an alias itself)') {
            throw new UnknownIndexNameOrNameIsAnAliasItselfException();
        }

        if ($message === 'alias does not exist') {
            throw new AliasDoesNotExistException();
        }

        if (str_contains($message, 'err unknown command \'ft.')) {
            throw new UnknownRediSearchCommandException($message);
        }

        if (in_array($message, ['document already in index', 'document already exists'])) {
            throw new DocumentAlreadyInIndexException($arguments[0], $arguments[1]);
        }

        if ($result instanceof \Exception) {
            throw $result;
        }
    }
}
