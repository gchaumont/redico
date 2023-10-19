<?php

namespace Redico\Console\Index;

use Redico\Eloquent\Model;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Redis;
use Redico\Client;

/**
 * Keeps the Client and performs the queries
 * Takes care of monitoring all queries.
 */
class ListIndexes extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'redico:index:list
                                {--connection= : Elasticsearch connection}
                                ';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'List all Redis indexes';

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle()
    {


        $connection = Redis::connection($this->option('connection', 'default'));

        $client = new Client($connection);

        $r = $client->command('FT._LIST', []);

        dump($r);
    }
}
