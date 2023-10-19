<?php

namespace Redico\Console\Index;

use Redico\Eloquent\Model;
use Illuminate\Console\Command;

/**
 * Keeps the Client and performs the queries
 * Takes care of monitoring all queries.
 */
class GetInfo extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'redico:index:info {model}  
                                {--connection= : Elasticsearch connection}
                                ';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Get info about a Redis index';

    /**
     * Execute the console command.
     *
     * @return mixed
     */
    public function handle()
    {
        $class = $this->argument('model');

        /** @var Model $model */
        $model = new $class();

        if ($this->option('connection')) {
            $model->setConnection($this->option('connection'));
        }


        $r = $model->getConnection()->getClient()->command('FT.INFO', $model::indexConfig()->getName());

        dump($r);
    }
}
