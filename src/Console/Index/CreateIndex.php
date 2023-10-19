<?php

namespace Redico\Console\Index;

use Redico\Eloquent\Model;
use Illuminate\Console\Command;

/**
 * Keeps the Client and performs the queries
 * Takes care of monitoring all queries.
 */
class CreateIndex extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'redico:index:create {model}  
                                {--connection= : Elasticsearch connection}
                                {--fresh= : Delete index if it exists}
                                ';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Create a Redis index';

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



        if ($this->option('fresh')) {
            $this->call('redico:index:delete',  [
                'model' => $model::class,
                '--force' => true,
                '--delete-docs' => true,
                '--connection' =>  $model->getConnection()->getName(),
            ]);
        }


        $index = $model::indexConfig();

        $response = $model->getConnection()->getClient()->command('FT.CREATE', $index->getDefinition());

        if ($response == false) {
            throw new \Exception("{$class} Index could not be created");
        }

        return $this->info("{$class} Index Created");
    }
}
