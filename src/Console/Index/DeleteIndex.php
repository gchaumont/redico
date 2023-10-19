<?php

namespace Redico\Console\Index;

use Redico\Eloquent\Model;
use Illuminate\Console\Command;
use Redico\Exceptions\UnknownIndexNameException;

/**
 * Keeps the Client and performs the queries
 * Takes care of monitoring all queries.
 */
class DeleteIndex extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'redico:index:delete {model}  
                                {--connection= : Elasticsearch connection}
                                {--force= :  Force the operation to run }
                                {--delete-docs= :  Delete all documents in the index}
                                ';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Delete a Redis index';

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

        if ($this->option('force') || $this->confirm('Are you sure you want to delete this index?')) {

            try {
                $this->info('Dropping index: ' . $model::indexConfig()->getName());

                $params = [$model::indexConfig()->getName()];
                if ($this->option('delete-docs')) {
                    $params[] = 'DD';
                }

                $response = $model->getConnection()->getClient()->command('FT.DROPINDEX', $params);

                if ($response == false) {
                    throw new \Exception("{$class} Index could not be deleted");
                }
            } catch (UnknownIndexNameException) {
                $this->error("{$class} Index does not exist");
            }
        }


        return $this->info("{$class} Index Deleted");
    }
}
