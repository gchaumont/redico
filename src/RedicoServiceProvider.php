<?php

namespace Redico;;

use Redico\Eloquent\Model;
use Redico\ConnectionResolver;
use Redico\Console\Index\CreateIndex;
use Illuminate\Support\ServiceProvider;
use Illuminate\Database\ConnectionResolverInterface;
use Redico\Console\Index\CreateIndex as IndexCreateIndex;
use Redico\Console\Index\DeleteIndex;
use Redico\Console\Index\GetInfo;
use Redico\Console\Index\ListIndexes;

/**
 *  Redico ServiceProvider.
 */
class RedicoServiceProvider extends ServiceProvider
{
    public function boot()
    {
        Model::setConnectionResolver($this->app['redisearch_resolver']);

        Model::setEventDispatcher($this->app['events']);

        $this->registerCommands();
    }

    public function register()
    {
        // Add Eloquent Database driver.
        $this->app->resolving('db', static function ($db) {
            $db->extend('redico', static function ($config, $name) {
                $config['name'] = $name;

                return new Connection($config);
            });
        });



        $this->app->singleton('redisearch_resolver', static function ($app): ConnectionResolverInterface {
            return new ConnectionResolver($app);
        });
    }

    public function registerCommands(): void
    {
        if ($this->app->runningInConsole()) {
            $this->commands([
                CreateIndex::class,
                DeleteIndex::class,
                GetInfo::class,
                ListIndexes::class,
            ]);
        }
    }
}
