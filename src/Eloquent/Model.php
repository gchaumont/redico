<?php

namespace Redico\Eloquent;

use Redico\Connection;
use Redico\Index\Index;
use Illuminate\Support\Arr;
use Illuminate\Support\Str;
use Redico\Scripting\Script;
use Illuminate\Contracts\Database\Eloquent\Castable;
use Illuminate\Database\Eloquent\Model as BaseModel;
use Illuminate\Contracts\Database\Eloquent\CastsAttributes;
use Illuminate\Database\Eloquent\Builder as EloquentBuilder;

/**
 * Model.
 * @method static \Redico\Eloquent\Builder whereIn()
 */
abstract class Model extends BaseModel implements Castable
{
    public $incrementing = false;

    protected $keyType = 'string';

    protected $dateFormat = 'c';

    protected $connection = 'default';

    /**
     * The connection resolver instance.
     *
     * @var \Illuminate\Database\ConnectionResolverInterface
     */
    protected static $resolver;


    abstract static function indexConfig(): Index;

    /**
     * Create a new Eloquent query builder for the model.
     *
     * @param \Illuminate\Database\Query\Builder $query
     *
     * @return \Illuminate\Database\Eloquent\Builder|static
     */
    public function newEloquentBuilder($query)
    {
        return new Builder($query);
    }

    // /**
    //  * Create a new Eloquent Collection instance.
    //  *
    //  * @return \Illuminate\Database\Eloquent\Collection
    //  */
    // public function newCollection(array $models = [])
    // {
    //     return new Collection($models);
    // }

    /**
     * Create a new model instance that is existing.
     *
     * @param array       $attributes
     * @param null|string $connection
     *
     * @return static
     */
    public function newFromBuilder($attributes = [], $connection = null)
    {
        $new = parent::newFromBuilder($attributes, $connection);

        // $new->setAttribute($new->getKeyName(), $hit['_id']);
        // $new->setAttribute('_id',  $hit['_id']);

        return $new;
    }

    /**
     * Get the database connection for the model.
     *
     * @return Connection
     */
    public function getConnection()
    {
        return parent::getConnection();
    }

    /**
     * Qualify the given column name by the model's table.
     *
     * @param string $column
     *
     * @return string
     */
    public function qualifyColumn($column)
    {
        return $column;
    }

    /**
     * Get the name of the caster class to use when casting from / to this cast target.
     *
     * @return string
     */
    public static function castUsing(array $arguments)
    {
        $class = static::class;

        return new class($class) implements CastsAttributes
        {
            public function __construct(protected string $class)
            {
            }

            public function get($model, $key, $value, $attributes)
            {
                $class = $this->class;

                if (is_subclass_of($class, Model::class)) {
                    return (new $class())->forceFill($value);
                }

                return new $class($value);
            }

            public function set($model, $key, $value, $attributes)
            {
                return [$key => $value->getAttributes()];
            }
        };
    }

    /**
     * {@inheritdoc}
     */
    public function getAttribute($key)
    {
        if (!$key) {
            return;
        }

        // Dot notation support.
        if (Str::contains($key, '.') && Arr::has($this->attributes, $key)) {
            $value = $this->getAttributeValue(Str::before($key, '.'));

            return  Arr::get($value, Str::after($key, '.'));
        }

        // This checks for embedded relation support.
        if (method_exists($this, $key) && !method_exists(self::class, $key)) {
            return $this->getRelationValue($key);
        }

        return parent::getAttribute($key);
    }

    protected function getStorableEnumValue($value)
    {
        if ($value === null) {
            return null;
        }

        return parent::getStorableEnumValue($value);
    }



    /**
     * Retrieve the model for a bound value.
     *
     * @param \Illuminate\Database\Eloquent\Model|\Illuminate\Database\Eloquent\Relations\Relation $query
     * @param mixed                                                                                $value
     * @param null|string                                                                          $field
     *
     * @return \Illuminate\Database\Eloquent\Relations\Relation
     */
    public function resolveRouteBindingQuery($query, $value, $field = null)
    {
        $field ??= $this->getRouteKeyName();

        if ($field == $this->getKeyName()) {
            return collect([static::find($value)]);
        }

        return $query->where($field, $value);
    }

    protected function getAttributesForInsert()
    {
        $attributes = $this->getAttributes();
        $attributes['_key'] = $this->getTable() . '::' . $this->getKey();

        return $attributes;
    }

    /**
     * Get an enum case instance from a given class and value.
     *
     * @param string     $enumClass
     * @param int|string $value
     *
     * @return \BackedEnum|\UnitEnum
     */
    protected function getEnumCaseFromValue($enumClass, $value)
    {
        return is_subclass_of($enumClass, \BackedEnum::class)
            ? $enumClass::tryFrom($value)
            : constant($enumClass . '::' . $value);
    }

    /**
     * {@inheritdoc}
     */
    protected function getAttributeFromArray($key)
    {
        // Support keys in dot notation.
        if (Str::contains($key, '.')) {
            return Arr::get($this->attributes, $key);
        }

        return parent::getAttributeFromArray($key);
    }

    protected function setKeysForSaveQuery($query)
    {
        $query->getQuery()->model_id = $this->getKeyForSaveQuery();
        $query->getQuery()->index_id  = $this->_index ?? $this->getTable();
        // $query->where($this->getKeyName(), '=', $this->getKeyForSaveQuery());

        return $query;
    }

    /**
     * Begin querying the model.
     *
     * @return \Redico\Eloquent\Builder
     */
    public static function query()
    {
        return (new static)->newQuery();
    }

    public function update(array|Script $attributes = [], array $options = [])
    {
        if (!$this->exists) {
            return false;
        }

        if ($attributes instanceof Script) {
            $query = $this->newModelQuery();
            $this->setKeysForSaveQuery($query);

            return $this->performScriptedUpdate($query, $attributes);
        }

        return $this->fill($attributes)->save($options);
    }




    /**
     * Perform the actual delete query on this model instance.
     */
    protected function performDeleteOnModel()
    {
        $this->newModelQuery()->delete($this->getKey());
        // $this->setKeysForSaveQuery($this->newModelQuery())->delete();

        $this->exists = false;
    }
}
