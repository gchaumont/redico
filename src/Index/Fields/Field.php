<?php

namespace Redico\Index\Fields;



/**
 * Defines the configuration of a Redis index.
 */
abstract class Field
{
    // public string $on = 'HASH'; // HASH | JSON

    // public null|string|array $prefix = null;

    // public null|string $language = null;

    public function __construct(public string $name)
    {
    }

    abstract public function getType(): string;

    public function getName(): string
    {
        return $this->name;
    }

    public function getDefinition(): array
    {
        $config = [
            'name' => $this->getName(),
            'type' => $this->getType(),
        ];

        // if ($this->prefix) {
        //     $config['prefix'] = $this->prefix;
        // }

        // if ($this->language) {
        //     $config['language'] = $this->language;
        // }

        return array_values($config);
    }
}
