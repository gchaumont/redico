<?php

namespace Redico\Index\Fields;

use Redico\Index\Fields\Concerns\NoIndex;
use Redico\Index\Fields\Concerns\Sortable;

/**
 * Defines the configuration of a Redis index.
 */
class TextField extends Field
{
    use Sortable;
    use NoIndex;

    protected float $weight = 1.0;

    protected bool $noStem = false;

    public function getType(): string
    {
        return 'TEXT';
    }

    public function weight(float $weight): self
    {
        $this->weight = $weight;

        return $this;
    }

    public function noStem(bool $noStem = true): self
    {
        $this->noStem = $noStem;

        return $this;
    }

    public function getDefinition(): array
    {
        $config = parent::getDefinition();

        $config['WEIGHT'] = 'WEIGHT';
        $config['WEIGHT_VALUE'] = $this->weight;

        if ($this->noStem) {
            $config['NOSTEM'] = 'NOSTEM';
        }

        if ($this->isSortable()) {
            $config['SORTABLE'] = 'SORTABLE';
        }

        if ($this->isNoindex()) {
            $config['NOINDEX'] = 'NOINDEX';
        }

        return array_values($config);
    }
}
