<?php

namespace Redico\Index\Fields;

use Redico\Index\Fields\Concerns\NoIndex;
use Redico\Index\Fields\Concerns\Sortable;

/**
 * Defines the configuration of a Redis index.
 */
class NumericField extends Field
{
    use Sortable;
    use NoIndex;

    public function getType(): string
    {
        return 'NUMERIC';
    }


    public function getDefinition(): array
    {
        $config = parent::getDefinition();

        if ($this->isSortable()) {
            $config['SORTABLE'] = 'SORTABLE';
        }

        if ($this->isNoindex()) {
            $config['NOINDEX'] = 'NOINDEX';
        }

        return array_values($config);
    }
}
