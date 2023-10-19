<?php

namespace Redico\Index\Fields;

use Redico\Index\Fields\Concerns\NoIndex;

/**
 * Defines the configuration of a Redis index.
 */
class GeoField extends Field
{
    use NoIndex;

    public function getType(): string
    {
        return 'GEO';
    }
    public function getDefinition(): array
    {
        $config = parent::getDefinition();

        if ($this->isNoindex()) {
            $config['NOINDEX'] = 'NOINDEX';
        }

        return array_values($config);
    }
}
