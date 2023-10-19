<?php

namespace Redico\Index\Fields;

use Redico\Index\Fields\Concerns\NoIndex;
use Redico\Index\Fields\Concerns\Sortable;

/**
 * Defines the configuration of a Redis index.
 */
class TagField extends Field
{
    use Sortable;
    use NoIndex;

    protected string $separator = ',';

    public function getType(): string
    {
        return 'TAG';
    }

    public function separator(string $separator): self
    {
        $this->separator = $separator;

        return $this;
    }

    public function getDefinition(): array
    {
        $config = parent::getDefinition();
        $config['SEPARATOR'] = 'SEPARATOR';
        $config['SEPARATOR_VALUE'] = $this->separator;


        if ($this->isSortable()) {
            $config['SORTABLE'] = 'SORTABLE';
        }

        if ($this->isNoindex()) {
            $config['NOINDEX'] = 'NOINDEX';
        }
        return array_values($config);
    }
}
