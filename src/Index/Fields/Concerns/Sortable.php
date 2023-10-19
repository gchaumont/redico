<?php

namespace Redico\Index\Fields\Concerns;



/**
 * Defines the configuration of a Redis index.
 */
trait Sortable
{
    protected bool $sortable = false;

    public function sortable(bool $sortable = true): static
    {
        $this->sortable = $sortable;

        return $this;
    }

    public function isSortable(): bool
    {
        return $this->sortable;
    }
}
