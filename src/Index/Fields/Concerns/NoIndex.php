<?php

namespace Redico\Index\Fields\Concerns;

/**
 * 
 */
trait NoIndex
{
    protected bool $noindex = false;

    public function noindex(bool $noindex = true): static
    {
        $this->noindex = $noindex;

        return $this;
    }

    public function isNoindex(): bool
    {
        return $this->noindex;
    }
}
