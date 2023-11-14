<?php

namespace Redico\Query;

/*
 *  Redis LUA script
 */

class Script
{
    public function __construct(
        public string $script,
        public array $keys,
        public array $args,
    ) {
    }

    public function compile(): array
    {
        return [
            $this->script,
            count($this->keys),
            ...$this->keys,
            ...$this->args,
        ];
    }
}
