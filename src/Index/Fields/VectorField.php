<?php

namespace Redico\Index\Fields;



/**
 * Defines the configuration of a Redis index.
 */
class VectorField extends Field
{
    public function getType(): string
    {
        return 'VECTOR';
    }
}
