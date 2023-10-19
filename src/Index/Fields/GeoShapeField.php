<?php

namespace Redico\Index\Fields;



/**
 * Defines the configuration of a Redis index.
 */
class GeoShapeField extends Field
{
    public function getType(): string
    {
        return 'GEOSHAPE';
    }
}
