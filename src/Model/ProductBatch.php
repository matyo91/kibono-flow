<?php

declare(strict_types=1);

namespace App\Model;

final readonly class ProductBatch
{
    /**
     * @param list<string> $rows
     */
    public function __construct(
        public string $id,
        public array $rows,
    ) {}
}
