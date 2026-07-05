<?php

declare(strict_types=1);

namespace App\Console;

use App\Model\ProductBatch;

final class InMemoryQueue
{
    /** @var list<ProductBatch> */
    private array $messages;

    private int $position = 0;

    public function __construct(string $queuePath)
    {
        $this->messages = [];

        if (!is_file($queuePath)) {
            return;
        }

        $handle = fopen($queuePath, 'r');
        if (false === $handle) {
            return;
        }

        $index = 0;
        while (($line = fgets($handle)) !== false) {
            $line = trim($line);
            if ('' === $line) {
                continue;
            }

            /** @var list<string> $rows */
            $rows = json_decode($line, true, 512, JSON_THROW_ON_ERROR);
            $this->messages[] = new ProductBatch(sprintf('batch-%03d', ++$index), $rows);
        }

        fclose($handle);
    }

    public function next(): ?ProductBatch
    {
        if ([] === $this->messages) {
            return null;
        }

        $message = $this->messages[$this->position];
        $this->position = ($this->position + 1) % \count($this->messages);

        return $message;
    }
}
