<?php

declare(strict_types=1);

namespace App\Flow;

use App\Model\ProductBatch;
use Flow\FlowFactory;
use Flow\FlowInterface;
use Generator;
use Kiboko\Component\Bucket\AcceptanceResultBucket;
use Kiboko\Component\Pipeline\Pipeline;
use Kiboko\Component\Pipeline\PipelineRunner;
use Kiboko\Component\Pipeline\StepCode;
use Kiboko\Contract\Bucket\ResultBucketInterface;
use Kiboko\Contract\Pipeline\ExtractorInterface;
use Kiboko\Contract\Pipeline\FlushableInterface;
use Kiboko\Contract\Pipeline\LoaderInterface;
use Kiboko\Contract\Pipeline\NullState;
use Kiboko\Contract\Pipeline\NullStepRejection;
use Kiboko\Contract\Pipeline\NullStepState;
use Kiboko\Contract\Pipeline\TransformerInterface;
use Psr\Log\NullLogger;
use Symfony\Component\Console\Style\SymfonyStyle;

final class ProductSyncFlowFactory
{
    public function __construct(
        private readonly FlowFactory $flowFactory,
        private readonly string $projectDir,
    ) {}

    /**
     * @return FlowInterface<ProductBatch>
     */
    public function create(SymfonyStyle $io): FlowInterface
    {
        $outputDir = $this->projectDir.'/var/output';
        if (!is_dir($outputDir)) {
            mkdir($outputDir, 0777, true);
        }

        return $this->flowFactory->create(static function () use ($io, $outputDir) {
            yield static function (ProductBatch $batch) use ($io) {
                $io->section(sprintf('[extract] batch %s (%d rows)', $batch->id, \count($batch->rows)));
                $start = hrtime(true);

                $pipeline = new Pipeline(new PipelineRunner(new NullLogger()), new NullState());
                $pipeline->extract(
                    StepCode::fromString('extractor'),
                    new class($batch->rows) implements ExtractorInterface {
                        /** @param list<string> $rows */
                        public function __construct(private array $rows) {}

                        public function extract(): iterable
                        {
                            foreach ($this->rows as $row) {
                                yield new AcceptanceResultBucket([$row]);
                            }
                        }
                    },
                    new NullStepRejection(),
                    new NullStepState()
                );

                $io->writeln(sprintf('  extract done in %.2f ms', (hrtime(true) - $start) / 1e6));

                return $pipeline;
            };

            yield static function (Pipeline $pipeline) use ($io) {
                $io->section('[transform] normalize SKU, uppercase, rot13');
                $start = hrtime(true);

                $pipeline->transform(
                    StepCode::fromString('transformer'),
                    new class() implements TransformerInterface, FlushableInterface {
                        /** @return Generator<null|ResultBucketInterface<mixed>> */
                        public function transform(): Generator
                        {
                            $line = yield;
                            $line = yield new AcceptanceResultBucket(array_map(
                                static fn (string $item): string => strtoupper(str_rot13($item)),
                                $line
                            ));
                            $line = yield new AcceptanceResultBucket(array_map(
                                static fn (string $item): string => strtoupper(str_rot13($item)),
                                $line
                            ));
                            yield new AcceptanceResultBucket(array_map(
                                static fn (string $item): string => strtoupper(str_rot13($item)),
                                $line
                            ));
                        }

                        public function flush(): ResultBucketInterface
                        {
                            return new AcceptanceResultBucket([strtoupper(str_rot13('flush-row'))]);
                        }
                    },
                    new NullStepRejection(),
                    new NullStepState()
                );

                $io->writeln(sprintf('  transform done in %.2f ms', (hrtime(true) - $start) / 1e6));

                return $pipeline;
            };

            yield static function (Pipeline $pipeline) use ($io, $outputDir) {
                $io->section('[load] accumulate and write output file');
                $start = hrtime(true);
                $batchId = 'unknown';

                $pipeline->load(
                    StepCode::fromString('loader'),
                    new class($outputDir) implements LoaderInterface, FlushableInterface {
                        private string $batchId = 'unknown';

                        public function __construct(private readonly string $outputDir) {}

                        /** @return Generator<null|ResultBucketInterface<mixed>> */
                        public function load(): Generator
                        {
                            $line = yield;
                            $line = yield new AcceptanceResultBucket(array_map(
                                static fn (string $item): string => str_rot13($item),
                                $line
                            ));
                            $line = yield new AcceptanceResultBucket(array_map(
                                static fn (string $item): string => str_rot13($item),
                                $line
                            ));
                            yield new AcceptanceResultBucket(array_map(
                                static fn (string $item): string => str_rot13($item),
                                $line
                            ));
                        }

                        public function flush(): ResultBucketInterface
                        {
                            return new AcceptanceResultBucket([str_rot13('flush-load')]);
                        }
                    },
                    new NullStepRejection(),
                    new NullStepState()
                );

                $io->writeln(sprintf('  load done in %.2f ms', (hrtime(true) - $start) / 1e6));

                return $pipeline;
            };

            yield static function (Pipeline $pipeline) use ($io, $outputDir) {
                $io->section('[walk] print pipeline results');
                $start = hrtime(true);
                $allItems = [];
                $iteration = 0;

                foreach ($pipeline->walk() as $items) {
                    ++$iteration;
                    $allItems = array_merge($allItems, $items);
                    $io->writeln(sprintf('  walk iteration %d: %s', $iteration, implode(', ', $items)));
                }

                $outputFile = $outputDir.'/products-'.date('Ymd-His').'.json';
                file_put_contents($outputFile, json_encode($allItems, JSON_PRETTY_PRINT));

                $io->success(sprintf(
                    'walk done in %.2f ms — %d items written to %s',
                    (hrtime(true) - $start) / 1e6,
                    \count($allItems),
                    $outputFile
                ));

                return [
                    'items' => \count($allItems),
                    'iterations' => $iteration,
                    'output' => $outputFile,
                ];
            };
        });
    }
}
