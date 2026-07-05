<?php

declare(strict_types=1);

namespace App\Console;

use Blackfire\Client;
use Blackfire\ClientConfiguration;
use Blackfire\Probe;
use Psr\Log\LoggerInterface;

final class BlackfireSignalHandler
{
    private static ?Probe $probe = null;

    public function __construct(
        private readonly LoggerInterface $logger,
    ) {}

    /**
     * @return list<int>
     */
    public function getSubscribedSignals(): array
    {
        $signals = [\SIGTERM, \SIGINT];

        if (class_exists(Client::class)) {
            $signals[] = \SIGUSR2;
        }

        return $signals;
    }

    public function handleSignal(int $signal, callable $onStop): int|false
    {
        if (\in_array($signal, [\SIGTERM, \SIGINT], true)) {
            $onStop('Signal received, stopping after current task.');
        }

        if (\SIGUSR2 === $signal && class_exists(Client::class)) {
            $client = new Client(new ClientConfiguration(
                $_SERVER['BLACKFIRE_CLIENT_ID'] ?? $_ENV['BLACKFIRE_CLIENT_ID'] ?? '',
                $_SERVER['BLACKFIRE_CLIENT_TOKEN'] ?? $_ENV['BLACKFIRE_CLIENT_TOKEN'] ?? '',
            ));

            if (null === self::$probe) {
                $this->logger->notice('Blackfire profile started.');
                self::$probe = $client->createProbe();
            } else {
                $profile = $client->endProbe(self::$probe);
                $this->logger->notice('Blackfire profile finished.', [
                    'url' => $profile->getUrl(),
                ]);
                self::$probe = null;
            }
        }

        return false;
    }
}
