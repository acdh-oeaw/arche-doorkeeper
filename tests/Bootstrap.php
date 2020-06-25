<?php

/*
 * The MIT License
 *
 * Copyright 2019 Austrian Centre for Digital Humanities.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

namespace acdhOeaw\arche;

use DirectoryIterator;
use PHPUnit\Runner\AfterLastTestHook;
use PHPUnit\Runner\BeforeFirstTestHook;
use SebastianBergmann\CodeCoverage\CodeCoverage;
use SebastianBergmann\CodeCoverage\Report\Clover;
use SebastianBergmann\CodeCoverage\Report\Html\Facade;

/**
 * Description of CoverageGen
 *
 * @author zozlak
 */
class Bootstrap implements AfterLastTestHook, BeforeFirstTestHook {

    private $config;

    public function __construct() {
        $this->config = json_decode(json_encode(yaml_parse_file(__DIR__ . '/../config.yaml')));
    }

    public function executeBeforeFirstTest(): void {
        $paths = [
            __DIR__ . '/../log/' . basename($this->config->transactionController->logging->file),
            __DIR__ . '/../log/' . basename($this->config->rest->logging->file),
        ];
        foreach ($paths as $i) {
            if (file_exists($i)) {
                unlink($i);
            }
        }

        system('rm -fR ' . escapeshellarg(__DIR__ . '/../build/logs/'));
        system('mkdir -p ' . escapeshellarg(__DIR__ . '/../build/logs/'));
    }

    public function executeAfterLastTest(): void {
        $localDir   = realpath(__DIR__ . '/../src');
        $testEnvDir = '/home/www-data/arche-doorkeeper/src';
        $cc         = new CodeCoverage();
        $cc->filter()->addDirectoryToWhitelist($localDir);
        foreach (new DirectoryIterator(__DIR__ . '/../build/logs') as $i) {
            if ($i->getExtension() === 'json') {
                $rawData = json_decode(file_get_contents($i->getPathname()), true);
                $data    = [];
                foreach ($rawData as $k => $v) {
                    $data[str_replace($testEnvDir, $localDir, $k)] = $v;
                }
                $cc->append($data, '');
            }
        }
        $outDir = __DIR__ . '/../build/logs/';
        $writer = new Clover();
        $writer->process($cc, $outDir . 'clover.xml');
        $writer = new Facade();
        $writer->process($cc, $outDir);
    }

}
