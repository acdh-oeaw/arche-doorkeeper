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

namespace acdhOeaw\arche\doorkeeper\tests;

use DirectoryIterator;
use PHPUnit\Runner\AfterLastTestHook;
use PHPUnit\Runner\BeforeFirstTestHook;
use SebastianBergmann\CodeCoverage\CodeCoverage;
use SebastianBergmann\CodeCoverage\Filter;
use SebastianBergmann\CodeCoverage\RawCodeCoverageData;
use SebastianBergmann\CodeCoverage\Report\Clover;
use SebastianBergmann\CodeCoverage\Report\Html\Facade;
use SebastianBergmann\CodeCoverage\Driver\Xdebug2Driver;
use SebastianBergmann\CodeCoverage\Driver\Xdebug3Driver;

/**
 * Description of CoverageGen
 *
 * @author zozlak
 */
class Bootstrap implements AfterLastTestHook, BeforeFirstTestHook {

    private object $config;

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
        $testEnvDir = '/home/www-data/arche-doorkeeper/src';
        $localDir   = realpath(__DIR__ . '/../src');
        $filter = new Filter();
        $filter->includeDirectory($localDir);
        $driver = phpversion('xdebug') < '3' ? new Xdebug2Driver($filter) : new Xdebug3Driver($filter);
        $cc     = new CodeCoverage($driver, $filter);
        foreach (new DirectoryIterator(__DIR__ . '/../build/logs') as $i) {
            if ($i->getExtension() === 'json') {
                $rawData = (array) json_decode((string) file_get_contents($i->getPathname()), true);
                $data    = [];
                foreach ($rawData as $k => $v) {
                    $data[str_replace($testEnvDir, $localDir, $k)] = $v;
                }
                $data = RawCodeCoverageData::fromXdebugWithoutPathCoverage($data);
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
