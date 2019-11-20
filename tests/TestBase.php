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

use acdhOeaw\acdhRepoLib\Repo;

/**
 * Description of TestBase
 *
 * @author zozlak
 */
class TestBase extends \PHPUnit\Framework\TestCase {

    /**
     *
     * @var \acdhOeaw\acdhRepoLib\Repo
     */
    static protected $repo;

    static public function setUpBeforeClass(): void {
        parent::setUpBeforeClass();

        $cfg           = yaml_parse_file(__DIR__ . '/../config-sample.yaml');
        $restCfg       = yaml_parse_file($cfg['doorkeeper']['restConfigSrcPath']);
        $cfg           = array_merge_recursive($restCfg, $cfg);
        $clientAuthCfg = [
            'auth' => [
                'httpHeader' => [
                    'eppn' => $cfg['accessControl']['adminRoles'][0]
                ]
            ]
        ];
        $cfg           = array_merge_recursive($cfg, $clientAuthCfg);
        yaml_emit_file($cfg['doorkeeper']['restConfigDstPath'], $cfg);

        self::$repo = Repo::factory($cfg['doorkeeper']['restConfigDstPath']);

        if (file_exists($cfg['rest']['logging']['file'])) {
            unlink($cfg['rest']['logging']['file']);
        }
    }

    public function tearDown(): void {
        parent::tearDown();

        if (self::$repo->inTransaction()) {
            self::$repo->rollback();
        }
    }

}
