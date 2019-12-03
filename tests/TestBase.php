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

use PDO;
use EasyRdf\Graph;
use EasyRdf\Literal;
use EasyRdf\Literal\Date;
use EasyRdf\Resource;
use zozlak\RdfConstants as RDF;
use acdhOeaw\acdhRepoLib\Repo;
use acdhOeaw\acdhRepoLib\exception\NotFound;

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
    static protected $config;

    /**
     *
     * @var \acdhOeaw\arche\Ontology
     */
    static protected $ontology;

    static public function setUpBeforeClass(): void {
        parent::setUpBeforeClass();

        $localCfg       = yaml_parse_file(__DIR__ . '/../config-sample.yaml');
        self::$config   = json_decode(json_encode(yaml_parse_file($localCfg['doorkeeper']['restConfigDstPath'])));
        self::$repo     = Repo::factory(self::$config->doorkeeper->restConfigDstPath);
        self::$ontology = new Ontology(new PDO(self::$config->dbConnStr->admin), self::$repo->getBaseUrl() . '%');
    }

    /**
     *
     * @var \acdhOeaw\acdhRepoLib\RepoResource[]
     */
    protected $toDelete;

    public function setUp(): void {
        parent::setUp();
        $this->toDelete = [];
    }

    public function tearDown(): void {
        parent::tearDown();

        if (self::$repo->inTransaction()) {
            self::$repo->rollback();
        }

        if (count($this->toDelete) > 0) {
            self::$repo->begin();
            foreach ($this->toDelete as $i) {
                try {
                    $i->delete(true, false);
                } catch (NotFound $e) {
                    
                }
            }
            self::$repo->commit();
        }
    }

    static protected function toStr(array $v): array {
        return array_map(function($x) {
            return (string) $x;
        }, $v);
    }

    static protected function createMetadata(array $props = [],
                                             string $class = null): Resource {
        $idProp    = self::$config->schema->id;
        $labelProp = self::$config->schema->label;

        $r = (new Graph())->resource('.');

        if (!isset($props[$idProp])) {
            $r->addResource($idProp, 'https://random/' . microtime(true));
        }
        if (!isset($props[$labelProp])) {
            $r->addLiteral($labelProp, 'Sample label');
        }

        foreach ($props as $p => $i) {
            if (!is_array($i)) {
                $i = [$i];
            }
            foreach ($i as $v) {
                if (is_object($v)) {
                    $r->add($p, $v);
                } elseif (preg_match('|^https?://.|', $v)) {
                    $r->addResource($p, $v);
                } else {
                    $r->addLiteral($p, $v);
                }
            }
        }

        if (!empty($class)) {
            $r->addResource(RDF::RDF_TYPE, $class);
            $class = self::$ontology->getClass($class);
            foreach ($class->properties as $i) {
                if ($i->min > 0 && $r->get($i->property) === null) {
                    $r->add($i->property, self::createSampleProperty($i));
                }
            }
        }

        return $r;
    }

    static protected function createSampleProperty(PropertyDesc $i) {
        if ($i->type === RDF::OWL_DATATYPE_PROPERTY) {
            switch ($i->range) {
                case RDF::XSD_DECIMAL:
                case RDF::XSD_BYTE;
                case RDF::XSD_DOUBLE;
                case RDF::XSD_FLOAT;
                case RDF::XSD_INT;
                case RDF::XSD_INTEGER;
                case RDF::XSD_NON_NEGATIVE_INTEGER;
                case RDF::XSD_POSITIVE_INTEGER;
                    return new Literal(123);
                case RDF::XSD_NON_POSITIVE_INTEGER;
                case RDF::XSD_NEGATIVE_INTEGER;
                    return new Literal(-321);
                case RDF::XSD_DATE:
                case RDF::XSD_DATE_TIME:
                    return new Date('2019-01-01');
                default:
                    return new Literal('sample');
            }
        } else {
            return new Resource('https://sample');
        }
    }

}
