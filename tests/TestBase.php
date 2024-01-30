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

use PDO;
use Stringable;
use zozlak\RdfConstants as RDF;
use rdfInterface\TermInterface;
use quickRdf\DataFactory as DF;
use quickRdf\DatasetNode;
use termTemplates\PredicateTemplate as PT;
use acdhOeaw\arche\lib\Repo;
use acdhOeaw\arche\lib\Schema;
use acdhOeaw\arche\lib\exception\NotFound;
use acdhOeaw\arche\lib\schema\Ontology;
use acdhOeaw\arche\lib\schema\PropertyDesc;

/**
 * Description of TestBase
 *
 * @author zozlak
 */
class TestBase extends \PHPUnit\Framework\TestCase {

    static protected Repo $repo;
    static protected Schema $schema;
    static protected object $config;

    /**
     * A sample resource URI which is guaranteed to exist in the repository
     */
    static protected string $sampleResUri;
    static protected Ontology $ontology;

    static public function setUpBeforeClass(): void {
        parent::setUpBeforeClass();

        self::$config   = json_decode(json_encode(yaml_parse_file(__DIR__ . '/../config.yaml')));
        self::$repo     = Repo::factory(__DIR__ . '/../config.yaml');
        self::$schema   = self::$repo->getSchema();
        self::$ontology = Ontology::factoryDb(new PDO(self::$config->dbConn->admin), self::$schema);

        self::$sampleResUri = 'https://orcid.org/0000-0001-5853-2534';
    }

    /**
     *
     * @var \acdhOeaw\arche\lib\RepoResource[]
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
                    $i->delete(true, true, self::$config->schema->parent);
                } catch (NotFound $e) {
                    
                }
            }
            self::$repo->commit();
        }
    }

    /**
     * 
     * @param array<string | Stringable> $v
     * @return array<string>
     */
    static protected function toStr(array $v): array {
        return array_map(function ($x) {
            return (string) $x;
        }, $v);
    }

    /**
     * 
     * @param array<mixed> $props
     * @param string $class
     * @return DatasetNode
     */
    static protected function createMetadata(array $props = [],
                                             string $class = null): DatasetNode {
        $idProp    = self::$schema->id;
        $labelProp = self::$schema->label;

        $r = DF::namedNode('https://id.acdh.oeaw.ac.at/test/' . microtime(true) . rand());
        $d = new DatasetNode($r);

        if (!isset($props[(string) $idProp])) {
            $d->add(DF::quad($r, $idProp, $r));
        }
        if (!isset($props[(string) $labelProp])) {
            $d->add(DF::quad($r, $labelProp, DF::literal('Sample label', 'en')));
        }

        foreach ($props as $p => $i) {
            $pDef = self::$ontology->getProperty(null, $p);
            if (!is_array($i)) {
                $i = [$i];
            }
            $pn = DF::namedNode($p);
            foreach ($i as $v) {
                if (is_object($v)) {
                    $d->add(DF::quad($r, $pn, $v));
                    continue;
                }
                if ($pDef === null) {
                    $pDef = (object) [
                            'type'    => preg_match('|^https?://.|', $v) ? RDF::OWL_OBJECT_PROPERTY : RDF::OWL_DATATYPE_PROPERTY,
                            'langTag' => true,
                    ];
                }
                if ($pDef->type === RDF::OWL_OBJECT_PROPERTY) {
                    $v = DF::namedNode($v);
                } else {
                    $v = DF::literal($v, $pDef->langTag ? 'en' : null);
                }
                $d->add(DF::quad($r, $pn, $v));
            }
        }

        if (!empty($class)) {
            $d->add(DF::quad($r, DF::namedNode(RDF::RDF_TYPE), DF::namedNode($class)));
            $class = self::$ontology->getClass($class);
            foreach ($class->getProperties() as $i) {
                if ($i->min > 0 && $d->none(new PT(DF::namedNode($i->uri))) && $i->automatedFill === false) {
                    $d->add(DF::quad($r, DF::namedNode($i->uri), self::createSampleProperty($i)));
                }
            }
        }

        return $d;
    }

    static protected function createSampleProperty(PropertyDesc $i): TermInterface {
        if ($i->type === RDF::OWL_DATATYPE_PROPERTY) {
            foreach ($i->range as $j) {
                switch ($j) {
                    case RDF::XSD_DECIMAL:
                    case RDF::XSD_BYTE;
                    case RDF::XSD_DOUBLE;
                    case RDF::XSD_FLOAT;
                    case RDF::XSD_INT;
                    case RDF::XSD_INTEGER;
                    case RDF::XSD_NON_NEGATIVE_INTEGER;
                    case RDF::XSD_POSITIVE_INTEGER;
                        return DF::literal(123, null, $j);
                    case RDF::XSD_NON_POSITIVE_INTEGER;
                    case RDF::XSD_NEGATIVE_INTEGER;
                        return DF::literal(-321, null, $j);
                    case RDF::XSD_DATE:
                        return DF::literal('2019-01-01', null, $j);
                    case RDF::XSD_DATE_TIME:
                        return DF::literal('2019-01-01T12:00:00', null, $j);
                }
            }
            return DF::literal('sample', 'en');
        } elseif (!empty($i->vocabs)) {
            return DF::namedNode(current($i->vocabularyValues)->concept[0]);
        } else {
            return DF::namedNode(self::$sampleResUri);
        }
    }

    static public function getPropertyDefault(string $property): ?string {
        return self::$ontology->getProperty([], $property)->defaultValue ?? null;
    }
}
