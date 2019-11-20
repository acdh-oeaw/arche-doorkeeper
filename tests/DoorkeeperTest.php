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
use EasyRdf\Resource;
use GuzzleHttp\Exception\ClientException;
use zozlak\RdfConstants as RDF;
use acdhOeaw\acdhRepoLib\Repo;
use acdhOeaw\acdhRepoLib\RepoResource;
use acdhOeaw\acdhRepoLib\exception\NotFound;

/**
 * Description of DoorkeeperTest
 *
 * @author zozlak
 */
class DoorkeeperTest extends \PHPUnit\Framework\TestCase {

    /**
     *
     * @var \acdhOeaw\acdhRepoLib\Repo
     */
    static protected $repo;
    static protected $config;
    static protected $ontology;

    static public function setUpBeforeClass(): void {
        parent::setUpBeforeClass();

        $localCfg       = yaml_parse_file(__DIR__ . '/../config-sample.yaml');
        self::$config   = json_decode(json_encode(yaml_parse_file($localCfg['doorkeeper']['restConfigDstPath'])));
        self::$repo     = Repo::factory(self::$config->doorkeeper->restConfigDstPath);
        self::$ontology = new Ontology(new PDO(self::$config->dbConnStr->admin), self::$config->schema->namespaces->ontology);
    }

    /**
     *
     * @var \acdhOeaw\acdhRepoLib\RepoResource[]
     */
    private $toDelete;

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
                    $i->delete(true, true);
                } catch (NotFound $e) {
                    
                }
            }
            self::$repo->commit();
        }
    }

    static private function toStr(array $v): array {
        return array_map(function($x) {
            return (string) $x;
        }, $v);
    }

    static private function createMetadata(array $props = []): Resource {
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

        return $r;
    }

    public function testIdCount(): void {
        $im = (new Graph())->resource('.');
        self::$repo->begin();
        try {
            self::$repo->createResource($im);
        } catch (ClientException $e) {
            $resp = $e->getResponse();
            $this->assertEquals(400, $resp->getStatusCode());
            $this->assertEquals('No non-repository id', (string) $resp->getBody());
        }
    }

    public function testOntologyIdCount(): void {
        $prop  = self::$config->schema->id;
        $label = self::$config->schema->label;
        $nmsp  = self::$config->schema->namespaces->ontology;

        $im = (new Graph())->resource('.');
        $im->addResource($prop, $nmsp . '/foo');
        $im->addLiteral($label, 'bar');
        self::$repo->begin();
        $r  = self::$repo->createResource($im);
        $this->assertIsObject($r);
        self::$repo->rollback();

        $im = (new Graph())->resource('.');
        $im->addResource($prop, $nmsp . '/foo');
        $im->addResource($prop, $nmsp . '/bar');
        $im->addLiteral($label, 'bar');
        self::$repo->begin();
        try {
            self::$repo->createResource($im);
        } catch (ClientException $e) {
            $resp = $e->getResponse();
            $this->assertEquals(400, $resp->getStatusCode());
            $this->assertEquals('More than one ontology id', (string) $resp->getBody());
        }
        self::$repo->rollback();

        $im = (new Graph())->resource('.');
        $im->addResource($prop, $nmsp . '/foo');
        $im->addResource($prop, 'https://my/id');
        $im->addLiteral($label, 'bar');
        self::$repo->begin();
        try {
            self::$repo->createResource($im);
        } catch (ClientException $e) {
            $resp = $e->getResponse();
            $this->assertEquals(400, $resp->getStatusCode());
            $this->assertEquals('Ontology resource can not have additional ids', (string) $resp->getBody());
        }
        self::$repo->rollback();
    }

    public function testChangeId(): void {
        $id1              = 'https://foo/bar/' . time() . rand();
        $id2              = 'https://bar/foo/' . time() . rand();
        $im               = self::createMetadata([
                self::$config->schema->id => $id1,
        ]);
        self::$repo->begin();
        $r                = self::$repo->createResource($im);
        self::$repo->commit();
        $this->toDelete[] = $r;

        self::$repo->begin();
        $im  = self::createMetadata([
                self::$config->schema->id => $id2,
        ]);
        $r->setMetadata($im);
        $r->updateMetadata(RepoResource::UPDATE_OVERWRITE);
        $ids = array_values(array_diff($r->getIds(), [$r->getUri()]));
        $this->assertCount(1, $ids);
        $this->assertEquals($id2, $ids[0]);
        self::$repo->commit();

        self::$repo->begin();
        $r->delete(true);
        self::$repo->commit();
    }

    public function testIdNormalization(): void {
        $im  = self::createMetadata([
                self::$config->schema->id => 'http://x.geonames.org/123/vienna.html',
        ]);
        self::$repo->begin();
        $r   = self::$repo->createResource($im);
        $ids = array_values(array_diff($r->getIds(), [$r->getUri()]));
        $this->assertCount(1, $ids);
        $this->assertEquals('https://www.geonames.org/123', $ids[0]);
    }

    public function testMaintainRange(): void {
        $im = self::createMetadata([
                'https://vocabs.acdh.oeaw.ac.at/schema#hasUpdatedDate' => '2017',
                'https://vocabs.acdh.oeaw.ac.at/schema#hasBinarySize'  => '300.54',
                'https://other/property'                               => new Literal('test value', 'en'),
        ]);

        self::$repo->begin();
        $r  = self::$repo->createResource($im);
        $om = $r->getGraph();

        $date = $om->getLiteral('https://vocabs.acdh.oeaw.ac.at/schema#hasUpdatedDate');
        $this->assertEquals(RDF::XSD_DATE, $date->getDatatypeUri());
        $this->assertEquals('2017-01-01', (string) $date);

        $int = $om->getLiteral('https://vocabs.acdh.oeaw.ac.at/schema#hasBinarySize');
        $this->assertEquals(RDF::XSD_BYTE, $int->getDatatypeUri());
        $this->assertEquals(300, $int->getValue());

        $str = $om->getLiteral('https://other/property');
        $this->assertEquals(RDF::XSD_STRING, $str->getDatatypeUri() ?? RDF::XSD_STRING);
        $this->assertEquals('en', $str->getLang());
    }

    public function testCardinalities(): void {
        $im = self::createMetadata([
                RDF::RDF_TYPE => 'https://vocabs.acdh.oeaw.ac.at/schema#Collection',
        ]);
        self::$repo->begin();
        try {
            self::$repo->createResource($im);
        } catch (ClientException $e) {
            $resp = $e->getResponse();
            $this->assertEquals(400, $resp->getStatusCode());
            $this->assertRegExp('/^Min property count for .* but resource has 0$/', (string) $resp->getBody());
        }

        $class = self::$ontology->getClass('https://vocabs.acdh.oeaw.ac.at/schema#Collection');
        foreach ($class->properties as $i) {
            if ($i->min > 0) {
                if ($i->type === 'http://www.w3.org/2002/07/owl#DatatypeProperty') {
                    $im->addLiteral($i->property, 'sample');
                } else {
                    $im->addResource($i->property, 'https://sample');
                }
            }
        }
        $r = self::$repo->createResource($im);
        $this->assertIsObject($r);
    }

    public function testDefaultProperties(): void {
        $im = self::createMetadata([
            RDF::RDF_TYPE => 'https://vocabs.acdh.oeaw.ac.at/schema#RepoObject',
        ]);
        $skip = [
            self::$config->schema->hosting,
            self::$config->schema->accessRestriction,
        ];
        $class = self::$ontology->getClass('https://vocabs.acdh.oeaw.ac.at/schema#RepoObject');
        foreach ($class->properties as $i) {
            if ($i->min > 0 && !in_array($i->property, $skip)) {
                if ($i->type === 'http://www.w3.org/2002/07/owl#DatatypeProperty') {
                    $im->addLiteral($i->property, 'sample');
                } else {
                    $im->addResource($i->property, 'https://sample');
                }
            }
        }
        self::$repo->begin();
        $r = self::$repo->createResource($im);
        $rm = $r->getGraph();
        
        $rh = new RepoResource((string) $rm->get(self::$config->schema->hosting), self::$repo);
        $this->assertContains(self::$config->doorkeeper->default->hosting, $rh->getIds());
        
        $rar = new RepoResource((string) $rm->get(self::$config->schema->accessRestriction), self::$repo);
        $this->assertContains(self::$config->doorkeeper->default->accessRestriction, $rar->getIds());
        
        //$this->assertEquals('format, extent & available date should be set up in the repository schema', '');
    }

    public function testAccessRights(): void {
        
    }

    public function testTitle(): void {
        
    }

    public function testCollectionExtent(): void {
        
    }

}
