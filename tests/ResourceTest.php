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

use EasyRdf\Graph;
use EasyRdf\Literal;
use GuzzleHttp\Client;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Exception\ClientException;
use zozlak\RdfConstants as RDF;
use acdhOeaw\acdhRepoLib\RepoResource;
use acdhOeaw\acdhRepoLib\BinaryPayload;

/**
 * Description of DoorkeeperTest
 *
 * @author zozlak
 */
class ResourceTest extends TestBase {

    public function testIdCount(): void {
        $im = (new Graph())->resource('.');
        self::$repo->begin();
        try {
            self::$repo->createResource($im);
            $this->assertTrue(false);
        } catch (ClientException $e) {
            $resp   = $e->getResponse();
            $this->assertEquals(400, $resp->getStatusCode());
            $errors = explode("\n", (string) $resp->getBody());
            $this->assertContains('No non-repository id', $errors);
        }
    }

    public function testOntologyIdCount(): void {
        $prop  = self::$config->schema->id;
        $label = self::$config->schema->label;
        $nmsp  = self::$config->schema->namespaces->ontology;

        $im = (new Graph())->resource('.');
        $im->addResource($prop, $nmsp . '/foo');
        $im->addLiteral($label, 'bar', 'en');
        self::$repo->begin();
        $r  = self::$repo->createResource($im);
        $this->assertIsObject($r);
        self::$repo->rollback();

        $im = (new Graph())->resource('.');
        $im->addResource($prop, $nmsp . '/foo');
        $im->addResource($prop, $nmsp . '/bar');
        $im->addLiteral($label, 'bar', 'en');
        self::$repo->begin();
        try {
            self::$repo->createResource($im);
            $this->assertTrue(false);
        } catch (ClientException $e) {
            $resp = $e->getResponse();
            $this->assertEquals(400, $resp->getStatusCode());
            $this->assertEquals('More than one ontology id', (string) $resp->getBody());
        }
        self::$repo->rollback();

        $im = (new Graph())->resource('.');
        $im->addResource($prop, $nmsp . '/foo');
        $im->addResource($prop, 'https://my/id');
        $im->addLiteral($label, 'bar', 'en');
        self::$repo->begin();
        try {
            self::$repo->createResource($im);
            $this->assertTrue(false);
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
                'https://vocabs.acdh.oeaw.ac.at/schema#hasCreatedDate' => '2017',
                'https://vocabs.acdh.oeaw.ac.at/schema#hasBinarySize'  => '300.54',
                'https://other/property'                               => new Literal('test value', 'en'),
        ]);

        self::$repo->begin();
        $r  = self::$repo->createResource($im);
        $om = $r->getGraph();

        $date = $om->getLiteral('https://vocabs.acdh.oeaw.ac.at/schema#hasCreatedDate');
        $this->assertEquals(RDF::XSD_DATE, $date->getDatatypeUri());
        $this->assertEquals('2017-01-01', (string) $date);

        $int = $om->getLiteral('https://vocabs.acdh.oeaw.ac.at/schema#hasBinarySize');
        $this->assertEquals(RDF::XSD_NON_NEGATIVE_INTEGER, $int->getDatatypeUri());
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
            $this->assertTrue(false);
        } catch (ClientException $e) {
            $resp = $e->getResponse();
            $this->assertEquals(400, $resp->getStatusCode());
            $this->assertRegExp('/^Min property count for .* but resource has 0$/', (string) $resp->getBody());
        }

        $class = self::$ontology->getClass('https://vocabs.acdh.oeaw.ac.at/schema#Collection');
        foreach ($class->getProperties() as $i) {
            if ($i->min > 0) {
                $im->add($i->uri, self::createSampleProperty($i));
            }
        }
        $r = self::$repo->createResource($im);
        $this->assertIsObject($r);
    }

    public function testDefaultProperties(): void {
        $accessRestProp   = self::$config->schema->accessRestriction;
        $creationDateProp = self::$config->schema->creationDate;
        $im               = self::createMetadata([
                RDF::RDF_TYPE => 'https://vocabs.acdh.oeaw.ac.at/schema#RepoObject',
        ]);
        $skip             = [
            self::$config->schema->hosting, $accessRestProp, $creationDateProp
        ];
        $class            = self::$ontology->getClass('https://vocabs.acdh.oeaw.ac.at/schema#RepoObject');
        foreach ($class->getProperties() as $i) {
            if ($i->min > 0 && !in_array($i->uri, $skip)) {
                $im->add($i->property[0], self::createSampleProperty($i));
            }
        }
        self::$repo->begin();
        $r  = self::$repo->createResource($im, new BinaryPayload('foo bar', null, 'text/plain'));
        $rm = $r->getGraph();

        $rh = new RepoResource((string) $rm->get(self::$config->schema->hosting), self::$repo);
        $this->assertContains(self::$config->doorkeeper->default->hosting, $rh->getIds());

        $rar = new RepoResource((string) $rm->get($accessRestProp), self::$repo);
        $this->assertContains(self::$config->doorkeeper->default->accessRestriction, $rar->getIds());

        $this->assertEquals(date('Y-m-d'), substr($rm->getLiteral($creationDateProp), 0, 10));
        $this->assertEquals('text/plain', (string) $rm->getLiteral(self::$config->schema->mime));
        $this->assertEquals('7', (string) $rm->getLiteral(self::$config->schema->binarySizeCumulative));
    }

    public function testAccessRightsAuto(): void {
        $im = self::createMetadata([], 'https://vocabs.acdh.oeaw.ac.at/schema#RepoObject');
        self::$repo->begin();
        $r  = self::$repo->createResource($im);
        $om = $r->getGraph();
        $ar = new RepoResource((string) $om->getResource(self::$config->schema->accessRestriction), self::$repo);
        $this->assertContains(self::$config->doorkeeper->default->accessRestriction, $ar->getIds());
        $this->assertContains(self::$config->doorkeeper->rolePublic, self::toStr($om->all(self::$config->accessControl->schema->read)));
        $this->assertNotContains(self::$config->doorkeeper->rolePublic, self::toStr($om->all(self::$config->accessControl->schema->write)));
    }

    public function testAccessRightsAcademic(): void {
        $accessRestProp = self::$config->schema->accessRestriction;
        $im             = self::createMetadata([
                $accessRestProp => 'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/academic',
                ], 'https://vocabs.acdh.oeaw.ac.at/schema#RepoObject');
        self::$repo->begin();
        $r              = self::$repo->createResource($im);
        $om             = $r->getGraph();
        $ar             = new RepoResource((string) $om->getResource($accessRestProp), self::$repo);
        $this->assertContains('https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/academic', $ar->getIds());
        $this->assertContains(self::$config->doorkeeper->roleAcademic, self::toStr($om->all(self::$config->accessControl->schema->read)));
        $this->assertNotContains(self::$config->doorkeeper->roleAcademic, self::toStr($om->all(self::$config->accessControl->schema->write)));

        $client = new Client(['http_errors' => false]);
        $resp   = $client->send(new Request('get', $r->getUri()));
        $this->assertEquals(403, $resp->getStatusCode());
        $resp   = $client->send(new Request('get', $r->getUri(), ['eppn' => self::$config->doorkeeper->roleAcademic]));
        $this->assertEquals(204, $resp->getStatusCode());
    }

    public function testAccessRightsRestricted(): void {
        $accessRestProp = self::$config->schema->accessRestriction;
        $im             = self::createMetadata([
                $accessRestProp                   => 'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/restricted',
                self::$config->schema->accessRole => 'foo',
                ], 'https://vocabs.acdh.oeaw.ac.at/schema#RepoObject');
        self::$repo->begin();
        $r              = self::$repo->createResource($im);
        $om             = $r->getGraph();
        $ar             = new RepoResource((string) $om->getResource($accessRestProp), self::$repo);
        $this->assertContains('https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/restricted', $ar->getIds());
        $this->assertNotContains(self::$config->doorkeeper->rolePublic, self::toStr($om->all(self::$config->accessControl->schema->read)));
        $this->assertNotContains(self::$config->doorkeeper->roleAcademic, self::toStr($om->all(self::$config->accessControl->schema->write)));
        $this->assertContains('foo', self::toStr($om->all(self::$config->schema->accessRole)));

        $client = new Client(['http_errors' => false]);
        $resp   = $client->send(new Request('get', $r->getUri()));
        $this->assertEquals(403, $resp->getStatusCode());
        $resp   = $client->send(new Request('get', $r->getUri(), ['eppn' => self::$config->doorkeeper->roleAcademic]));
        $this->assertEquals(403, $resp->getStatusCode());
        $resp   = $client->send(new Request('get', $r->getUri(), ['eppn' => 'foo']));
        $this->assertEquals(204, $resp->getStatusCode());
    }

    /**
     * 
     * @depends testAccessRightsAuto
     * @depends testAccessRightsAcademic
     * @depends testAccessRightsRestricted
     */
    public function testAccessRightsRise(): void {
        $accessRestProp = self::$config->schema->accessRestriction;
        $client         = new Client(['http_errors' => false]);

        $im = self::createMetadata([
                $accessRestProp => 'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/restricted',
                ], 'https://vocabs.acdh.oeaw.ac.at/schema#RepoObject');
        self::$repo->begin();
        $r  = self::$repo->createResource($im);

        $meta = (new Graph())->resource('.');
        $meta->addResource($accessRestProp, 'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/academic');
        $r->setMetadata($meta);
        $r->updateMetadata(RepoResource::UPDATE_MERGE);
        $resp = $client->send(new Request('get', $r->getUri()));
        $this->assertEquals(403, $resp->getStatusCode());
        $resp = $client->send(new Request('get', $r->getUri(), ['eppn' => self::$config->doorkeeper->roleAcademic]));
        $this->assertEquals(204, $resp->getStatusCode());

        $meta = (new Graph())->resource('.');
        $meta->addResource($accessRestProp, 'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/public');
        $r->setMetadata($meta);
        $r->updateMetadata(RepoResource::UPDATE_MERGE);
        $resp = $client->send(new Request('get', $r->getUri()));
        $this->assertEquals(204, $resp->getStatusCode());
    }

    /**
     * 
     * @depends testAccessRightsAuto
     * @depends testAccessRightsAcademic
     * @depends testAccessRightsRestricted
     */
    public function testAccessRightsLower(): void {
        $accessRestrProp = self::$config->schema->accessRestriction;
        $client          = new Client(['http_errors' => false]);

        $im = self::createMetadata([
                $accessRestrProp => 'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/public',
                ], 'https://vocabs.acdh.oeaw.ac.at/schema#RepoObject');
        self::$repo->begin();
        $r  = self::$repo->createResource($im);

        $meta = (new Graph())->resource('.');
        $meta->addResource($accessRestrProp, 'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/academic');
        $r->setMetadata($meta);
        $r->updateMetadata(RepoResource::UPDATE_MERGE);
        $resp = $client->send(new Request('get', $r->getUri()));
        $this->assertEquals(403, $resp->getStatusCode());
        $resp = $client->send(new Request('get', $r->getUri(), ['eppn' => self::$config->doorkeeper->roleAcademic]));
        $this->assertEquals(204, $resp->getStatusCode());

        $meta = (new Graph())->resource('.');
        $meta->addResource($accessRestrProp, 'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/restricted');
        $meta->addLiteral(self::$config->schema->accessRole, 'bar');
        $r->setMetadata($meta);
        $r->updateMetadata(RepoResource::UPDATE_MERGE);
        $resp = $client->send(new Request('get', $r->getUri()));
        $this->assertEquals(403, $resp->getStatusCode());
        $resp = $client->send(new Request('get', $r->getUri(), ['eppn' => self::$config->doorkeeper->roleAcademic]));
        $this->assertEquals(403, $resp->getStatusCode());
        $resp = $client->send(new Request('get', $r->getUri(), ['eppn' => 'bar']));
        $this->assertEquals(204, $resp->getStatusCode());
    }

    public function testTitleAuto(): void {
        $titleProp = self::$config->schema->label;
        self::$repo->begin();

        // copied from other title-like property
        $im = self::createMetadata(['http://purl.org/dc/elements/1.1/title' => 'foo']);
        $im->delete($titleProp);
        $r  = self::$repo->createResource($im);
        $this->assertEquals('foo', (string) $r->getGraph()->getLiteral($titleProp));

        // combined from acdh:hasFirstName and acdh:hasLastName
        $im = self::createMetadata([
                'https://vocabs.acdh.oeaw.ac.at/schema#hasFirstName' => 'foo',
                'https://vocabs.acdh.oeaw.ac.at/schema#hasLastName'  => 'bar',
        ]);
        $im->delete($titleProp);
        $r  = self::$repo->createResource($im);
        $this->assertEquals('foo bar', (string) $r->getGraph()->getLiteral($titleProp));


        // combined from acdh:hasFirstName
        $im = self::createMetadata([
                'https://vocabs.acdh.oeaw.ac.at/schema#hasFirstName' => 'foo'
        ]);
        $im->delete($titleProp);
        $r  = self::$repo->createResource($im);
        $this->assertEquals('foo', (string) $r->getGraph()->getLiteral($titleProp));

        // combined from foaf:givenName and foaf:familyName
        $im = self::createMetadata([
                'http://xmlns.com/foaf/0.1/givenName'  => 'foo',
                'http://xmlns.com/foaf/0.1/familyName' => 'bar',
        ]);
        $im->delete($titleProp);
        $r  = self::$repo->createResource($im);
        $this->assertEquals('foo bar', (string) $r->getGraph()->getLiteral($titleProp));

        // many titles
        $im     = (new Graph())->resource('.');
        $im->addResource(self::$config->schema->id, 'https://id/prop' . time() . rand());
        $im->addLiteral($titleProp, new Literal('foo', 'en'));
        $im->addLiteral($titleProp, new Literal('bar', 'de'));
        $r      = self::$repo->createResource($im);
        $titles = self::toStr($r->getGraph()->allLiterals($titleProp));
        $this->assertEquals(2, count($titles));
        $this->assertContains('foo', $titles);
        $this->assertContains('bar', $titles);
    }

    /**
     * 
     * @depends testTitleAuto
     */
    public function testTitleErrors(): void {
        $titleProp = self::$config->schema->label;
        self::$repo->begin();

        // empty title
        $im = (new Graph())->resource('.');
        $im->addResource(self::$config->schema->id, 'https://id/prop' . time() . rand());
        $im->addLiteral($titleProp, '', 'en');
        try {
            self::$repo->createResource($im);
            $this->assertTrue(false);
        } catch (ClientException $e) {
            $resp = $e->getResponse();
            $this->assertEquals(400, $resp->getStatusCode());
            $this->assertEquals("$titleProp value is empty", (string) $resp->getBody());
        }

        // no title
        $im = (new Graph())->resource('.');
        $im->addResource(self::$config->schema->id, 'https://id/prop' . time() . rand());
        try {
            self::$repo->createResource($im);
            $this->assertTrue(false);
        } catch (ClientException $e) {
            $resp = $e->getResponse();
            $this->assertEquals(400, $resp->getStatusCode());
            $this->assertEquals("$titleProp is missing", (string) $resp->getBody());
        }

        // no language
        $im = (new Graph())->resource('.');
        $im->addResource(self::$config->schema->id, 'https://id/prop' . time() . rand());
        $im->addLiteral($titleProp, 'foo');
        $im->addLiteral($titleProp, 'bar');
        try {
            self::$repo->createResource($im);
            $this->assertTrue(false);
        } catch (ClientException $e) {
            $resp   = $e->getResponse();
            $this->assertEquals(400, $resp->getStatusCode());
            $errors = explode("\n", (string) $resp->getBody());
            $this->assertContains("more than one $titleProp property", $errors);
            $this->assertContains("Property $titleProp with value foo is not tagged with a language", $errors);
        }

        // with language
        $im = (new Graph())->resource('.');
        $im->addResource(self::$config->schema->id, 'https://id/prop' . time() . rand());
        $im->addLiteral($titleProp, new Literal('foo', 'en'));
        $im->addLiteral($titleProp, new Literal('bar', 'en'));
        try {
            self::$repo->createResource($im);
            $this->assertTrue(false);
        } catch (ClientException $e) {
            $resp = $e->getResponse();
            $this->assertEquals(400, $resp->getStatusCode());
            $this->assertEquals("more than one $titleProp property", (string) $resp->getBody());
        }
    }

}
