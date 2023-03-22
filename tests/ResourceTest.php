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

use EasyRdf\Graph;
use EasyRdf\Literal;
use GuzzleHttp\Client;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Exception\ClientException;
use GuzzleHttp\Exception\RequestException;
use zozlak\RdfConstants as RDF;
use acdhOeaw\arche\lib\RepoResource;
use acdhOeaw\arche\lib\BinaryPayload;

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
        $this->assertEquals('https://sws.geonames.org/123/', $ids[0]);
    }

    public function testMaintainRange(): void {
        $pid = 'https://foo.bar/' . rand();
        $im  = self::createMetadata([
                'https://vocabs.acdh.oeaw.ac.at/schema#hasCreatedStartDate' => '2017',
                'https://vocabs.acdh.oeaw.ac.at/schema#hasCreatedEndDate'   => '2017-03-08T20:45:17',
                'https://vocabs.acdh.oeaw.ac.at/schema#hasBinarySize'       => '300.54',
                'https://other/property'                                    => new Literal('test value', 'en'),
                'https://vocabs.acdh.oeaw.ac.at/schema#hasPid'              => $pid,
        ]);

        self::$repo->begin();
        $r  = self::$repo->createResource($im);
        $om = $r->getGraph();

        $date = $om->getLiteral('https://vocabs.acdh.oeaw.ac.at/schema#hasCreatedStartDate');
        $this->assertEquals(RDF::XSD_DATE, $date->getDatatypeUri());
        $this->assertEquals('2017-01-01', (string) $date);

        $date = $om->getLiteral('https://vocabs.acdh.oeaw.ac.at/schema#hasCreatedEndDate');
        $this->assertEquals(RDF::XSD_DATE, $date->getDatatypeUri());
        $this->assertEquals('2017-03-08', (string) $date);

        $int = $om->getLiteral('https://vocabs.acdh.oeaw.ac.at/schema#hasBinarySize');
        $this->assertEquals(RDF::XSD_NON_NEGATIVE_INTEGER, $int->getDatatypeUri());
        $this->assertEquals(300, $int->getValue());

        $str = $om->getLiteral('https://other/property');
        $this->assertEquals(RDF::XSD_STRING, $str->getDatatypeUri() ?? RDF::XSD_STRING);
        $this->assertEquals('en', $str->getLang());

        $uri = $om->getLiteral('https://vocabs.acdh.oeaw.ac.at/schema#hasPid');
        $this->assertEquals(RDF::XSD_ANY_URI, $uri->getDatatypeUri());
        $this->assertEquals($pid, $uri->getValue());
    }

    public function testWrongRange(): void {
        $pid = 'https://foo.bar/' . rand();
        self::$repo->begin();

        $im = self::createMetadata([
                'https://vocabs.acdh.oeaw.ac.at/schema#hasCreatedStartDate' => 'foo',
        ]);
        try {
            self::$repo->createResource($im);
            $this->assertTrue(false);
        } catch (RequestException $e) {
            $this->assertMatchesRegularExpression('/value does not match data type/', $e->getMessage());
        }

        $im = self::createMetadata([
                'https://vocabs.acdh.oeaw.ac.at/schema#hasBinarySize' => 'bar',
        ]);
        try {
            self::$repo->createResource($im);
            $this->assertTrue(false);
        } catch (RequestException $e) {
            $this->assertMatchesRegularExpression('/value does not match data type/', $e->getMessage());
        }

        self::$repo->rollback();
    }

    public function testCardinalitiesMin(): void {
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
            $this->assertMatchesRegularExpression('/^Min property count for .* but resource has 0$/', (string) $resp->getBody());
        }

        $class = self::$ontology->getClass('https://vocabs.acdh.oeaw.ac.at/schema#Collection');
        foreach ($class->getProperties() as $i) {
            if ($i->min > 0 && $im->get($i->uri) === null) {
                $im->add($i->uri, self::createSampleProperty($i));
            }
        }
        $r = self::$repo->createResource($im);
        $this->assertIsObject($r);
    }

    public function testPropertyType(): void {
        $im = self::createMetadata();
        $im->addResource('https://vocabs.acdh.oeaw.ac.at/schema#hasUrl', 'http://foo.bar/' . rand());
        self::$repo->begin();
        try {
            self::$repo->createResource($im);
            $this->assertTrue(false);
        } catch (ClientException $e) {
            $resp = $e->getResponse();
            $this->assertEquals(400, $resp->getStatusCode());
            $this->assertMatchesRegularExpression('/^URI value for a datatype property .*hasUrl/', (string) $resp->getBody());
        }
    }

    public function testCardinalitiesMax(): void {
        $idProp = self::$config->schema->id;
        $prop   = 'https://vocabs.acdh.oeaw.ac.at/schema#hasTransferDate';
        $im     = self::createMetadata([], 'https://vocabs.acdh.oeaw.ac.at/schema#Resource');
        $im->addLiteral($prop, '2020-07-01');
        self::$repo->begin();
        $r      = self::$repo->createResource($im);
        $this->assertIsObject($r);

        $im->deleteResource($idProp);
        $im->addResource($idProp, 'https://id.acdh.oeaw.ac.at/test/' . microtime(true));
        $im->addLiteral($prop, '2020-08-01');
        try {
            self::$repo->createResource($im);
            $this->assertTrue(false);
        } catch (ClientException $e) {
            $resp = $e->getResponse();
            $this->assertEquals(400, $resp->getStatusCode());
            $this->assertMatchesRegularExpression('/^Max property count for .* but resource has 2$/', (string) $resp->getBody());
        }
    }

    public function testDefaultProperties(): void {
        $mimeProp         = self::$config->schema->mime;
        $accessRestProp   = self::$config->schema->accessRestriction;
        $creationDateProp = self::$config->schema->creationDate;
        $im               = self::createMetadata([
                RDF::RDF_TYPE => 'https://vocabs.acdh.oeaw.ac.at/schema#Collection',
        ]);
        $skip             = [
            self::$config->schema->hosting, $accessRestProp, $creationDateProp
        ];
        $class            = self::$ontology->getClass('https://vocabs.acdh.oeaw.ac.at/schema#Collection');
        foreach ($class->getProperties() as $i) {
            if ($i->min > 0 && $im->get($i->uri) === null && !in_array($i->uri, $skip)) {
                $im->add($i->property[0], self::createSampleProperty($i));
            }
        }
        self::$repo->begin();
        $r  = self::$repo->createResource($im);
        $rm = $r->getGraph();
        $this->assertEquals(date('Y-m-d'), substr($rm->getLiteral($creationDateProp), 0, 10));
        // accessRestriction is only on BinaryContent (Resource/Metadata) and not on RepoObject
        $this->assertNull($rm->get($accessRestProp));
        $this->assertNull($rm->get($mimeProp));

        $rh = new RepoResource((string) $rm->get(self::$config->schema->hosting), self::$repo);
        $this->assertContains(self::getPropertyDefault(self::$config->schema->hosting), $rh->getIds());

        $im  = self::createMetadata([], 'https://vocabs.acdh.oeaw.ac.at/schema#Resource');
        $r   = self::$repo->createResource($im, new BinaryPayload('foo bar', null, 'text/plain'));
        $rm  = $r->getGraph();
        $this->assertEquals('text/plain', (string) $rm->getLiteral($mimeProp));
        $rar = new RepoResource((string) $r->getGraph()->get($accessRestProp), self::$repo);
        $this->assertContains(self::getPropertyDefault($accessRestProp), $rar->getIds());
    }

    public function testAccessRightsAuto(): void {
        $im = self::createMetadata([], 'https://vocabs.acdh.oeaw.ac.at/schema#Resource');
        self::$repo->begin();
        $r  = self::$repo->createResource($im);
        $om = $r->getGraph();
        $ar = new RepoResource((string) $om->getResource(self::$config->schema->accessRestriction), self::$repo);
        $this->assertContains(self::getPropertyDefault(self::$config->schema->accessRestriction), $ar->getIds());
        $this->assertContains(self::$config->doorkeeper->rolePublic, self::toStr($om->all(self::$config->accessControl->schema->read)));
        $this->assertNotContains(self::$config->doorkeeper->rolePublic, self::toStr($om->all(self::$config->accessControl->schema->write)));
    }

    public function testAccessRightsAcademic(): void {
        $accessRestProp = self::$config->schema->accessRestriction;
        $im             = self::createMetadata([
                $accessRestProp => 'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/academic',
                ], 'https://vocabs.acdh.oeaw.ac.at/schema#Resource');
        $bp             = new BinaryPayload('dummy content');
        self::$repo->begin();
        $r              = self::$repo->createResource($im, $bp);
        $om             = $r->getGraph();
        $ar             = new RepoResource((string) $om->getResource($accessRestProp), self::$repo);
        $this->assertContains('https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/academic', $ar->getIds());
        $this->assertContains(self::$config->doorkeeper->roleAcademic, self::toStr($om->all(self::$config->accessControl->schema->read)));
        $this->assertNotContains(self::$config->doorkeeper->roleAcademic, self::toStr($om->all(self::$config->accessControl->schema->write)));

        $client = new Client(['http_errors' => false, 'allow_redirects' => false]);
        $resp   = $client->send(new Request('get', $r->getUri()));
        $this->assertEquals(403, $resp->getStatusCode());
        $resp   = $client->send(new Request('get', $r->getUri(), ['eppn' => self::$config->doorkeeper->roleAcademic]));
        $this->assertEquals(200, $resp->getStatusCode());
    }

    public function testAccessRightsRestricted(): void {
        $accessRestProp = self::$config->schema->accessRestriction;
        $im             = self::createMetadata([
                $accessRestProp                   => 'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/restricted',
                self::$config->schema->accessRole => 'foo',
                ], 'https://vocabs.acdh.oeaw.ac.at/schema#Resource');
        $bp             = new BinaryPayload('dummy content');
        self::$repo->begin();
        $r              = self::$repo->createResource($im, $bp);
        $om             = $r->getGraph();
        $ar             = new RepoResource((string) $om->getResource($accessRestProp), self::$repo);
        $this->assertContains('https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/restricted', $ar->getIds());
        $this->assertNotContains(self::$config->doorkeeper->rolePublic, self::toStr($om->all(self::$config->accessControl->schema->read)));
        $this->assertNotContains(self::$config->doorkeeper->roleAcademic, self::toStr($om->all(self::$config->accessControl->schema->write)));
        $this->assertContains('foo', self::toStr($om->all(self::$config->schema->accessRole)));

        $client = new Client(['http_errors' => false, 'allow_redirects' => false]);
        $resp   = $client->send(new Request('get', $r->getUri()));
        $this->assertEquals(403, $resp->getStatusCode());
        $resp   = $client->send(new Request('get', $r->getUri(), ['eppn' => self::$config->doorkeeper->roleAcademic]));
        $this->assertEquals(403, $resp->getStatusCode());
        $resp   = $client->send(new Request('get', $r->getUri(), ['eppn' => 'foo']));
        $this->assertEquals(200, $resp->getStatusCode());
    }

    /**
     * 
     * @depends testAccessRightsAuto
     * @depends testAccessRightsAcademic
     * @depends testAccessRightsRestricted
     */
    public function testAccessRightsRise(): void {
        $accessRestProp = self::$config->schema->accessRestriction;
        $client         = new Client(['http_errors' => false, 'allow_redirects' => false]);

        $im = self::createMetadata([
                $accessRestProp => 'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/restricted',
                ], 'https://vocabs.acdh.oeaw.ac.at/schema#Resource');
        $bp = new BinaryPayload('dummy content');
        self::$repo->begin();
        $r  = self::$repo->createResource($im, $bp);

        $meta = (new Graph())->resource('.');
        $meta->addResource($accessRestProp, 'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/academic');
        $r->setMetadata($meta);
        $r->updateMetadata(RepoResource::UPDATE_MERGE);
        $resp = $client->send(new Request('get', $r->getUri()));
        $this->assertEquals(403, $resp->getStatusCode());
        $resp = $client->send(new Request('get', $r->getUri(), ['eppn' => self::$config->doorkeeper->roleAcademic]));
        $this->assertEquals(200, $resp->getStatusCode());

        $meta = (new Graph())->resource('.');
        $meta->addResource($accessRestProp, 'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/public');
        $r->setMetadata($meta);
        $r->updateMetadata(RepoResource::UPDATE_MERGE);
        $resp = $client->send(new Request('get', $r->getUri()));
        $this->assertEquals(200, $resp->getStatusCode());
    }

    /**
     * 
     * @depends testAccessRightsAuto
     * @depends testAccessRightsAcademic
     * @depends testAccessRightsRestricted
     */
    public function testAccessRightsLower(): void {
        $accessRestrProp = self::$config->schema->accessRestriction;
        $client          = new Client(['http_errors' => false, 'allow_redirects' => false]);

        $im = self::createMetadata([
                $accessRestrProp => 'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/public',
                ], 'https://vocabs.acdh.oeaw.ac.at/schema#Resource');
        $bp = new BinaryPayload('dummy content');
        self::$repo->begin();
        $r  = self::$repo->createResource($im, $bp);

        $meta = (new Graph())->resource('.');
        $meta->addResource($accessRestrProp, 'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/academic');
        $r->setMetadata($meta);
        $r->updateMetadata(RepoResource::UPDATE_MERGE);
        $resp = $client->send(new Request('get', $r->getUri()));
        $this->assertEquals(403, $resp->getStatusCode());
        $resp = $client->send(new Request('get', $r->getUri(), ['eppn' => self::$config->doorkeeper->roleAcademic]));
        $this->assertEquals(200, $resp->getStatusCode());

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
        $this->assertEquals(200, $resp->getStatusCode());
    }

    public function testTitleAuto(): void {
        $titleProp = self::$config->schema->label;
        self::$repo->begin();

        // copied from other title-like property
        $im = self::createMetadata(['http://purl.org/dc/elements/1.1/title' => new Literal('foo', 'en')]);
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

    public function testTitlePreserveOtherLang(): void {
        $titleProp = self::$config->schema->label;
        self::$repo->begin();

        $meta = self::createMetadata([$titleProp => new Literal('foo', 'en')]);
        $res  = self::$repo->createResource($meta);
        $meta->delete($titleProp);
        $meta->addLiteral($titleProp, 'bar', 'de');

        $res->setMetadata($meta);
        $res->updateMetadata(RepoResource::UPDATE_MERGE);
        $tmp    = $res->getGraph()->allLiterals($titleProp);
        $titles = [];
        foreach ($tmp as $i) {
            $titles[$i->getLang()] = (string) $i;
        }
        $this->assertEquals(2, count($titles));
        $this->assertArrayHasKey('en', $titles);
        $this->assertArrayHasKey('de', $titles);
        $this->assertEquals('foo', $titles['en']);
        $this->assertEquals('bar', $titles['de']);

        $res->setMetadata($meta);
        $res->updateMetadata(RepoResource::UPDATE_OVERWRITE);
        $titles = $res->getGraph()->allLiterals($titleProp);
        $this->assertEquals(1, count($titles));
        $this->assertEquals('bar', (string) $titles[0]);
        $this->assertEquals('de', $titles[0]->getLang());

        self::$repo->rollback();
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

    public function testPidGeneration(): void {
        $idProp  = self::$config->schema->id;
        $pidProp = self::$config->schema->pid;
        $pidNmsp = self::$config->doorkeeper->epicPid->resolver;
        $idNmsp  = self::$config->schema->namespaces->id;
        $idn     = rand();
        $im      = self::createMetadata([$idProp => $idNmsp . $idn]);
        self::$repo->begin();

        // no pid generated automatically
        $r  = self::$repo->createResource($im);
        $m1 = $r->getGraph();
        $this->assertEquals(0, count($m1->all($pidProp)));

        // pid generated automatically and promoted to an id
        $m1->addLiteral($pidProp, self::$config->doorkeeper->epicPid->createValue);
        $r->setGraph($m1);
        $r->updateMetadata();
        $m2   = $r->getGraph();
        $this->assertEquals(0, count($m2->allResources($pidProp)));
        $pids = $m2->allLiterals($pidProp);
        $this->assertEquals(1, count($pids));
        $this->assertStringStartsWith($pidNmsp, (string) $pids[0]);
        $this->assertContains((string) $pids[0], $this->toStr($m2->allResources($idProp)));

        self::$repo->rollback();
    }

    public function testPidPreserving(): void {
        $idProp      = self::$config->schema->id;
        $pidProp     = self::$config->schema->pid;
        $cmdiPidProp = self::$config->schema->cmdiPid;
        $pidNmsp     = self::$config->doorkeeper->epicPid->resolver;
        $idNmsp      = self::$config->schema->namespaces->id;
        $httpsPid    = $pidNmsp . self::$config->doorkeeper->epicPid->prefix . '/123';
        $httpPid     = str_replace('https://', 'http://', $httpsPid);
        self::$repo->begin();

        // existing pid not overwritten but promoted to an id
        $idn  = rand();
        $im   = self::createMetadata([
                $idProp  => $idNmsp . $idn,
                $pidProp => $httpPid,
        ]);
        $r    = self::$repo->createResource($im);
        $m1   = $r->getGraph();
        $pids = $m1->allLiterals($pidProp);
        $this->assertEquals(1, count($pids));
        $this->assertEquals($httpsPid, (string) $pids[0]);
        $this->assertContains($httpsPid, $this->toStr($m1->allResources($idProp)));

        // pid refreshed from one stored as an id
        $m2   = $r->getGraph();
        $m2->delete($pidProp);
        $m2->addLiteral($pidProp, self::$config->doorkeeper->epicPid->createValue);
        $r->setGraph($m2);
        $r->updateMetadata();
        $m3   = $r->getGraph();
        $pids = $m3->allLiterals($pidProp);
        $this->assertEquals(1, count($pids));
        $this->assertEquals($httpsPid, (string) $pids[0]);
        $this->assertContains($httpsPid, $this->toStr($m3->allResources($idProp)));

        self::$repo->rollback();
    }

    public function testUnknownProperty(): void {
        $cfgFile = __DIR__ . '/../config/yaml/config-repo.yaml';
        $cfg     = yaml_parse_file($cfgFile);
        $im      = self::createMetadata(['https://vocabs.acdh.oeaw.ac.at/schema#foo' => 'bar']);

        // turn off the check
        $cfg['doorkeeper']['checkUnknownProperties'] = false;
        yaml_emit_file($cfgFile, $cfg);

        self::$repo->begin();
        $r = self::$repo->createResource($im);
        self::$repo->rollback();

        // turn on the check
        $cfg['doorkeeper']['checkUnknownProperties'] = true;
        yaml_emit_file($cfgFile, $cfg);

        self::$repo->begin();
        try {
            $r = self::$repo->createResource($im);
            $this->assertTrue(false);
        } catch (ClientException $e) {
            $resp = $e->getResponse();
            $this->assertEquals(400, $resp->getStatusCode());
            $this->assertEquals("property https://vocabs.acdh.oeaw.ac.at/schema#foo is in the ontology namespace but is not included in the ontology", (string) $resp->getBody());
        }

        $idProp = self::$config->schema->id;
        $im->delete($idProp);
        $im->addResource($idProp, self::$config->schema->namespaces->ontology . 'test');
        $r      = self::$repo->createResource($im);
        $this->assertInstanceOf(RepoResource::class, $r);
        self::$repo->rollback();
    }

    public function testCmdiPid(): void {
        $ycfgFile                                     = __DIR__ . '/../config/yaml/config-repo.yaml';
        $ycfg                                         = yaml_parse_file($ycfgFile);
        // turn off the check
        $ycfg['doorkeeper']['checkUnknownProperties'] = false;
        yaml_emit_file($ycfgFile, $ycfg);

        $cfg        = self::$config->doorkeeper->epicPid;
        $idNmsp     = self::$config->schema->namespaces->id;
        $cmdiIdNmsp = self::$config->schema->namespaces->cmdi;
        $pidProp    = self::$config->schema->cmdiPid;
        $pidProp2   = self::$config->schema->pid;
        $idProp     = self::$config->schema->id;
        $rid        = $idNmsp . rand();

        $im = self::createMetadata([
                $idProp                 => $rid,
                $cfg->clarinSetProperty => $cfg->clarinSet,
        ]);
        self::$repo->begin();

        $r      = self::$repo->createResource($im);
        $m      = $r->getGraph();
        $pids   = self::toStr($m->all($pidProp));
        $pids2  = self::toStr($m->all($pidProp2));
        $this->assertEquals(1, count($pids));
        $this->assertEquals(1, count($pids2));
        $this->assertStringStartsWith($cfg->resolver, $pids[0]);
        $this->assertStringStartsWith($cfg->resolver, $pids2[0]);
        $ids    = self::toStr($m->all($idProp));
        $this->assertEquals(4, count($ids)); // $rid, repo, cmdi, pid
        $cmdiId = null;
        foreach ($ids as $i) {
            if (strpos($i, 'http://127.0.0.1/api/') === 0) {
                $cmdiId = str_replace('http://127.0.0.1/api/', $cmdiIdNmsp, $i);
                break;
            }
        }
        $this->assertContains($cmdiId, $ids);

        self::$repo->rollback();

        // turn on the check
        $ycfg['doorkeeper']['checkUnknownProperties'] = true;
        yaml_emit_file($ycfgFile, $ycfg);
    }

    public function testBiblatex(): void {
        $idNmsp       = self::$config->schema->namespaces->id;
        $idProp       = self::$config->schema->id;
        $biblatexProp = self::$config->schema->biblatex;
        $rid          = $idNmsp . rand();

        $meta = self::createMetadata([
                $idProp       => $rid,
                $biblatexProp => " @dataset{foo,\nauthor = {Baz, Bar}\n}",
        ]);
        self::$repo->begin();

        $r = self::$repo->createResource($meta);

        $meta = $r->getMetadata();
        $meta->delete($biblatexProp);
        $meta->addLiteral($biblatexProp, "author = {Baz, Bar}");
        $r->setMetadata($meta);
        $r->updateMetadata();

        $meta->delete($biblatexProp);
        $meta->addLiteral($biblatexProp, "not a valid biblatex");
        $r->setMetadata($meta);
        try {
            $r->updateMetadata();
            $this->assertTrue(false);
        } catch (ClientException $e) {
            $resp = $e->getResponse();
            $this->assertEquals(400, $resp->getStatusCode());
            $this->assertStringStartsWith("Invalid BibLaTeX entry", (string) $resp->getBody());
        }

        self::$repo->rollback();
    }

    public function testVocabularyValues(): void {
        $class     = 'https://vocabs.acdh.oeaw.ac.at/schema#Collection';
        $meta1     = self::createMetadata([], $class);
        $classDesc = self::$ontology->getClass($class);
        foreach ($classDesc->getProperties() as $i) {
            if (!empty($i->vocabs)) {
                $propDesc = $i;
                break;
            }
        }
        $values = $propDesc->vocabularyValues;

        self::$repo->begin();

        // full URI
        $meta1->delete($propDesc->uri);
        $meta1->addResource($propDesc->uri, current($values)->concept[0]);
        $r     = self::$repo->createResource($meta1);
        $meta2 = $r->getMetadata();
        $value = (string) $meta2->getResource($propDesc->uri);

        // label
        $meta2->delete($propDesc->uri);
        $meta2->addLiteral($propDesc->uri, current($values)->getLabel('de'));
        $r->setMetadata($meta2);
        $r->updateMetadata();
        $meta3 = $r->getMetadata();
        $this->assertEquals($value, (string) $meta3->getResource($propDesc->uri));

        // wrong value
        $value = 'foo';
        $meta2->delete($propDesc->uri);
        $meta2->addLiteral($propDesc->uri, $value);
        $r->setMetadata($meta2);
        try {
            $r->updateMetadata();
            $this->assertTrue(false);
        } catch (ClientException $e) {
            $resp = $e->getResponse();
            $this->assertEquals(400, $resp->getStatusCode());
            $this->assertStringContainsString("property $propDesc->uri value $value is not in the $propDesc->vocabs vocabulary", (string) $resp->getBody());
        }

        // label as a resource
        $value = current($values)->getLabel('de');
        $meta2->delete($propDesc->uri);
        $meta2->addResource($propDesc->uri, $value);
        $r->setMetadata($meta2);
        try {
            $r->updateMetadata();
            $this->assertTrue(false);
        } catch (ClientException $e) {
            $resp = $e->getResponse();
            $this->assertEquals(400, $resp->getStatusCode());
            $this->assertStringContainsString("property $propDesc->uri value $value is not in the $propDesc->vocabs vocabulary", (string) $resp->getBody());
        }

        self::$repo->rollback();
    }

    public function testRangeUri(): void {
        $prop  = 'https://vocabs.acdh.oeaw.ac.at/schema#hasMetadataCreator';
        $class = 'https://vocabs.acdh.oeaw.ac.at/schema#Collection';
        $meta  = self::createMetadata([], $class);

        self::$repo->begin();
        $r    = self::$repo->createResource($meta);
        $meta = $r->getMetadata();

        $meta->delete($prop);
        $meta->addResource($prop, 'https://unasccepted/namespace');
        $r->setMetadata($meta);
        try {
            $r->updateMetadata();
            $this->assertTrue(false);
        } catch (ClientException $e) {
            $resp = $e->getResponse();
            $this->assertEquals(400, $resp->getStatusCode());
            $this->assertStringContainsString("https://unasccepted/namespace doesn't match any rule", (string) $resp->getBody());
        }
        self::$repo->rollback();
    }

    public function testBadIdentifier(): void {
        $idProp = self::$config->schema->id;

        $meta1 = self::createMetadata();
        $meta1->addResource($idProp, 'http://unable/to/normalize1');
        $meta2 = self::createMetadata([], 'https://vocabs.acdh.oeaw.ac.at/schema#Collection');
        $meta2->addResource($idProp, 'http://unable/to/normalize2');

        self::$repo->begin();
        self::$repo->createResource($meta1);
        try {
            self::$repo->createResource($meta2);
            $this->assertTrue(false);
        } catch (ClientException $e) {
            $resp = $e->getResponse();
            $this->assertEquals(400, $resp->getStatusCode());
            $this->assertStringContainsString("http://unable/to/normalize2 doesn't match any rule", (string) $resp->getBody());
        }
        self::$repo->rollback();
    }
//    public function testRangeUri(): void {
//        \acdhOeaw\arche\lib\ingest\MetadataCollection::$debug = true;
//        $graph = new \acdhOeaw\arche\lib\ingest\MetadataCollection(self::$repo, __DIR__ . '/kraus_processed.nt');
//        $graph->preprocess();
//        self::$repo->begin();
//        $graph->import(self::$repo->getSchema()->namespaces->id, \acdhOeaw\arche\lib\ingest\MetadataCollection::SKIP, \acdhOeaw\arche\lib\ingest\MetadataCollection::ERRMODE_PASS, 6, 7);
//        //self::$repo->commit();
//        self::$repo->rollback();
//    }
}
