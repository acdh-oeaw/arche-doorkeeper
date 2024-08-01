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

#use EasyRdf\Graph;
#use EasyRdf\Literal;

use GuzzleHttp\Client;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Exception\ClientException;
use GuzzleHttp\Exception\RequestException;
use zozlak\RdfConstants as RDF;
use rdfInterface\LiteralInterface;
use quickRdf\DataFactory as DF;
use quickRdf\DatasetNode;
use termTemplates\PredicateTemplate as PT;
use termTemplates\NamedNodeTemplate as NNT;
use acdhOeaw\arche\lib\RepoResource;
use acdhOeaw\arche\lib\BinaryPayload;
use acdhOeaw\arche\lib\exception\NotFound;

/**
 * Description of DoorkeeperTest
 *
 * @author zozlak
 */
class ResourceTest extends TestBase {

    public function testIdCount(): void {
        $im = new DatasetNode(DF::namedNode('.'));
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
        $prop  = self::$schema->id;
        $label = self::$schema->label;
        $nmsp  = self::$schema->namespaces->ontology;
        $id    = DF::namedNode('.');

        $im = new DatasetNode($id);
        $im->add(DF::quad($id, $prop, DF::namedNode($nmsp . '/foo')));
        $im->add(DF::quad($id, $label, DF::literal('bar', 'en')));
        self::$repo->begin();
        $r  = self::$repo->createResource($im);
        $this->assertIsObject($r);
        self::$repo->rollback();

        $im = new DatasetNode($id);
        $im->add(DF::quad($id, $prop, DF::namedNode($nmsp . '/foo')));
        $im->add(DF::quad($id, $prop, DF::namedNode($nmsp . '/bar')));
        $im->add(DF::quad($id, $label, DF::literal('bar', 'en')));
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

        $im = new DatasetNode($id);
        $im->add(DF::quad($id, $prop, DF::namedNode($nmsp . '/foo')));
        $im->add(DF::quad($id, $prop, DF::namedNode('https://my/id')));
        $im->add(DF::quad($id, $label, DF::literal('bar', 'en')));
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
                (string) self::$schema->id => $id1,
        ]);
        self::$repo->begin();
        $r                = self::$repo->createResource($im);
        self::$repo->commit();
        $this->toDelete[] = $r;

        self::$repo->begin();
        $im  = self::createMetadata([
                (string) self::$schema->id => $id2,
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
                (string) self::$schema->id => 'http://x.geonames.org/123/vienna.html',
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
                'https://other/property'                                    => DF::literal('test value', 'en'),
                'https://vocabs.acdh.oeaw.ac.at/schema#hasPid'              => $pid,
        ]);

        self::$repo->begin();
        $r  = self::$repo->createResource($im);
        $om = $r->getGraph();

        $date = $om->getObject(new PT(DF::namedNode('https://vocabs.acdh.oeaw.ac.at/schema#hasCreatedStartDate')));
        $this->assertInstanceOf(LiteralInterface::class, $date);
        $this->assertEquals(RDF::XSD_DATE, $date->getDatatype());
        $this->assertEquals('2017-01-01', (string) $date);

        $date = $om->getObject(new PT(DF::namedNode('https://vocabs.acdh.oeaw.ac.at/schema#hasCreatedEndDate')));
        $this->assertInstanceOf(LiteralInterface::class, $date);
        $this->assertEquals(RDF::XSD_DATE, $date->getDatatype());
        $this->assertEquals('2017-03-08', (string) $date);

        $int = $om->getObject(new PT(DF::namedNode('https://vocabs.acdh.oeaw.ac.at/schema#hasBinarySize')));
        $this->assertInstanceOf(LiteralInterface::class, $int);
        $this->assertEquals(RDF::XSD_NON_NEGATIVE_INTEGER, $int->getDatatype());
        $this->assertEquals(300, (int) $int->getValue());

        $str = $om->getObject(new PT(DF::namedNode('https://other/property')));
        $this->assertInstanceOf(LiteralInterface::class, $str);
        $this->assertEquals(RDF::RDF_LANG_STRING, $str->getDatatype());
        $this->assertEquals('en', $str->getLang());

        $uri = $om->getObject(new PT(DF::namedNode('https://vocabs.acdh.oeaw.ac.at/schema#hasPid')));
        $this->assertInstanceOf(LiteralInterface::class, $uri);
        $this->assertEquals(RDF::XSD_ANY_URI, $uri->getDatatype());
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
            $this->assertMatchesRegularExpression('/^Min property count for .* is .* but resource has 0$/m', (string) $resp->getBody());
        }

        $class = self::$ontology->getClass('https://vocabs.acdh.oeaw.ac.at/schema#Collection');
        foreach ($class->getProperties() as $i) {
            $prop = DF::namedNode($i->uri);
            if ($i->min > 0 && $im->none(new PT($prop))) {
                $x = self::createSampleProperty($i);
                $im->add(DF::quadNoSubject($prop, self::createSampleProperty($i)));
            }
        }
        $r = self::$repo->createResource($im);
        $this->assertIsObject($r);
    }

    public function testPropertyType(): void {
        $im   = self::createMetadata();
        $prop = DF::namedNode('https://vocabs.acdh.oeaw.ac.at/schema#hasUrl');
        $im->add(DF::quadNoSubject($prop, DF::namedNode('http://foo.bar/' . rand())));
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
        $idProp = self::$schema->id;
        $prop   = DF::namedNode('https://vocabs.acdh.oeaw.ac.at/schema#hasTransferDate');
        $im     = self::createMetadata([], 'https://vocabs.acdh.oeaw.ac.at/schema#Resource');
        $im->add(DF::quadNoSubject($prop, DF::literal('2020-07-01')));
        self::$repo->begin();
        $r      = self::$repo->createResource($im);
        $this->assertIsObject($r);

        $im->delete(new PT($idProp));
        $im->add(DF::quadNoSubject($idProp, DF::namedNode('https://id.acdh.oeaw.ac.at/test/' . microtime(true))));
        $im->add(DF::quadNoSubject($prop, DF::literal('2020-08-01')));
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
        $mimeProp         = self::$schema->mime;
        $accessRestProp   = self::$schema->accessRestriction;
        $creationDateProp = self::$schema->creationDate;
        $im               = self::createMetadata([
                RDF::RDF_TYPE => 'https://vocabs.acdh.oeaw.ac.at/schema#Collection',
        ]);
        $skip             = [
            self::$schema->hosting, $accessRestProp, $creationDateProp
        ];
        $class            = self::$ontology->getClass('https://vocabs.acdh.oeaw.ac.at/schema#Collection');
        foreach ($class->getProperties() as $i) {
            if ($i->min > 0 && $im->none(new PT($i->uri)) && !in_array($i->uri, $skip)) {
                $im->add(DF::quadNoSubject(DF::namedNode($i->property[0]), self::createSampleProperty($i)));
            }
        }
        self::$repo->begin();
        $r  = self::$repo->createResource($im);
        $rm = $r->getGraph();
        $this->assertEquals(date('Y-m-d'), substr((string) $rm->getObject(new PT($creationDateProp)), 0, 10));
        // accessRestriction is only on BinaryContent (Resource/Metadata) and not on RepoObject
        $this->assertNull($rm->getObject(new PT($accessRestProp)));
        $this->assertNull($rm->getObject(new PT($mimeProp)));

        $tmpl = new PT(self::$schema->hosting);
        $this->assertTrue($rm->any($tmpl));
        $rh   = new RepoResource((string) $rm->getObject($tmpl), self::$repo);
        $this->assertContains(self::getPropertyDefault((string) self::$schema->hosting), $rh->getIds());

        $im  = self::createMetadata([], 'https://vocabs.acdh.oeaw.ac.at/schema#Resource');
        $r   = self::$repo->createResource($im, new BinaryPayload('foo bar', null, 'text/plain'));
        $rm  = $r->getGraph();
        $this->assertEquals('text/plain', (string) $rm->getObject(new PT($mimeProp)));
        $rar = new RepoResource((string) $r->getGraph()->getObject(new PT($accessRestProp)), self::$repo);
        $this->assertContains(self::getPropertyDefault($accessRestProp), $rar->getIds());
    }

    public function testAccessRightsAuto(): void {
        $im   = self::createMetadata([], 'https://vocabs.acdh.oeaw.ac.at/schema#Resource');
        self::$repo->begin();
        $r    = self::$repo->createResource($im);
        $om   = $r->getGraph();
        $tmpl = new PT(self::$schema->accessRestriction);
        $this->assertTrue($om->any($tmpl));
        $ar   = new RepoResource((string) $om->getObject($tmpl), self::$repo);
        $this->assertContains(self::getPropertyDefault((string) self::$schema->accessRestriction), $ar->getIds());
        $this->assertContains(self::$config->doorkeeper->rolePublic, $om->listObjects(new PT(self::$config->accessControl->schema->read))->getValues());
        $this->assertNotContains(self::$config->doorkeeper->rolePublic, $om->listObjects(new PT(self::$config->accessControl->schema->write))->getValues());
    }

    public function testAccessRightsAcademic(): void {
        $accessRestProp = self::$schema->accessRestriction;
        $im             = self::createMetadata([
                (string) $accessRestProp => 'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/academic',
                ], 'https://vocabs.acdh.oeaw.ac.at/schema#Resource');
        $bp             = new BinaryPayload('dummy content');
        self::$repo->begin();
        $r              = self::$repo->createResource($im, $bp);
        $om             = $r->getGraph();
        $ar             = new RepoResource((string) $om->getObject(new PT($accessRestProp)), self::$repo);
        $this->assertContains('https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/academic', $ar->getIds());
        $this->assertContains(self::$config->doorkeeper->roleAcademic, $om->listObjects(new PT(self::$config->accessControl->schema->read))->getValues());
        $this->assertNotContains(self::$config->doorkeeper->roleAcademic, $om->listObjects(new PT(self::$config->accessControl->schema->write))->getValues());

        $client = new Client(['http_errors' => false, 'allow_redirects' => false]);
        $resp   = $client->send(new Request('get', (string) $r->getUri()));
        $this->assertEquals(401, $resp->getStatusCode());
        $resp   = $client->send(new Request('get', (string) $r->getUri(), ['eppn' => self::$config->doorkeeper->roleAcademic]));
        $this->assertEquals(200, $resp->getStatusCode());
    }

    public function testAccessRightsRestricted(): void {
        $accessRestProp = self::$schema->accessRestriction;
        $im             = self::createMetadata([
                (string) $accessRestProp           => 'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/restricted',
                (string) self::$schema->accessRole => 'foo',
                ], 'https://vocabs.acdh.oeaw.ac.at/schema#Resource');
        $bp             = new BinaryPayload('dummy content');
        self::$repo->begin();
        $r              = self::$repo->createResource($im, $bp);
        $om             = $r->getGraph();
        $ar             = new RepoResource((string) $om->getObject(new PT($accessRestProp)), self::$repo);
        $this->assertContains('https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/restricted', $ar->getIds());
        $this->assertNotContains(self::$config->doorkeeper->rolePublic, $om->listObjects(new PT(self::$config->accessControl->schema->read))->getValues());
        $this->assertNotContains(self::$config->doorkeeper->roleAcademic, $om->listObjects(new PT(self::$config->accessControl->schema->write))->getValues());
        $this->assertContains('foo', $om->listObjects(new PT(self::$schema->accessRole))->getValues());

        $client = new Client(['http_errors' => false, 'allow_redirects' => false]);
        $resp   = $client->send(new Request('get', (string) $r->getUri()));
        $this->assertEquals(401, $resp->getStatusCode());
        $resp   = $client->send(new Request('get', (string) $r->getUri(), ['eppn' => self::$config->doorkeeper->roleAcademic]));
        $this->assertEquals(403, $resp->getStatusCode());
        $resp   = $client->send(new Request('get', (string) $r->getUri(), ['eppn' => 'foo']));
        $this->assertEquals(200, $resp->getStatusCode());
    }

    /**
     * 
     * @depends_ testAccessRightsAuto
     * @depends testAccessRightsAcademic
     * @depends testAccessRightsRestricted
     */
    public function testAccessRightsRise(): void {
        $accessRestProp = self::$schema->accessRestriction;
        $client         = new Client(['http_errors' => false, 'allow_redirects' => false]);

        $meta = [(string) $accessRestProp => 'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/restricted'];
        $im   = self::createMetadata($meta, 'https://vocabs.acdh.oeaw.ac.at/schema#Resource');
        $bp   = new BinaryPayload('dummy content');
        self::$repo->begin();
        $r    = self::$repo->createResource($im, $bp);

        $meta = new DatasetNode(DF::namedNode('.'));
        $meta->add(DF::quadNoSubject($accessRestProp, DF::namedNode('https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/academic')));
        $r->setMetadata($meta);
        $r->updateMetadata(RepoResource::UPDATE_MERGE);
        $resp = $client->send(new Request('get', (string) $r->getUri()));
        $this->assertEquals(401, $resp->getStatusCode());
        $resp = $client->send(new Request('get', (string) $r->getUri(), ['eppn' => self::$config->doorkeeper->roleAcademic]));
        $this->assertEquals(200, $resp->getStatusCode());

        $id   = DF::namedNode('.');
        $meta = new DatasetNode($id);
        $meta->add(DF::quad($id, $accessRestProp, DF::namedNode('https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/public')));
        $r->setMetadata($meta);
        $r->updateMetadata(RepoResource::UPDATE_MERGE);
        $resp = $client->send(new Request('get', (string) $r->getUri()));
        $this->assertEquals(200, $resp->getStatusCode());
    }

    /**
     * 
     * @depends_ testAccessRightsAuto
     * @depends testAccessRightsAcademic
     * @depends testAccessRightsRestricted
     */
    public function testAccessRightsLower(): void {
        $accessRestrProp = self::$schema->accessRestriction;
        $client          = new Client(['http_errors' => false, 'allow_redirects' => false]);
        $id              = DF::namedNode('.');

        $meta = [(string) $accessRestrProp => 'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/public'];
        $im   = self::createMetadata($meta, 'https://vocabs.acdh.oeaw.ac.at/schema#Resource');
        $bp   = new BinaryPayload('dummy content');
        self::$repo->begin();
        $r    = self::$repo->createResource($im, $bp);

        $meta = new DatasetNode($id);
        $meta->add(DF::quad($id, $accessRestrProp, DF::namedNode('https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/academic')));
        $r->setMetadata($meta);
        $r->updateMetadata(RepoResource::UPDATE_MERGE);
        $resp = $client->send(new Request('get', (string) $r->getUri()));
        $this->assertEquals(401, $resp->getStatusCode());
        $resp = $client->send(new Request('get', (string) $r->getUri(), ['eppn' => self::$config->doorkeeper->roleAcademic]));
        $this->assertEquals(200, $resp->getStatusCode());

        $meta = new DatasetNode($id);
        $meta->add(DF::quad($id, $accessRestrProp, DF::namedNode('https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/restricted')));
        $meta->add(DF::quad($id, self::$schema->accessRole, DF::literal('bar')));
        $r->setMetadata($meta);
        $r->updateMetadata(RepoResource::UPDATE_MERGE);
        $resp = $client->send(new Request('get', (string) $r->getUri()));
        $this->assertEquals(401, $resp->getStatusCode());
        $resp = $client->send(new Request('get', (string) $r->getUri(), ['eppn' => self::$config->doorkeeper->roleAcademic]));
        $this->assertEquals(403, $resp->getStatusCode());
        $resp = $client->send(new Request('get', (string) $r->getUri(), ['eppn' => 'bar']));
        $this->assertEquals(200, $resp->getStatusCode());
    }

    public function testTitleAuto(): void {
        $titleProp = self::$schema->label;
        $titleTmpl = new PT($titleProp);
        self::$repo->begin();

        // copied from other title-like property
        $im = self::createMetadata(['http://purl.org/dc/elements/1.1/title' => DF::literal('foo', 'en')]);
        $im->delete($titleTmpl);
        $r  = self::$repo->createResource($im);
        $this->assertEquals('foo', (string) $r->getGraph()->getObject($titleTmpl));

        // combined from acdh:hasFirstName and acdh:hasLastName
        $im = self::createMetadata([
                'https://vocabs.acdh.oeaw.ac.at/schema#hasFirstName' => 'foo',
                'https://vocabs.acdh.oeaw.ac.at/schema#hasLastName'  => 'bar',
        ]);
        $im->delete($titleTmpl);
        $r  = self::$repo->createResource($im);
        $this->assertEquals('foo bar', (string) $r->getGraph()->getObject($titleTmpl));

        // combined from acdh:hasFirstName
        $im = self::createMetadata([
                'https://vocabs.acdh.oeaw.ac.at/schema#hasFirstName' => 'foo'
        ]);
        $im->delete($titleTmpl);
        $r  = self::$repo->createResource($im);
        $this->assertEquals('foo', (string) $r->getGraph()->getObject($titleTmpl));

        // combined from foaf:givenName and foaf:familyName
        $im = self::createMetadata([
                'http://xmlns.com/foaf/0.1/givenName'  => 'foo',
                'http://xmlns.com/foaf/0.1/familyName' => 'bar',
        ]);
        $im->delete($titleTmpl);
        $r  = self::$repo->createResource($im);
        $this->assertEquals('foo bar', (string) $r->getGraph()->getObject($titleTmpl));

        // many titles
        $id     = DF::namedNode('.');
        $im     = new DatasetNode($id);
        $im->add(DF::quad($id, self::$schema->id, DF::namedNode('https://id/prop' . time() . rand())));
        $im->add(DF::quad($id, $titleProp, DF::literal('foo', 'en')));
        $im->add(DF::quad($id, $titleProp, DF::literal('bar', 'de')));
        $r      = self::$repo->createResource($im);
        $titles = $r->getGraph()->listObjects($titleTmpl)->getValues();
        $this->assertEquals(2, count($titles));
        $this->assertContains('foo', $titles);
        $this->assertContains('bar', $titles);
    }

    public function testTitlePreserveOtherLang(): void {
        $titleProp = self::$schema->label;
        $titleTmpl = new PT($titleProp);
        self::$repo->begin();

        $meta = self::createMetadata([(string) $titleProp => DF::literal('foo', 'en')]);
        $res  = self::$repo->createResource($meta);
        $meta->delete($titleTmpl);
        $meta->add(DF::quadNoSubject($titleProp, DF::literal('bar', 'de')));

        $res->setMetadata($meta);
        $res->updateMetadata(RepoResource::UPDATE_MERGE);
        $tmp    = $res->getGraph()->listObjects($titleTmpl);
        $titles = [];
        foreach ($tmp as $i) {
            $this->assertInstanceOf(LiteralInterface::class, $i);
            $titles[$i->getLang()] = (string) $i;
        }
        $this->assertEquals(2, count($titles));
        $this->assertArrayHasKey('en', $titles);
        $this->assertArrayHasKey('de', $titles);
        $this->assertEquals('foo', $titles['en']);
        $this->assertEquals('bar', $titles['de']);

        $res->setMetadata($meta);
        $res->updateMetadata(RepoResource::UPDATE_OVERWRITE);
        $titles = iterator_to_array($res->getGraph()->listObjects($titleTmpl));
        $this->assertEquals(1, count($titles));
        $this->assertEquals('bar', (string) $titles[0]);
        $this->assertInstanceOf(LiteralInterface::class, $titles[0]);
        $this->assertEquals('de', $titles[0]->getLang());

        self::$repo->rollback();
    }

    /**
     * 
     * @depends testTitleAuto
     */
    public function testTitleErrors(): void {
        $id        = DF::namedNode('.');
        $titleProp = self::$schema->label;
        self::$repo->begin();

        // empty title
        $im = new DatasetNode($id);
        $im->add(DF::quad($id, self::$schema->id, DF::namedNode('https://id/prop' . time() . rand())));
        $im->add(DF::quad($id, $titleProp, DF::literal('', 'en')));
        try {
            self::$repo->createResource($im);
            $this->assertTrue(false);
        } catch (ClientException $e) {
            $resp = $e->getResponse();
            $this->assertEquals(400, $resp->getStatusCode());
            $this->assertEquals("$titleProp value is empty", (string) $resp->getBody());
        }

        // no title
        $im = new DatasetNode($id);
        $im->add(DF::quad($id, self::$schema->id, DF::namedNode('https://id/prop' . time() . rand())));
        try {
            self::$repo->createResource($im);
            $this->assertTrue(false);
        } catch (ClientException $e) {
            $resp = $e->getResponse();
            $this->assertEquals(400, $resp->getStatusCode());
            $this->assertEquals("$titleProp is missing", (string) $resp->getBody());
        }

        // no language
        $im = new DatasetNode($id);
        $im->add(DF::quad($id, self::$schema->id, DF::namedNode('https://id/prop' . time() . rand())));
        $im->add(DF::quad($id, $titleProp, DF::literal('foo')));
        $im->add(DF::quad($id, $titleProp, DF::literal('bar')));
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
        $im = new DatasetNode($id);
        $im->add(DF::quad($id, self::$schema->id, DF::namedNode('https://id/prop' . time() . rand())));
        $im->add(DF::quad($id, $titleProp, DF::literal('foo', 'en')));
        $im->add(DF::quad($id, $titleProp, DF::literal('bar', 'en')));
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
        $idProp     = self::$schema->id;
        $accessProp = self::$schema->accessRestriction;
        $pidProp    = self::$schema->pid;
        $pidNmsp    = self::$config->doorkeeper->epicPid->resolver;
        $idNmsp     = self::$schema->namespaces->id;
        $pidTmpl    = new PT($pidProp);
        $restricted = DF::namedNode('https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/restricted');
        $academic   = DF::namedNode('https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/academic');
        $public     = DF::namedNode('https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/public');
        $im         = self::createMetadata([(string) $idProp => $idNmsp . rand()]);
        self::$repo->begin();

        // no pid generated automatically
        $r  = self::$repo->createResource($im);
        $m1 = $r->getGraph();
        $this->assertTrue($m1->none(new PT($pidProp)));

        // pid generated automatically and promoted to an id
        $m1->add(DF::quadNoSubject($pidProp, DF::literal(self::$config->doorkeeper->epicPid->createValue)));
        $r->setGraph($m1);
        $r->updateMetadata();
        $m2   = $r->getGraph();
        $this->assertTrue($m2->none($pidTmpl->withObject(new NNT(null, NNT::ANY))));
        $pids = $m2->listObjects($pidTmpl)->getValues();
        $this->assertEquals(1, count($pids));
        $this->assertStringStartsWith($pidNmsp, (string) $pids[0]);
        $this->assertContains((string) $pids[0], $m2->listObjects(new PT($idProp))->getValues());

        // pid generated automatically and promoted to an id
        // for all resources of class TopCollection/Collection/Resource/Metadata with non-restricted access

        $classes = [
            self::$schema->classes->resource,
            self::$schema->classes->metadata,
        ];
        foreach ($classes as $class) {
            $r = self::$repo->createResource(self::createMetadata([(string) $accessProp => $restricted], $class));
            $m = $r->getGraph();
            $this->assertTrue($m->none($pidTmpl), $class);

            $r = self::$repo->createResource(self::createMetadata([(string) $accessProp => $academic], $class));
            $m = $r->getGraph();
            $this->assertEquals(1, count($m->copy($pidTmpl)), $class);

            $r = self::$repo->createResource(self::createMetadata([(string) $accessProp => $public], $class));
            $m = $r->getGraph();
            $this->assertEquals(1, count($m->copy($pidTmpl)), $class);
        }
        $classes = [
            self::$schema->classes->resource,
            self::$schema->classes->topCollection,
            self::$schema->classes->collection,
            self::$schema->classes->resource,
            self::$schema->classes->metadata,
        ];
        foreach ($classes as $class) {
            $r = self::$repo->createResource(self::createMetadata([], $class));
            $m = $r->getGraph();
            $this->assertEquals(1, count($m->copy($pidTmpl)), $class);
        }
        self::$repo->rollback();
    }

    public function testPidPreserving(): void {
        $idProp      = self::$schema->id;
        $idTmpl      = new PT($idProp);
        $pidProp     = self::$schema->pid;
        $pidTmpl     = new PT($pidProp);
        $cmdiPidProp = self::$schema->cmdiPid;
        $pidNmsp     = self::$config->doorkeeper->epicPid->resolver;
        $idNmsp      = self::$schema->namespaces->id;
        $httpsPid    = $pidNmsp . self::$config->doorkeeper->epicPid->prefix . '/123';
        $httpPid     = str_replace('https://', 'http://', $httpsPid);
        self::$repo->begin();

        // existing pid not overwritten but promoted to an id
        $idn  = rand();
        $im   = self::createMetadata([
                (string) $idProp  => $idNmsp . $idn,
                (string) $pidProp => $httpPid,
        ]);
        $r    = self::$repo->createResource($im);
        $m1   = $r->getGraph();
        $pids = $m1->listObjects($pidTmpl)->getValues();
        $this->assertEquals(1, count($pids));
        $this->assertEquals($httpsPid, $pids[0]);
        $this->assertContains($httpsPid, $m1->listObjects($idTmpl)->getValues());

        // pid refreshed from one stored as an id
        $m2   = $r->getGraph();
        $m2->delete($pidTmpl);
        $m2->add(DF::quadNoSubject($pidProp, DF::literal(self::$config->doorkeeper->epicPid->createValue)));
        $r->setGraph($m2);
        $r->updateMetadata();
        $m3   = $r->getGraph();
        $pids = $m3->listObjects($pidTmpl)->getValues();
        $this->assertEquals(1, count($pids));
        $this->assertEquals($httpsPid, $pids[0]);
        $this->assertContains($httpsPid, $m3->listObjects($idTmpl)->getValues());

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

        $idProp = self::$schema->id;
        $im->delete(new PT($idProp));
        $im->add(DF::quadNoSubject($idProp, DF::namedNode(self::$schema->namespaces->ontology . 'test')));
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

        $cfg         = self::$config->doorkeeper->epicPid;
        $idNmsp      = self::$schema->namespaces->id;
        $cmdiIdNmsp  = self::$schema->namespaces->cmdi;
        $cmdiPidProp = self::$schema->cmdiPid;
        $pidProp     = self::$schema->pid;
        $idProp      = self::$schema->id;
        $rid         = $idNmsp . rand();

        $im = self::createMetadata([
                (string) $idProp                 => $rid,
                (string) $cfg->clarinSetProperty => $cfg->clarinSet,
        ]);
        self::$repo->begin();

        $r        = self::$repo->createResource($im);
        $m        = $r->getGraph();
        $cmdiPids = $m->listObjects(new PT($cmdiPidProp))->getValues();
        $pids     = $m->listObjects(new PT($pidProp))->getValues();
        $this->assertEquals(1, count($cmdiPids));
        $this->assertEquals(1, count($pids));
        $this->assertStringStartsWith($cfg->resolver, $cmdiPids[0]);
        $this->assertStringStartsWith($cfg->resolver, $pids[0]);
        $ids      = $m->listObjects(new PT($idProp))->getValues();
        $this->assertEquals(4, count($ids)); // $rid, repo, cmdi, pid
        $cmdiId   = null;
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
        $idNmsp       = self::$schema->namespaces->id;
        $idProp       = self::$schema->id;
        $biblatexProp = self::$schema->biblatex;
        $rid          = $idNmsp . rand();

        $meta = self::createMetadata([
                (string) $idProp       => $rid,
                (string) $biblatexProp => " @dataset{foo,\nauthor = {Baz, Bar}\n}",
        ]);
        self::$repo->begin();

        $r = self::$repo->createResource($meta);

        $meta = $r->getMetadata();
        $meta->delete(new PT($biblatexProp));
        $meta->add(DF::quadNoSubject($biblatexProp, DF::literal("author = {Baz, Bar}")));
        $r->setMetadata($meta);
        $r->updateMetadata();

        $meta->delete(new PT($biblatexProp));
        $meta->add(DF::quadNoSubject($biblatexProp, DF::literal("not a valid biblatex")));
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
        $propDesc  = null;
        foreach ($classDesc->getProperties() as $i) {
            if (!empty($i->vocabs)) {
                $propDesc = $i;
                break;
            }
        }
        $this->assertIsObject($propDesc);
        $values   = $propDesc->vocabularyValues;
        $propUri  = DF::namedNode($propDesc->uri);
        $propTmpl = new PT($propUri);

        self::$repo->begin();

        // full URI
        $meta1->delete($propTmpl);
        $meta1->add(DF::quadNoSubject($propUri, DF::namedNode(current($values)->concept[0])));
        $r     = self::$repo->createResource($meta1);
        $meta2 = $r->getMetadata();
        $value = (string) $meta2->getObject($propTmpl);

        // label
        $meta2->delete($propTmpl);
        $meta2->add(DF::quadNoSubject($propUri, DF::literal(current($values)->getLabel('de'))));
        $r->setMetadata($meta2);
        $r->updateMetadata();
        $meta3 = $r->getMetadata();
        $this->assertEquals($value, (string) $meta3->getObject($propTmpl));

        // wrong value
        $value = DF::literal('foo');
        $meta2->delete($propTmpl);
        $meta2->add(DF::quadNoSubject($propUri, $value));
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
        $value = DF::namedNode(current($values)->getLabel('de'));
        $meta2->delete($propTmpl);
        $meta2->add(DF::quadNoSubject($propUri, $value));
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
        $prop  = DF::namedNode('https://vocabs.acdh.oeaw.ac.at/schema#hasMetadataCreator');
        $class = DF::namedNode('https://vocabs.acdh.oeaw.ac.at/schema#Collection');
        $meta  = self::createMetadata([], $class);

        self::$repo->begin();
        $r    = self::$repo->createResource($meta);
        $meta = $r->getMetadata();

        $meta->delete(new PT($prop));
        $meta->add(DF::quadNoSubject($prop, DF::namedNode('https://unasccepted/namespace')));
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
        $idProp = self::$schema->id;

        $meta1 = self::createMetadata();
        $meta1->add(DF::quadNoSubject($idProp, DF::namedNode('http://unable/to/normalize1')));
        $meta2 = self::createMetadata([], 'https://vocabs.acdh.oeaw.ac.at/schema#Collection');
        $meta2->add(DF::quadNoSubject($idProp, DF::namedNode('http://unable/to/normalize2')));

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

    public function testWkt(): void {
        $latProp = self::$schema->latitude;
        $lonProp = self::$schema->longitude;
        $wktProp = self::$schema->wkt;
        self::$repo->begin();

        // no pid generated automatically
        $r = self::$repo->createResource(self::createMetadata());
        $m = $r->getGraph();
        $this->assertNull($m->getObject(new PT($latProp)));
        $this->assertNull($m->getObject(new PT($lonProp)));
        $this->assertNull($m->getObject(new PT($wktProp)));

        $m->add(DF::quadNoSubject($lonProp, DF::literal(16.5)));
        $m->add(DF::quadNoSubject($latProp, DF::literal(48.1)));
        $r->setGraph($m);
        $r->updateMetadata();
        $m = $r->getGraph();
        $this->assertEquals('48.1', (string) $m->getObject(new PT($latProp)));
        $this->assertEquals('16.5', (string) $m->getObject(new PT($lonProp)));
        $this->assertEquals('POINT(16.5 48.1)', (string) $m->getObject(new PT($wktProp)));

        self::$repo->rollback();
    }

    public function testTechnicalProperty(): void {
        $prop  = 'https://vocabs.acdh.oeaw.ac.at/schema#hasLiteralIdentifier';
        $class = DF::namedNode('https://vocabs.acdh.oeaw.ac.at/schema#Collection');
        $meta  = self::createMetadata([$prop => 'some id'], $class);
        self::$repo->begin();
        try {
            self::$repo->createResource($meta);
            $this->assertTrue(false);
        } catch (ClientException $e) {
            $resp = $e->getResponse();
            $this->assertEquals(400, $resp->getStatusCode());
            $this->assertStringContainsString("Properties with a wrong domain: $prop", (string) $resp->getBody());
        }
        self::$repo->rollback();
    }

    public function testNormalizeObjectValue(): void {
        $prop  = 'https://vocabs.acdh.oeaw.ac.at/schema#hasActor';
        $value = 'https://orcid.org/0000-0001-5853-2534/';
        $class = DF::namedNode('https://vocabs.acdh.oeaw.ac.at/schema#Collection');
        $meta  = self::createMetadata([$prop => $value], $class);
        self::$repo->begin();
        $res   = self::$repo->createResource($meta);
        $ref   = self::$repo->getResourceById(substr($value, 0, -1)); // normalized form
        try {
            self::$repo->getResourceById($value);
            $this->assertTrue(false);
        } catch (NotFound $ex) {
            $this->assertTrue(true);
        }
        self::$repo->rollback();
    }

    public function testOpenAireOaipmhSet(): void {
        $class = DF::namedNode('https://vocabs.acdh.oeaw.ac.at/schema#TopCollection');
        $meta  = self::createMetadata([], $class);
        self::$repo->begin();
        $res   = self::$repo->createResource($meta);
        $set   = $res->getMetadata()->getObject(new PT(self::$schema->oaipmhSet));
        $this->assertEquals(\acdhOeaw\arche\doorkeeper\Resource::OPENAIRE_OAIPMH_SET, (string) $set);
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
