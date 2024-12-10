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

use GuzzleHttp\Exception\ClientException;
use rdfInterface\LiteralInterface;
use quickRdf\DataFactory as DF;
use termTemplates\PredicateTemplate as PT;
use acdhOeaw\arche\lib\BinaryPayload;
use zozlak\RdfConstants as RDF;

/**
 * Description of TransactionTest
 *
 * @author zozlak
 */
class TransactionTest extends TestBase {

    public function testCollectionExtent(): void {
        $schema    = self::$schema;
        $sizeProp  = $schema->binarySizeCumulative;
        $countProp = $schema->countCumulative;
        $rdfType   = DF::namedNode(RDF::RDF_TYPE);

        self::$repo->begin();
        $rCol1     = self::$repo->createResource(self::createMetadata([], $schema->classes->collection));
        $rCol2Meta = self::createMetadata([(string) $schema->parent => $rCol1->getUri()], $schema->classes->collection);
        $rCol2     = self::$repo->createResource($rCol2Meta);

        // add resources
        $bin1Size       = filesize(__FILE__);
        $bin2Size       = filesize(__DIR__ . '/../config-sample.yaml');
        $meta1          = self::createMetadata([(string) $schema->parent => $rCol1->getUri()]);
        $binary1        = new BinaryPayload(null, __FILE__);
        $rBin1          = self::$repo->createResource($meta1, $binary1);
        $meta2          = self::createMetadata([(string) $schema->parent => $rCol2->getUri()]);
        $binary2        = new BinaryPayload(null, __DIR__ . '/../config-sample.yaml');
        $rBin2          = self::$repo->createResource($meta2, $binary2);
        self::$repo->commit();
        $this->toDelete = array_merge($this->toDelete, [$rCol1, $rCol2, $rBin1, $rBin2]);

        $rBin1->loadMetadata(true);
        $rCol1->loadMetadata(true);
        $rCol2->loadMetadata(true);
        $rBin1Meta = $rBin1->getGraph();
        $rCol1Meta = $rCol1->getGraph();
        $rCol2Meta = $rCol2->getGraph();
        $this->assertNull($rBin1Meta->getObject(new PT($countProp)));
        $this->assertEquals($bin1Size + $bin2Size, $rCol1Meta->getObject(new PT($sizeProp))?->getValue());
        $this->assertEquals(3, $rCol1Meta->getObject(new PT($countProp))?->getValue());
        $this->assertEquals($bin2Size, $rCol2Meta->getObject(new PT($sizeProp))?->getValue());
        $this->assertEquals(1, $rCol2Meta->getObject(new PT($countProp))?->getValue());

        // update resources
        $bin1Size = filesize(__DIR__ . '/../config-sample.yaml');
        $bin2Size = filesize(__DIR__ . '/Bootstrap.php');
        self::$repo->begin();
        $rBin1->updateContent(new BinaryPayload(null, __DIR__ . '/../config-sample.yaml'));
        $rBin2->updateContent(new BinaryPayload(null, __DIR__ . '/Bootstrap.php'));
        self::$repo->commit();

        $rCol1->loadMetadata(true);
        $rCol2->loadMetadata(true);
        $rCol1Meta = $rCol1->getGraph();
        $rCol2Meta = $rCol2->getGraph();
        $this->assertEquals($bin1Size + $bin2Size, $rCol1Meta->getObject(new PT($sizeProp))?->getValue());
        $this->assertEquals(3, (int) $rCol1Meta->getObject(new PT($countProp))?->getValue());
        $this->assertEquals($bin2Size, (int) $rCol2Meta->getObject(new PT($sizeProp))?->getValue());
        $this->assertEquals(1, (int) $rCol2Meta->getObject(new PT($countProp))?->getValue());

        // delete resources
        self::$repo->begin();
        $rBin1->delete(true);
        self::$repo->commit();

        $rCol1->loadMetadata(true);
        $rCol2->loadMetadata(true);
        $rCol1Meta = $rCol1->getGraph();
        $rCol2Meta = $rCol2->getGraph();
        $this->assertEquals($bin2Size, $rCol1Meta->getObject(new PT($sizeProp))?->getValue());
        $this->assertEquals(2, (int) $rCol1Meta->getObject(new PT($countProp))?->getValue());
        $this->assertEquals($bin2Size, (int) $rCol2Meta->getObject(new PT($sizeProp))?->getValue());
        $this->assertEquals(1, (int) $rCol2Meta->getObject(new PT($countProp))?->getValue());

        // to remove the $rBin2 we need to change col2 type to a non-collection one
        self::$repo->begin();
        $rCol2Meta->delete(new PT($rdfType));
        $rCol2Meta->add(DF::quadNoSubject($rdfType, DF::namedNode('https://foo')));
        $rCol2->setMetadata($rCol2Meta);
        $rCol2->updateMetadata();
        $rBin2->delete(true);
        self::$repo->commit();

        // col2 is not a collection now so it shouldn't have collection-specific properties
        $rCol1->loadMetadata(true);
        $rCol2->loadMetadata(true);
        $rCol1Meta = $rCol1->getGraph();
        $rCol2Meta = $rCol2->getGraph();
        $this->assertEquals(0, $rCol1Meta->getObject(new PT($sizeProp))?->getValue());
        $this->assertEquals(1, (int) $rCol1Meta->getObject(new PT($countProp))?->getValue());
        $this->assertNull($rCol2Meta->getObject(new PT($sizeProp)));
        $this->assertNull($rCol2Meta->getObject(new PT($countProp)));
    }

    public function testCollectionExtent2(): void {
        // move between two independent collections
        $schema     = self::$schema;
        $sizeProp   = $schema->binarySizeCumulative;
        $countProp  = $schema->countCumulative;
        $parentProp = $schema->parent;
        $collClass  = $schema->classes->collection;
        $binPath1   = __FILE__;
        $binSize1   = filesize($binPath1);
        $binary1    = new BinaryPayload(null, $binPath1);
        $binPath2   = dirname(__FILE__) . '/ResourceTest.php';
        $binSize2   = filesize($binPath2);
        $binary2    = new BinaryPayload(null, $binPath2);

        self::$repo->begin();
        $rCol1          = self::$repo->createResource(self::createMetadata([], $collClass));
        $rCol2          = self::$repo->createResource(self::createMetadata([], $collClass));
        $meta1          = self::createMetadata([(string) $parentProp => $rCol1->getUri()]);
        $rBin1          = self::$repo->createResource($meta1, $binary1);
        $meta2          = self::createMetadata([(string) $parentProp => $rCol2->getUri()]);
        $rBin2          = self::$repo->createResource($meta2, $binary2);
        $meta3          = self::createMetadata([(string) $parentProp => $rCol2->getUri()]);
        $rBin3          = self::$repo->createResource($meta3, $binary1);
        self::$repo->commit();
        $this->toDelete = array_merge(
            $this->toDelete,
            [$rCol1, $rCol2, $rBin1, $rBin2, $rBin3]
        );

        $rCol1->loadMetadata(true);
        $rCol2->loadMetadata(true);
        $rCol1Meta = $rCol1->getGraph();
        $rCol2Meta = $rCol2->getGraph();
        $this->assertEquals($binSize1, (int) $rCol1Meta->getObject(new PT($sizeProp))?->getValue());
        $this->assertEquals($binSize1 + $binSize2, (int) $rCol2Meta->getObject(new PT($sizeProp))?->getValue());
        $this->assertEquals(1, (int) $rCol1Meta->getObject(new PT($countProp))?->getValue());
        $this->assertEquals(2, (int) $rCol2Meta->getObject(new PT($countProp))?->getValue());

        self::$repo->begin();
        $meta2->delete(new PT($parentProp));
        $meta2->add(DF::quadNoSubject($parentProp, DF::namedNode($rCol1->getUri())));
        $rBin2->setMetadata($meta2);
        $rBin2->updateMetadata();
        self::$repo->commit();

        $rCol1->loadMetadata(true);
        $rCol2->loadMetadata(true);
        $rCol1Meta = $rCol1->getGraph();
        $rCol2Meta = $rCol2->getGraph();
        $this->assertEquals($binSize1 + $binSize2, (int) $rCol1Meta->getObject(new PT($sizeProp))?->getValue());
        $this->assertEquals(2, (int) $rCol1Meta->getObject(new PT($countProp))?->getValue());
        $this->assertEquals($binSize1, (int) $rCol2Meta->getObject(new PT($sizeProp))?->getValue());
        $this->assertEquals(1, (int) $rCol2Meta->getObject(new PT($countProp))?->getValue());
    }

    public function testCollectionAggregates(): void {
        $schema         = self::$schema;
        $accessProp     = $schema->accessRestriction;
        $accessAggProp  = $schema->accessRestrictionAgg;
        $licenseProp    = $schema->license;
        $licenseAggProp = $schema->licenseAgg;
        $parentProp     = $schema->parent;
        $collClass      = $schema->classes->collection;

        self::$repo->begin();
        $rCol1 = self::$repo->createResource(self::createMetadata([], $collClass));

        $rCol1->loadMetadata(true);
        $rCol1Meta = $rCol1->getGraph();
        $this->assertEquals('', $rCol1Meta->getObject(new PT($licenseAggProp))?->getValue());
        $this->assertEquals('', $rCol1Meta->getObject(new PT($accessAggProp))?->getValue());

        // add resources
        $meta           = self::createMetadata([
            (string) $parentProp  => $rCol1->getUri(),
            (string) $accessProp  => 'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/academic',
            (string) $licenseProp => 'https://vocabs.acdh.oeaw.ac.at/archelicenses/mit',
        ]);
        $rBin1          = self::$repo->createResource($meta, new BinaryPayload(null, __FILE__));
        $meta           = self::createMetadata([
            (string) $parentProp  => $rCol1->getUri(),
            (string) $accessProp  => 'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/academic',
            (string) $licenseProp => 'https://vocabs.acdh.oeaw.ac.at/archelicenses/cc-by-4-0',
        ]);
        $rBin2          = self::$repo->createResource($meta, new BinaryPayload(null, __FILE__));
        $meta           = self::createMetadata([
            (string) $parentProp  => $rCol1->getUri(),
            (string) $accessProp  => 'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/restricted',
            (string) $licenseProp => 'https://vocabs.acdh.oeaw.ac.at/archelicenses/noc-nc',
        ]);
        $rBin3          = self::$repo->createResource($meta, new BinaryPayload(null, __FILE__));
        self::$repo->commit();
        $this->toDelete = array_merge($this->toDelete, [$rCol1, $rBin1, $rBin2, $rBin3]);

        $rCol1->loadMetadata(true);
        $rCol1Meta   = $rCol1->getGraph();
        $licenseVals = iterator_to_array($rCol1Meta->listObjects(new PT($licenseAggProp)));
        $ref         = ["CC BY 4.0: 1 / MIT: 1 / NoC-NC: 1", "CC BY 4.0: 1 / MIT: 1 / NoC-NC: 1"];
        $this->assertEquals($ref, array_map(fn($x) => (string) $x, $licenseVals));
        $ref         = [RDF::RDF_LANG_STRING, RDF::RDF_LANG_STRING];
        $this->assertEquals($ref, array_map(fn(LiteralInterface $x) => $x->getDatatype(), $licenseVals));
        $this->assertContains('en', array_map(fn(LiteralInterface $x) => $x->getLang(), $licenseVals));
        $this->assertContains('de', array_map(fn(LiteralInterface $x) => $x->getLang(), $licenseVals));
        $accessVals  = iterator_to_array($rCol1Meta->listObjects(new PT($accessAggProp)));
        $ref         = [RDF::RDF_LANG_STRING, RDF::RDF_LANG_STRING];
        $this->assertEquals($ref, array_map(fn(LiteralInterface $x) => $x->getDatatype(), $accessVals));
        $accessVals  = array_combine(array_map(fn(LiteralInterface $x) => $x->getLang(), $accessVals), array_map(fn($x) => (string) $x, $accessVals));
        $ref         = [
            'en' => 'academic: 2 / restricted: 1',
            'de' => 'akademisch: 2 / eingeschränkt: 1',
        ];
    }

    public function testTopCollectionAggregates(): void {
        self::$config->schema->classes->collection = 'https://vocabs.acdh.oeaw.ac.at/schema#TopCollection';
        self::$schema                              = new \acdhOeaw\arche\lib\Schema(self::$config->schema);
        $this->testCollectionAggregates();
    }

    /**
     * Resources created from object values which passed the namespace check
     * should be allowed and others should be denied.
     * 
     * @return void
     */
    public function testAutoGenResource(): void {
        $schema         = self::$schema;
        $class          = $schema->classes->resource;
        $notCheckedProp = $schema->parent;
        $checkedProp    = DF::namedNode('https://vocabs.acdh.oeaw.ac.at/schema#hasDepositor');
        $invalidRes     = DF::namedNode('https://bar/' . rand());
        $validRes       = DF::namedNode('https://orcid.org/0000-0003-0065-8112'); //  random but existing ORCID

        self::$repo->begin();
        $r                = self::$repo->createResource(self::createMetadata([(string) $checkedProp => $validRes], $class));
        $this->toDelete[] = $r;
        self::$repo->commit();

        $m = $r->getMetadata();
        $m->add(DF::quadNoSubject($notCheckedProp, $invalidRes));
        $r->setMetadata($m);
        self::$repo->begin();
        $r->updateMetadata();
        try {
            self::$repo->commit();
            $this->assertTrue(false);
        } catch (ClientException $e) {
            $resp   = $e->getResponse();
            $this->assertEquals(400, $resp->getStatusCode());
            $errors = explode("\n", (string) $resp->getBody());
            $this->assertMatchesRegularExpression("|Transaction created resources without any metadata:.*$invalidRes|", $errors[0]);
            sleep(1); // to avoid removing resources between the transaction is fully rolled back
        }
    }

    public function testIsNewVersionOf(): void {
        $schema  = self::$schema;
        $verProp = $schema->isNewVersionOf;

        $new1m = self::createMetadata();
        $new2m = self::createMetadata();
        $old1m = self::createMetadata();
        $old2m = self::createMetadata();

        self::$repo->begin();
        $new1r            = self::$repo->createResource($new1m);
        $this->toDelete[] = $new1r;
        $new2r            = self::$repo->createResource($new2m);
        $this->toDelete[] = $new2r;

        $old1m->add(DF::quadNoSubject($verProp, DF::namedNode($new1r->getUri())));
        $old1m->add(DF::quadNoSubject($verProp, DF::namedNode($new2r->getUri())));

        $old1r            = self::$repo->createResource($old1m);
        $this->toDelete[] = $old1r;

        // this should succeed
        self::$repo->commit();

        $old2m->add(DF::quadNoSubject($verProp, DF::namedNode($new1r->getUri())));
        self::$repo->begin();
        $old2r            = self::$repo->createResource($old2m);
        $this->toDelete[] = $old2r;
        try {
            self::$repo->commit();
            $this->assertTrue(false);
        } catch (ClientException $e) {
            $resp = $e->getResponse();
            $this->assertEquals(400, $resp->getStatusCode());
            $this->assertStringContainsString("More than one $verProp pointing to some resources:", (string) $resp->getBody());
            sleep(1); // to avoid removing resources before the transaction is fully rolled back
        }
    }

    public function testEmptyCollection(): void {
        $parentProp = (string) self::$schema->parent;
        $tcm        = self::createMetadata([], 'https://vocabs.acdh.oeaw.ac.at/schema#TopCollection');
        $cm         = self::createMetadata([], 'https://vocabs.acdh.oeaw.ac.at/schema#Collection');

        self::$repo->begin();
        $tcr            = self::$repo->createResource($tcm);
        $cr             = self::$repo->createResource($cm);
        $this->toDelete = array_merge($this->toDelete, [$tcr, $cr]);
        try {
            self::$repo->commit();
            $this->assertTrue(false);
        } catch (ClientException $e) {
            $msg  = (string) $e->getResponse()->getBody();
            $tcid = preg_replace('|^.*/|', '', $tcr->getUri());
            $cid  = preg_replace('|^.*/|', '', $cr->getUri());
            $this->assertStringStartsWith("Transaction created empty collections: ", $msg);
            $this->assertStringContainsString($tcid, $msg);
            $this->assertStringContainsString($cid, $msg);
            sleep(1); // to avoid removing resources before the transaction is fully rolled back
        }

        // creating an empty collection by moving a resource to another collection
        self::$repo->begin();
        $tcm            = self::createMetadata([], 'https://vocabs.acdh.oeaw.ac.at/schema#TopCollection');
        $cm             = self::createMetadata([], 'https://vocabs.acdh.oeaw.ac.at/schema#Collection');
        $tcr            = self::$repo->createResource($tcm);
        $cr             = self::$repo->createResource($cm);
        $rtcm           = self::createMetadata([$parentProp => (string) $tcr->getUri()], 'https://vocabs.acdh.oeaw.ac.at/schema#Resource');
        $rcm            = self::createMetadata([$parentProp => (string) $cr->getUri()], 'https://vocabs.acdh.oeaw.ac.at/schema#Resource');
        $rtcr           = self::$repo->createResource($rtcm);
        $rcr            = self::$repo->createResource($rcm);
        $this->toDelete = array_merge($this->toDelete, [$tcr, $cr, $rtcr, $rcr]);
        self::$repo->commit();

        $rcm = $rcr->getMetadata();
        $rcm->forEach(fn($q) => $q->withObject($rcr->getUri()), new PT($parentProp));
        $rcr->setMetadata($rcm);
        self::$repo->begin();
        $rcr->updateMetadata();
        try {
            self::$repo->commit();
            $this->assertTrue(false);
        } catch (ClientException $e) {
            $msg  = (string) $e->getResponse()->getBody();
            $tcid = preg_replace('|^.*/|', '', $tcr->getUri());
            $cid  = preg_replace('|^.*/|', '', $cr->getUri());
            $this->assertStringStartsWith("Transaction created empty collections: ", $msg);
            $this->assertStringNotContainsString($tcid, $msg);
            $this->assertStringContainsString($cid, $msg);
        }
    }

    public function testNotAllChildrenHasNextItem(): void {
        $parentProp = (string) self::$schema->parent;
        $nextProp   = (string) self::$schema->nextItem;

        // without hasNextItem
        self::$repo->begin();
        $tcr = self::$repo->createResource(self::createMetadata([], 'https://vocabs.acdh.oeaw.ac.at/schema#TopCollection'));
        $cr  = self::$repo->createResource(self::createMetadata([$parentProp => $tcr->getUri()], 'https://vocabs.acdh.oeaw.ac.at/schema#Collection'));
        $rr  = self::$repo->createResource(self::createMetadata([$parentProp => $cr->getUri()], 'https://vocabs.acdh.oeaw.ac.at/schema#Resource'));
        $m[] = self::createMetadata([$parentProp => $tcr->getUri()], 'https://vocabs.acdh.oeaw.ac.at/schema#Collection');
        $r[] = self::$repo->createResource(end($m));
        for ($i = 0; $i < 3; $i++) {
            $m[] = self::createMetadata([$parentProp => $r[0]->getUri()]);
            $r[] = self::$repo->createResource(end($m));
        }
        self::$repo->commit();
        $this->assertTrue(true);
        $this->toDelete = array_merge($this->toDelete, $r, [$tcr, $cr]);

        // correct hasNextItem
        self::$repo->begin();
        for ($i = 0; $i < 3; $i++) {
            $m[$i]->add(DF::quad($r[$i]->getUri(), self::$schema->nextItem, $r[$i + 1]->getUri()));
            $r[$i]->setMetadata($m[$i]);
            $r[$i]->updateMetadata();
        }
        self::$repo->commit();
        $this->assertTrue(true);

        // drop one hasNextItem to create a broken chain
        self::$repo->begin();
        $m[2]->delete(new PT(self::$schema->nextItem));
        $m[2]->add(DF::quad($r[2]->getUri(), self::$schema->delete, self::$schema->nextItem));
        $r[2]->setMetadata($m[2]);
        $r[2]->updateMetadata();
        try {
            self::$repo->commit();
            $this->assertTrue(false);
        } catch (ClientException $ex) {
            $msg = (string) $ex->getResponse()->getBody();
            $this->assertEquals(400, $ex->getCode());
            $this->assertStringStartsWith("Collections containing incomplete $nextProp sequence: ", $msg);
            $this->assertStringEndsWith(" (2 < 3)", $msg);
        }

        // same should go if removed from a collection
        self::$repo->begin();
        $m[0]->delete(new PT(self::$schema->nextItem));
        $m[0]->add(DF::quad($r[0]->getUri(), self::$schema->delete, self::$schema->nextItem));
        $r[0]->setMetadata($m[0]);
        $r[0]->updateMetadata();
        try {
            self::$repo->commit();
            $this->assertTrue(false);
        } catch (ClientException $ex) {
            $msg = (string) $ex->getResponse()->getBody();
            $this->assertEquals(400, $ex->getCode());
            $this->assertStringStartsWith("Collections containing incomplete $nextProp sequence: ", $msg);
            $this->assertStringEndsWith(" (2 < 3)", $msg);
        }
        // give transaction controller a little time
        sleep(1);
    }
}
