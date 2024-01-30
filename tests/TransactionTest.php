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
        $rCol1          = self::$repo->createResource(self::createMetadata([], $schema->classes->collection));
        $rCol2Meta      = self::createMetadata([(string) $schema->parent => $rCol1->getUri()], $schema->classes->collection);
        $rCol2          = self::$repo->createResource($rCol2Meta);
        self::$repo->commit();
        $this->toDelete = array_merge($this->toDelete, [$rCol1, $rCol2]);

        // add resources
        $bin1Size       = filesize(__FILE__);
        $bin2Size       = filesize(__DIR__ . '/../config-sample.yaml');
        self::$repo->begin();
        $meta1          = self::createMetadata([(string) $schema->parent => $rCol1->getUri()]);
        $binary1        = new BinaryPayload(null, __FILE__);
        $rBin1          = self::$repo->createResource($meta1, $binary1);
        $meta2          = self::createMetadata([(string) $schema->parent => $rCol2->getUri()]);
        $binary2        = new BinaryPayload(null, __DIR__ . '/../config-sample.yaml');
        $rBin2          = self::$repo->createResource($meta2, $binary2);
        self::$repo->commit();
        $this->toDelete = array_merge($this->toDelete, [$rBin1, $rBin2]);

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
        $rBin2->delete(true);
        self::$repo->commit();

        $rCol1->loadMetadata(true);
        $rCol2->loadMetadata(true);
        $rCol1Meta = $rCol1->getGraph();
        $rCol2Meta = $rCol2->getGraph();
        $this->assertEquals($bin1Size, $rCol1Meta->getObject(new PT($sizeProp))?->getValue());
        $this->assertEquals(2, (int) $rCol1Meta->getObject(new PT($countProp))?->getValue());
        $this->assertEquals(0, (int) $rCol2Meta->getObject(new PT($sizeProp))?->getValue());
        $this->assertEquals(0, (int) $rCol2Meta->getObject(new PT($countProp))?->getValue());

        self::$repo->begin();
        $rBin1->delete(true);
        self::$repo->commit();

        $rCol1->loadMetadata(true);
        $rCol2->loadMetadata(true);
        $rCol1Meta = $rCol1->getGraph();
        $rCol2Meta = $rCol2->getGraph();
        $this->assertEquals(0, (int) $rCol1Meta->getObject(new PT($sizeProp))?->getValue());
        $this->assertEquals(1, (int) $rCol1Meta->getObject(new PT($countProp))?->getValue());
        $this->assertEquals(0, (int) $rCol2Meta->getObject(new PT($sizeProp))?->getValue());
        $this->assertEquals(0, (int) $rCol2Meta->getObject(new PT($countProp))?->getValue());

        self::$repo->begin();
        $rCol1Meta->delete(new PT($rdfType));
        $rCol1Meta->add(DF::quadNoSubject($rdfType, DF::namedNode('https://foo')));
        $rCol1->setMetadata($rCol1Meta);
        $rCol1->updateMetadata();
        self::$repo->commit();

        // col1 is not a schema.classes.collection now so it shouldn't have collection-specific properties
        $rCol1->loadMetadata(true);
        $rCol1Meta = $rCol1->getGraph();
        $this->assertNull($rCol1Meta->getObject(new PT($sizeProp)));
        $this->assertNull($rCol1Meta->getObject(new PT($countProp)));
    }

    public function testCollectionExtent2(): void {
        // move between two independent collections
        $schema     = self::$schema;
        $sizeProp   = $schema->binarySizeCumulative;
        $countProp  = $schema->countCumulative;
        $parentProp = $schema->parent;
        $collClass  = $schema->classes->collection;

        self::$repo->begin();
        $rCol1          = self::$repo->createResource(self::createMetadata([], $collClass));
        $rCol2          = self::$repo->createResource(self::createMetadata([], $collClass));
        self::$repo->commit();
        $this->toDelete = array_merge($this->toDelete, [$rCol1, $rCol2]);

        // add resources
        $binSize        = filesize(__FILE__);
        self::$repo->begin();
        $meta           = self::createMetadata([(string) $parentProp => $rCol1->getUri()]);
        $binary         = new BinaryPayload(null, __FILE__);
        $rBin           = self::$repo->createResource($meta, $binary);
        self::$repo->commit();
        $this->toDelete = array_merge($this->toDelete, [$rBin]);

        $rCol1->loadMetadata(true);
        $rCol2->loadMetadata(true);
        $rCol1Meta = $rCol1->getGraph();
        $rCol2Meta = $rCol2->getGraph();
        $this->assertEquals($binSize, (int) $rCol1Meta->getObject(new PT($sizeProp))?->getValue());
        $this->assertEquals(1, (int) $rCol1Meta->getObject(new PT($countProp))?->getValue());
        $this->assertEquals(0, (int) $rCol2Meta->getObject(new PT($sizeProp))?->getValue());
        $this->assertEquals(0, (int) $rCol2Meta->getObject(new PT($countProp))?->getValue());

        self::$repo->begin();
        $meta->delete(new PT($parentProp));
        $meta->add(DF::quadNoSubject($parentProp, DF::namedNode($rCol2->getUri())));
        $rBin->setMetadata($meta);
        $rBin->updateMetadata();
        self::$repo->commit();

        $rCol1->loadMetadata(true);
        $rCol2->loadMetadata(true);
        $rCol1Meta = $rCol1->getGraph();
        $rCol2Meta = $rCol2->getGraph();
        $this->assertEquals(0, (int) $rCol1Meta->getObject(new PT($sizeProp))?->getValue());
        $this->assertEquals(0, (int) $rCol1Meta->getObject(new PT($countProp))?->getValue());
        $this->assertEquals($binSize, (int) $rCol2Meta->getObject(new PT($sizeProp))?->getValue());
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
        $rCol1          = self::$repo->createResource(self::createMetadata([], $collClass));
        self::$repo->commit();
        $this->toDelete = array_merge($this->toDelete, [$rCol1]);

        $rCol1->loadMetadata(true);
        $rCol1Meta = $rCol1->getGraph();
        $this->assertEquals('', $rCol1Meta->getObject(new PT($licenseAggProp))?->getValue());
        $this->assertEquals('', $rCol1Meta->getObject(new PT($accessAggProp))?->getValue());

        // add resources
        self::$repo->begin();
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
        $this->toDelete = array_merge($this->toDelete, [$rBin1, $rBin2, $rBin3]);

        $rCol1->loadMetadata(true);
        $rCol1Meta = $rCol1->getGraph();
        $tmp       = $rCol1Meta->getObject(new PT($licenseAggProp));
        $this->assertInstanceOf(LiteralInterface::class, $tmp);
        $ref       = $tmp->getLang() === 'en' ? "Attribution 4.0 International (CC BY 4.0) 1\nMIT license 1\nNo Copyright - Non-Commercial Use Only 1" : "Kein Urheberrechtsschutz - nur nicht kommerzielle Nutzung erlaubt 1\nMIT Lizenz 1\nNamensnennung 4.0 International (CC BY 4.0) 1";
        $this->assertEquals($ref, $tmp->getValue());
        $tmp       = $rCol1Meta->getObject(new PT($accessAggProp));
        $this->assertInstanceOf(LiteralInterface::class, $tmp);
        $ref       = $tmp->getLang() === 'en' ? "academic 2\nrestricted 1" : "akademisch 2\neingeschränkt 1";
        $this->assertEquals($ref, $tmp->getValue());
    }

    public function testTopCollectionAggregates(): void {
        self::$config->schema->classes->collection = 'https://vocabs.acdh.oeaw.ac.at/schema#TopCollection';
        self::$schema = new \acdhOeaw\arche\lib\Schema(self::$config->schema);
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
        $collClass      = $schema->classes->collection;
        $notCheckedProp = $schema->parent;
        $checkedProp    = DF::namedNode('https://vocabs.acdh.oeaw.ac.at/schema#hasDepositor');
        $invalidRes     = DF::namedNode('https://bar/' . rand());
        $validRes       = DF::namedNode('https://orcid.org/0000-0003-0065-8112'); //  random but existing ORCID

        self::$repo->begin();
        $r                = self::$repo->createResource(self::createMetadata([(string) $checkedProp => $validRes], $collClass));
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
        echo "old2: " . $old2r->getUri() . "\n";
        $this->toDelete[] = $old2r;
        try {
            self::$repo->commit();
            $this->assertTrue(false);
        } catch (ClientException $e) {
            $resp = $e->getResponse();
            $this->assertEquals(400, $resp->getStatusCode());
            $this->assertStringContainsString("More than one $verProp pointing to some resources:", (string) $resp->getBody());
            sleep(1); // to avoid removing resources between the transaction is fully rolled back
        }
    }
}
