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

use acdhOeaw\acdhRepoLib\BinaryPayload;
use zozlak\RdfConstants as RDF;

/**
 * Description of TransactionTest
 *
 * @author zozlak
 */
class TransactionTest extends TestBase {

    public function testCollectionExtent(): void {
        $sizeProp  = self::$config->schema->binarySizeCumulative;
        $countProp = self::$config->schema->countCumulative;

        self::$repo->begin();
        $rCol1          = self::$repo->createResource(self::createMetadata());
        $rCol2Meta      = self::createMetadata([self::$config->schema->parent => $rCol1->getUri()], self::$config->schema->classes->collection);
        $rCol2          = self::$repo->createResource($rCol2Meta);
        self::$repo->commit();
        $this->toDelete = array_merge($this->toDelete, [$rCol1, $rCol2]);

        // add resources
        $bin1Size       = filesize(__FILE__);
        $bin2Size       = filesize(__DIR__ . '/../config-sample.yaml');
        self::$repo->begin();
        $meta1          = self::createMetadata([self::$config->schema->parent => $rCol1->getUri()]);
        $binary1        = new BinaryPayload(null, __FILE__);
        $rBin1          = self::$repo->createResource($meta1, $binary1);
        $meta2          = self::createMetadata([self::$config->schema->parent => $rCol2->getUri()]);
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
        $this->assertNull($rBin1Meta->getLiteral($countProp));
        $this->assertEquals($bin1Size + $bin2Size, $rCol1Meta->getLiteral($sizeProp)->getValue());
        $this->assertEquals(3, $rCol1Meta->getLiteral($countProp)->getValue());
        $this->assertEquals($bin2Size, $rCol2Meta->getLiteral($sizeProp)->getValue());
        $this->assertEquals(1, $rCol2Meta->getLiteral($countProp)->getValue());

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
        $this->assertEquals($bin1Size + $bin2Size, $rCol1Meta->getLiteral($sizeProp)->getValue());
        $this->assertEquals(3, $rCol1Meta->getLiteral($countProp)->getValue());
        $this->assertEquals($bin2Size, $rCol2Meta->getLiteral($sizeProp)->getValue());
        $this->assertEquals(1, $rCol2Meta->getLiteral($countProp)->getValue());

        // delete resources
        self::$repo->begin();
        $rBin2->delete(true);
        self::$repo->commit();

        $rCol1->loadMetadata(true);
        $rCol2->loadMetadata(true);
        $rCol1Meta = $rCol1->getGraph();
        $rCol2Meta = $rCol2->getGraph();
        $this->assertEquals($bin1Size, $rCol1Meta->getLiteral($sizeProp)->getValue());
        $this->assertEquals(2, $rCol1Meta->getLiteral($countProp)->getValue());
        $this->assertEquals(0, $rCol2Meta->getLiteral($sizeProp)->getValue());
        $this->assertEquals(0, $rCol2Meta->getLiteral($countProp)->getValue());

        self::$repo->begin();
        $rBin1->delete(true);
        self::$repo->commit();

        $rCol1->loadMetadata(true);
        $rCol2->loadMetadata(true);
        $rCol1Meta = $rCol1->getGraph();
        $rCol2Meta = $rCol2->getGraph();
        $this->assertEquals(0, $rCol1Meta->getLiteral($sizeProp)->getValue());
        $this->assertEquals(1, $rCol1Meta->getLiteral($countProp)->getValue());
        $this->assertEquals(0, $rCol2Meta->getLiteral($sizeProp)->getValue());
        $this->assertEquals(0, $rCol2Meta->getLiteral($countProp)->getValue());

        self::$repo->begin();
        $rCol2->delete(true);
        self::$repo->commit();

        // col1 is an empty metadata-only resource not marked as schema.classes.collection now so it shouldn't have collection-specific properties
        $rCol1->loadMetadata(true);
        $rCol1Meta = $rCol1->getGraph();
        $this->assertNull($rCol1Meta->getLiteral($sizeProp));
        $this->assertNull($rCol1Meta->getLiteral($countProp));
    }

    public function testCollectionExtent2(): void {
        // move between two independent collections
        $sizeProp   = self::$config->schema->binarySizeCumulative;
        $countProp  = self::$config->schema->countCumulative;
        $parentProp = self::$config->schema->parent;
        $collClass  = self::$config->schema->classes->collection;

        self::$repo->begin();
        $rCol1          = self::$repo->createResource(self::createMetadata([], $collClass));
        $rCol2          = self::$repo->createResource(self::createMetadata([], $collClass));
        self::$repo->commit();
        $this->toDelete = array_merge($this->toDelete, [$rCol1, $rCol2]);

        // add resources
        $binSize        = filesize(__FILE__);
        self::$repo->begin();
        $meta           = self::createMetadata([$parentProp => $rCol1->getUri()]);
        $binary         = new BinaryPayload(null, __FILE__);
        $rBin           = self::$repo->createResource($meta, $binary);
        self::$repo->commit();
        $this->toDelete = array_merge($this->toDelete, [$rBin]);

        $rCol1->loadMetadata(true);
        $rCol2->loadMetadata(true);
        $rCol1Meta = $rCol1->getGraph();
        $rCol2Meta = $rCol2->getGraph();
        $this->assertEquals($binSize, $rCol1Meta->getLiteral($sizeProp)->getValue());
        $this->assertEquals(1, $rCol1Meta->getLiteral($countProp)->getValue());
        $this->assertEquals(0, $rCol2Meta->getLiteral($sizeProp)->getValue());
        $this->assertEquals(0, $rCol2Meta->getLiteral($countProp)->getValue());

        self::$repo->begin();
        $meta->deleteResource($parentProp);
        $meta->addResource($parentProp, $rCol2->getUri());
        $rBin->setMetadata($meta);
        $rBin->updateMetadata();
        self::$repo->commit();

        $rCol1->loadMetadata(true);
        $rCol2->loadMetadata(true);
        $rCol1Meta = $rCol1->getGraph();
        $rCol2Meta = $rCol2->getGraph();
        $this->assertEquals(0, $rCol1Meta->getLiteral($sizeProp)->getValue());
        $this->assertEquals(0, $rCol1Meta->getLiteral($countProp)->getValue());
        $this->assertEquals($binSize, $rCol2Meta->getLiteral($sizeProp)->getValue());
        $this->assertEquals(1, $rCol2Meta->getLiteral($countProp)->getValue());
    }

    public function testCollectionAggregates(): void {
        $accessProp     = self::$config->schema->accessRestriction;
        $accessAggProp  = self::$config->schema->accessRestrictionAgg;
        $licenseProp    = self::$config->schema->license;
        $licenseAggProp = self::$config->schema->licenseAgg;
        $parentProp     = self::$config->schema->parent;
        $collClass      = self::$config->schema->classes->collection;

        self::$repo->begin();
        $rCol1          = self::$repo->createResource(self::createMetadata([], $collClass));
        self::$repo->commit();
        $this->toDelete = array_merge($this->toDelete, [$rCol1]);

        $rCol1->loadMetadata(true);
        $rCol1Meta = $rCol1->getGraph();
        $this->assertEquals('', $rCol1Meta->getLiteral($licenseAggProp)->getValue());
        $this->assertEquals('', $rCol1Meta->getLiteral($accessAggProp)->getValue());

        // add resources
        self::$repo->begin();
        $meta           = self::createMetadata([
                $parentProp  => $rCol1->getUri(),
                $accessProp  => 'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/academic',
                $licenseProp => 'https://vocabs.acdh.oeaw.ac.at/archelicenses/mit',
        ]);
        $rBin1          = self::$repo->createResource($meta, new BinaryPayload(null, __FILE__));
        $meta           = self::createMetadata([
                $parentProp  => $rCol1->getUri(),
                $accessProp  => 'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/academic',
                $licenseProp => 'https://vocabs.acdh.oeaw.ac.at/archelicenses/cc-by-4-0',
        ]);
        $rBin2          = self::$repo->createResource($meta, new BinaryPayload(null, __FILE__));
        $meta           = self::createMetadata([
                $parentProp  => $rCol1->getUri(),
                $accessProp  => 'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/restricted',
                $licenseProp => 'https://vocabs.acdh.oeaw.ac.at/archelicenses/noc-nc',
        ]);
        $rBin3          = self::$repo->createResource($meta, new BinaryPayload(null, __FILE__));
        self::$repo->commit();
        $this->toDelete = array_merge($this->toDelete, [$rBin1, $rBin2, $rBin3]);

        $rCol1->loadMetadata(true);
        $rCol1Meta = $rCol1->getGraph();
        $tmp       = $rCol1Meta->getLiteral($licenseAggProp);
        $ref       = $tmp->getLang() === 'en' ? "Attribution 4.0 International (CC BY 4.0) 1\nMIT license 1\nNo Copyright - Non-Commercial Use Only 1" : "Kein Urheberrechtsschutz - nur nicht kommerzielle Nutzung erlaubt 1\nMIT Lizenz 1\nNamensnennung 4.0 International (CC BY 4.0) 1";
        $this->assertEquals($ref, $tmp->getValue());
        $tmp       = $rCol1Meta->getLiteral($accessAggProp);
        $ref       = $tmp->getLang() === 'en' ? "academic 2\nrestricted 1" : "akademisch 2\neingeschränkt 1";
        $this->assertEquals($ref, $tmp->getValue());
    }

    public function testTopCollectionAggregates(): void {
        self::$config->schema->classes->collection = 'https://vocabs.acdh.oeaw.ac.at/schema#TopCollection';
        $this->testCollectionAggregates();
    }
}
