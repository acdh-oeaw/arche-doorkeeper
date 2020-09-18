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
        $rCol2          = self::$repo->createResource(self::createMetadata([
                self::$config->schema->parent => $rCol1->getUri(),
        ]));
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
    }

    public function testCollectionExtent2(): void {
        // move between two independent collections
        $sizeProp   = self::$config->schema->binarySizeCumulative;
        $countProp  = self::$config->schema->countCumulative;
        $parentProp = self::$config->schema->parent;

        self::$repo->begin();
        $rCol1          = self::$repo->createResource(self::createMetadata());
        $rCol2          = self::$repo->createResource(self::createMetadata());
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

}
