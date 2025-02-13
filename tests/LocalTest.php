<?php

/*
 * The MIT License
 *
 * Copyright 2024 zozlak.
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

use quickRdf\DataFactory as DF;
use termTemplates\PredicateTemplate as PT;
use acdhOeaw\arche\doorkeeper\Resource;
use acdhOeaw\arche\doorkeeper\CheckAttribute;
use acdhOeaw\arche\doorkeeper\DoorkeeperException;

/**
 * Description of LocalTest
 *
 * @author zozlak
 */
class LocalTest extends TestBase {

    public function testDuplicatedLang(): void {
        $prop     = DF::namedNode('https://vocabs.acdh.oeaw.ac.at/schema#hasCompleteness');
        $class    = 'https://vocabs.acdh.oeaw.ac.at/schema#Collection';
        $meta     = self::createMetadata([], $class);
        $meta->delete(new PT($prop));
        $meta->add(DF::quadNoSubject($prop, DF::literal('foo', 'en')));
        $meta->add(DF::quadNoSubject($prop, DF::literal('bar', 'en')));
        $resource = new Resource($meta, self::$repo->getSchema(), self::$ontology);
        try {
            $resource->runTests(CheckAttribute::class);
            $this->assertTrue(false);
        } catch (DoorkeeperException $e) {
            $this->assertEquals('Max property count for https://vocabs.acdh.oeaw.ac.at/schema#hasCompleteness is 1 but resource has 2', $e->getMessage());
        }
    }
}
