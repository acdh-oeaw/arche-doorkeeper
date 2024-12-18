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

namespace acdhOeaw\arche\doorkeeper;

use ReflectionClass;
use ReflectionMethod;

/**
 * Description of RunTestsTrait
 *
 * @author zozlak
 */
trait RunTestsTrait {

    /**
     * 
     * @param bool $throwException if `true`, the method throws a DoorkeeperException
     *   on errors or null otherwise. If `false`, method returns a (possibly empty)
     *   array of DoorkeeperExceptions.
     * @return array<DoorkeeperException>|null
     */
    public function runTests(string $attribute = CheckAttribute::class,
                             int $filter = ReflectionMethod::IS_PUBLIC,
                             bool $throwException = true): array | null {
        $rc       = new ReflectionClass(static::class);
        $closures = [];
        foreach ($rc->getMethods($filter) as $method) {
            if (count(array_filter($method->getAttributes(), fn($x) => $x->getName() === $attribute)) > 0) {
                $closures[$method->getName()] = $method->getClosure($this);
            }
        }
        ksort($closures);

        $errors = [];
        foreach ($closures as $i) {
            try {
                $i();
            } catch (DoorkeeperException $ex) {
                $errors[] = $ex;
            }
        }
        if (count($errors) > 0 && $throwException) {
            throw new DoorkeeperException(implode("\n", array_map(fn($x) => $x->getMessage(), $errors)));
        }
        return $throwException ? null : $errors;
    }
}
