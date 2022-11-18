#!/usr/bin/php
<?php

/*
 * Script downloading a given ARCHE collection and preprocessing the data in a way
 * to make it suitable for testing doorkeeper named entity URI resolution checks.
 * 
 * It creates three files:
 * 
 * - testdata.nt - a dataset containing only resources pointing to resources of
 *   class acdh:Person
 * - testdata_persons.nt - a dataset containing only resources of class acdh:Person
 * - testdata_minimal.nt - a dataset with a single subject containing the most
 *   triples pointing to resources of class acdh:Person among the whole input
 *   dataset
 * 
 * Sample usage:
 *   ./prepare_named_entities_test_data.php 'https://arche.acdh.oeaw.ac.at/api/188397/metadata?readMode=99999_0_1_0&format=application/n-triples'
 */

use termTemplates\QuadTemplate as QT;
use termTemplates\NotTemplate as Not;
use termTemplates\AnyOfTemplate as Any;
use termTemplates\ValueTemplate as VT;
use zozlak\RdfConstants as RC;

if ($argc < 2) {
    die("Usage: $argv[0] archeResourceUri\n");
}

include __DIR__ . '/../vendor/autoload.php';
$df           = $dataFactory  = new quickRdf\DataFactory();
$TYPE         = $df->namedNode(RC::RDF_TYPE);
$PERSON       = $df->namedNode('https://vocabs.acdh.oeaw.ac.at/schema#Person');
$ORGANIZATION = $df->namedNode('https://vocabs.acdh.oeaw.ac.at/schema#Organization');
$ID           = $df->namedNode('https://vocabs.acdh.oeaw.ac.at/schema#hasIdentifier');
$TMPL_ARCHE   = new QT(object: new VT('https://arche.acdh.oeaw.ac.at', VT::STARTS));
$TMPL_ACDH    = new QT(object: new VT('https://id.acdh.oeaw.ac.at', VT::STARTS));
$skip         = new QT(
    predicate: new Not(new Any([
            $df->namedNode('https://vocabs.acdh.oeaw.ac.at/schema#hasAccessRestriction'),
            $df->namedNode('https://vocabs.acdh.oeaw.ac.at/schema#hasOaiSet'),
            ]))
);
$license      = [
    $df->namedNode('https://vocabs.acdh.oeaw.ac.at/schema#hasLicense'),
    $df->namedNode('https://vocabs.acdh.oeaw.ac.at/archelicenses/cc0-1-0')
];
$category     = [
    $df->namedNode('https://vocabs.acdh.oeaw.ac.at/schema#hasCategory'),
    $df->namedNode('https://vocabs.acdh.oeaw.ac.at/archecategory/image'),
];
$lifecycle    = [
    $df->namedNode('https://vocabs.acdh.oeaw.ac.at/schema#hasLifeCycleStatus'),
    $df->namedNode('https://vocabs.acdh.oeaw.ac.at/archelifecyclestatus/completed'),
];
if (file_exists($argv[1])) {
    $source = $argv[1];
} else {
    $client = new GuzzleHttp\Client();
    $source = $client->get($argv[1]);
}
$dt           = new quickRdf\Dataset();
$dt->add(quickRdfIo\Util::parse($source, $df));
$persons      = iterator_to_array($dt->copy(new QT(predicate: $TYPE, object: $PERSON))->listSubjects());
$personsMap   = [];
foreach ($persons as $person) {
    $ids = $dt->copy(new QT($person, $ID));
    // prefer external ids but use internal if nothing else exists
    $archeId = $ids->delete($TMPL_ARCHE);
    $acdhId = $ids->delete($TMPL_ACDH);
    if (count($ids) == 0) {
        $ids->add($archeId);
        $ids->add($acdhId);
    }
    // select id at random
    $n   = rand(0, count($ids) - 1);
    $ids = $ids->getIterator();
    while ($n > 0) {
        $ids->next();
        $n--;
    }
    $personsMap[$person->getValue()] = $ids->current()->getObject();
}
$withPersons     = $dt->copy(new QT(predicate: new Not($ID), object: new Any($persons)))->listSubjects();
$output          = new quickRdf\Dataset();
$pointingPersons = new quickRdf\Dataset();
$maxPersons      = 0;
$max             = null;
foreach ($withPersons as $subject) {
    $data = $dt->copy(new QT($subject));
    $tmp  = $data->map(fn($x) => $x->withObject($personsMap[$x->getObject()->getValue()]), fn($x) => isset($personsMap[$x->getObject()->getValue()]));
    $pointingPersons->add($tmp);
    $tmpP = iterator_to_array($tmp->listObjects());
    $data = $data->copy($skip);
    $data->forEach(fn($x) => isset($personsMap[$x->getObject()->getValue()]) ? $x->withObject($personsMap[$x->getObject()->getValue()]) : $x);
    $data->forEach(fn($x) => $x->getPredicate()->equals($license[0]) ? $x->withObject($license[1]) : $x);
    $data->forEach(fn($x) => $x->getPredicate()->equals($category[0]) ? $x->withObject($category[1]) : $x);
    $data->forEach(fn($x) => $x->getPredicate()->equals($lifecycle[0]) ? $x->withObject($lifecycle[1]) : $x);
    $output->add($data);
    if (count($tmpP) > $maxPersons) {
        $maxPersons = count($tmpP);
        $max        = $data;
    }
}
echo "input: " . count($dt) . "\ndistinct persons: " . count($persons) . "\noutput: " . count($output) . "\npointing persons: " . count($pointingPersons) . "\nminimal example persons: " . $maxPersons . "\nminimal example: " . count($max) . "\n";
quickRdfIo\Util::serialize($output, 'application/n-triples', 'testdata.nt');
quickRdfIo\Util::serialize($pointingPersons, 'application/n-triples', 'testdata_persons.nt');
quickRdfIo\Util::serialize($max, 'application/n-triples', 'testdata_minimal.nt');
