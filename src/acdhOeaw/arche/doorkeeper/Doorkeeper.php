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

namespace acdhOeaw\arche\doorkeeper;

use DateTime;
use Exception;
use PDO;
use RuntimeException;
use GuzzleHttp\Client;
use rdfInterface\DatasetNodeInterface;
use rdfInterface\LiteralInterface;
use rdfInterface\NamedNodeInterface;
use quickRdf\DataFactory as DF;
use termTemplates\LiteralTemplate as LT;
use termTemplates\NamedNodeTemplate as NNT;
use termTemplates\NotTemplate as NOT;
use termTemplates\PredicateTemplate as PT;
use termTemplates\ValueTemplate as VT;
use RenanBr\BibTexParser\Listener as BiblatexL;
use RenanBr\BibTexParser\Parser as BiblatexP;
use RenanBr\BibTexParser\Exception\ParserException as BiblatexE1;
use RenanBr\BibTexParser\Exception\ProcessorException as BiblatexE2;
use acdhOeaw\UriNormalizer;
use acdhOeaw\UriNormalizerCache;
use acdhOeaw\UriNormalizerException;
use acdhOeaw\UriNormRules;
use acdhOeaw\epicHandle\HandleService;
use acdhOeaw\arche\core\Transaction;
use acdhOeaw\arche\core\Metadata;
use acdhOeaw\arche\core\Resource as Res;
use acdhOeaw\arche\core\RestController as RC;
use acdhOeaw\arche\lib\Schema;
use acdhOeaw\arche\lib\schema\Ontology;
use acdhOeaw\arche\lib\schema\PropertyDesc;
use zozlak\RdfConstants as RDF;

/**
 * Description of Doorkeeper
 *
 * @author zozlak
 */
class Doorkeeper {

    const DB_LOCK_TIMEOUT      = 1000;
    const NON_NEGATIVE_NUMBERS = [RDF::XSD_NON_NEGATIVE_INTEGER, RDF::XSD_UNSIGNED_LONG,
        RDF::XSD_UNSIGNED_INT, RDF::XSD_UNSIGNED_SHORT, RDF::XSD_UNSIGNED_BYTE];
    const LITERAL_TYPES        = [RDF::XSD_ANY_URI,
        RDF::XSD_DATE, RDF::XSD_DATE_TIME, RDF::XSD_DECIMAL,
        RDF::XSD_FLOAT, RDF::XSD_DOUBLE, RDF::XSD_INTEGER, RDF::XSD_NEGATIVE_INTEGER,
        RDF::XSD_NON_NEGATIVE_INTEGER, RDF::XSD_NON_POSITIVE_INTEGER, RDF::XSD_POSITIVE_INTEGER,
        RDF::XSD_LONG, RDF::XSD_INT, RDF::XSD_SHORT, RDF::XSD_BYTE, RDF::XSD_UNSIGNED_LONG,
        RDF::XSD_UNSIGNED_INT, RDF::XSD_UNSIGNED_SHORT, RDF::XSD_UNSIGNED_BYTE, RDF::XSD_BOOLEAN];

    static private Ontology $ontology;
    static private Schema $schema;
    static private UriNormalizer $uriNorm;

    static public function onResEdit(int $id, DatasetNodeInterface $meta,
                                     ?string $path): DatasetNodeInterface {
        self::$schema = new Schema(RC::$config->schema);
        self::loadOntology();
        $pdo          = new PDO(RC::$config->dbConn->admin);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $pdo->query("SET application_name TO doorkeeper");
        $pdo->query("SET lock_timeout TO " . self::DB_LOCK_TIMEOUT);
        $pdo->beginTransaction();
        $errors       = [];
        // checkTitleProp before checkCardinalities!
        $functions    = [
            'maintainDefaultValues', 'maintainWkt', 'maintainAccessRights', 'maintainPropertyRange',
            'normalizeIds', 'checkTitleProp', 'checkPropertyTypes', 'checkCardinalities',
            'checkIdCount', 'checkLanguage', 'checkUnknownProperties', 'checkBiblatex',
            'maintainCmdiPid', 'maintainPid', // so no PIDs are minted if checks fail
        ];
        foreach ($functions as $f) {
            try {
                self::$f($meta, $pdo);
            } catch (DoorkeeperException $ex) {
                $errors[] = $ex->getMessage();
            }
        }
        if (count($errors) > 0) {
            throw new DoorkeeperException(implode("\n", $errors));
        }
        $pdo->commit();

        return $meta;
    }

    /**
     * 
     * @param string $method
     * @param int $txId
     * @param array<int> $resourceIds
     * @return void
     * @throws DoorkeeperException
     */
    static public function onTxCommit(string $method, int $txId,
                                      array $resourceIds): void {
        self::$schema = new Schema(RC::$config->schema);
        self::loadOntology();
        // current state database handler
        $pdo          = new PDO(RC::$config->dbConn->admin);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $pdo->query("SET application_name TO doorkeeper");
        $pdo->query("SET lock_timeout TO " . self::DB_LOCK_TIMEOUT);
        $pdo->beginTransaction();

        $errors    = [];
        $functions = ['checkIsNewVersionOf', 'checkAutoCreatedResources'];
        foreach ($functions as $f) {
            try {
                self::$f($pdo, $txId, $resourceIds);
            } catch (DoorkeeperException $ex) {
                $errors[] = $ex->getMessage();
            }
        }
        if (count($errors) > 0) {
            throw new DoorkeeperException(implode("\n", $errors));
        }
        self::updateCollections($pdo, $txId, $resourceIds);

        $pdo->commit();
    }

    static private function maintainWkt(DatasetNodeInterface $meta): void {
        $latProp = self::$schema->latitude;
        $lonProp = self::$schema->longitude;
        $wktProp = self::$schema->wkt;
        if ($meta->any(new PT($wktProp))) {
            return;
        }
        $lat = (string) $meta->getObject(new PT($latProp));
        $lon = (string) $meta->getObject(new PT($lonProp));
        if (!empty($lat) && !empty($lon)) {
            $meta->add(DF::quadNoSubject($wktProp, DF::literal("POINT($lon $lat)")));
        }
    }

    static private function maintainPid(DatasetNodeInterface $meta): void {
        $cfg      = RC::$config->doorkeeper->epicPid;
        $idProp   = self::$schema->id;
        $idNmsp   = (string) self::$schema->namespaces->id;
        $pidProp  = self::$schema->pid;
        $propRead = RC::$config->accessControl->schema->read;
        $pidTmpl  = new PT($pidProp);
        $res      = $meta->getNode();
        $ids      = $meta->listObjects(new PT($idProp))->getValues();

        $classesAlways        = [
            self::$schema->classes->topCollection,
            self::$schema->classes->collection,
        ];
        $classesNotRestricted = [
            self::$schema->classes->resource,
            self::$schema->classes->metadata,
        ];
        $rolesNotRestricted   = [
            RC::$config->doorkeeper->rolePublic,
            RC::$config->doorkeeper->roleAcademic,
        ];
        $class                = $meta->listObjects(new PT(RDF::RDF_TYPE))->getValues();
        $roles                = $meta->listObjects(new PT($propRead))->getValues();
        $public               = count(array_intersect($class, $classesAlways)) > 0 ||
            count(array_intersect($class, $classesNotRestricted)) > 0 && count(array_intersect($roles, $rolesNotRestricted)) > 0;

        $idSubNmsps = [];
        foreach (self::$schema->namespaces as $i) {
            $i = (string) $i;
            if (str_starts_with($i, $idNmsp) && $i !== $idNmsp) {
                $idSubNmsps[] = $i;
            }
        }

        $curPid = null;
        $id     = null;
        foreach ($ids as $i) {
            if (str_starts_with($i, $cfg->resolver)) {
                $curPid = $i;
            }
            if (str_starts_with($i, $idNmsp)) {
                $flag = true;
                foreach ($idSubNmsps as $j) {
                    $flag &= !str_starts_with($i, $j);
                }
                if ($flag) {
                    $id = $i;
                }
            }
        }
        $pidLit = $meta->getObject($pidTmpl);
        $pidLit = $pidLit !== null ? (string) $pidLit : null;
        if (($pidLit === $cfg->createValue || $public) && $id !== null) {
            if (empty($cfg->pswd)) {
                RC::$log->info("\t\tskipping PID (re)generation - no EPIC password provided");
                return;
            }
            $ps = new HandleService($cfg->url, $cfg->prefix, $cfg->user, $cfg->pswd);
            if ($curPid === null) {
                $meta->delete($pidTmpl);
                $pid = $ps->create($id);
                $pid = str_replace($cfg->url, $cfg->resolver, $pid);
                RC::$log->info("\t\tregistered PID $pid pointing to " . $id);
                $meta->add(DF::Quad($res, $pidProp, DF::literal($pid, null, RDF::XSD_ANY_URI)));
            } else {
                $meta->delete($pidTmpl);
                $meta->add(DF::quad($res, $pidProp, DF::literal($curPid, null, RDF::XSD_ANY_URI)));
                $pid = str_replace($cfg->resolver, $cfg->url, $curPid);
                $ret = $ps->update($pid, $id);
                RC::$log->info("\t\trecreated PID $curPid pointing to " . $id . " with return code " . $ret);
            }
        }
        // standardize PID if needed
        if ($pidLit !== null) {
            if (!isset(self::$uriNorm)) {
                self::$uriNorm = new UriNormalizer();
            }
            $stdPid = self::$uriNorm->normalize($pidLit, false);
            if ($stdPid !== $pidLit) {
                $meta->delete($pidTmpl);
                $meta->add(DF::quad($res, $pidProp, DF::literal($stdPid, null, RDF::XSD_ANY_URI)));
                RC::$log->info("\t\tPID $pidLit standardized to $stdPid");
            }
        }
        // promote PIDs to IDs
        foreach ($meta->getIterator($pidTmpl) as $i) {
            $i = (string) $i->getObject();
            if ($i === '') {
                throw new DoorkeeperException("Empty PID");
            }
            if ($i !== $cfg->createValue && !in_array($i, $ids)) {
                RC::$log->info("\t\tpromoting PID $i to an id");
                $meta->add(DF::quad($res, $idProp, DF::literal($i)));
            }
        }
    }

    /**
     * CMDI records must have their very own PIDs but this requires special handling
     * as in ARCHE CMDI is just a metadata serialization format and not a separate
     * repository resource.
     * 
     * Because of that we need to store CMDI PIDs in a separate property and we want
     * it to be automatically filled in for resources in a given 
     * (`cfg.schema.doorkeeper.epicPid.clarinSetProperty` with value 
     * `cfg.doorkeeper.epicPid.clarinSet`) OAI-PMH set.
     * 
     * This method takes care of it.
     * @param DatasetNodeInterface $meta
     * @param PDO $pdo
     * @return void
     * @throws DoorkeeperException
     */
    static private function maintainCmdiPid(DatasetNodeInterface $meta, PDO $pdo): void {
        $cfg         = RC::$config->doorkeeper->epicPid;
        $cmdiPidProp = self::$schema->cmdiPid;
        $pidProp     = self::$schema->pid;
        $pidNmsp     = (string) self::$schema->namespaces->cmdi;
        $setProp     = $cfg->clarinSetProperty;
        $idProp      = self::$schema->id;
        $idNmsp      = RC::getBaseUrl();
        if ($meta->any(new PT($cmdiPidProp)) || $meta->none(new PT($setProp))) {
            // CMDI PID exists or OAI-PMH set property doesn't exist - nothing to do
            return;
        }

        $query      = $pdo->prepare("SELECT i1.ids FROM identifiers i1 JOIN identifiers i2 USING (id) WHERE i2.ids = ?");
        $query->execute([$cfg->clarinSet]);
        $clarinSets = $query->fetchAll(PDO::FETCH_COLUMN);

        $sets = $meta->listObjects(new PT($setProp))->getValues();
        if (count(array_intersect($sets, $clarinSets)) > 0) {
            if (empty($cfg->pswd)) {
                RC::$log->info("\t\tskipping CMDI PID generation - no EPIC password provided");
                return;
            }
            $res = $meta->getNode();
            $id  = null;
            $id  = (string) $meta->getObject(new PT($idProp, new VT($idNmsp, VT::STARTS)));
            if (!empty($id)) {
                $id      = $pidNmsp . substr((string) $id, strlen($idNmsp));
                $ps      = new HandleService($cfg->url, $cfg->prefix, $cfg->user, $cfg->pswd);
                $cmdiPid = $ps->create($id);
                $cmdiPid = str_replace($cfg->url, $cfg->resolver, $cmdiPid);
                RC::$log->info("\t\tregistered CMDI PID $cmdiPid pointing to " . $id);
                $meta->add(DF::quad($res, $cmdiPidProp, DF::literal($cmdiPid, null, RDF::XSD_ANY_URI)));
                $meta->add(DF::quad($res, self::$schema->id, DF::literal($id)));
            }
            // if normal PID is missing, trigger its generation
            if ($meta->none(new PT($pidProp))) {
                $meta->add(DF::quad($res, $pidProp, DF::literal("create")));
            }
        }
    }

    /**
     * Access rights should be maintained according to the 
     * `cfg.schema.accessRestriction` value:
     * 
     * - in all cases write access should be revoked from the public and academic
     * - when `public` read access to the public should be granted
     * - when `academic` read access to the `cfg.doorkeeper.roleAcademic` should 
     *   be granted and public read access should be revoked
     * - when `restricted` read rights should be revoked from public and 
     *   `cfg.doorkeeper.roleAcademic` and granted to users listed
     *   in `cfg.schema.accessRole`
     * 
     * @param DatasetNodeInterface $meta repository resource's metadata
     * @param PDO $pdo
     */
    static public function maintainAccessRights(DatasetNodeInterface $meta,
                                                PDO $pdo): void {
        $accessRestr = (string) $meta->getObject(new PT(self::$schema->accessRestriction));
        if (empty($accessRestr)) {
            return;
        }

        $propRead     = DF::namedNode(RC::$config->accessControl->schema->read);
        $propRoles    = self::$schema->accessRole;
        $rolePublic   = DF::namedNode(RC::$config->doorkeeper->rolePublic);
        $roleAcademic = DF::namedNode(RC::$config->doorkeeper->roleAcademic);
        $res          = $meta->getNode();

        $query          = $pdo->prepare("SELECT i1.ids FROM identifiers i1 JOIN identifiers i2 USING (id) WHERE i2.ids = ?");
        $query->execute([$accessRestr]);
        $accessRestrIds = $query->fetchAll(PDO::FETCH_COLUMN);

        RC::$log->info("\t\tmaintaining access rights for " . implode(', ', $accessRestrIds));

        if (in_array('https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/public', $accessRestrIds)) {
            $meta->add(DF::quad($res, $propRead, DF::literal($rolePublic)));
            RC::$log->info("\t\t\tpublic");
        } else {
            $meta->delete(new PT($propRead, new VT($rolePublic)));
            if (in_array('https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/academic', $accessRestrIds)) {
                $meta->add(DF::quad($res, $propRead, DF::literal($roleAcademic)));
                RC::$log->info("\t\t\tacademic");
            } else {
                $meta->delete(new PT($propRead, new VT($roleAcademic)));
                foreach ($meta->getIterator(new PT($propRoles)) as $i) {
                    $meta->add($i->withPredicate($propRead));
                }
                RC::$log->info("\t\t\trestricted");
            }
        }
    }

    static private function maintainPropertyRange(DatasetNodeInterface $meta): void {
        static $checkRangeUris = null;
        if ($checkRangeUris === null) {
            $checkRangeUris = array_keys((array) RC::$config->doorkeeper->checkRanges);
        }

        foreach ($meta->listPredicates() as $prop) {
            $propDesc = self::$ontology->getProperty($meta, $prop);
            if ($propDesc === null || !is_array($propDesc->range) || count($propDesc->range) === 0) {
                continue;
            }
            // at least as for arche-lib-schema 6.1.0 the range is always a single class
            // (even if identified by all it's ids) so we don't need to worry about
            // range's parent class rules relaxing the check
            $rangesToCheck = array_intersect($propDesc->range, $checkRangeUris);

            if (!empty($propDesc->vocabs)) {
                self::maintainPropertyRangeVocabs($meta, $propDesc, $prop);
            } elseif (count($rangesToCheck) > 0) {
                foreach ($rangesToCheck as $i) {
                    self::checkPropertyRangeUri($meta, $i, $prop);
                }
            } else {
                self::maintainPropertyRangeLiteral($meta, $propDesc, $prop);
            }
        }
    }

    static private function maintainPropertyRangeVocabs(DatasetNodeInterface $meta,
                                                        PropertyDesc $propDesc,
                                                        NamedNodeInterface $prop): void {
        if (RC::$config->doorkeeper->checkVocabularyValues === false) {
            return;
        }
        $res = $meta->getNode();
        foreach ($meta->listObjects(new PT($prop)) as $v) {
            $vs  = (string) $v;
            $vid = $propDesc->checkVocabularyValue($vs, Ontology::VOCABSVALUE_ID);
            if (!empty($vid) && !($v instanceof NamedNodeInterface)) {
                // the value is right, just it's literal and it should be a named node
            } elseif (empty($vid)) {
                if ($v instanceof NamedNodeInterface) {
                    // value is not a proper id and it's a named node
                    throw new DoorkeeperException("property $prop value $vs is not in the $propDesc->vocabs vocabulary");
                }
                $vid = $propDesc->checkVocabularyValue($vs, Ontology::VOCABSVALUE_ALL);
                if (empty($vid)) {
                    throw new DoorkeeperException("property $prop value $vs is not in the $propDesc->vocabs vocabulary");
                }
                // map to the right value
                $meta->delete(new PT($prop, $v));
                $meta->add(DF::quad($res, $prop, DF::namedNode($vid)));
                RC::$log->info("\t\tproperty $prop mapping literal value '$vs' to resource $vid");
            }
        }
    }

    static private function maintainPropertyRangeLiteral(DatasetNodeInterface $meta,
                                                         PropertyDesc $propDesc,
                                                         NamedNodeInterface $prop): void {
        $res   = $meta->getNode();
        $range = $propDesc->range;
        foreach ($meta->listObjects(new PT($prop, new LT(null, LT::ANY))) as $l) {
            $type = $l instanceof LiteralInterface ? $l->getDatatype() : null;
            if (in_array($type, $range)) {
                continue;
            }
            if (in_array(RDF::XSD_STRING, $range)) {
                $meta->delete(new PT($prop, $l));
                $meta->add(DF::quad($res, $prop, DF::literal((string) $l)));
                RC::$log->debug("\t\tcasting $prop value from $type to string");
            } else {
                try {
                    $rangeTmp = array_intersect($range, self::LITERAL_TYPES);
                    $rangeTmp = (reset($rangeTmp) ?: reset($range)) ?: RDF::XSD_STRING;
                    $value    = self::castLiteral($l, $rangeTmp);
                    $meta->delete(new PT($prop, $l));
                    $meta->add(DF::quad($res, $prop, $value));
                    RC::$log->debug("\t\tcasting $prop value from $type to $rangeTmp");
                } catch (RuntimeException $ex) {
                    RC::$log->info("\t\t" . $ex->getMessage());
                } catch (DoorkeeperException $ex) {
                    throw new DoorkeeperException("property $prop: " . $ex->getMessage(), $ex->getCode(), $ex);
                }
            }
        }
    }

    static private function checkPropertyRangeUri(DatasetNodeInterface $meta,
                                                  string $rangeUri, string $prop): void {
        static $rangeDefs = null;
        if ($rangeDefs === null) {
            $rangeDefs = RC::$config->doorkeeper->checkRanges;
        }
        static $client = null;
        if ($client === null) {
            $client = new Client();
        }
        static $cache = null;
        if ($cache === null) {
            $cache = new UriNormalizerCache('cache.sqlite');
        }
        static $normalizers = [];
        if (!isset($normalizers[$rangeUri])) {
            $rules                  = UriNormRules::getRules($rangeDefs->$rangeUri);
            $normalizers[$rangeUri] = new UriNormalizer($rules, '', $client, $cache);
        }

        /** @var UriNormalizer $norm */
        $norm = $normalizers[$rangeUri];
        foreach ($meta->listObjects(new PT($prop)) as $obj) {
            if (!($obj instanceof NamedNodeInterface)) {
                throw new DoorkeeperException("property $prop has a literal value");
            }
            try {
                $norm->resolve((string) $obj);
                RC::$log->debug("\t\t$prop value $obj resolved successfully");
            } catch (UriNormalizerException $ex) {
                throw new DoorkeeperException($ex->getMessage(), $ex->getCode(), $ex);
            }
        }
    }

    static private function maintainDefaultValues(DatasetNodeInterface $meta): void {
        $res = $meta->getNode();
        foreach ($meta->listObjects(new PT(RDF::RDF_TYPE)) as $class) {
            $c = self::$ontology->getClass($class);
            if ($c === null) {
                continue;
            }
            foreach ($c->properties as $p) {
                if (!empty($p->defaultValue) && $meta->none(new PT($p->uri))) {
                    $val = null;
                    switch ($p->type) {
                        case RDF::OWL_OBJECT_PROPERTY:
                            $val = DF::namedNode($p->defaultValue);
                            break;
                        case RDF::OWL_DATATYPE_PROPERTY:
                            $val = DF::literal($p->defaultValue, $p->langTag ? 'en' : null, reset($p->range) ?: RDF::XSD_STRING);
                            break;
                    }
                    if (is_object($val)) {
                        $meta->add(DF::quad($res, DF::namedNode($p->uri), $val));
                    }
                    RC::$log->info("\t\t$p->uri added with a default value of $p->defaultValue");
                }
            }
        }
    }

    static private function normalizeIds(DatasetNodeInterface $meta): void {
        if (!isset(self::$uriNorm)) {
            self::$uriNorm = new UriNormalizer();
        }
        $res = $meta->getNode();

        // enforce IDs to be in known namespaces for known classes
        $forceNormalize = false;
        foreach ($meta->listObjects(new PT(RDF::RDF_TYPE)) as $class) {
            $c = self::$ontology->getClass($class);
            if ($c !== null) {
                $forceNormalize = true;
                break;
            }
        }

        $idProp = self::$schema->id;
        foreach ($meta->listObjects(new PT($idProp)) as $id) {
            $ids = (string) $id;
            try {
                $std = self::$uriNorm->normalize($ids, $forceNormalize);
            } catch (UriNormalizerException $e) {
                throw new DoorkeeperException($e->getMessage());
            }
            if ($std !== $ids) {
                $meta->delete(new PT($idProp, $id));
                $meta->add(DF::quad($res, $idProp, DF::namedNode($std)));
                RC::$log->info("\t\tid URI $ids standardized to $std");
            }
        }
    }

    static private function checkLanguage(DatasetNodeInterface $meta): void {
        foreach ($meta->listPredicates() as $prop) {
            $p = self::$ontology->getProperty([], $prop);
            if (is_object($p) && $p->langTag) {
                $value = $meta->getObject(new PT($prop, new NOT(new LT(null, VT::ANY, ''))));
                if ($value !== null) {
                    throw new DoorkeeperException("Property $prop with value " . (string) $value . " is not tagged with a language");
                }
            }
        }
    }

    /**
     * Checks property types (datatype/object).
     * 
     * As the property type doesn't depend on the class context, the check can
     * and should be done for all metadata properties.
     * 
     * @param DatasetNodeInterface $meta
     * @throws DoorkeeperException
     */
    static private function checkPropertyTypes(DatasetNodeInterface $meta): void {
        foreach ($meta->listPredicates() as $p) {
            $pDef = self::$ontology->getProperty([], $p);
            if (is_object($pDef)) {
                if ($pDef->type === RDF::OWL_DATATYPE_PROPERTY && $meta->any(new PT($p, new NNT(null, NNT::ANY)))) {
                    throw new DoorkeeperException('URI value for a datatype property ' . $p);
                }
                if ($pDef->type === RDF::OWL_OBJECT_PROPERTY && $meta->any(new PT($p, new LT(null, LT::ANY)))) {
                    throw new DoorkeeperException('Literal value for an object property ' . $p);
                }
            }
        }
    }

    /**
     * Checks property cardinalities according to the ontology.
     * 
     * @param DatasetNodeInterface $meta
     * @throws DoorkeeperException
     */
    static private function checkCardinalities(DatasetNodeInterface $meta): void {
        //TODO - rewrite so it just iterates each triple once gathering counts of all properties
        $ontologyNmsp = self::$schema->namespaces->ontology;
        $inDomain     = [RDF::RDF_TYPE];
        foreach ($meta->listObjects(new PT(RDF::RDF_TYPE)) as $class) {
            $classDef = self::$ontology->getClass((string) $class);
            if ($classDef === null) {
                continue;
            }
            $inNmsp = str_starts_with($class, $ontologyNmsp);
            foreach ($classDef->properties as $p) {
                if ($inNmsp) {
                    // check property domains only for resources of ACDH classes
                    $inDomain = array_merge($inDomain, $p->property);
                }
                if (($p->min > 0 || $p->max !== null) && $p->automatedFill === false) {
                    $co  = $cd  = 0;
                    $cdl = ['' => 0];
                    foreach ($p->property as $i) {
                        foreach ($meta->getIterator(new PT($i)) as $j) {
                            $j = $j->getObject();
                            if ($j instanceof LiteralInterface) {
                                $cd++;
                                $lang       = $j->getLang() ?: '';
                                $cdl[$lang] = 1 + ($cdl[$lang] ?? 0);
                            } else {
                                $co++;
                            }
                        }
                    }
                    if ($p->min > 0 && $co + $cd < $p->min) {
                        throw new DoorkeeperException('Min property count for ' . $p->uri . ' is ' . $p->min . ' but resource has ' . ($co + $cd));
                    }
                    if ($p->max > 0 && $co + max($cdl) > $p->max) {
                        throw new DoorkeeperException('Max property count for ' . $p->uri . ' is ' . $p->max . ' but resource has ' . ($co + max($cdl)));
                    }
                }
            }
        }
        // check only resources of a class(es) defined in the ontology
        if (count($inDomain) > 1) {
            $outDomain = array_diff($meta->listPredicates()->getValues(), $inDomain); // properties allowed on resource classes
            $owlThing  = self::$ontology->getClass(RDF::OWL_THING);
            $outDomain = array_diff($outDomain, array_keys($owlThing->properties)); // properties allowed on all resources
            if (count($outDomain) > 0) {
                throw new DoorkeeperException("Properties with a wrong domain: " . implode(', ', $outDomain));
            }
        }
    }

    static private function checkBiblatex(DatasetNodeInterface $meta): void {
        $biblatexProp = self::$schema->biblatex ?? '';
        $biblatex     = trim((string) $meta->getObject(new PT($biblatexProp)));
        if (!empty($biblatex)) {
            if (substr($biblatex, 0, 1) !== '@') {
                $biblatex = "@dataset{foo,\n$biblatex\n}";
            }
            $listener = new BiblatexL();
            $parser   = new BiblatexP();
            $parser->addListener($listener);
            try {
                $parser->parseString($biblatex);
            } catch (BiblatexE1 $e) {
                $msg = $e->getMessage();
                throw new DoorkeeperException("Invalid BibLaTeX entry ($msg): $biblatex");
            } catch (BiblatexE2 $e) {
                $msg = $e->getMessage();
                throw new DoorkeeperException("Invalid BibLaTeX entry ($msg): $biblatex");
            }
            if (count($listener->export()) === 0) {
                throw new DoorkeeperException("Invalid BibLaTeX entry: $biblatex");
            }
        }
    }

    /**
     * Every resource must have at least one repository ID and one 
     * non-repository ID.
     * 
     * The non-repository ID is obligatory because repository IDs are sequential 
     * and don't have any meaning. Therefore it is very unlikely anyone knows 
     * them and uses them in the resources' metadata which causes a serious risk 
     * of mismatch on the next import.
     * 
     * Moreover resources representing ontology can have only one non-repository
     * ID (so they must always have exactly two).
     * 
     * @param DatasetNodeInterface $meta
     * @throws DoorkeeperException
     */
    static private function checkIdCount(DatasetNodeInterface $meta): void {
        $idProp       = self::$schema->id;
        $repoNmsp     = RC::getBaseUrl();
        $ontologyNmsp = self::$schema->namespaces->ontology;

        $ontologyIdCount = $repoIdCount     = $nonRepoIdCount  = 0;
        $ids             = $meta->listObjects(new PT($idProp))->getValues();
        foreach ($ids as $id) {
            $id           = (string) $id;
            $ontologyFlag = str_starts_with($id, $ontologyNmsp);
            $repoFlag     = str_starts_with($id, $repoNmsp);

            $ontologyIdCount += $ontologyFlag;
            $repoIdCount     += $repoFlag;
            $nonRepoIdCount  += !($ontologyFlag || $repoFlag);
        }

        if ($ontologyIdCount > 1) {
            throw new DoorkeeperException('More than one ontology id');
        }
        if ($nonRepoIdCount === 0 && $ontologyIdCount === 0) {
            throw new DoorkeeperException('No non-repository id');
        }
        if ($ontologyIdCount > 0 && $nonRepoIdCount !== 0) {
            throw new DoorkeeperException('Ontology resource can not have additional ids');
        }
    }

    /**
     * Every resource must have a title property (cfg.schema.label).
     * 
     * @param DatasetNodeInterface $meta
     * @throws DoorkeeperException
     */
    static private function checkTitleProp(DatasetNodeInterface $meta): void {
        $titleProp = self::$schema->label;
        $res       = $meta->getNode();

        // check existing titles
        $titles = iterator_to_array($meta->listObjects(new PT($titleProp)));
        $langs  = [];
        foreach ($titles as $i) {
            $lang = $i instanceof LiteralInterface ? $i->getLang() : '';
            if (isset($langs[$lang])) {
                throw new DoorkeeperException("more than one $titleProp property");
            }
            if (empty((string) $i)) {
                throw new DoorkeeperException("$titleProp value is empty");
            }
            $langs[$lang] = (string) $i;
        }

        // preserve old titles when needed
        $mode = RC::getRequestParameter('metadataWriteMode');
        if ($mode === Metadata::SAVE_MERGE) {
            $query = RC::$pdo->prepare("
                SELECT value, lang 
                FROM metadata 
                WHERE property = ? AND id = ?
            ");
            $id    = preg_replace('|^.*/|', '', (string) $res);
            $query->execute([$titleProp, $id]);
            while ($i     = $query->fetchObject()) {
                if (!isset($langs[$i->lang])) {
                    $titles[] = '';
                    $meta->add(DF::quad($res, $titleProp, DF::literal($i->value, $i->lang)));
                }
            }
        }

        // if everything's fine, just return
        if (count($titles) > 0) {
            return;
        }

        // try to create a title if it's missing
        $searchProps = [
            [
                'https://vocabs.acdh.oeaw.ac.at/schema#hasFirstName',
                'https://vocabs.acdh.oeaw.ac.at/schema#hasLastName',
            ],
            [
                'http://xmlns.com/foaf/0.1/givenName',
                'http://xmlns.com/foaf/0.1/familyName',
            ],
            ['http://purl.org/dc/elements/1.1/title'],
            ['http://purl.org/dc/terms/title'],
            ['http://www.w3.org/2004/02/skos/core#prefLabel'],
            ['http://www.w3.org/2000/01/rdf-schema#label'],
            ['http://xmlns.com/foaf/0.1/name'],
        ];
        $langs       = [];
        foreach ($searchProps as $parts) {
            foreach ($parts as $prop) {
                foreach ($meta->listObjects(new PT($prop)) as $value) {
                    $lang         = $value instanceof LiteralInterface ? $value->getLang() : '';
                    $langs[$lang] = ($langs[$lang] ?? '') . ' ' . (string) $value;
                }
            }
        }

        $count = 0;
        foreach ($langs as $lang => $title) {
            $title = trim($title);
            if (!empty($title)) {
                RC::$log->info("\t\tsetting title to $title");
                $meta->add(DF::quad($res, $titleProp, DF::literal($title, (string) $lang)));
                $count++;
            }
        }
        if ($count === 0) {
            throw new DoorkeeperException("$titleProp is missing");
        }
    }

    /**
     * If a property is in the ontology namespace it has to be part of the ontology.
     * 
     * @param DatasetNodeInterface $meta
     * @return void
     */
    static private function checkUnknownProperties(DatasetNodeInterface $meta): void {
        if (RC::$config->doorkeeper->checkUnknownProperties === false) {
            return;
        }
        $idProp = self::$schema->id;
        $nmsp   = self::$schema->namespaces->ontology;
        foreach ($meta->listObjects(new PT($idProp))->getValues() as $i) {
            if (str_starts_with($i, $nmsp)) {
                return; // apply on non-ontology resources only
            }
        }
        foreach ($meta->listPredicates()->getValues() as $p) {
            if (str_starts_with($p, $nmsp) && self::$ontology->getProperty([], $p) === null) {
                throw new DoorkeeperException("property $p is in the ontology namespace but is not included in the ontology");
            }
        }
    }

    /**
     * isNewVersionOf can't create cycles so creation of the 2nd (and higher order)
     * isNewVersionOf links to a resource is forbidden.
     * 
     * As the doorkeeper can't lock resources in an effective way checking it is
     * possible only at the transaction commit.
     * 
     * @param PDO $pdo
     * @param int $txId
     * @param array<int> $resourceIds
     * @return void
     * @throws DoorkeeperException
     */
    static public function checkIsNewVersionOf(PDO $pdo, int $txId,
                                               array $resourceIds): void {
        $nvProp    = self::$schema->isNewVersionOf;
        $query     = "
            WITH err AS (
                SELECT target_id, string_agg(id::text, ', ' ORDER BY id) AS old
                FROM
                    relations rl1
                    JOIN resources r1 USING (id)
                WHERE
                    rl1.property = ?
                    AND r1.state = ?
                    AND EXISTS (
                        SELECT 1
                        FROM
                            resources r2
                            JOIN relations rl2 USING (id)
                            JOIN resources r3 ON rl2.target_id = r3.id
                        WHERE
                            rl2.property = ?
                            AND r2.transaction_id = ?
                            AND r2.state = ?
                            AND r3.state = ?
                            AND rl1.target_id = rl2.target_id
                    )

                GROUP BY 1
                HAVING count(*) > 1
            )
            SELECT string_agg(target_id || ' (' || old || ')', ', ' ORDER BY target_id)
            FROM err
        ";
        $param     = [
            $nvProp,
            Transaction::STATE_ACTIVE,
            $nvProp,
            $txId,
            Transaction::STATE_ACTIVE,
            Transaction::STATE_ACTIVE,
        ];
        $t         = microtime(true);
        $query     = $pdo->prepare($query);
        $query->execute($param);
        $conflicts = $query->fetchColumn();
        $t         = microtime(true) - $t;
        RC::$log->debug("\t\tcheckIsNewVersionOf performed in $t s");
        if ($conflicts !== null) {
            throw new DoorkeeperException("More than one $nvProp pointing to some resources: $conflicts");
        }
    }

    /**
     * 
     * @param PDO $pdo
     * @param int $txId
     * @param array<int> $resourceIds
     * @return void
     * @throws DoorkeeperException
     */
    static private function checkAutoCreatedResources(PDO $pdo, int $txId,
                                                      array $resourceIds): void {
        if (RC::$config->doorkeeper->checkAutoCreatedResources === false) {
            return;
        }

        // find all properties which range has been checked
        // all resources pointed with this properties are excluded from
        // the no metadata check
        $validRanges = array_keys((array) RC::$config->doorkeeper->checkRanges);
        $validProps  = [Transaction::STATE_ACTIVE, $txId];
        foreach (self::$ontology->getProperties() as $prop) {
            if (count(array_intersect($prop->range, $validRanges))) {
                $validProps = array_merge($validProps, $prop->property);
            }
        }
        $plh = substr(str_repeat(', ?', count($validProps) - 2), 2);
        if ($plh === '') {
            $validQuery = "SELECT 1::bigint AS id WHERE false";
            $validProps = [];
        } else {
            $validQuery = "
                SELECT DISTINCT target_id AS id
                FROM
                    resources r
                    JOIN relations rl ON rl.target_id = r.id
                WHERE
                    state = ?
                    AND transaction_id = ?
                    AND rl.property in ($plh)
            ";
        }

        $query      = "
            WITH valid AS ($validQuery)
            SELECT string_agg(ids, ', ' ORDER BY ids) AS invalid
            FROM
                resources r
                JOIN identifiers USING (id)
            WHERE
                state = ?
                AND transaction_id = ?
                AND NOT EXISTS (SELECT 1 FROM metadata WHERE id = r.id AND property = ?)
                AND NOT EXISTS (SELECT 1 FROM valid WHERE id = r.id)
        ";
        $param      = array_merge(
            $validProps,
            [Transaction::STATE_ACTIVE, $txId, self::$schema->label]
        );
        $t          = microtime(true);
        $query      = $pdo->prepare($query);
        $query->execute($param);
        $invalidRes = $query->fetchColumn();
        $t          = microtime(true) - $t;
        RC::$log->debug("\t\tcheckAutoCreatedResources performed in $t s");
        if (!empty($invalidRes)) {
            throw new DoorkeeperException("Transaction created resources without any metadata: $invalidRes");
        }
    }

    /**
     * 
     * @param PDO $pdo
     * @param int $txId
     * @param array<int> $resourceIds
     * @return void
     * @throws DoorkeeperException
     */
    static private function updateCollections(PDO $pdo, int $txId,
                                              array $resourceIds): void {
        RC::$log->info("\t\tupdating collections affected by the transaction");

        $t0     = microtime(true);
        $query  = $pdo->prepare("
            SELECT id 
            FROM resources 
            WHERE transaction_id = ?
        ");
        $query->execute([$txId]);
        $resIds = $query->fetchAll(PDO::FETCH_COLUMN);

        // find all pre-transaction parents of resources affected by the current transaction
        if (count($resIds) > 0) {
            $pdoOld    = RC::$transaction->getPreTransactionDbHandle();
            $query     = "
            WITH RECURSIVE t(id, n) AS (
                SELECT * FROM (VALUES %ids%) t1
              UNION
                SELECT target_id, 1
                FROM t JOIN relations USING (id)
                WHERE property = ?
            )
            SELECT DISTINCT id FROM t WHERE n > 0
        ";
            $query     = str_replace('%ids%', substr(str_repeat('(?::bigint, 0), ', count($resIds)), 0, -2), $query);
            $query     = $pdoOld->prepare($query);
            $query->execute(array_merge($resIds, [self::$schema->parent]));
            $parentIds = $query->fetchAll(PDO::FETCH_COLUMN);
        } else {
            $parentIds = [];
        }

        // find all affected parents (gr, pp) and their children
        $query = "
            CREATE TEMPORARY TABLE _resources AS
            SELECT p.id AS cid, (get_relatives(p.id, ?, 999999, 0)).*
            FROM (
                SELECT DISTINCT gr.id
                FROM resources r, LATERAL get_relatives(r.id, ?, 0) gr
                WHERE r.transaction_id = ?
              " . (count($parentIds) > 0 ? "UNION SELECT * FROM (VALUES %ids%) pp" : "") . "
            ) p
        ";
        $query = str_replace('%ids%', substr(str_repeat('(?::bigint), ', count($parentIds)), 0, -2), $query);
        $param = array_merge(
            [self::$schema->parent, self::$schema->parent, $txId],
            $parentIds,
        );
        $query = $pdo->prepare($query);
        $query->execute($param);
        $t1    = microtime(true);

        // try to lock resources to be updated
        // TODO - how to lock them in a way which doesn't cause a deadlock
//        $query  = $pdo->prepare("
//            UPDATE resources SET transaction_id = ? 
//            WHERE id IN (SELECT DISTINCT cid FROM _resources) AND transaction_id IS NULL
//        ");
//        $query->execute([$txId]);
//        $query  = $pdo->prepare("
//            SELECT 
//                count(*) AS all, 
//                coalesce(sum((transaction_id = ?)::int), 0) AS locked
//            FROM 
//                (SELECT DISTINCT cid AS id FROM _resources) t
//                JOIN resources r USING (id)
//        ");
//        $query->execute([$txId]);
//        $result = $query->fetch(PDO::FETCH_OBJ);
//        if ($result !== false && $result->all !== $result->locked) {
//            $msg = "Some resources locked by another transaction (" . ($result->all - $result->locked) . " out of " . $result->all . ")";
//            throw new DoorkeeperException($msg, 409);
//        }
        // perform the actual metadata update
        self::updateCollectionSize($pdo);
        $t2 = microtime(true);
        self::updateCollectionAggregates($pdo);
        $t3 = microtime(true);

        $query = $pdo->query("
            SELECT json_agg(c.cid) FROM (SELECT DISTINCT cid FROM _resources) c
        ");
        RC::$log->debug("\t\t\tupdated resources: " . $query->fetchColumn());
        RC::$log->debug("\t\t\ttiming: init " . ($t1 - $t0) . " size " . ($t2 - $t1) . " aggregates " . ($t3 - $t2));
    }

    static private function updateCollectionSize(PDO $pdo): void {
        $sizeProp      = self::$schema->binarySize;
        $acdhSizeProp  = self::$schema->binarySizeCumulative;
        $acdhCountProp = self::$schema->countCumulative;
        $collClass     = self::$schema->classes->collection;
        $topCollClass  = self::$schema->classes->topCollection;

        // compute children size and count
        $query = "
            CREATE TEMPORARY TABLE _collsizeupdate AS
            SELECT id, size, count, binres, value IS NOT NULL AS collection
            FROM
                (
                    SELECT 
                        r.cid AS id, 
                        coalesce(sum(CASE ra.state = ? WHEN true THEN chm.value_n ELSE 0 END), 0) AS size, 
                        greatest(sum((ra.state = ?)::int) - 1, 0) AS count,
                        coalesce(bool_and(r.cid = chm.id), false) AS binres
                    FROM
                        _resources r
                        LEFT JOIN resources ra USING (id)
                        LEFT JOIN metadata chm ON chm.id = r.id AND chm.property = ?
                    GROUP BY 1
                ) s
                LEFT JOIN (
                    SELECT * FROM metadata WHERE property = ? AND substring(value, 1, 1000) IN (?, ?)
                ) m USING (id)
        ";
        $param = [
            Res::STATE_ACTIVE, Res::STATE_ACTIVE, // case in main select
            $sizeProp, RDF::RDF_TYPE, $collClass, $topCollClass  // chm, m, m, m
        ];
        $query = $pdo->prepare($query);
        $query->execute($param);

        // remove old values
        $query = $pdo->prepare("
            DELETE FROM metadata WHERE id IN (SELECT id FROM _collsizeupdate) AND property IN (?, ?)
        ");
        $query->execute([$acdhSizeProp, $acdhCountProp]);
        // insert new values
        $query = $pdo->prepare("
            INSERT INTO metadata (id, property, type, lang, value_n, value)
                SELECT id, ?, ?, '', size, size 
                FROM _collsizeupdate JOIN resources USING (id) 
                WHERE state = ? AND (collection OR binres)
              UNION
                SELECT id, ?, ?, '', count, count 
                FROM _collsizeupdate JOIN resources USING (id) 
                WHERE state = ? AND (collection)
        ");
        $query->execute([
            $acdhSizeProp, RDF::XSD_DECIMAL, Res::STATE_ACTIVE,
            $acdhCountProp, RDF::XSD_DECIMAL, Res::STATE_ACTIVE
        ]);
    }

    static private function updateCollectionAggregates(PDO $pdo): void {
        $accessProp     = self::$schema->accessRestriction;
        $accessAggProp  = self::$schema->accessRestrictionAgg;
        $licenseProp    = self::$schema->license;
        $licenseAggProp = self::$schema->licenseAgg;
        $labelProp      = self::$schema->label;
        $collClass      = self::$schema->classes->collection;
        $topCollClass   = self::$schema->classes->topCollection;

        // compute aggregates
        $query = "
            CREATE TEMPORARY TABLE _aggupdate AS
            WITH activecol AS (
                SELECT r.cid
                FROM
                    _resources r
                    JOIN resources r2 ON r.cid = r2.id AND r2.state = ?
                    JOIN metadata m1 ON r.cid = m1.id AND m1.property = ? AND substring(m1.value, 1, 1000) IN (?, ?)
                WHERE r.cid = r.id
            )
                SELECT 
                    id, ?::text AS property, lang, 
                    string_agg(value || ' ' || count, E'\\n' ORDER BY count DESC, value) AS value
                FROM (
                    SELECT cid AS id, rl.property, lang, value, count(*) AS count
                    FROM
                        _resources r
                        JOIN activecol USING (cid)
                        JOIN resources r1 ON r.id = r1.id AND r1.state = ?
                        JOIN relations rl ON r.id = rl.id AND rl.property = ?
                        JOIN metadata m ON rl.target_id = m.id AND m.property = ?
                    GROUP BY 1, 2, 3, 4
                ) a1
                GROUP BY 1, 2, 3
              UNION
                SELECT 
                    id, ?::text AS property, lang, 
                    string_agg(value || ' ' || count, E'\\n' ORDER BY count DESC, value) AS value
                FROM (
                    SELECT cid AS id, rl.property, lang, value, count(*) AS count
                    FROM
                        _resources r
                        JOIN activecol USING (cid)
                        JOIN resources r1 ON r.id = r1.id AND r1.state = ?
                        JOIN relations rl ON r.id = rl.id AND rl.property = ?
                        JOIN metadata m ON rl.target_id = m.id AND m.property = ?
                    GROUP BY 1, 2, 3, 4
                ) a2
                GROUP BY 1, 2, 3
        ";
        $param = [
            Res::STATE_ACTIVE, RDF::RDF_TYPE, $collClass, $topCollClass, // activecol
            $licenseAggProp, Res::STATE_ACTIVE, $licenseProp, $labelProp, // a1
            $accessAggProp, Res::STATE_ACTIVE, $accessProp, $labelProp, // a2
        ];
        $query = $pdo->prepare($query);
        $query->execute($param);
        RC::$log->info('[-----');
        RC::$log->info((string) json_encode($pdo->query("SELECT * FROM _resources")->fetchAll(PDO::FETCH_OBJ)));
        RC::$log->info((string) json_encode($pdo->query("SELECT * FROM _aggupdate")->fetchAll(PDO::FETCH_OBJ)));

        // add empty property values for empty collections
        $query = "
            INSERT INTO _aggupdate
            SELECT id, property, 'en', ''
            FROM 
                (VALUES (?), (?)) p (property),
                (
                    SELECT cid AS id
                    FROM
                        _resources r
                        JOIN resources r2 ON r.cid = r2.id AND r2.state = ?
                        JOIN metadata m1 ON r.cid = m1.id AND m1.property = ? AND substring(m1.value, 1, 1000) IN (?, ?)
                    WHERE r.cid = r.id
                ) ac
            WHERE NOT EXISTS (SELECT id, property FROM _aggupdate)
        ";
        $param = [$licenseAggProp, $accessAggProp, Res::STATE_ACTIVE, RDF::RDF_TYPE,
            $collClass, $topCollClass];
        $query = $pdo->prepare($query);
        $query->execute($param);
        RC::$log->info((string) json_encode($pdo->query("SELECT * FROM _aggupdate")->fetchAll(PDO::FETCH_OBJ)));

        // remove old values
        $query = $pdo->prepare("
            DELETE FROM metadata WHERE id IN (SELECT id FROM _collsizeupdate) AND property IN (?, ?)
        ");
        $query->execute([$licenseAggProp, $accessAggProp]);
        // insert new values
        $query = $pdo->prepare("
            INSERT INTO metadata (id, property, type, lang, value)
            SELECT id, property, ?::text, lang, value FROM _aggupdate
        ");
        $query->execute([RDF::XSD_STRING]);
    }

    static private function loadOntology(): void {
        if (!isset(self::$ontology)) {
            $cacheFile      = RC::$config->doorkeeper->ontologyCacheFile ?? '';
            $cacheTtl       = RC::$config->doorkeeper->ontologyCacheTtl ?? 600;
            self::$ontology = Ontology::factoryDb(RC::$pdo, self::$schema, $cacheFile, $cacheTtl);
        }
    }

    static private function castLiteral(LiteralInterface $l, string $range): LiteralInterface {
        $numValue = null;
        switch ($range) {
            case RDF::XSD_ANY_URI:
                $value = $l->withDatatype(RDF::XSD_ANY_URI);
                break;
            case RDF::XSD_DATE:
            case RDF::XSD_DATE_TIME:
                $l     = (string) $l;
                if (is_numeric($l)) {
                    $l .= '-01-01';
                }
                try {
                    $ldt = new DateTime($l);
                } catch (Exception $e) {
                    throw new DoorkeeperException("value does not match data type: $l ($range)");
                }
                if ($range === RDF::XSD_DATE) {
                    $value = DF::literal($ldt->format('Y-m-d'), null, $range);
                } else {
                    $value = DF::literal($l, null, $range);
                }
                break;
            case RDF::XSD_DECIMAL:
            case RDF::XSD_FLOAT:
            case RDF::XSD_DOUBLE:
                $l = (string) $l;
                if (!is_numeric($l)) {
                    throw new DoorkeeperException("value does not match data type: $l ($range)");
                }
                $numValue = (float) $l;
                $value    = DF::literal($numValue, null, $range);
                break;
            case RDF::XSD_INTEGER:
            case RDF::XSD_NEGATIVE_INTEGER:
            case RDF::XSD_NON_NEGATIVE_INTEGER:
            case RDF::XSD_NON_POSITIVE_INTEGER:
            case RDF::XSD_POSITIVE_INTEGER:
            case RDF::XSD_LONG:
            case RDF::XSD_INT:
            case RDF::XSD_SHORT:
            case RDF::XSD_BYTE:
            case RDF::XSD_UNSIGNED_LONG:
            case RDF::XSD_UNSIGNED_INT:
            case RDF::XSD_UNSIGNED_SHORT:
            case RDF::XSD_UNSIGNED_BYTE:
                $l        = (string) $l;
                if (!is_numeric($l)) {
                    throw new DoorkeeperException("value does not match data type: $l ($range)");
                }
                $numValue = (int) $l;
                $value    = DF::literal($numValue, null, $range);
                break;
            case RDF::XSD_BOOLEAN:
                $value    = DF::literal((string) ((bool) $l), null, $range);
                break;
            default:
                throw new RuntimeException('unknown range data type: ' . $range);
        }
        if ($numValue !== null) {
            $c1 = in_array($range, self::NON_NEGATIVE_NUMBERS) && $numValue < 0.0;
            $c2 = $range == RDF::XSD_NON_POSITIVE_INTEGER && $numValue > 0.0;
            $c3 = $range == RDF::XSD_NEGATIVE_INTEGER && $numValue >= 0.0;
            $c4 = $range == RDF::XSD_POSITIVE_INTEGER && $numValue <= 0.0;
            if ($c1 || $c2 || $c3 || $c4) {
                throw new DoorkeeperException('value does not match data type: ' . $value->getValue() . ' (' . $range . ')');
            }
        }
        return $value;
    }
}
