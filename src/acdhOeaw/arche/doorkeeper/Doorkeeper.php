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
use PDOStatement;
use RuntimeException;
use EasyRdf\Literal;
use EasyRdf\Resource;
use EasyRdf\Literal\Boolean as lBoolean;
use EasyRdf\Literal\Date as lDate;
use EasyRdf\Literal\DateTime as lDateTime;
use EasyRdf\Literal\Decimal as lDecimal;
use EasyRdf\Literal\Integer as lInteger;
use RenanBr\BibTexParser\Listener as BiblatexL;
use RenanBr\BibTexParser\Parser as BiblatexP;
use RenanBr\BibTexParser\Exception\ParserException as BiblatexE1;
use RenanBr\BibTexParser\Exception\ProcessorException as BiblatexE2;
use acdhOeaw\UriNormalizer;
use acdhOeaw\epicHandle\HandleService;
use acdhOeaw\arche\core\Transaction;
use acdhOeaw\arche\core\Resource as Res;
use acdhOeaw\arche\core\RestController as RC;
use acdhOeaw\arche\lib\schema\Ontology;
use zozlak\RdfConstants as RDF;

/**
 * Description of Doorkeeper
 *
 * @author zozlak
 */
class Doorkeeper {

    const NON_NEGATIVE_NUMBERS = [RDF::XSD_NON_NEGATIVE_INTEGER, RDF::XSD_UNSIGNED_LONG,
        RDF::XSD_UNSIGNED_INT, RDF::XSD_UNSIGNED_SHORT, RDF::XSD_UNSIGNED_BYTE];
    const LITERAL_TYPES        = [RDF::XSD_ANY_URI,
        RDF::XSD_DATE, RDF::XSD_DATE_TIME, RDF::XSD_DECIMAL,
        RDF::XSD_FLOAT, RDF::XSD_DOUBLE, RDF::XSD_INTEGER, RDF::XSD_NEGATIVE_INTEGER,
        RDF::XSD_NON_NEGATIVE_INTEGER, RDF::XSD_NON_POSITIVE_INTEGER, RDF::XSD_POSITIVE_INTEGER,
        RDF::XSD_LONG, RDF::XSD_INT, RDF::XSD_SHORT, RDF::XSD_BYTE, RDF::XSD_UNSIGNED_LONG,
        RDF::XSD_UNSIGNED_INT, RDF::XSD_UNSIGNED_SHORT, RDF::XSD_UNSIGNED_BYTE, RDF::XSD_BOOLEAN];

    static private Ontology $ontology;
    static private UriNormalizer $uriNorm;

    static public function onResEdit(int $id, Resource $meta, ?string $path): Resource {
        self::loadOntology();
        $pdo       = new PDO(RC::$config->dbConn->admin);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $pdo->beginTransaction();
        $errors    = [];
        // checkTitleProp before checkCardinalities!
        $functions = [
            'maintainPid', 'maintainCmdiPid', 'maintainDefaultValues',
            'maintainAccessRights', 'maintainPropertyRange',
            'normalizeIds', 'checkTitleProp', 'checkPropertyTypes', 'checkCardinalities',
            'checkIdCount', 'checkLanguage', 'checkUnknownProperties', 'checkBiblatex'
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
        // current state database handler
        $pdo = new PDO(RC::$config->dbConn->admin);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $pdo->beginTransaction();

        $errors    = [];
        $functions = ['checkAutoCreatedResources'];
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

    static private function maintainPid(Resource $meta): void {
        $cfg     = RC::$config->doorkeeper->epicPid;
        $idProp  = RC::$config->schema->id;
        $idNmsp  = RC::$config->schema->namespaces->id;
        $pidProp = RC::$config->schema->pid;
        $ids     = self::toString($meta->allResources($idProp));

        $nIdNmsp    = strlen($idNmsp);
        $idSubNmsps = [];
        foreach (RC::$config->schema->namespaces as $i) {
            if (substr($i, 0, $nIdNmsp) === $idNmsp && $i !== $idNmsp) {
                $idSubNmsps[] = $i;
            }
        }

        $curPid = null;
        $id     = null;
        foreach ($ids as $i) {
            if (strpos($i, $cfg->resolver) === 0) {
                $curPid = $i;
            }
            if (substr($i, 0, $nIdNmsp) === $idNmsp) {
                $flag = true;
                foreach ($idSubNmsps as $j) {
                    $flag &= substr($i, 0, strlen($j)) !== $j;
                }
                if ($flag) {
                    $id = $i;
                }
            }
        }
        $pidLit = $meta->getLiteral($pidProp);
        $pidLit = $pidLit !== null ? (string) $pidLit : null;
        if ($pidLit === $cfg->createValue && $id !== null) {
            if (empty($cfg->pswd)) {
                RC::$log->info("\t\tskipping PID (re)generation - no EPIC password provided");
                return;
            }
            $ps = new HandleService($cfg->url, $cfg->prefix, $cfg->user, $cfg->pswd);
            if ($curPid === null) {
                $meta->delete($pidProp);
                $pid = $ps->create($id);
                $pid = str_replace($cfg->url, $cfg->resolver, $pid);
                RC::$log->info("\t\tregistered PID $pid pointing to " . $id);
                $meta->addLiteral($pidProp, new Literal($pid, null, RDF::XSD_ANY_URI));
            } else {
                $meta->delete($pidProp);
                $meta->addLiteral($pidProp, new Literal($curPid, null, RDF::XSD_ANY_URI));
                $pid = str_replace($cfg->resolver, $cfg->url, $curPid);
                $ret = $ps->update($pid, $id);
                RC::$log->info("\t\trecreated PID $pid pointing to " . $id . " with return code " . $ret);
            }
        }
        // promote PIDs to IDs
        foreach ($meta->all($pidProp) as $i) {
            $i = (string) $i;
            if ($i === '') {
                throw new DoorkeeperException("Empty PID");
            }
            if ($i !== $cfg->createValue && !in_array($i, $ids)) {
                RC::$log->info("\t\tpromoting PID $i to an id");
                $meta->addResource($idProp, $i);
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
     * @param Resource $meta
     * @param \PDO $pdo
     * @return void
     * @throws DoorkeeperException
     */
    static private function maintainCmdiPid(Resource $meta, PDO $pdo): void {
        $cfg     = RC::$config->doorkeeper->epicPid;
        $pidProp = RC::$config->schema->cmdiPid;
        $pidNmsp = RC::$config->schema->namespaces->cmdi;
        $setProp = $cfg->clarinSetProperty;
        $idProp  = RC::$config->schema->id;
        $idNmsp  = RC::getBaseUrl();
        if ($meta->getLiteral($pidProp) !== null || $meta->getResource($setProp) === null) {
            // CMDI PID exists or OAI-PMH set property doesn't exist - nothing to do
            return;
        }

        $query      = $pdo->prepare("SELECT i1.ids FROM identifiers i1 JOIN identifiers i2 USING (id) WHERE i2.ids = ?");
        $query->execute([$cfg->clarinSet]);
        $clarinSets = $query->fetchAll(PDO::FETCH_COLUMN);

        $clarin = false;
        foreach ($meta->allResources($setProp) as $set) {
            if (in_array((string) $set, $clarinSets)) {
                $clarin = true;
                break;
            }
        }
        if ($clarin) {
            if (empty($cfg->pswd)) {
                RC::$log->info("\t\tskipping CMDI PID generation - no EPIC password provided");
                return;
            }
            $id = null;
            foreach ($meta->allResources($idProp) as $i) {
                if (strpos($i, $idNmsp) === 0) {
                    $id = $pidNmsp . substr((string) $i, strlen($idNmsp));
                    break;
                }
            }
            if (!empty($id)) {
                $ps  = new HandleService($cfg->url, $cfg->prefix, $cfg->user, $cfg->pswd);
                $pid = $ps->create($id);
                $pid = str_replace($cfg->url, $cfg->resolver, $pid);
                RC::$log->info("\t\tregistered CMDI PID $pid pointing to " . $id);
                $meta->addLiteral($pidProp, new Literal($pid, null, RDF::XSD_ANY_URI));
                $meta->addResource(RC::$config->schema->id, $id);
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
     * @param \EasyRdf\Resource $meta repository resource's metadata
     * @param \PDO $pdo
     */
    static public function maintainAccessRights(Resource $meta, PDO $pdo): void {
        $accessRestr = (string) $meta->getResource(RC::$config->schema->accessRestriction);
        if (empty($accessRestr)) {
            return;
        }

        $propRead     = RC::$config->accessControl->schema->read;
        $propRoles    = RC::$config->schema->accessRole;
        $rolePublic   = RC::$config->doorkeeper->rolePublic;
        $roleAcademic = RC::$config->doorkeeper->roleAcademic;

        $query          = $pdo->prepare("SELECT i1.ids FROM identifiers i1 JOIN identifiers i2 USING (id) WHERE i2.ids = ?");
        $query->execute([$accessRestr]);
        $accessRestrIds = $query->fetchAll(PDO::FETCH_COLUMN);

        RC::$log->info("\t\tmaintaining access rights for " . implode(', ', $accessRestrIds));

        if (in_array('https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/public', $accessRestrIds)) {
            $meta->addLiteral($propRead, $rolePublic);
            RC::$log->info("\t\t\tpublic");
        } else {
            $meta->delete($propRead, $rolePublic);
            if (in_array('https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/academic', $accessRestrIds)) {
                $meta->addLiteral($propRead, $roleAcademic);
                RC::$log->info("\t\t\tacademic");
            } else {
                $meta->delete($propRead, $roleAcademic);
                foreach ($meta->all($propRoles) as $role) {
                    $meta->addLiteral($propRead, $role);
                }
                RC::$log->info("\t\t\trestricted");
            }
        }
    }

    static private function maintainPropertyRange(Resource $meta): void {
        foreach ($meta->propertyUris() as $prop) {
            $propDesc = self::$ontology->getProperty($meta, $prop);
            if ($propDesc === null || !is_array($propDesc->range) || count($propDesc->range) === 0) {
                continue;
            } else {
                $range = $propDesc->range;
            }
            foreach ($meta->allLiterals($prop) as $l) {
                /* @var $l \EasyRdf\Literal */
                $type = $l->getDatatypeUri() ?? RDF::XSD_STRING;
                if (in_array($type, $range)) {
                    continue;
                }
                if (in_array(RDF::XSD_STRING, $range)) {
                    $meta->delete($prop, $l);
                    $meta->addLiteral($prop, (string) $l);
                    RC::$log->info("\t\tcasting $prop value from $type to string");
                } else {
                    try {
                        $rangeTmp = array_intersect($range, self::LITERAL_TYPES);
                        $rangeTmp = reset($rangeTmp) ?? reset($range);
                        $value    = self::castLiteral($l, $rangeTmp);
                        $meta->delete($prop, $l);
                        $meta->addLiteral($prop, $value);
                        RC::$log->info("\t\tcasting $prop value from $type to $rangeTmp");
                    } catch (RuntimeException $ex) {
                        RC::$log->info('    ' . $ex->getMessage());
                    } catch (DoorkeeperException $ex) {
                        throw new DoorkeeperException('property ' . $prop . ': ' . $ex->getMessage(), $ex->getCode(), $ex);
                    }
                }
            }
        }
    }

    static private function maintainDefaultValues(Resource $meta): void {
        foreach ($meta->allResources(RDF::RDF_TYPE) as $class) {
            $c = self::$ontology->getClass($class);
            if ($c === null) {
                continue;
            }
            foreach ($c->properties as $p) {
                if (!empty($p->defaultValue) && $meta->get($p->uri) === null) {
                    switch ($p->type) {
                        case RDF::OWL_OBJECT_PROPERTY:
                            $val = new Resource($p->defaultValue);
                            break;
                        case RDF::OWL_DATATYPE_PROPERTY:
                            $val = new Literal($p->defaultValue, $p->langTag ? 'en' : null, $p->range);
                            break;
                    }
                    $meta->add($p->uri, $val ?? null);
                    RC::$log->info("\t\t$p->uri added with a default value of $p->defaultValue");
                }
            }
        }
    }

    static private function normalizeIds(Resource $meta): void {
        if (!isset(self::$uriNorm)) {
            self::$uriNorm = UriNormalizer::factory();
        }

        $idProp = RC::$config->schema->id;
        foreach ($meta->allResources($idProp) as $id) {
            $ids = (string) $id;
            $std = self::$uriNorm->normalize($id);
            if ($std !== (string) $id) {
                $meta->deleteResource($idProp, $ids);
                $meta->addResource($idProp, $std);
                RC::$log->info("\t\tid URI $ids standardized to $std");
            }
        }
    }

    static private function checkLanguage(Resource $meta): void {
        foreach ($meta->propertyUris() as $prop) {
            $p = self::$ontology->getProperty([], $prop);
            if (is_object($p) && $p->langTag) {
                foreach ($meta->allLiterals($prop) as $value) {
                    /* @var $value \EasyRdf\Literal */
                    if (empty($value->getLang())) {
                        throw new DoorkeeperException("Property $prop with value " . (string) $value . " is not tagged with a language");
                    }
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
     * @param \EasyRdf\Resource $meta
     * @throws DoorkeeperException
     */
    static private function checkPropertyTypes(Resource $meta): void {
        foreach ($meta->propertyUris() as $p) {
            $pDef = self::$ontology->getProperty([], $p);
            if (is_object($pDef)) {
                if ($pDef->type === RDF::OWL_DATATYPE_PROPERTY && $meta->getResource($p) !== null) {
                    throw new DoorkeeperException('URI value for a datatype property ' . $p);
                }
                if ($pDef->type === RDF::OWL_OBJECT_PROPERTY && $meta->getLiteral($p) !== null) {
                    throw new DoorkeeperException('Literal value for an object property ' . $p);
                }
            }
        }
    }

    /**
     * Checks property cardinalities according to the ontology.
     * 
     * @param \EasyRdf\Resource $meta
     * @throws DoorkeeperException
     */
    static private function checkCardinalities(Resource $meta): void {
        $ontologyNmsp = RC::$config->schema->namespaces->ontology;
        $inDomain     = [RDF::RDF_TYPE];
        foreach ($meta->allResources(RDF::RDF_TYPE) as $class) {
            $classDef = self::$ontology->getClass((string) $class);
            if ($classDef === null) {
                continue;
            }
            $inNmsp = strpos($class, $ontologyNmsp) === 0;
            foreach ($classDef->properties as $p) {
                if ($inNmsp) {
                    // check property domains only for resources of ACDH classes
                    $inDomain = array_merge($inDomain, $p->property);
                }
                if (($p->min > 0 || $p->max !== null) && $p->automatedFill === false) {
                    $co  = $cd  = 0;
                    $cdl = ['' => 0];
                    foreach ($p->property as $i) {
                        foreach ($meta->all($i) as $j) {
                            if ($j instanceof Literal) {
                                $cd++;
                                $lang       = $j->getLang() ?? '';
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
            $outDomain = array_diff($meta->propertyUris(), $inDomain); // properties allowed on resource classes
            $owlThing  = self::$ontology->getClass(RDF::OWL_THING);
            $outDomain = array_diff($outDomain, array_keys($owlThing->properties)); // properties allowed on all resources
            if (count($outDomain) > 0) {
                throw new DoorkeeperException("Properties with a wrong domain: " . implode(', ', $outDomain));
            }
        }
    }

    static private function checkBiblatex(Resource $meta): void {
        $biblatexProp = RC::$config->schema->biblatex ?? 'foo';
        $biblatex     = $meta->getLiteral($biblatexProp);
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
     * @param \EasyRdf\Resource $meta
     * @throws DoorkeeperException
     */
    static private function checkIdCount(Resource $meta): void {
        $idProp       = RC::$config->schema->id;
        $repoNmsp     = RC::getBaseUrl();
        $ontologyNmsp = RC::$config->schema->namespaces->ontology;

        $ontologyIdCount = $repoIdCount     = $nonRepoIdCount  = 0;
        $ids             = $meta->allResources($idProp);
        foreach ($ids as $id) {
            $id           = (string) $id;
            $ontologyFlag = strpos($id, $ontologyNmsp) === 0;
            $repoFlag     = strpos($id, $repoNmsp) === 0;

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
     * @param \EasyRdf\Resource $meta
     * @throws DoorkeeperException
     */
    static private function checkTitleProp(Resource $meta): void {
        $titleProp = RC::$config->schema->label;

        // check existing titles
        $titles = $meta->allLiterals($titleProp);
        $langs  = [];
        foreach ($titles as $i) {
            $lang = $i->getLang();
            if (isset($langs[$lang])) {
                throw new DoorkeeperException("more than one $titleProp property");
            }
            if (empty((string) $i)) {
                throw new DoorkeeperException("$titleProp value is empty");
            }
            $langs[$lang] = (string) $i;
        }
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
                foreach ($meta->allLiterals($prop) as $value) {
                    $lang         = $value->getLang();
                    $langs[$lang] = ($langs[$lang] ?? '') . ' ' . (string) $value;
                }
            }
        }

        $count = 0;
        foreach ($langs as $lang => $title) {
            $title = trim($title);
            if (!empty($title)) {
                RC::$log->info("\t\tsetting title to $title");
                $meta->addLiteral($titleProp, $title, $lang);
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
     * @param Resource $meta
     * @return void
     */
    static private function checkUnknownProperties(Resource $meta): void {
        if (RC::$config->doorkeeper->checkUnknownProperties === false) {
            return;
        }
        $idProp = RC::$config->schema->id;
        $nmsp   = RC::$config->schema->namespaces->ontology;
        $n      = strlen($nmsp);
        foreach ($meta->allResources($idProp) as $i) {
            if (substr((string) $i, 0, $n) === $nmsp) {
                return; // apply on non-ontology resources only
            }
        }
        foreach ($meta->propertyUris() as $p) {
            if (substr($p, 0, $n) === $nmsp && self::$ontology->getProperty([], $p) === null) {
                throw new DoorkeeperException("property $p is in the ontology namespace but is not included in the ontology");
            }
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
        $query      = "
            SELECT string_agg(ids, ', ' ORDER BY ids) AS invalid
            FROM
                resources r
                JOIN identifiers USING (id)
            WHERE
                state = ?
                AND transaction_id = ?
                AND NOT EXISTS (SELECT 1 FROM metadata WHERE id = r.id AND property = ?)
        ";
        $param      = [Transaction::STATE_ACTIVE, $txId, RC::$config->schema->label];
        $query      = $pdo->prepare($query);
        $query->execute($param);
        $invalidRes = $query->fetchColumn();
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
        $prolongQuery = $pdo->prepare("UPDATE transactions SET last_request = clock_timestamp() WHERE transaction_id = ?");

        RC::$log->info("\t\tUpdating collections affected by the transaction");

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
            $query->execute(array_merge($resIds, [RC::$config->schema->parent]));
            $parentIds = $query->fetchAll(PDO::FETCH_COLUMN);
        } else {
            $parentIds = [];
        }
        $prolongQuery->execute([$txId]);

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
            [RC::$config->schema->parent, RC::$config->schema->parent, $txId],
            $parentIds,
        );
        $query = $pdo->prepare($query);
        $query->execute($param);
        $prolongQuery->execute([$txId]);

        // try to lock resources to be updated
        $query  = $pdo->prepare("
            UPDATE resources SET transaction_id = ? 
            WHERE id IN (SELECT DISTINCT cid FROM _resources) AND transaction_id IS NULL
        ");
        $query->execute([$txId]);
        $query  = $pdo->prepare("
            SELECT 
                count(*) AS all, 
                coalesce(sum((transaction_id = ?)::int), 0) AS locked
            FROM 
                (SELECT DISTINCT cid AS id FROM _resources) t
                JOIN resources r USING (id)
        ");
        $query->execute([$txId]);
        $result = $query->fetch(PDO::FETCH_OBJ);
        if ($result !== false && $result->all !== $result->locked) {
            $msg = "Some resources locked by another transaction (" . ($result->all - $result->locked) . " out of " . $result->all . ")";
            throw new DoorkeeperException($msg, 409);
        }
        $prolongQuery->execute([$txId]);

        // perform the actual metadata update
        self::updateCollectionSize($pdo, $prolongQuery, $txId);
        self::updateCollectionAggregates($pdo, $prolongQuery, $txId);

        $query = $pdo->query("
            SELECT json_agg(c.cid) FROM (SELECT DISTINCT cid FROM _resources) c
        ");
        RC::$log->debug("\t\t\tupdated resources: " . $query->fetchColumn());
    }

    static private function updateCollectionSize(PDO $pdo,
                                                 PDOStatement $prolongQuery,
                                                 int $txId): void {
        $sizeProp      = RC::$config->schema->binarySize;
        $acdhSizeProp  = RC::$config->schema->binarySizeCumulative;
        $acdhCountProp = RC::$config->schema->countCumulative;
        $collClass     = RC::$config->schema->classes->collection;
        $topCollClass  = RC::$config->schema->classes->topCollection;

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

        $prolongQuery->execute([$txId]);
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
                WHERE state = ? AND (collection OR count > 0 OR binres)
              UNION
                SELECT id, ?, ?, '', count, count 
                FROM _collsizeupdate JOIN resources USING (id) 
                WHERE state = ? AND (collection OR count > 0)
        ");
        $query->execute([
            $acdhSizeProp, RDF::XSD_DECIMAL, Res::STATE_ACTIVE,
            $acdhCountProp, RDF::XSD_DECIMAL, Res::STATE_ACTIVE
        ]);
    }

    static private function updateCollectionAggregates(PDO $pdo,
                                                       PDOStatement $prolongQuery,
                                                       int $txId): void {
        $accessProp     = RC::$config->schema->accessRestriction;
        $accessAggProp  = RC::$config->schema->accessRestrictionAgg;
        $licenseProp    = RC::$config->schema->license;
        $licenseAggProp = RC::$config->schema->licenseAgg;
        $labelProp      = RC::$config->schema->label;
        $collClass      = RC::$config->schema->classes->collection;
        $topCollClass   = RC::$config->schema->classes->topCollection;

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
        $prolongQuery->execute([$txId]);
        RC::$log->info('[-----');
        RC::$log->info(json_encode($pdo->query("SELECT * FROM _resources")->fetchAll(PDO::FETCH_OBJ)));
        RC::$log->info(json_encode($pdo->query("SELECT * FROM _aggupdate")->fetchAll(PDO::FETCH_OBJ)));

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
        RC::$log->info(json_encode($pdo->query("SELECT * FROM _aggupdate")->fetchAll(PDO::FETCH_OBJ)));
        $prolongQuery->execute([$txId]);

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
            $cfg            = (object) [
                    'ontologyNamespace' => RC::$config->schema->namespaces->ontology,
                    'parent'            => RC::$config->schema->parent,
                    'label'             => RC::$config->schema->label,
            ];
            self::$ontology = new Ontology(RC::$pdo, $cfg);
        }
    }

    /**
     * 
     * @param array<mixed> $a
     * @return array<string>
     */
    static private function toString(array $a): array {
        return array_map(function ($x) {
            return (string) $x;
        }, $a);
    }

    static private function castLiteral(Literal $l, string $range): Literal {
        switch ($range) {
            case RDF::XSD_ANY_URI:
                $value = new Literal((string) $l, null, RDF::XSD_ANY_URI);
                break;
            case RDF::XSD_DATE:
            case RDF::XSD_DATE_TIME:
                $l     = is_numeric((string) $l) ? $l . '-01-01' : (string) $l;
                try {
                    new DateTime($l);
                } catch (Exception $e) {
                    throw new DoorkeeperException("value does not match data type: $l ($range)");
                }
                $value = $range === RDF::XSD_DATE ? new lDate($l, null, $range) : new lDateTime($l, null, $range);
                break;
            case RDF::XSD_DECIMAL:
            case RDF::XSD_FLOAT:
            case RDF::XSD_DOUBLE:
                $l     = (string) $l;
                if (!is_numeric($l)) {
                    throw new DoorkeeperException("value does not match data type: $l ($range)");
                }
                $value = new lDecimal($l, null, $range);
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
                $l     = (string) $l;
                if (!is_numeric($l)) {
                    throw new DoorkeeperException("value does not match data type: $l ($range)");
                }
                $value = new lInteger((int) $l, null, $range);
                break;
            case RDF::XSD_BOOLEAN:
                $value = new lBoolean((string) ((bool) $l), null, $range);
                break;
            default:
                throw new RuntimeException('unknown range data type: ' . $range);
        }
        $c1 = in_array($range, self::NON_NEGATIVE_NUMBERS) && $value->getValue() < 0;
        $c2 = $range == RDF::XSD_NON_POSITIVE_INTEGER && $value->getValue() > 0;
        $c3 = $range == RDF::XSD_NEGATIVE_INTEGER && $value->getValue() >= 0;
        $c4 = $range == RDF::XSD_POSITIVE_INTEGER && $value->getValue() <= 0;
        if ($c1 || $c2 || $c3 || $c4) {
            throw new DoorkeeperException('value does not match data type: ' . $value->getValue() . ' (' . $range . ')');
        }
        return $value;
    }
}
