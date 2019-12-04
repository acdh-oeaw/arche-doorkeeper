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

use PDO;
use RuntimeException;
use EasyRdf\Literal;
use EasyRdf\Resource;
use EasyRdf\Literal\Boolean as lBoolean;
use EasyRdf\Literal\Date as lDate;
use EasyRdf\Literal\DateTime as lDateTime;
use EasyRdf\Literal\Decimal as lDecimal;
use EasyRdf\Literal\Integer as lInteger;
use acdhOeaw\UriNormalizer;
use acdhOeaw\epicHandle\HandleService;
use acdhOeaw\acdhRepo\Resource as Res;
use acdhOeaw\acdhRepo\RestController as RC;
use zozlak\RdfConstants as RDF;

/**
 * Description of Doorkeeper
 *
 * @author zozlak
 */
class Doorkeeper {

    const NON_NEGATIVE_NUMBERS = [RDF::XSD_NON_NEGATIVE_INTEGER, RDF::XSD_UNSIGNED_LONG,
        RDF::XSD_UNSIGNED_INT, RDF::XSD_UNSIGNED_SHORT, RDF::XSD_UNSIGNED_BYTE];

    /**
     *
     * @var \acdhOeaw\arche\Ontology
     */
    static private $ontology;

    /**
     *
     * @var \acdhOeaw\UriNormalizer
     */
    static private $uriNorm;

    static public function onResEdit(int $id, Resource $meta, ?string $path): Resource {
        self::maintainPid($meta);

        self::loadOntology();
        if (self::$ontology->isA($meta, RC::$config->schema->classes->repoObject)) {
            self::maintainHosting($meta);
            self::maintainBinarySize($meta);
        }
        self::maintainAccessRestriction($meta);
        self::maintainAccessRights($meta);
        self::maintainPropertyRange($meta);
        self::normalizeIds($meta);
        self::checkCardinalities($meta);
        self::checkIdCount($meta);
        self::checkTitleProp($meta);
        self::checkLanguage($meta);

        return $meta;
    }

    static public function onTxCommit(string $method, int $txId,
                                      array $resourceIds): void {
        // current state database handler
        $pdo = new PDO(RC::$config->dbConnStr->admin);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $pdo->beginTransaction();

        self::updateCollectionExtent($pdo, $txId, $resourceIds);

        $pdo->commit();
    }

    static private function maintainPid(Resource $meta): void {
        $cfg     = RC::$config->doorkeeper->epicPid;
        $idProp  = RC::$config->schema->id;
        $pidProp = RC::$config->schema->acdh->pid;
        $curPid  = null;
        foreach ($meta->allResources($idProp) as $i) {
            if (strpos((string) $i, $cfg->resolver) === 0) {
                $curPid = (string) $i;
                break;
            }
        }
        if ($meta->getLiteral($pidProp)) {
            if ($curPid === null) {
                if (empty($cfg->pswd)) {
                    RC::$log->info("\t\tskipping PID generation - no EPIC password provided");
                    return;
                }
                $meta->delete($pidProp);
                $ps  = new HandleService($cfg->url, $cfg->prefix, $cfg->user, $cfg->pswd);
                $pid = $ps->create($meta->getUri());
                $pid = str_replace($c->url, $cfg->resolver, $pid);
                RC::$log->info("\t\tregistered PID $pid pointing to " . $meta->getUri());
                $meta->addResource($pidProp, $pid);
            } else {
                $meta->delete($pidProp);
                $meta->addResource($pidProp, $curPid);
                RC::$log->info("\t\trecreating PID $curPid pointing to " . $meta->getUri());
            }
        }
        // promote PIDs to IDs
        $ids = self::toString($meta->allResources($idProp));
        foreach ($meta->allResources($pidProp) as $i) {
            $i = (string) $i;
            if (!in_array($i, $ids)) {
                RC::$log->info("\t\tpromoting PID $i to an id");
                $meta->addResource($idProp, $i);
            }
        }
    }

    static private function maintainHosting(Resource $meta): void {
        $prop  = RC::$config->schema->acdh->hosting;
        $value = $meta->getResource($prop);
        if ($value === null) {
            $meta->addResource($prop, RC::$config->doorkeeper->default->hosting);
            RC::$log->info("\t\t$prop added");
        }
    }

    static private function maintainBinarySize(Resource $meta): void {
        $prop = RC::$config->schema->acdh->binarySize;
        $meta->delete($prop);
        $meta->add($prop, $meta->getLiteral(RC::$config->schema->binarySize));
    }

    static private function maintainAccessRestriction(Resource $meta): void {
        $isRepoObj   = self::$ontology->isA($meta, RC::$config->schema->classes->repoObject);
        $isSharedObj = self::$ontology->isA($meta, RC::$config->schema->classes->sharedObject);
        $isContainer = self::$ontology->isA($meta, RC::$config->schema->classes->container);
        if (!$isRepoObj && !$isSharedObj && !$isContainer) {
            return;
        }

        $prop      = RC::$config->schema->acdh->accessRestriction;
        $resources = $meta->allResources($prop);
        $literals  = $meta->allLiterals($prop);
        $allowed   = [
            'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/public',
            'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/academic',
            'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/restricted'
        ];
        $condCount = count($resources) == 0 || count($literals) > 0 || count($resources) > 1;
        $condValue = count($resources) > 0 && !in_array((string) $resources[0], $allowed);
        if ($condCount || $condValue) {
            $default = RC::$config->doorkeeper->default->accessRestriction;
            $meta->delete($prop);
            $meta->addResource($prop, $default);
            RC::$log->info("\t\t$prop = $default added");
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
     */
    static public function maintainAccessRights(Resource $meta): void {
        $accessRestr = (string) $meta->getResource(RC::$config->schema->acdh->accessRestriction);
        if (empty($accessRestr)) {
            return;
        }

        RC::$log->info("\t\tmaintaining access rights");
        $propRead     = RC::$config->accessControl->schema->read;
        $propRoles    = RC::$config->schema->acdh->accessRole;
        $rolePublic   = RC::$config->doorkeeper->rolePublic;
        $roleAcademic = RC::$config->doorkeeper->roleAcademic;

        if ($accessRestr === 'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/public') {
            $meta->addLiteral($propRead, $rolePublic);
            RC::$log->info("\t\t\t\tpublic");
        } else {
            $meta->delete($propRead, $rolePublic);
            if ($accessRestr === 'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/academic') {
                $meta->addLiteral($propRead, $roleAcademic);
                RC::$log->info("\t\t\t\tacademic");
            } else {
                $meta->delete($propRead, $roleAcademic);
                foreach ($meta->all($propRoles) as $role) {
                    $meta->addLiteral($propRead, $role);
                }
                RC::$log->info("\t\t\t\trestricted");
            }
        }
    }

    static private function maintainPropertyRange(Resource $meta): void {
        foreach ($meta->propertyUris() as $prop) {
            $range = self::$ontology->getProperty($meta, $prop);
            if ($range === null || $range->range === null) {
                continue;
            } else {
                $range = $range->range;
            }
            foreach ($meta->allLiterals($prop) as $l) {
                /* @var $l \EasyRdf\Literal */
                $type = $l->getDatatypeUri() ?? RDF::XSD_STRING;
                if ($type === $range) {
                    continue;
                }
                if ($range === RDF::XSD_STRING) {
                    $meta->delete($prop, $l);
                    $meta->addLiteral($prop, (string) $l);
                    RC::$log->info("\t\tcasting $prop value from $type to string");
                } else {
                    try {
                        $value = self::castLiteral($l, $range);
                        $meta->delete($prop, $l);
                        $meta->addLiteral($prop, $value);
                        RC::$log->info("\t\tcasting $prop value from $type to $range");
                    } catch (RuntimeException $ex) {
                        RC::$log->info('    ' . $ex->getMessage());
                    } catch (DoorkeeperException $ex) {
                        throw new DoorkeeperException('property ' . $prop . ': ' . $ex->getMessage(), $ex->getCode(), $ex);
                    }
                }
            }
        }
    }

    static private function normalizeIds(Resource $meta): void {
        if (self::$uriNorm === null) {
            self::$uriNorm = new UriNormalizer((array) RC::$config->doorkeeper->uriMappings);
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
        $toCheck = [
            'https://vocabs.acdh.oeaw.ac.at/schema#hasTitle',
            'https://vocabs.acdh.oeaw.ac.at/schema#hasAlternativeTitle',
            'https://vocabs.acdh.oeaw.ac.at/schema#hasDescription',
            'https://vocabs.acdh.oeaw.ac.at/schema#hasTechnicalInfo',
            'https://vocabs.acdh.oeaw.ac.at/schema#hasAppliedMethodDescription',
            'https://vocabs.acdh.oeaw.ac.at/schema#hasExtent',
            'https://vocabs.acdh.oeaw.ac.at/schema#hasArrangement',
            'https://vocabs.acdh.oeaw.ac.at/schema#hasNamingScheme',
            'https://vocabs.acdh.oeaw.ac.at/schema#hasCompleteness',
            'https://vocabs.acdh.oeaw.ac.at/schema#hasEditorialPractice',
            'https://vocabs.acdh.oeaw.ac.at/schema#hasSeriesInformation',
            'https://vocabs.acdh.oeaw.ac.at/schema#hasTableOfContents',
            'https://vocabs.acdh.oeaw.ac.at/schema#hasNote',
            'https://vocabs.acdh.oeaw.ac.at/schema#hasTemporalCoverage',
            'https://vocabs.acdh.oeaw.ac.at/schema#hasCity',
            'https://vocabs.acdh.oeaw.ac.at/schema#hasCountry',
            'https://vocabs.acdh.oeaw.ac.at/schema#hasRegion',
        ];
        foreach ($toCheck as $prop) {
            foreach ($meta->allLiterals($prop) as $value) {
                /* @var $value \EasyRdf\Literal */
                if (empty($value->getLang())) {
                    throw new DoorkeeperException("Property $prop with value " . (string) $value . " is not tagged with a language");
                }
            }
        }
    }

    /**
     * Checks property cardinalities according to the ontology.
     * 
     * Here and now only min count is checked.
     * 
     * @param \EasyRdf\Resource $meta
     * @throws DoorkeeperException
     */
    static private function checkCardinalities(Resource $meta): void {
        foreach ($meta->allResources(RDF::RDF_TYPE) as $class) {
            $classDef = self::$ontology->getClass((string) $class);
            if ($classDef === null) {
                continue;
            }
            foreach ($classDef->properties as $p) {
                if ($p->min > 0) {
                    $count = count($meta->all($p->property));
                    if ($count < $p->min) {
                        throw new DoorkeeperException('Min property count for ' . $p->property . ' is ' . $p->min . ' but resource has ' . $count);
                    }
                }
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
            if ((string) $i === '') {
                throw new DoorkeeperException("$titleProp value is empty");
            }
            $langs[$lang] = '';
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

    static private function updateCollectionExtent(PDO $pdo, int $txId,
                                                   array $resourceIds): void {
        $sizeProp      = RC::$config->schema->binarySize;
        $acdhSizeProp  = RC::$config->schema->acdh->binarySize;
        $acdhCountProp = RC::$config->schema->acdh->count;

        RC::$log->info("\t\tUpdating size of collections affected by the transaction");

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
                SELECT target_id, n + 1
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

        // find all affected parents (gr, pp), then find all the children (ch) and their size (chm)
        // finaly group by parent and compute their size 
        $query = "
            CREATE TEMPORARY TABLE _collsizeupdate AS
            SELECT 
                p.id, 
                coalesce(sum(CASE ra.state = ? WHEN true THEN chm.value_n ELSE 0 END), 0) AS size, 
                greatest(sum((ra.state = 'active')::int) - 1, 0) AS count
            FROM
                (
                    SELECT DISTINCT gr.id
                    FROM resources r, LATERAL get_relatives(r.id, ?, 0) gr
                    WHERE r.transaction_id = ?
                  " . (count($parentIds) > 0 ? "UNION SELECT * FROM (VALUES %ids%) pp" : "") . "
                ) p,
                LATERAL get_relatives(p.id, ?, 999999, 0) ch
                LEFT JOIN resources ra ON ra.id = ch.id
                LEFT JOIN metadata chm ON chm.id = ch.id AND chm.property = ?
            GROUP BY 1
        ";
        $query = str_replace('%ids%', substr(str_repeat('(?::bigint), ', count($parentIds)), 0, -2), $query);
        $param = array_merge(
            [Res::STATE_ACTIVE, RC::$config->schema->parent, $txId], // case in main select, gr, r
            $parentIds,
            [RC::$config->schema->parent, $sizeProp]// p, ch, chm
        );
        $query = $pdo->prepare($query);
        $query->execute($param);

        // try to lock resources to be updated
        $query  = $pdo->prepare("
            UPDATE resources SET transaction_id = ? 
            WHERE id IN (SELECT id FROM _collsizeupdate) AND transaction_id IS NULL
        ");
        $query->execute([$txId]);
        $query  = $pdo->prepare("
            SELECT 
                count(*) AS all, 
                coalesce(sum((transaction_id = ?)::int), 0) AS locked
            FROM 
                _collsizeupdate
                JOIN resources r USING (id)
        ");
        $query->execute([$txId]);
        $result = $query->fetch(PDO::FETCH_OBJ);
        if ($result !== false && $result->all !== $result->locked) {
            $msg = "Some resources locked by another transaction (" . ($result->all - $result->locked) . " out of " . $result->all . ")";
            throw new DoorkeeperException($msg, 409);
        }

        // remove old values
        $query = $pdo->prepare("
            DELETE FROM metadata WHERE id IN (SELECT id FROM _collsizeupdate) AND property IN (?, ?)
        ");
        $query->execute([$acdhSizeProp, $acdhCountProp]);
        // insert new values
        $query = $pdo->prepare("
            INSERT INTO metadata (id, property, type, lang, value_n, value)
                SELECT id, ?, ?, '', size, size FROM _collsizeupdate
              UNION
                SELECT id, ?, ?, '', count, count FROM _collsizeupdate
        ");
        $query->execute([$acdhSizeProp, RDF::XSD_DECIMAL, $acdhCountProp, RDF::XSD_DECIMAL]);

        $query = $pdo->query("
            SELECT json_agg(row_to_json(c)) FROM (SELECT * FROM _collsizeupdate) c
        ");
        RC::$log->debug("\t\t\tupdated resources: " . $query->fetchColumn());
    }

    static private function loadOntology(): void {
        if (self::$ontology === null) {
            self::$ontology = new Ontology(RC::$pdo, RC::getBaseUrl() . '%');
        }
    }

    static private function toString(array $a): array {
        return array_map(function($x) {
            return (string) $x;
        }, $a);
    }

    static private function castLiteral(Literal $l, string $range): Literal {
        switch ($range) {
            case RDF::XSD_DATE:
                $value = new lDate(is_numeric((string) $l) ? $l . '-01-01' : (string) $l, null, $range);
                break;
            case RDF::XSD_DATE_TIME:
                $value = new lDateTime(is_numeric((string) $l) ? $l . '-01-01' : (string) $l, null, $range);
                break;
            case RDF::XSD_DECIMAL:
            case RDF::XSD_FLOAT:
            case RDF::XSD_DOUBLE:
                $value = new lDecimal((string) $l, null, $range);
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
                $value = new lInteger((int) ((string) $l), null, $range);
                break;
            case RDF::XSD_BOOLEAN:
                $value = new lBoolean((string) $l, null, $range);
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
