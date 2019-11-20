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

use DateTime;
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
     * @var type \acdhOeaw\arche\Ontology
     */
    static private $ontology;

    /**
     *
     * @var \acdhOeaw\UriNormalizer
     */
    static private $uriNorm;

    static public function onResEdit(Resource $meta, ?string $path): Resource {
        self::maintainPid($meta);

        self::loadOntology();
        if (self::$ontology->isA($meta, RC::$config->schema->classes->repoObject)) {
            self::maintainHosting($meta);
        }
        self::maintainAccessRestriction($meta);
        self::maintainAccessRights($meta);
        self::maintainPropertyRange($meta);
        self::normalizeIds($meta);
        self::checkCardinalities($meta);
        self::checkIdCount($meta);
        self::checkTitleProp($meta);

        return $meta;
    }

    static public function onTxCommit(string $method, int $txId,
                                      array $resourceIds): void {
        self::updateCollectionExtent($txId, $resourceIds);
    }

    static private function maintainPid(Resource $meta): void {
        $cfg     = RC::$config->doorkeeper->epicPid;
        $idProp  = RC::$config->schema->id;
        $pidProp = RC::$config->schema->pid;
        $curPid  = null;
        foreach ($meta->allResources($idProp) as $i) {
            if (strpos((string) $i, $cfg->resolver) === 0) {
                $curPid = (string) $i;
                break;
            }
        }
        if ($meta->getLiteral($pidProp)) {
            if ($curPid === null) {
                if ($cfg->pswd === '') {
                    RC::$log->info('  skipping PID generation - no EPIC password provided');
                    return;
                }
                $meta->delete($pidProp);
                $ps  = new HandleService($c->url, $cfg->prefix, $cfg->user, $cfg->pswd);
                $pid = $ps->create($meta->getUri());
                $pid = str_replace($c->url, $cfg->resolver, $pid);
                RC::$log->info('  registered PID ' . $pid . ' pointing to ' . $meta->getUri());
                $meta->addResource($pidProp, $pid);
            } else {
                $meta->delete($pidProp);
                $meta->addResource($pidProp, $curPid);
                RC::$log->info('  recreating PID ' . $curPid . ' pointing to ' . $meta->getUri());
            }
        }
        // promote PIDs to IDs
        $ids = self::toString($meta->allResources($idProp));
        foreach ($meta->allResources($pidProp) as $i) {
            $i = (string) $i;
            if (!in_array($i, $ids)) {
                $meta->addResource($idProp, $i);
            }
        }
    }

    static private function maintainHosting(Resource $meta): void {
        $prop  = RC::$config->schema->hosting;
        $value = $meta->getResource($prop);
        if ($value === null) {
            $meta->addResource($prop, RC::$config->doorkeeper->default->hosting);
            RC::$log->info('  ' . $prop . ' added');
        }
    }

    static private function maintainAccessRestriction(Resource $meta): void {
        $isRepoObj   = self::$ontology->isA($meta, RC::$config->schema->classes->repoObject);
        $isSharedObj = self::$ontology->isA($meta, RC::$config->schema->classes->sharedObject);
        $isContainer = self::$ontology->isA($meta, RC::$config->schema->classes->container);
        if (!$isRepoObj && !$isSharedObj && !$isContainer) {
            return;
        }

        $prop      = RC::$config->schema->accessRestriction;
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
            RC::$log->info('  ' . $prop . ' = ' . $default . ' added');
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
        $accessRestr = (string) $meta->getResource(RC::$config->schema->accessRestriction);
        if (empty($accessRestr)) {
            return;
        }

        RC::$log->info('  maintaining access rights');
        $propRead     = RC::$config->accessControl->schema->read;
        $propWrite    = RC::$config->accessControl->schema->write;
        $propRoles    = RC::$config->schema->accessRole;
        $rolePublic   = RC::$config->doorkeeper->rolePublic;
        $roleAcademic = RC::$config->doorkeeper->roleAcademic;

        $meta->delete($propWrite, $rolePublic);
        $meta->delete($propWrite, $roleAcademic);

        if ($accessRestr === 'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/public') {
            $meta->addLiteral($propRead, $rolePublic);
            RC::$log->info('    public');
        } else {
            $meta->delete($propRead, $rolePublic);
            if ($accessRestr === 'https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/academic') {
                $meta->addLiteral($propRead, $roleAcademic);
                RC::$log->info('    academic');
            } else {
                $meta->delete($propRead, $roleAcademic);
                foreach ($meta->all($propRoles) as $role) {
                    $meta->addLiteral($propRead, $role);
                }
                RC::$log->info('    restricted');
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
                    RC::$log->info('    casting ' . $prop . ' value from ' . $type . ' to string');
                } elseif ($type === RDF::XSD_STRING) {
                    try {
                        $value = self::castLiteral($l, $range);
                        $meta->delete($prop, $l);
                        $meta->addLiteral($prop, $value);
                        RC::$log->info('    casting ' . $prop . ' value from ' . $type . ' to ' . $range);
                    } catch (RuntimeException $ex) {
                        RC::$log->info('    ' . $ex->getMessage());
                    } catch (DoorkeeperException $ex) {
                        throw new DoorkeeperException('property ' . $prop . ': ' . $ex->getMessage(), $ex->getCode(), $ex);
                    }
                } else {
                    RC::$log->info('    unknown type: ' . $type);
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
                RC::$log->info("  id URI $ids standardized to $std");
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
        $searchProps = [
            'http://purl.org/dc/elements/1.1/title',
            'http://purl.org/dc/terms/title',
            'http://www.w3.org/2004/02/skos/core#prefLabel',
            'http://www.w3.org/2000/01/rdf-schema#label',
            'http://xmlns.com/foaf/0.1/name',
        ];
        $titleProp   = RC::$config->schema->label;

        // special case acdh:hasFirstName and acdh:hasLastName
        $first = $meta->getLiteral('https://vocabs.acdh.oeaw.ac.at/schema#hasFirstName');
        $last  = $meta->getLiteral('https://vocabs.acdh.oeaw.ac.at/schema#hasLastName');
        $title = trim((string) $first . ' ' . (string) $last);

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
        if (count($titles) > 0 && ($title === '' || (string) $titles[0] === $title)) {
            return;
        }

        if (empty($title)) {
            // no direct hit - search for candidates
            foreach ($searchProps as $prop) {
                $matches = $meta->allLiterals($prop);
                if (count($matches) > 0 && trim($matches[0]) !== '') {
                    $title = trim((string) $matches[0]);
                }
            }
        }

        // special case - foaf:givenName and foaf:familyName
        if ($title === '') {
            $given  = $meta->getLiteral('http://xmlns.com/foaf/0.1/givenName');
            $family = $meta->getLiteral('http://xmlns.com/foaf/0.1/familyName');
            $title  = trim((string) $given . ' ' . (string) $family);
        }

        if (empty($title)) {
            throw new DoorkeeperException("$titleProp is missing");
        }

        // if we are here, the title has to be updated
        RC::$log->info("    setting title to $title");
        $meta->delete($titleProp);
        $meta->addLiteral($titleProp, $title);
    }

    static private function updateCollectionExtent(int $txId, array $resourceIds): void {
        return;
        $relProp   = RC::$config->schema->parent;
        $extProp   = RC::$config->schema->extent;
        $countProp = RC::$config->schema->count;

        RC::$log->info('  Updating size of collections affected by the transaction');

        $query = "
            CREATE TEMPORARY TABLE _collsizeupdate AS
            WITH
                RECURSIVE r(id, trid) AS (
                    SELECT id, id FROM resources WHERE transaction_id = ?
                  UNION
                    SELECT target_id, trid
                    FROM 
                        relations 
                        JOIN r USING (id)
                    WHERE
                        property = ?
                ),
                d AS (
                    SELECT 
                        id, 
                        coalesce(cur::bigint, 0) - coalesce(prev::bigint, 0) AS sizediff,
                        (prev IS NULL)::int - (cur IS NULL)::int AS countdiff
                    FROM (
                        SELECT
                            id,
                            first_value(mh.value) OVER (PARTITION BY id ORDER BY date) AS prev,
                            m.value AS cur
                        FROM 
                            transactions t
                            JOIN resources r USING (transaction_id)
                            LEFT JOIN metadata_history mh USING (id)
                            LEFT JOIN metadata m USING (id)
                        WHERE
                            transaction_id = ?
                            AND (mh.property = ? AND mh.date >= t.started OR mh.id IS NULL)
                            AND (m.property = ? OR m.id IS NULL)
                    ) d1
                )
            SELECT id, ? AS property, m.value, coalesce(m.value::bigint, 0) + sizediff AS newval
            FROM 
                r 
                JOIN d USING (trid)
                LEFT JOIN metadata m USING (id)
            WHERE
                (m.property = ? OR m.id IS NULL)
          UNION
            SELECT id, ? AS property, m.value, coalesce(m.value::bigint, 0) + countdiff
            FROM 
                r 
                JOIN d USING (trid)
                LEFT JOIN metadata m USING (id)
            WHERE
                (m.property = ? OR m.id IS NULL)
        ";
        $param = [
            $txId, $relProp, // r table
            $txId, $extProp, $extProp, // d table
            $extProp, $extProp, $countProp, $countProp, // final select
        ];
        $query = RC::$pdo->prepare($query);
        $query->execute($param);

        RC::$pdo->query("
            INSERT INTO metadata_history (id, property, type, lang, value)
            SELECT id, property, type, lang, m.value
            FROM 
                metadata m
                JOIN _collsizeupdate USING (id, property)
        ");
        RC::$pdo->query("
            DELETE FROM metadata WHERE (id, property) IN (SELECT id, property FROM _collsizeupdate)
        ");
        $query = RC::$pdo->prepare("
            INSERT INTO metadata (id, property, type, lang, value_n, value)
            SELECT id, property, ?, '', newval, newval FROM _collsizeupdate
        ");
        $query->execute([RDF::XSD_DECIMAL]);

        $query = RC::$pdo->query("
            SELECT json_agg(row_to_json(c)) FROM _collsizeupdate c;
        ");
        RC::$log->debug('    ' . $query->fetchColumn());
    }

    static private function loadOntology(): void {
        if (self::$ontology === null) {
            self::$ontology = new Ontology(RC::$pdo, RC::$config->schema->namespaces->ontology);
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
