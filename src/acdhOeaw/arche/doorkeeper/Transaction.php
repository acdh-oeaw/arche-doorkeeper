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

use Psr\Log\LoggerInterface;
use PDO;
use zozlak\RdfConstants as RDF;
use acdhOeaw\arche\lib\Schema;
use acdhOeaw\arche\lib\schema\Ontology;
use acdhOeaw\arche\core\Resource as Resource;
use acdhOeaw\arche\core\RestController as RC;
use acdhOeaw\arche\core\Transaction as ArcheTransaction;

/**
 *
 * @author zozlak
 */
class Transaction {

    const DB_LOCK_TIMEOUT = 1000;
    use RunTestsTrait;

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
        $pdo->query("SET application_name TO doorkeeper");
        $pdo->query("SET lock_timeout TO " . self::DB_LOCK_TIMEOUT);
        $pdo->beginTransaction();

        $schema    = new Schema(RC::$config->schema);
        $cacheFile = RC::$config->doorkeeper->ontologyCacheFile ?? '';
        $cacheTtl  = RC::$config->doorkeeper->onFtologyCacheTtl ?? 600;
        $ontology  = Ontology::factoryDb($pdo, $schema, $cacheFile, $cacheTtl);
        $tx        = new Transaction($txId, $pdo, $schema, $ontology, RC::$log);

        $tx->runTests();
        $tx->updateCollections();

        $pdo->commit();
    }

    /**
     * Stores ids of all pre-transaction parents of resources affected by the current transaction
     * @var array<int>
     */
    private array $parentIds;

    public function __construct(private int $txId, private PDO $pdo,
                                private Schema $schema,
                                private Ontology $ontology,
                                private LoggerInterface | null $log = null) {
        
    }

    /**
     * isNewVersionOf can't create cycles so creation of the 2nd (and higher order)
     * isNewVersionOf links to a resource is forbidden.
     * 
     * As the doorkeeper can't lock resources in an effective way checking it is
     * possible only at the transaction commit.
     * 
     * @throws DoorkeeperException
     */
    #[CheckAttribute]
    public function checkIsNewVersionOf(): void {
        $nvProp    = $this->schema->isNewVersionOf;
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
            ArcheTransaction::STATE_ACTIVE,
            $nvProp,
            $this->txId,
            ArcheTransaction::STATE_ACTIVE,
            ArcheTransaction::STATE_ACTIVE,
        ];
        $t         = microtime(true);
        $query     = $this->pdo->prepare($query);
        $query->execute($param);
        $conflicts = $query->fetchColumn();
        $t         = microtime(true) - $t;
        $this->log?->debug("\t\tcheckIsNewVersionOf performed in $t s");
        if ($conflicts !== null) {
            throw new DoorkeeperException("More than one $nvProp pointing to some resources: $conflicts");
        }
    }

    /**
     * 
     * @throws DoorkeeperException
     */
    #[CheckAttribute]
    public function checkAutoCreatedResources(): void {
        if (RC::$config->doorkeeper->checkAutoCreatedResources === false) {
            return;
        }

        // find all properties which range has been checked
        // all resources pointed with this properties are excluded from
        // the no metadata check
        $validRanges = array_keys((array) RC::$config->doorkeeper->checkRanges);
        $validProps  = [ArcheTransaction::STATE_ACTIVE, $this->txId];
        foreach ($this->ontology->getProperties() as $prop) {
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
            [ArcheTransaction::STATE_ACTIVE, $this->txId, $this->schema->label]
        );
        $t          = microtime(true);
        $query      = $this->pdo->prepare($query);
        $query->execute($param);
        $invalidRes = $query->fetchColumn();
        $t          = microtime(true) - $t;
        $this->log?->debug("\t\tcheckAutoCreatedResources performed in $t s");
        if (!empty($invalidRes)) {
            throw new DoorkeeperException("Transaction created resources without any metadata: $invalidRes");
        }
    }

    /**
     * 
     * @throws DoorkeeperException
     */
    #[CheckAttribute]
    public function checkEmptyCollections(): void {
        $parentIds  = $this->fetchPreTransactionParentIds();
        $idsReplace = '';
        if (count($parentIds) > 0) {
            $idsReplace = 'OR r.id IN (' . substr(str_repeat('?::bigint, ', count($parentIds)), 0, -2) . ')';
        }
        $query      = "
            SELECT string_agg(DISTINCT ids, ', ')
            FROM
                resources r
                JOIN metadata c USING (id)
                JOIN identifiers USING (id)
            WHERE
                r.state = ?
                AND (r.transaction_id = ? %ids%)
                AND c.property = ?
                AND c.value IN (?, ?)
                AND NOT EXISTS (SELECT 1 FROM relations ch WHERE ch.property = ? AND ch.target_id = r.id)
        ";
        $query      = str_replace('%ids%', $idsReplace, $query);
        $param      = array_merge(
            [ArcheTransaction::STATE_ACTIVE, $this->txId],
            $parentIds,
            [
                RDF::RDF_TYPE,
                $this->schema->classes->topCollection, $this->schema->classes->collection,
                $this->schema->parent,
            ]
        );
        $t          = microtime(true);
        $query      = $this->pdo->prepare($query);
        $query->execute($param);
        $invalidRes = $query->fetchColumn();
        $t          = microtime(true) - $t;
        $this->log?->debug("\t\tcheckEmptyCollections performed in $t s");
        if (!empty($invalidRes)) {
            throw new DoorkeeperException("Transaction created empty collections: $invalidRes");
        }
    }

    /**
     * 
     * @throws DoorkeeperException
     */
    #[CheckAttribute]
    public function checkHasNextItem(): void {
        $nextProp   = (string) $this->schema->nextItem;
        $parentProp = (string) $this->schema->parent;
        $query      = "
            CREATE TEMPORARY TABLE hni AS (
            -- active collections containing transaction resources
            WITH cols AS (
                WITH
                    t1 AS (SELECT id FROM resources WHERE transaction_id = ? AND state = ?),
                    t2 AS (
                        SELECT * FROM t1
                      UNION
                        SELECT rs.id 
                        FROM relations r JOIN resources rs ON r.target_id = rs.id AND r.property = ? AND rs.state = ?
                        WHERE EXISTS (SELECT 1 FROM t1 WHERE id = r.id)
                    )
                SELECT * 
                FROM t2 
                WHERE EXISTS (SELECT 1 FROM metadata WHERE id = t2.id AND property = ? AND value IN (?, ?))
            ),
            -- cols and their all active children
            ch AS (
                SELECT id AS pid, id, true AS col, true AS tocheck
                FROM cols
              UNION
                SELECT c.id AS pid, r.id, m.value IN (?, ?) AS col, false AS tocheck
                FROM
                    cols c
                    JOIN relations r ON r.target_id = c.id AND r.property= ?
                    JOIN resources rs ON rs.id = r.id AND state = ?
                    JOIN metadata m ON m.id = r.id AND m.property = ?
            ),
            -- nextProp chain from cols
            ni AS (
                WITH
                    RECURSIVE t(previd, nextid, pid) AS (
                        SELECT null::bigint, id, pid FROM ch WHERE tocheck
                      UNION
                        SELECT r.id, r.target_id, p.target_id
                        FROM
                            t
                            JOIN relations r ON r.id = t.nextid AND r.property = ?
                            JOIN resources rs ON rs.id = t.nextid AND rs.state = ?
                            LEFT JOIN relations p ON r.target_id = p.id AND p.property = ?
                    ),
                    u AS (
                        SELECT * FROM t
                      UNION
                        -- not to miss free children with next item when collection does not have a next item
                        SELECT ch.id AS previd, r.target_id AS nextid, p.target_id AS pid
                        FROM
                            ch
                            JOIN relations r ON r.id = ch.id AND r.property = ?
                            JOIN resources rs ON rs.id = ch.id AND rs.state = ?
                            LEFT JOIN relations p ON r.target_id = p.id AND p.property = ?
                    )
                SELECT t1.previd, t1.nextid AS id, t1.pid, t2.nextid 
                FROM
                    u t1
                    LEFT JOIN u t2 ON t1.nextid = t2.previd AND t1.pid = t2.pid
                WHERE t1.previd IS NOT NULL OR t2.nextid IS NOT NULL
            )
            -- ch and ni put together
            SELECT * 
            FROM 
                ch 
                FULL JOIN ni USING (id, pid) 
            )
        ";
        $param      = [
            $this->txId, ArcheTransaction::STATE_ACTIVE, $parentProp, ArcheTransaction::STATE_ACTIVE, // col
            RDF::RDF_TYPE, $this->schema->classes->topCollection, $this->schema->classes->collection, // col
            $this->schema->classes->topCollection, $this->schema->classes->collection, // ch
            $parentProp, ArcheTransaction::STATE_ACTIVE, RDF::RDF_TYPE, // ch
            $nextProp, ArcheTransaction::STATE_ACTIVE, $parentProp, // ni
            $nextProp, ArcheTransaction::STATE_ACTIVE, $parentProp, // ni
        ];
        $this->log->debug(new \zozlak\queryPart\QueryPart($query, $param));
        $query      = $this->pdo->prepare($query);
        $t0         = microtime(true);
        $query->execute($param);
//        $debug      = "\ndrop table hni; create temporary table hni (id bigint, pid bigint, col bool, tocheck bool, previd bigint, nextid bigint);\n";
//        foreach ($this->pdo->query("SELECT id, pid, col::text, tocheck::text, previd, nextid FROM hni ORDER BY pid, id")->fetchAll(PDO::FETCH_OBJ) as $i) {
//            $debug .= "INSERT INTO hni VALUES ($i->id, $i->pid, " . ($i->col ?? 'null') . ", " . ($i->tocheck ?? 'null') . ", " . ($i->previd ?? 'null') . ", " . ($i->nextid ?? 'null') . ");\n";
//        }
//        $this->log->info($debug);
        $this->pdo->query("DELETE FROM hni WHERE pid IN (SELECT pid FROM hni GROUP BY pid HAVING count(previd) = 0 AND count(nextid) = 0)");
        $t1 = microtime(true);

        $errors = [];

        $query = $this->pdo->query("SELECT id FROM hni WHERE col IS NULL");
        foreach ($query->fetchAll(PDO::FETCH_COLUMN) as $i) {
            $errors[] = "Resource $i is pointed with the next item from outside of its parent collection";
        }

        $query = $this->pdo->query("
            SELECT pid, string_agg(previd::text, ', ') AS gapin
            FROM hni
            GROUP BY pid
            HAVING count(previd) + 1 != count(*);
        ");
        foreach ($query->fetchAll(PDO::FETCH_OBJ) as $i) {
            $errors[] = "Collection $i->pid has a gap in the next item chain in one of resources $i->gapin";
        }

        $query = $this->pdo->query("
            SELECT pid, string_agg(nextid::text, ', ') AS gapin
            FROM hni
            GROUP BY pid
            HAVING count(nextid) + 1 != count(*);
        ");
        foreach ($query->fetchAll(PDO::FETCH_OBJ) as $i) {
            $errors[] = "Collection $i->pid has a gap in the next item chain in one of resources $i->gapin";
        }

        $query = $this->pdo->query("SELECT id, previd FROM hni WHERE col AND id = pid AND previd IS NOT NULL");
        foreach ($query->fetchAll(PDO::FETCH_OBJ) as $i) {
            $errors[] = "Collection $i->id id pointed with the next item from an invalid resource $i->previd";
        }

        $query = $this->pdo->query("SELECT id FROM hni WHERE col AND id = pid AND nextid IS NULL");
        foreach ($query->fetchAll(PDO::FETCH_COLUMN) as $i) {
            $errors[] = "Collection $i does not point with the next item to its first child";
        }

        $query = $this->pdo->query("SELECT id FROM hni WHERE (NOT col OR id <> pid) AND previd IS NULL");
        foreach ($query->fetchAll(PDO::FETCH_COLUMN) as $i) {
            $errors[] = "Resource $i is not pointed with any next item";
        }

        $query = $this->pdo->query("
            SELECT id 
            FROM hni 
            WHERE (NOT col OR id <> pid) 
            GROUP BY 1 
            HAVING count(*) > 1
        ");
        foreach ($query->fetchAll(PDO::FETCH_COLUMN) as $i) {
            $errors[] = "Resource $i has multiple has next item properties";
        }

        $query = $this->pdo->query("
            SELECT id 
            FROM hni 
            WHERE col AND id = pid 
            GROUP BY 1 
            HAVING count(*) > 1;
        ");
        foreach ($query->fetchAll(PDO::FETCH_OBJ) as $i) {
            $errors[] = "Collection $i points with next item to more than one child resource";
        }

        $t2 = microtime(true);
        $this->log?->debug("\t\tcheckHasNextItem performed in: init " . ($t1 - $t0) . " s checks " . ($t2 - $t1) . " s");

        if (count($errors) > 0) {
            throw new DoorkeeperException("$nextProp errors:\n" . implode("\n", $errors));
        }
    }

    /**
     * 
     * @return void
     * @throws DoorkeeperException
     */
    public function updateCollections(): void {
        $parentIds = $this->fetchPreTransactionParentIds();
        $this->log?->info("\t\tupdating collections affected by the transaction");
        $t0        = microtime(true);
        // find all affected parents (gr, pp) and their children
        $query     = "
            CREATE TEMPORARY TABLE _resources AS
            WITH RECURSIVE t(cid, id) AS (
                SELECT * FROM (
                    SELECT DISTINCT gr.id AS cid, gr.id
                    FROM resources r, LATERAL get_relatives(r.id, ?, 0) gr
                    WHERE r.transaction_id = ?
                    " . (count($parentIds) > 0 ? "UNION SELECT id, id FROM (VALUES %ids%) pp (id)" : "") . "
                ) t0
              UNION
                SELECT t.cid, r.id
                FROM t JOIN relations r ON t.id = r.target_id AND r.property = ?
            )
            SELECT * FROM t;
        ";
        $query     = str_replace('%ids%', substr(str_repeat('(?::bigint), ', count($parentIds)), 0, -2), $query);
        $param     = array_merge(
            [$this->schema->parent, $this->txId],
            $parentIds,
            [$this->schema->parent],
        );
        $query     = $this->pdo->prepare($query);
        $query->execute($param);
        $t1        = microtime(true);

        // try to lock resources to be updated
        // TODO - how to lock them in a way which doesn't cause a deadlock
        //        $query  = $this->pdo->prepare("
        //            UPDATE resources SET transaction_id = ? 
        //            WHERE id IN (SELECT DISTINCT cid FROM _resources) AND transaction_id IS NULL
        //        ");
        //        $query->execute([$this->txId]);
        //        $query  = $this->pdo->prepare("
        //            SELECT 
        //                count(*) AS all, 
        //                coalesce(sum((transaction_id = ?)::int), 0) AS locked
        //            FROM 
        //                (SELECT DISTINCT cid AS id FROM _resources) t
        //                JOIN resources r USING (id)
        //        ");
        //        $query->execute([$this->txId]);
        //        $result = $query->fetch(PDO::FETCH_OBJ);
        //        if ($result !== false && $result->all !== $result->locked) {
        //            $msg = "Some resources locked by another transaction (" . ($result->all - $result->locked) . " out of " . $result->all . ")";
        //            throw new DoorkeeperException($msg, 409);
        //        }
        // perform the actual metadata update
        $this->updateCollectionSize();
        $t2 = microtime(true);
        $this->updateCollectionAggregates();
        $t3 = microtime(true);

        $query = $this->pdo->query("
            SELECT json_agg(c.cid) FROM (SELECT DISTINCT cid FROM _resources) c
        ");
        $this->log?->debug("\t\t\tupdated resources: " . $query->fetchColumn());
        $this->log?->debug("\t\t\ttiming: init " . ($t1 - $t0) . " size " . ($t2 - $t1) . " aggregates " . ($t3 - $t2));
    }

    private function updateCollectionSize(): void {
        $sizeProp      = $this->schema->binarySize;
        $acdhSizeProp  = $this->schema->binarySizeCumulative;
        $acdhCountProp = $this->schema->countCumulative;
        $collClass     = $this->schema->classes->collection;
        $topCollClass  = $this->schema->classes->topCollection;

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
            Resource::STATE_ACTIVE, Resource::STATE_ACTIVE, // case in main select
            $sizeProp, RDF::RDF_TYPE, $collClass, $topCollClass  // chm, m, m, m
        ];
        $query = $this->pdo->prepare($query);
        $query->execute($param);

        // remove old values
        $query = $this->pdo->prepare("
            DELETE FROM metadata WHERE id IN (SELECT id FROM _collsizeupdate) AND property IN (?, ?)
        ");
        $query->execute([$acdhSizeProp, $acdhCountProp]);
        // insert new values
        $query = $this->pdo->prepare("
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
            $acdhSizeProp, $this->getPropertyType($acdhSizeProp), Resource::STATE_ACTIVE,
            $acdhCountProp, $this->getPropertyType($acdhCountProp), Resource::STATE_ACTIVE
        ]);
    }

    private function updateCollectionAggregates(): void {
        $accessProp     = $this->schema->accessRestriction;
        $accessAggProp  = $this->schema->accessRestrictionAgg;
        $licenseProp    = $this->schema->license;
        $licenseAggProp = $this->schema->licenseAgg;
        $labelProp      = $this->schema->label;
        $collClass      = $this->schema->classes->collection;
        $topCollClass   = $this->schema->classes->topCollection;

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
                    string_agg(value || ': ' || count, ' / ' ORDER BY count DESC, value) AS value
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
                    string_agg(value || ': ' || count, ' / ' ORDER BY count DESC, value) AS value
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
            Resource::STATE_ACTIVE, RDF::RDF_TYPE, $collClass, $topCollClass, // activecol
            $licenseAggProp, Resource::STATE_ACTIVE, $licenseProp, $labelProp, // a1
            $accessAggProp, Resource::STATE_ACTIVE, $accessProp, $labelProp, // a2
        ];
        $query = $this->pdo->prepare($query);
        $query->execute($param);
        $this->log?->info('[-----');
        $this->log?->info((string) json_encode($this->pdo->query("SELECT * FROM _resources")->fetchAll(PDO::FETCH_OBJ)));
        $this->log?->info((string) json_encode($this->pdo->query("SELECT * FROM _aggupdate")->fetchAll(PDO::FETCH_OBJ)));

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
        $param = [$licenseAggProp, $accessAggProp, Resource::STATE_ACTIVE, RDF::RDF_TYPE,
            $collClass, $topCollClass];
        $query = $this->pdo->prepare($query);
        $query->execute($param);
        $this->log?->info((string) json_encode($this->pdo->query("SELECT * FROM _aggupdate")->fetchAll(PDO::FETCH_OBJ)));

        // remove old values
        $query = $this->pdo->prepare("
            DELETE FROM metadata WHERE id IN (SELECT id FROM _collsizeupdate) AND property IN (?, ?)
        ");
        $query->execute([$licenseAggProp, $accessAggProp]);
        // insert new values
        $query = $this->pdo->prepare("
            INSERT INTO metadata (id, property, type, lang, value)
            SELECT id, property, ?::text, lang, value FROM _aggupdate
        ");
        $query->execute([RDF::RDF_LANG_STRING]);
    }

    /**
     * Returns ids of all pre-transaction parents of resources affected by the current transaction
     * 
     * @return array<int>
     */
    private function fetchPreTransactionParentIds(): array {
        if (!isset($this->parentIds)) {
            $this->log?->info("\t\tfinding collections affected by the transaction");
            $t0     = microtime(true);
            $query  = $this->pdo->prepare("
                SELECT id 
                FROM resources 
                WHERE transaction_id = ?
            ");
            $query->execute([$this->txId]);
            $resIds = $query->fetchAll(PDO::FETCH_COLUMN);

            // find all pre-transaction parents of resources affected by the current transaction
            if (count($resIds) === 0) {
                $this->parentIds = [];
            } else {
                $pdoOld          = RC::$transaction->getPreTransactionDbHandle();
                $pdoOld->query("CREATE TEMPORARY TABLE _res (id bigint)");
                $pdoOld->pgsqlCopyFromArray('_res', $resIds);
                $query           = "
                    WITH RECURSIVE t(id, n) AS (
                        SELECT id, 0 FROM _res
                      UNION
                        SELECT target_id, 1
                        FROM t JOIN relations USING (id)
                        WHERE property = ?
                    )
                    SELECT DISTINCT id FROM t WHERE n > 0
                ";
                $query           = $pdoOld->prepare($query);
                $query->execute([$this->schema->parent]);
                $this->parentIds = $query->fetchAll(PDO::FETCH_COLUMN);
                $pdoOld->query("DROP TABLE _res");
            }
            $t1 = microtime(true);
            $this->log?->debug("\t\t\t" . count($this->parentIds) . " collections found");
            $this->log?->debug("\t\t\ttiming: " . ($t1 - $t0));
        }
        return $this->parentIds;
    }

    private function getPropertyType(string $prop): string | false {
        $propDesc = $this->ontology->getProperty(null, $prop);
        if ($propDesc === null) {
            return false;
        }
        $range = array_filter($propDesc->range, fn($x) => str_starts_with($x, RDF::NMSP_XSD));
        return reset($range);
    }
}
