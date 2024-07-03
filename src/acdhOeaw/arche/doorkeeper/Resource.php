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
use Psr\Log\LoggerInterface;
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
use acdhOeaw\arche\core\Metadata;
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
class Resource {

    const DB_LOCK_TIMEOUT      = 1000;
    const NON_NEGATIVE_NUMBERS = [RDF::XSD_NON_NEGATIVE_INTEGER, RDF::XSD_UNSIGNED_LONG,
        RDF::XSD_UNSIGNED_INT, RDF::XSD_UNSIGNED_SHORT, RDF::XSD_UNSIGNED_BYTE];
    const LITERAL_TYPES        = [RDF::XSD_ANY_URI,
        RDF::XSD_DATE, RDF::XSD_DATE_TIME, RDF::XSD_DECIMAL,
        RDF::XSD_FLOAT, RDF::XSD_DOUBLE, RDF::XSD_INTEGER, RDF::XSD_NEGATIVE_INTEGER,
        RDF::XSD_NON_NEGATIVE_INTEGER, RDF::XSD_NON_POSITIVE_INTEGER, RDF::XSD_POSITIVE_INTEGER,
        RDF::XSD_LONG, RDF::XSD_INT, RDF::XSD_SHORT, RDF::XSD_BYTE, RDF::XSD_UNSIGNED_LONG,
        RDF::XSD_UNSIGNED_INT, RDF::XSD_UNSIGNED_SHORT, RDF::XSD_UNSIGNED_BYTE, RDF::XSD_BOOLEAN];
    use RunTestsTrait;

    static public function onResEdit(int $id, DatasetNodeInterface $meta,
                                     ?string $path): DatasetNodeInterface {
        $pdo = new PDO(RC::$config->dbConn->admin);
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $pdo->query("SET application_name TO doorkeeper");
        $pdo->query("SET lock_timeout TO " . self::DB_LOCK_TIMEOUT);
        $pdo->beginTransaction();

        $schema     = new Schema(RC::$config->schema);
        $cacheFile  = RC::$config->doorkeeper->ontologyCacheFile ?? '';
        $cacheTtl   = RC::$config->doorkeeper->ontologyCacheTtl ?? 600;
        $ontology   = Ontology::factoryDb(RC::$pdo, $schema, $cacheFile, $cacheTtl);
        $doorkeeper = new Resource($meta, $schema, $ontology, $pdo, RC::$log);

        $errors = [];
        foreach ([PreCheckAttribute::class, CheckAttribute::class, PostCheckAttribute::class] as $checkType) {
            if ($checkType === PostCheckAttribute::class && count($errors) > 0) {
                continue;
            }
            try {
                $doorkeeper->runTests($checkType);
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

    private UriNormalizer $uriNorm;

    public function __construct(private DatasetNodeInterface $meta,
                                private Schema $schema,
                                private Ontology $ontology,
                                private PDO | null $pdo = null,
                                private LoggerInterface | null $log = null) {
        $this->uriNorm = new UriNormalizer();
    }

    #[PreCheckAttribute]
    public function pre01MaintainDefaultValues(): void {
        $res = $this->meta->getNode();
        foreach ($this->meta->listObjects(new PT(RDF::RDF_TYPE)) as $class) {
            $c = $this->ontology->getClass($class);
            if ($c === null) {
                continue;
            }
            foreach ($c->properties as $p) {
                if (!empty($p->defaultValue) && $this->meta->none(new PT($p->uri))) {
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
                        $this->meta->add(DF::quad($res, DF::namedNode($p->uri), $val));
                    }
                    $this->log?->info("\t\t$p->uri added with a default value of $p->defaultValue");
                }
            }
        }
    }

    #[PreCheckAttribute]
    public function pre02MaintainWkt(): void {
        $latProp = $this->schema->latitude;
        $lonProp = $this->schema->longitude;
        $wktProp = $this->schema->wkt;
        if ($this->meta->any(new PT($wktProp))) {
            return;
        }
        $lat = (string) $this->meta->getObject(new PT($latProp));
        $lon = (string) $this->meta->getObject(new PT($lonProp));
        if (!empty($lat) && !empty($lon)) {
            $this->meta->add(DF::quadNoSubject($wktProp, DF::literal("POINT($lon $lat)")));
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
     */
    #[PreCheckAttribute]
    public function pre03MaintainAccessRights(): void {
        $accessRestr = (string) $this->meta->getObject(new PT($this->schema->accessRestriction));
        if (empty($accessRestr)) {
            return;
        }

        $propRead     = DF::namedNode(RC::$config->accessControl->schema->read);
        $propRoles    = $this->schema->accessRole;
        $rolePublic   = DF::namedNode(RC::$config->doorkeeper->rolePublic);
        $roleAcademic = DF::namedNode(RC::$config->doorkeeper->roleAcademic);
        $res          = $this->meta->getNode();

        $query          = $this->pdo->prepare("SELECT i1.ids FROM identifiers i1 JOIN identifiers i2 USING (id) WHERE i2.ids = ?");
        $query->execute([$accessRestr]);
        $accessRestrIds = $query->fetchAll(PDO::FETCH_COLUMN);

        $this->log?->info("\t\tmaintaining access rights for " . implode(', ', $accessRestrIds));

        if (in_array('https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/public', $accessRestrIds)) {
            $this->meta->add(DF::quad($res, $propRead, DF::literal($rolePublic)));
            $this->log?->info("\t\t\tpublic");
        } else {
            $this->meta->delete(new PT($propRead, new VT($rolePublic)));
            if (in_array('https://vocabs.acdh.oeaw.ac.at/archeaccessrestrictions/academic', $accessRestrIds)) {
                $this->meta->add(DF::quad($res, $propRead, DF::literal($roleAcademic)));
                $this->log?->info("\t\t\tacademic");
            } else {
                $this->meta->delete(new PT($propRead, new VT($roleAcademic)));
                foreach ($this->meta->getIterator(new PT($propRoles)) as $i) {
                    $this->meta->add($i->withPredicate($propRead));
                }
                $this->log?->info("\t\t\trestricted");
            }
        }
    }

    #[PreCheckAttribute]
    public function pre04MaintainPropertyRange(): void {
        static $checkRangeUris = null;
        if ($checkRangeUris === null) {
            $checkRangeUris = array_keys((array) RC::$config->doorkeeper->checkRanges);
        }

        foreach ($this->meta->listPredicates() as $prop) {
            $propDesc = $this->ontology->getProperty($this->meta, $prop);
            if ($propDesc === null || !is_array($propDesc->range) || count($propDesc->range) === 0) {
                continue;
            }
            // at least as for arche-lib-schema 6.1.0 the range is always a single class
            // (even if identified by all it's ids) so we don't need to worry about
            // range's parent class rules relaxing the check
            $rangesToCheck = array_intersect($propDesc->range, $checkRangeUris);

            if (!empty($propDesc->vocabs)) {
                self::maintainPropertyRangeVocabs($propDesc, $prop);
            } elseif (count($rangesToCheck) > 0) {
                foreach ($rangesToCheck as $i) {
                    self::verifyPropertyRangeUri($i, $prop);
                }
            } else {
                self::maintainPropertyRangeLiteral($propDesc, $prop);
            }
        }
    }

    #[PreCheckAttribute]
    public function pre05NormalizeIds(): void {
        $res = $this->meta->getNode();

        // enforce IDs to be in known namespaces for known classes
        $forceNormalize = false;
        foreach ($this->meta->listObjects(new PT(RDF::RDF_TYPE)) as $class) {
            $c = $this->ontology->getClass($class);
            if ($c !== null) {
                $forceNormalize = true;
                break;
            }
        }

        $idProp = $this->schema->id;
        foreach ($this->meta->listObjects(new PT($idProp)) as $id) {
            $ids = (string) $id;
            try {
                $std = $this->uriNorm->normalize($ids, $forceNormalize);
            } catch (UriNormalizerException $e) {
                throw new DoorkeeperException($e->getMessage());
            }
            if ($std !== $ids) {
                $this->meta->delete(new PT($idProp, $id));
                $this->meta->add(DF::quad($res, $idProp, DF::namedNode($std)));
                $this->log?->info("\t\tid URI $ids standardized to $std");
            }
        }
    }

    /**
     * Every resource must have a title property (cfg.schema.label).
     * 
     * @throws DoorkeeperException
     */
    #[CheckAttribute]
    public function check01TitleProp(): void {
        $titleProp = $this->schema->label;
        $res       = $this->meta->getNode();

        // check existing titles
        $titles = iterator_to_array($this->meta->listObjects(new PT($titleProp)));
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
        if ($this->inArcheCoreContext()) {
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
                        $this->meta->add(DF::quad($res, $titleProp, DF::literal($i->value, $i->lang)));
                    }
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
                foreach ($this->meta->listObjects(new PT($prop)) as $value) {
                    $lang         = $value instanceof LiteralInterface ? $value->getLang() : '';
                    $langs[$lang] = ($langs[$lang] ?? '') . ' ' . (string) $value;
                }
            }
        }

        $count = 0;
        foreach ($langs as $lang => $title) {
            $title = trim($title);
            if (!empty($title)) {
                $this->log?->info("\t\tsetting title to $title");
                $this->meta->add(DF::quad($res, $titleProp, DF::literal($title, (string) $lang)));
                $count++;
            }
        }
        if ($count === 0) {
            throw new DoorkeeperException("$titleProp is missing");
        }
    }

    /**
     * Checks property types (datatype/object).
     * 
     * As the property type doesn't depend on the class context, the check can
     * and should be done for all metadata properties.
     * 
     * @throws DoorkeeperException
     */
    #[CheckAttribute]
    public function check02PropertyTypes(): void {
        foreach ($this->meta->listPredicates() as $p) {
            $pDef = $this->ontology->getProperty([], $p);
            if (is_object($pDef)) {
                if ($pDef->type === RDF::OWL_DATATYPE_PROPERTY && $this->meta->any(new PT($p, new NNT(null, NNT::ANY)))) {
                    throw new DoorkeeperException('URI value for a datatype property ' . $p);
                }
                if ($pDef->type === RDF::OWL_OBJECT_PROPERTY && $this->meta->any(new PT($p, new LT(null, LT::ANY)))) {
                    throw new DoorkeeperException('Literal value for an object property ' . $p);
                }
            }
        }
    }

    /**
     * Checks property cardinalities according to the ontology.
     * 
     * @throws DoorkeeperException
     */
    #[CheckAttribute]
    public function check03Cardinalities(): void {
        //TODO - rewrite so it just iterates each triple once gathering counts of all properties
        $ontologyNmsp = $this->schema->namespaces->ontology;
        $inDomain     = [RDF::RDF_TYPE];
        foreach ($this->meta->listObjects(new PT(RDF::RDF_TYPE)) as $class) {
            $classDef = $this->ontology->getClass((string) $class);
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
                        foreach ($this->meta->getIterator(new PT($i)) as $j) {
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
            $outDomain = array_diff($this->meta->listPredicates()->getValues(), $inDomain); // properties allowed on resource classes
            $owlThing  = $this->ontology->getClass(RDF::OWL_THING);
            $outDomain = array_diff($outDomain, array_keys($owlThing->properties)); // properties allowed on all resources
            if (count($outDomain) > 0) {
                throw new DoorkeeperException("Properties with a wrong domain: " . implode(', ', $outDomain));
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
     * @throws DoorkeeperException
     */
    #[CheckAttribute]
    public function check04IdCount(): void {
        $idProp       = $this->schema->id;
        $repoNmsp     = $this->inArcheCoreContext() ? RC::getBaseUrl() : '~';
        $ontologyNmsp = $this->schema->namespaces->ontology;

        $ontologyIdCount = $repoIdCount     = $nonRepoIdCount  = 0;
        $ids             = $this->meta->listObjects(new PT($idProp))->getValues();
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

    #[CheckAttribute]
    public function check05Language(): void {
        foreach ($this->meta->listPredicates() as $prop) {
            $p = $this->ontology->getProperty([], $prop);
            if (is_object($p) && $p->langTag) {
                $value = $this->meta->getObject(new PT($prop, new NOT(new LT(null, VT::ANY, ''))));
                if ($value !== null) {
                    throw new DoorkeeperException("Property $prop with value " . (string) $value . " is not tagged with a language");
                }
            }
        }
    }

    /**
     * If a property is in the ontology namespace it has to be part of the ontology.
     */
    #[CheckAttribute]
    public function check06UnknownProperties(): void {
        if ($this->inArcheCoreContext() && RC::$config->doorkeeper->checkUnknownProperties === false) {
            return;
        }
        $idProp = $this->schema->id;
        $nmsp   = $this->schema->namespaces->ontology;
        foreach ($this->meta->listObjects(new PT($idProp))->getValues() as $i) {
            if (str_starts_with($i, $nmsp)) {
                return; // apply on non-ontology resources only
            }
        }
        foreach ($this->meta->listPredicates()->getValues() as $p) {
            if (str_starts_with($p, $nmsp) && $this->ontology->getProperty([], $p) === null) {
                throw new DoorkeeperException("property $p is in the ontology namespace but is not included in the ontology");
            }
        }
    }

    #[CheckAttribute]
    public function check07Biblatex(): void {
        $biblatexProp = $this->schema->biblatex ?? '';
        $biblatex     = trim((string) $this->meta->getObject(new PT($biblatexProp)));
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
     * 
     * @throws DoorkeeperException
     */
    #[PostCheckAttribute]
    public function post01MaintainCmdiPid(): void {
        $cfg         = RC::$config->doorkeeper->epicPid;
        $cmdiPidProp = $this->schema->cmdiPid;
        $pidProp     = $this->schema->pid;
        $pidNmsp     = (string) $this->schema->namespaces->cmdi;
        $setProp     = $cfg->clarinSetProperty;
        $idProp      = $this->schema->id;
        $idNmsp      = RC::getBaseUrl();
        if ($this->meta->any(new PT($cmdiPidProp)) || $this->meta->none(new PT($setProp))) {
            // CMDI PID exists or OAI-PMH set property doesn't exist - nothing to do
            return;
        }

        $query      = $this->pdo->prepare("SELECT i1.ids FROM identifiers i1 JOIN identifiers i2 USING (id) WHERE i2.ids = ?");
        $query->execute([$cfg->clarinSet]);
        $clarinSets = $query->fetchAll(PDO::FETCH_COLUMN);

        $sets = $this->meta->listObjects(new PT($setProp))->getValues();
        if (count(array_intersect($sets, $clarinSets)) > 0) {
            if (empty($cfg->pswd)) {
                $this->log?->info("\t\tskipping CMDI PID generation - no EPIC password provided");
                return;
            }
            $res = $this->meta->getNode();
            $id  = null;
            $id  = (string) $this->meta->getObject(new PT($idProp, new VT($idNmsp, VT::STARTS)));
            if (!empty($id)) {
                $id      = $pidNmsp . substr((string) $id, strlen($idNmsp));
                $ps      = new HandleService($cfg->url, $cfg->prefix, $cfg->user, $cfg->pswd);
                $cmdiPid = $ps->create($id);
                $cmdiPid = str_replace($cfg->url, $cfg->resolver, $cmdiPid);
                $this->log?->info("\t\tregistered CMDI PID $cmdiPid pointing to " . $id);
                $this->meta->add(DF::quad($res, $cmdiPidProp, DF::literal($cmdiPid, null, RDF::XSD_ANY_URI)));
                $this->meta->add(DF::quad($res, $this->schema->id, DF::literal($id)));
            }
            // if normal PID is missing, trigger its generation
            if ($this->meta->none(new PT($pidProp))) {
                $this->meta->add(DF::quad($res, $pidProp, DF::literal("create")));
            }
        }
    }

    #[PostCheckAttribute]
    public function post02MaintainPid(): void {
        $cfg      = RC::$config->doorkeeper->epicPid;
        $idProp   = $this->schema->id;
        $idNmsp   = (string) $this->schema->namespaces->id;
        $pidProp  = $this->schema->pid;
        $propRead = RC::$config->accessControl->schema->read;
        $pidTmpl  = new PT($pidProp);
        $res      = $this->meta->getNode();
        $ids      = $this->meta->listObjects(new PT($idProp))->getValues();

        $classesAlways        = [
            $this->schema->classes->topCollection,
            $this->schema->classes->collection,
        ];
        $classesNotRestricted = [
            $this->schema->classes->resource,
            $this->schema->classes->metadata,
        ];
        $rolesNotRestricted   = [
            RC::$config->doorkeeper->rolePublic,
            RC::$config->doorkeeper->roleAcademic,
        ];
        $class                = $this->meta->listObjects(new PT(RDF::RDF_TYPE))->getValues();
        $roles                = $this->meta->listObjects(new PT($propRead))->getValues();
        $public               = count(array_intersect($class, $classesAlways)) > 0 ||
            count(array_intersect($class, $classesNotRestricted)) > 0 && count(array_intersect($roles, $rolesNotRestricted)) > 0;

        $idSubNmsps = [];
        foreach ($this->schema->namespaces as $i) {
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
        $pidLit = $this->meta->getObject($pidTmpl);
        $pidLit = $pidLit !== null ? (string) $pidLit : null;
        if (($pidLit === $cfg->createValue || $public) && $id !== null) {
            if (empty($cfg->pswd)) {
                $this->log?->info("\t\tskipping PID (re)generation - no EPIC password provided");
                return;
            }
            $ps = new HandleService($cfg->url, $cfg->prefix, $cfg->user, $cfg->pswd);
            if ($curPid === null) {
                $this->meta->delete($pidTmpl);
                $pid = $ps->create($id);
                $pid = str_replace($cfg->url, $cfg->resolver, $pid);
                $this->log?->info("\t\tregistered PID $pid pointing to " . $id);
                $this->meta->add(DF::Quad($res, $pidProp, DF::literal($pid, null, RDF::XSD_ANY_URI)));
            } else {
                $this->meta->delete($pidTmpl);
                $this->meta->add(DF::quad($res, $pidProp, DF::literal($curPid, null, RDF::XSD_ANY_URI)));
                $pid = str_replace($cfg->resolver, $cfg->url, $curPid);
                $ret = $ps->update($pid, $id);
                $this->log?->info("\t\trecreated PID $curPid pointing to " . $id . " with return code " . $ret);
            }
        }
        // standardize PID if needed
        if ($pidLit !== null) {
            if (!isset($this->uriNorm)) {
                $this->uriNorm = new UriNormalizer();
            }
            $stdPid = $this->uriNorm->normalize($pidLit, false);
            if ($stdPid !== $pidLit) {
                $this->meta->delete($pidTmpl);
                $this->meta->add(DF::quad($res, $pidProp, DF::literal($stdPid, null, RDF::XSD_ANY_URI)));
                $this->log?->info("\t\tPID $pidLit standardized to $stdPid");
            }
        }
        // promote PIDs to IDs
        foreach ($this->meta->getIterator($pidTmpl) as $i) {
            $i = (string) $i->getObject();
            if ($i === '') {
                throw new DoorkeeperException("Empty PID");
            }
            if ($i !== $cfg->createValue && !in_array($i, $ids)) {
                $this->log?->info("\t\tpromoting PID $i to an id");
                $this->meta->add(DF::quad($res, $idProp, DF::literal($i)));
            }
        }
    }

    private function maintainPropertyRangeVocabs(
        PropertyDesc $propDesc, NamedNodeInterface $prop): void {
        if (RC::$config->doorkeeper->checkVocabularyValues === false) {
            return;
        }
        $res = $this->meta->getNode();
        foreach ($this->meta->listObjects(new PT($prop)) as $v) {
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
                $this->meta->delete(new PT($prop, $v));
                $this->meta->add(DF::quad($res, $prop, DF::namedNode($vid)));
                $this->log?->info("\t\tproperty $prop mapping literal value '$vs' to resource $vid");
            }
        }
    }

    private function maintainPropertyRangeLiteral(
        PropertyDesc $propDesc, NamedNodeInterface $prop): void {
        $res   = $this->meta->getNode();
        $range = $propDesc->range;
        foreach ($this->meta->listObjects(new PT($prop, new LT(null, LT::ANY))) as $l) {
            $type = $l instanceof LiteralInterface ? $l->getDatatype() : null;
            if (in_array($type, $range)) {
                continue;
            }
            if (in_array(RDF::XSD_STRING, $range)) {
                $this->meta->delete(new PT($prop, $l));
                $this->meta->add(DF::quad($res, $prop, DF::literal((string) $l)));
                $this->log?->debug("\t\tcasting $prop value from $type to string");
            } else {
                try {
                    $rangeTmp = array_intersect($range, self::LITERAL_TYPES);
                    $rangeTmp = (reset($rangeTmp) ?: reset($range)) ?: RDF::XSD_STRING;
                    $value    = self::castLiteral($l, $rangeTmp);
                    $this->meta->delete(new PT($prop, $l));
                    $this->meta->add(DF::quad($res, $prop, $value));
                    $this->log?->debug("\t\tcasting $prop value from $type to $rangeTmp");
                } catch (RuntimeException $ex) {
                    $this->log?->info("\t\t" . $ex->getMessage());
                } catch (DoorkeeperException $ex) {
                    throw new DoorkeeperException("property $prop: " . $ex->getMessage(), $ex->getCode(), $ex);
                }
            }
        }
    }

    private function verifyPropertyRangeUri(
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
        foreach ($this->meta->listObjects(new PT($prop)) as $obj) {
            if (!($obj instanceof NamedNodeInterface)) {
                throw new DoorkeeperException("property $prop has a literal value");
            }
            try {
                $norm->resolve((string) $obj);
                $this->log?->debug("\t\t$prop value $obj resolved successfully");

                $objNorm = $norm->normalize((string) $obj);
                if ($objNorm !== (string) $obj) {
                    $objNorm = DF::namedNode($objNorm);
                    $this->meta->forEach(fn($q) => $q->withObject($objNorm), new PT($prop, $obj));
                }
            } catch (UriNormalizerException $ex) {
                throw new DoorkeeperException($ex->getMessage(), $ex->getCode(), $ex);
            }
        }
    }

    private function castLiteral(LiteralInterface $l, string $range): LiteralInterface {
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

    private function inArcheCoreContext(): bool {
        return class_exists(RC::class);
    }
}
