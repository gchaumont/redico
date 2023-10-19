<?php

namespace Redico\Index;

use Illuminate\Support\Collection;
use Redico\Index\Fields\Field;
use Redico\Exceptions\NoFieldsInIndexException;
use Redico\Exceptions\UnknownIndexNameException;

/**
 * Defines the configuration of a Redis index.
 */
class Index
{
    public string $on = 'HASH'; // HASH | JSON

    public null|string $language = null;

    private bool $noOffsetsEnabled = false;

    private bool $noFieldsEnabled = false;

    private bool $noFrequenciesEnabled = false;

    private null|array $stopWords = null;

    private array|null $prefixes = null;

    public function __construct(
        public string $name,
        public array $fields,
    ) {
    }

    public function getDefinition(): array
    {
        $properties = ['name' => $this->name];

        if (!is_null($this->prefixes)) {
            $properties[] = 'PREFIX';
            $properties[] = count($this->prefixes);
            $properties = array_merge($properties, $this->prefixes);
        }
        if ($this->isNoOffsetsEnabled()) {
            $properties[] = 'NOOFFSETS';
        }
        if ($this->isNoFieldsEnabled()) {
            $properties[] = 'NOFIELDS';
        }
        if ($this->isNoFrequenciesEnabled()) {
            $properties[] = 'NOFREQS';
        }
        if (!is_null($this->stopWords)) {
            $properties[] = 'STOPWORDS';
            $properties[] = count($this->stopWords);
            $properties = array_merge($properties, $this->stopWords);
        }
        $properties[] = 'SCHEMA';

        $fieldDefinitions = [];
        foreach ($this->fields as $field) {
            if ($field instanceof Field) {
                $fieldDefinitions = array_merge($fieldDefinitions, $field->getDefinition());
            }
        }

        if (count($fieldDefinitions) === 0) {
            throw new NoFieldsInIndexException();
        }

        return array_values(array_merge($properties, $fieldDefinitions));
    }

    public function getName(): string
    {
        return $this->name;
    }


    /**
     * @return array
     */
    public function getFields(): Collection
    {
        $fields = [];
        foreach ($this->fields as $field) {
            if ($field instanceof Field) {
                $fields[$field->getName()] = clone $field;
            }
        }
        return collect($fields);
    }

    public function addField(Field $field): static
    {
        $this->fields[] = $field;

        return $this;
    }


    /**
     * @return bool
     */
    public function exists(): bool
    {
        try {
            $this->info();
            return true;
        } catch (UnknownIndexNameException $exception) {
            return false;
        }
    }

    /**
     * @param string $name
     * @return array
     */
    public function tagValues(string $name): array
    {
        return $this->rawCommand('FT.TAGVALS', [$this->getIndexName(), $name]);
    }

    /**
     * @return mixed
     */
    public function drop()
    {
        return $this->rawCommand('FT.DROP', [$this->getIndexName()]);
    }

    /**
     * @return mixed
     */
    public function info()
    {
        return $this->rawCommand('FT.INFO', [$this->getIndexName()]);
    }

    /**
     * @param $id
     * @param bool $deleteDocument
     * @return bool
     */
    public function delete($id, $deleteDocument = false)
    {
        $arguments = [$this->getIndexName(), $id];
        if ($deleteDocument) {
            $arguments[] = 'DD';
        }
        return boolval($this->rawCommand('FT.DEL', $arguments));
    }

    /**
     * @param null $id
     * @return DocumentInterface
     * @throws Exceptions\FieldNotInSchemaException
     */
    public function makeDocument($id = null): DocumentInterface
    {
        $fields = $this->getFields();
        $document = AbstractDocumentFactory::makeFromArray($fields, $fields, $id);
        return $document;
    }

    /**
     * @return AggregateBuilderInterface
     */
    public function makeAggregateBuilder(): AggregateBuilderInterface
    {
        return new AggregateBuilder($this->getRedisClient(), $this->getIndexName());
    }

    /**
     * @return RediSearchRedisClient
     */
    public function getRedisClient(): RediSearchRedisClient
    {
        return $this->redisClient;
    }

    /**
     * @param RediSearchRedisClient $redisClient
     * @return IndexInterface
     */
    public function setRedisClient(RediSearchRedisClient $redisClient): IndexInterface
    {
        $this->redisClient = $redisClient;
        return $this;
    }


    /**
     * @return bool
     */
    public function isNoOffsetsEnabled(): bool
    {
        return $this->noOffsetsEnabled;
    }

    /**
     * @param bool $noOffsetsEnabled
     * @return IndexInterface
     */
    public function noOffsetsEnabled(bool $noOffsetsEnabled): static
    {
        $this->noOffsetsEnabled = $noOffsetsEnabled;
        return $this;
    }

    /**
     * @return bool
     */
    public function isNoFieldsEnabled(): bool
    {
        return $this->noFieldsEnabled;
    }

    /**
     * @param bool $noFieldsEnabled
     * @return IndexInterface
     */
    public function noFieldsEnabled(bool $noFieldsEnabled): static
    {
        $this->noFieldsEnabled = $noFieldsEnabled;

        return $this;
    }

    /**
     * @return bool
     */
    public function isNoFrequenciesEnabled(): bool
    {
        return $this->noFrequenciesEnabled;
    }


    public function noFrequenciesEnabled(bool $noFrequenciesEnabled): static
    {
        $this->noFrequenciesEnabled = $noFrequenciesEnabled;

        return $this;
    }

    public function stopWords(array $stopWords = []): static
    {
        $this->stopWords = $stopWords;

        return $this;
    }


    public function prefixes(array $prefixes = []): static
    {
        $this->prefixes = $prefixes;

        return $this;
    }

    /**
     * @return QueryBuilder
     */
    protected function makeQueryBuilder(): QueryBuilder
    {
        return (new QueryBuilder($this->redisClient, $this->getIndexName()));
    }

    /**
     * @param string $fieldName
     * @param array $values
     * @param array|null $charactersToEscape
     * @return QueryBuilderInterface
     */
    public function tagFilter(string $fieldName, array $values, array $charactersToEscape = null): QueryBuilderInterface
    {
        return $this->makeQueryBuilder()->tagFilter($fieldName, $values, $charactersToEscape);
    }

    /**
     * @param string $fieldName
     * @param $min
     * @param $max
     * @return QueryBuilderInterface
     */
    public function numericFilter(string $fieldName, $min, $max = null): QueryBuilderInterface
    {
        return $this->makeQueryBuilder()->numericFilter($fieldName, $min, $max);
    }

    /**
     * @param string $fieldName
     * @param float $longitude
     * @param float $latitude
     * @param float $radius
     * @param string $distanceUnit
     * @return QueryBuilderInterface
     */
    public function geoFilter(string $fieldName, float $longitude, float $latitude, float $radius, string $distanceUnit = 'km'): QueryBuilderInterface
    {
        return $this->makeQueryBuilder()->geoFilter($fieldName, $longitude, $latitude, $radius, $distanceUnit);
    }

    /**
     * @param string $fieldName
     * @param $order
     * @return QueryBuilderInterface
     */
    public function sortBy(string $fieldName, $order = 'ASC'): QueryBuilderInterface
    {
        return $this->makeQueryBuilder()->sortBy($fieldName, $order);
    }

    /**
     * @param string $scoringFunction
     * @return QueryBuilderInterface
     */
    public function scorer(string $scoringFunction): QueryBuilderInterface
    {
        return $this->makeQueryBuilder()->scorer($scoringFunction);
    }

    /**
     * @param string $languageName
     * @return QueryBuilderInterface
     */
    public function language(string $languageName): QueryBuilderInterface
    {
        return $this->makeQueryBuilder()->language($languageName);
    }

    /**
     * @param string $query
     * @return string
     */
    public function explain(string $query): string
    {
        return $this->makeQueryBuilder()->explain($query);
    }

    /**
     * @param string $query
     * @param bool $documentsAsArray
     * @return SearchResult
     * @throws \Ehann\RedisRaw\Exceptions\RedisRawCommandException
     */
    public function search(string $query = '', bool $documentsAsArray = false): SearchResult
    {
        return $this->makeQueryBuilder()->search($query, $documentsAsArray);
    }

    /**
     * @return QueryBuilderInterface
     */
    public function noContent(): QueryBuilderInterface
    {
        return $this->makeQueryBuilder()->noContent();
    }

    /**
     * @param int $offset
     * @param int $pageSize
     * @return QueryBuilderInterface
     */
    public function limit(int $offset, int $pageSize = 10): QueryBuilderInterface
    {
        return $this->makeQueryBuilder()->limit($offset, $pageSize);
    }

    /**
     * @param int $number
     * @param array $fields
     * @return QueryBuilderInterface
     */
    public function inFields(int $number, array $fields): QueryBuilderInterface
    {
        return $this->makeQueryBuilder()->inFields($number, $fields);
    }

    /**
     * @param int $number
     * @param array $keys
     * @return QueryBuilderInterface
     */
    public function inKeys(int $number, array $keys): QueryBuilderInterface
    {
        return $this->makeQueryBuilder()->inKeys($number, $keys);
    }

    /**
     * @param int $slop
     * @return QueryBuilderInterface
     */
    public function slop(int $slop): QueryBuilderInterface
    {
        return $this->makeQueryBuilder()->slop($slop);
    }

    /**
     * @return QueryBuilderInterface
     */
    public function noStopWords(): QueryBuilderInterface
    {
        return $this->makeQueryBuilder()->noStopWords();
    }

    /**
     * @return QueryBuilderInterface
     */
    public function withPayloads(): QueryBuilderInterface
    {
        return $this->makeQueryBuilder()->withPayloads();
    }

    /**
     * @return QueryBuilderInterface
     */
    public function withScores(): QueryBuilderInterface
    {
        return $this->makeQueryBuilder()->withScores();
    }

    /**
     * @return QueryBuilderInterface
     */
    public function verbatim(): QueryBuilderInterface
    {
        return $this->makeQueryBuilder()->verbatim();
    }

    /**
     * @param array $documents
     * @param bool $disableAtomicity
     * @param bool $replace
     */
    public function addMany(array $documents, $disableAtomicity = false, $replace = false)
    {
        $result = null;

        $pipe = $this->redisClient->multi($disableAtomicity);
        foreach ($documents as $document) {
            if (is_array($document)) {
                $document = $this->arrayToDocument($document);
            }
            $this->_add($document->setReplace($replace));
        }
        try {
            $pipe->exec();
        } catch (RedisException $exception) {
            $result = $exception->getMessage();
        } catch (RawCommandErrorException $exception) {
            $result = $exception->getPrevious()->getMessage();
        }

        if ($result) {
            $this->redisClient->validateRawCommandResults($result, 'PIPE', [$this->indexName, '*MANY']);
        }
    }

    /**
     * @param DocumentInterface $document
     * @param bool $isFromHash
     * @return mixed
     */
    protected function _add(DocumentInterface $document, bool $isFromHash = false)
    {
        if (is_null($document->getId())) {
            $document->setId(uniqid(true));
        }

        $properties = $isFromHash ?
            $document->getHashDefinition($this->prefixes) :
            $document->getDefinition();
        if (!$isFromHash) {
            array_unshift($properties, $this->getIndexName());
        }

        $command = $isFromHash ? 'HSET' : 'FT.ADD';
        return $this->rawCommand($command, $properties);
    }

    /**
     * @param $document
     * @return DocumentInterface
     * @throws Exceptions\FieldNotInSchemaException
     */
    public function arrayToDocument($document): DocumentInterface
    {
        return is_array($document) ? AbstractDocumentFactory::makeFromArray($document, $this->getFields()) : $document;
    }

    /**
     * @param $document
     * @return bool
     * @throws Exceptions\FieldNotInSchemaException
     */
    public function add($document): bool
    {
        return $this->_add($this->arrayToDocument($document));
    }

    /**
     * @param $document
     * @return bool
     * @throws Exceptions\FieldNotInSchemaException
     */
    public function replace($document): bool
    {
        return $this->_add($this->arrayToDocument($document)->setReplace(true));
    }

    /**
     * @param array $documents
     * @param bool $disableAtomicity
     */
    public function replaceMany(array $documents, $disableAtomicity = false)
    {
        $this->addMany($documents, $disableAtomicity, true);
    }

    /**
     * @param $document
     * @return bool
     * @throws Exceptions\FieldNotInSchemaException
     */
    public function addHash($document): bool
    {
        $typedDocument = $this->arrayToDocument($document);
        return $this->_add($typedDocument, true);
    }

    /**
     * @param $document
     * @return bool
     * @throws Exceptions\FieldNotInSchemaException
     */
    public function replaceHash($document): bool
    {
        return $this->_add($this->arrayToDocument($document)->setReplace(true), true);
    }

    /**
     * @param array $fields
     * @return QueryBuilderInterface
     */
    public function return(array $fields): QueryBuilderInterface
    {
        return $this->makeQueryBuilder()->return($fields);
    }

    /**
     * @param array $fields
     * @param int $fragmentCount
     * @param int $fragmentLength
     * @param string $separator
     * @return QueryBuilderInterface
     */
    public function summarize(array $fields, int $fragmentCount = 3, int $fragmentLength = 50, string $separator = '...'): QueryBuilderInterface
    {
        return $this->makeQueryBuilder()->summarize($fields, $fragmentCount, $fragmentLength, $separator);
    }

    /**
     * @param array $fields
     * @param string $openTag
     * @param string $closeTag
     * @return QueryBuilderInterface
     */
    public function highlight(array $fields, string $openTag = '<strong>', string $closeTag = '</strong>'): QueryBuilderInterface
    {
        return $this->makeQueryBuilder()->highlight($fields, $openTag, $closeTag);
    }

    /**
     * @param string $expander
     * @return QueryBuilderInterface
     */
    public function expander(string $expander): QueryBuilderInterface
    {
        return $this->makeQueryBuilder()->expander($expander);
    }

    /**
     * @param string $payload
     * @return QueryBuilderInterface
     */
    public function payload(string $payload): QueryBuilderInterface
    {
        return $this->makeQueryBuilder()->payload($payload);
    }

    /**
     * @param string $query
     * @return int
     */
    public function count(string $query = ''): int
    {
        return $this->makeQueryBuilder()->count($query);
    }

    /**
     * @param string $name
     * @return bool
     */
    public function addAlias(string $name): bool
    {
        return $this->rawCommand('FT.ALIASADD', [$name, $this->getIndexName()]);
    }

    /**
     * @param string $name
     * @return bool
     */
    public function updateAlias(string $name): bool
    {
        return $this->rawCommand('FT.ALIASUPDATE', [$name, $this->getIndexName()]);
    }

    /**
     * @param string $name
     * @return bool
     */
    public function deleteAlias(string $name): bool
    {
        return $this->rawCommand('FT.ALIASDEL', [$name]);
    }
}
