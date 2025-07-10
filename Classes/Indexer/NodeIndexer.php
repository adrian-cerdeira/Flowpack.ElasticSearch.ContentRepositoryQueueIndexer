<?php
declare(strict_types=1);

namespace Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\Indexer;

/*
 * This file is part of the Flowpack.ElasticSearch.ContentRepositoryQueueIndexer package.
 *
 * (c) Contributors of the Neos Project - www.neos.io
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Flowpack\ElasticSearch\ContentRepositoryAdaptor;
use Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\Command\NodeIndexQueueCommandController;
use Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\IndexingJob;
use Flowpack\ElasticSearch\ContentRepositoryQueueIndexer\RemovalJob;
use Flowpack\JobQueue\Common\Job\JobManager;
use Neos\ContentRepository\Domain\Model\NodeInterface;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Persistence\PersistenceManagerInterface;

/**
 * NodeIndexer for use in batch jobs
 *
 * @Flow\Scope("singleton")
 */
class NodeIndexer extends ContentRepositoryAdaptor\Indexer\NodeIndexer
{
    /**
     * @var JobManager
     * @Flow\Inject
     */
    protected $jobManager;

    /**
     * @var PersistenceManagerInterface
     * @Flow\Inject
     */
    protected $persistenceManager;

    /**
     * @var bool
     * @Flow\InjectConfiguration(path="enableLiveAsyncIndexing")
     */
    protected $enableLiveAsyncIndexing;
    #[\Neos\Flow\Annotations\Inject]
    protected \Neos\ContentRepositoryRegistry\ContentRepositoryRegistry $contentRepositoryRegistry;

    /**
     * @param \Neos\ContentRepository\Core\Projection\ContentGraph\Node $node
     * @param string|null $targetWorkspaceName In case indexing is triggered during publishing, a target workspace name will be passed in
     * @throws ContentRepositoryAdaptor\Exception
     */
    public function indexNode(\Neos\ContentRepository\Core\Projection\ContentGraph\Node $node, $targetWorkspaceName = null): void
    {
        // TODO 9.0 migration: !! Node::isRemoved() - the new CR *never* returns removed nodes; so you can simplify your code and just assume removed == FALSE in all scenarios.

        if( $node->isRemoved() ){
            $this->removeNode($node, $targetWorkspaceName);
            return;
        }
        if ($this->enableLiveAsyncIndexing !== true) {
            parent::indexNode($node, $targetWorkspaceName);

            return;
        }

        if ($this->settings['indexAllWorkspaces'] === false) {
            if ($targetWorkspaceName !== null && $targetWorkspaceName !== 'live') {
                return;
            }

            if ($targetWorkspaceName === null && $node->workspaceName !== 'live') {
                return;
            }
        }

        $indexingJob = new IndexingJob($this->indexNamePostfix, $targetWorkspaceName, $this->nodeAsArray($node));
        $this->jobManager->queue(NodeIndexQueueCommandController::LIVE_QUEUE_NAME, $indexingJob);
    }

    /**
     * @param \Neos\ContentRepository\Core\Projection\ContentGraph\Node $node
     * @param string|null $targetWorkspaceName In case indexing is triggered during publishing, a target workspace name will be passed in
     * @throws ContentRepositoryAdaptor\Exception
     * @throws \Flowpack\ElasticSearch\Exception
     * @throws \Neos\Flow\Persistence\Exception\IllegalObjectTypeException
     * @throws \Neos\Utility\Exception\FilesException
     */
    public function removeNode(\Neos\ContentRepository\Core\Projection\ContentGraph\Node $node, string $targetWorkspaceName = null): void
    {
        if ($this->enableLiveAsyncIndexing !== true) {
            parent::removeNode($node, $targetWorkspaceName);

            return;
        }

        if ($this->settings['indexAllWorkspaces'] === false) {
            if ($targetWorkspaceName !== null && $targetWorkspaceName !== 'live') {
                return;
            }

            if ($targetWorkspaceName === null && $node->workspaceName !== 'live') {
                return;
            }
        }

        $dimensionCombinations = $this->dimensionService->getDimensionCombinationsForIndexing($node);
        // TODO 9.0 migration: !! Node::getWorkspace() does not make sense anymore concept-wise. In Neos < 9, it pointed to the workspace where the node was *at home at*. Now, the closest we have here is the node identity.

        $targetWorkspaceName = $targetWorkspaceName ?? $node->getWorkspace()->getName();

        if (array_filter($dimensionCombinations) === []) {
            $removalJob = new RemovalJob($this->indexNamePostfix, $targetWorkspaceName, $this->nodeAsArray($node));
            $this->jobManager->queue(NodeIndexQueueCommandController::LIVE_QUEUE_NAME, $removalJob);
        } else {
            foreach ($dimensionCombinations as $combination) {

                // TODO 9.0 migration: Check if you could change your code to work with the NodeAggregateId value object instead.

                $nodeFromContext = $this->createContentContext($targetWorkspaceName, $combination)->getNodeByIdentifier($node->aggregateId->value);
                // TODO 9.0 migration: !! Node::isRemoved() - the new CR *never* returns removed nodes; so you can simplify your code and just assume removed == FALSE in all scenarios.

                if ($nodeFromContext instanceof \Neos\ContentRepository\Core\Projection\ContentGraph\Node && !$nodeFromContext->isRemoved()) {
                    continue;
                }
                // TODO 9.0 migration: !! Node::getWorkspace() does not make sense anymore concept-wise. In Neos < 9, it pointed to the workspace where the node was *at home at*. Now, the closest we have here is the node identity.

                // TODO 9.0 migration: !! Node::getWorkspace() does not make sense anymore concept-wise. In Neos < 9, it pointed to the workspace where the node was *at home at*. Now, the closest we have here is the node identity.
                $subgraph = $this->contentRepositoryRegistry->subgraphForNode($node);
                // TODO 9.0 migration: Try to remove the (string) cast and make your code more type-safe.

                // TODO 9.0 migration: Try to remove the (string) cast and make your code more type-safe.


                $fakeNodeArray = [
                    'persistenceObjectIdentifier' => 'fake',
                    'workspace' => $node->getWorkspace()->getName(),
                    'path' => (string) $subgraph->findNodePath($node->aggregateId),
                    'identifier' => $node->aggregateId->value,
                    'nodeType' => $node->nodeTypeName->value,
                    'dimensions' => $combination
                ];

                $removalJob = new RemovalJob($this->indexNamePostfix, $targetWorkspaceName, [$fakeNodeArray]);
                $this->jobManager->queue(NodeIndexQueueCommandController::LIVE_QUEUE_NAME, $removalJob);
            }
        }
    }

    /**
     * Returns an array of data from the node for use as job payload.
     *
     * @param \Neos\ContentRepository\Core\Projection\ContentGraph\Node $node
     * @return array
     */
    protected function nodeAsArray(\Neos\ContentRepository\Core\Projection\ContentGraph\Node $node): array
    {
        // TODO 9.0 migration: !! Node::getNodeData() - the new CR is not based around the concept of NodeData anymore. You need to rewrite your code here.

        // TODO 9.0 migration: !! Node::getNodeData() - the new CR is not based around the concept of NodeData anymore. You need to rewrite your code here.

        // TODO 9.0 migration: !! Node::getNodeData() - the new CR is not based around the concept of NodeData anymore. You need to rewrite your code here.

        // TODO 9.0 migration: !! Node::getNodeData() - the new CR is not based around the concept of NodeData anymore. You need to rewrite your code here.
        $subgraph = $this->contentRepositoryRegistry->subgraphForNode($node);
        // TODO 9.0 migration: Try to remove the (string) cast and make your code more type-safe.

        return [
            [
                'persistenceObjectIdentifier' => $this->persistenceManager->getIdentifierByObject($node->getNodeData()),
                'identifier' => $node->aggregateId->value,
                'dimensions' => $node->getContext()->getDimensions(),
                'workspace' => $node->getWorkspace()->getName(),
                'nodeType' => $node->nodeTypeName->value,
                'path' => (string) $subgraph->findNodePath($node->aggregateId)
            ]
        ];
    }
}
