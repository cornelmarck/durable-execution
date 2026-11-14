import { useAllQueueStats, useQueues } from "../api/hooks";
import { QueuesTable } from "../components/QueuesTable";
import { Card, Empty, ErrorState, Loading } from "../components/ui";

export function Queues() {
  const queues = useQueues();
  const stats = useAllQueueStats(queues.data?.map((q) => q.name) ?? []);

  return (
    <>
      <div className="page-head">
        <div>
          <h1>Queues</h1>
          <p>Run counts and consumer lag per queue.</p>
        </div>
      </div>
      <Card>
        {queues.isPending ? (
          <Loading />
        ) : queues.isError ? (
          <ErrorState error={queues.error} />
        ) : queues.data.length === 0 ? (
          <Empty>No queues yet.</Empty>
        ) : (
          <QueuesTable queues={queues.data} stats={stats.byName} />
        )}
      </Card>
    </>
  );
}
