CREATE OR REPLACE FUNCTION notify_new_run() RETURNS trigger AS $$
DECLARE
    queue TEXT;
BEGIN
    SELECT q.name INTO queue
    FROM tasks t JOIN queues q ON q.id = t.queue_id
    WHERE t.id = NEW.task_id;

    PERFORM pg_notify('new_run', queue);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_notify_new_run
    AFTER INSERT ON runs
    FOR EACH ROW
    EXECUTE FUNCTION notify_new_run();
