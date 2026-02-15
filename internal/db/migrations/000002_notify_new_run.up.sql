CREATE OR REPLACE FUNCTION notify_new_run() RETURNS trigger AS $$
DECLARE
    queue TEXT;
BEGIN
    -- Only notify on INSERT or when status changes to 'pending' (e.g. woken from sleep)
    IF TG_OP = 'UPDATE' AND NEW.status != 'pending' THEN
        RETURN NEW;
    END IF;

    SELECT q.name INTO queue
    FROM tasks t JOIN queues q ON q.id = t.queue_id
    WHERE t.id = NEW.task_id;

    PERFORM pg_notify('new_run', queue);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_notify_new_run
    AFTER INSERT OR UPDATE ON runs
    FOR EACH ROW
    EXECUTE FUNCTION notify_new_run();
