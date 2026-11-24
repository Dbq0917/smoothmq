package sqlite

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/poundifdef/smoothmq/config"
	"github.com/poundifdef/smoothmq/models"
)

func newTestQueue(t *testing.T) (*SQLiteQueue, string) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "queue.sqlite")
	q := NewSQLiteQueue(config.SQLiteConfig{Path: path})
	t.Cleanup(func() {
		if err := q.Shutdown(); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})

	return q, path
}

func createTestQueue(t *testing.T, q *SQLiteQueue) {
	t.Helper()

	err := q.CreateQueue(1, models.QueueProperties{
		Name:              "orders",
		RateLimit:         -1,
		MaxRetries:        -1,
		VisibilityTimeout: 30,
	})
	if err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
}

func countRows(t *testing.T, q *SQLiteQueue, table string) int {
	t.Helper()

	var count int
	if err := q.DBG.Raw("SELECT COUNT(*) FROM " + table).Scan(&count).Error; err != nil {
		t.Fatalf("count %s: %v", table, err)
	}

	return count
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}

	t.Fatal("condition was not met before timeout")
}

func TestDelayedMessageDoesNotEnterReadyQueueEarly(t *testing.T) {
	q, _ := newTestQueue(t)
	createTestQueue(t, q)

	delayedID, err := q.Enqueue(1, "orders", "cancel-order", map[string]string{"order_id": "42"}, 3600)
	if err != nil {
		t.Fatalf("Enqueue delayed: %v", err)
	}

	readyID, err := q.Enqueue(1, "orders", "normal", nil, 0)
	if err != nil {
		t.Fatalf("Enqueue ready: %v", err)
	}

	if got := countRows(t, q, "messages"); got != 1 {
		t.Fatalf("ready queue has %d messages, want 1", got)
	}
	if got := countRows(t, q, "delayed_messages"); got != 1 {
		t.Fatalf("delayed store has %d messages, want 1", got)
	}

	stats := q.Stats(1, "orders")
	if stats.TotalMessages != 2 {
		t.Fatalf("total messages = %d, want 2", stats.TotalMessages)
	}
	if stats.Counts[models.MessageStatusQueued] != 1 {
		t.Fatalf("queued messages = %d, want 1", stats.Counts[models.MessageStatusQueued])
	}
	if stats.Counts[models.MessageStatusDelayed] != 1 {
		t.Fatalf("delayed messages = %d, want 1", stats.Counts[models.MessageStatusDelayed])
	}

	messages, err := q.Dequeue(1, "orders", 1, -1)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if len(messages) != 1 || messages[0].ID != readyID {
		t.Fatalf("Dequeue returned %v, want ready message %d", messages, readyID)
	}

	var delayed DelayedMessage
	if err := q.DBG.Where("id = ?", delayedID).First(&delayed).Error; err != nil {
		t.Fatalf("load delayed message: %v", err)
	}
	if delayed.Tries != 0 {
		t.Fatalf("delayed message was dequeued %d times, want 0", delayed.Tries)
	}

	message := q.Peek(1, "orders", delayedID)
	if message == nil || message.Status() != models.MessageStatusDelayed {
		t.Fatalf("Peek status = %v, want Delayed", message)
	}
}

func TestDelayedMessageIsPromotedWhenDue(t *testing.T) {
	q, _ := newTestQueue(t)
	createTestQueue(t, q)

	messageID, err := q.Enqueue(1, "orders", "cancel-order", map[string]string{"order_id": "42"}, 1)
	if err != nil {
		t.Fatalf("Enqueue delayed: %v", err)
	}

	waitFor(t, 4*time.Second, func() bool {
		return countRows(t, q, "messages") == 1 && countRows(t, q, "delayed_messages") == 0
	})

	var ready Message
	if err := q.DBG.Where("id = ?", messageID).First(&ready).Error; err != nil {
		t.Fatalf("load ready message: %v", err)
	}
	if ready.Tries != 0 {
		t.Fatalf("tries after promotion = %d, want 0", ready.Tries)
	}

	messages, err := q.Dequeue(1, "orders", 1, -1)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if len(messages) != 1 || messages[0].ID != messageID {
		t.Fatalf("Dequeue returned %v, want message %d", messages, messageID)
	}
	if messages[0].KeyValues["order_id"] != "42" {
		t.Fatalf("order_id = %q, want 42", messages[0].KeyValues["order_id"])
	}

	filtered := q.Filter(1, "orders", models.FilterCriteria{
		KV: map[string]string{"order_id": "42"},
	})
	if len(filtered) != 1 || filtered[0] != messageID {
		t.Fatalf("Filter returned %v, want [%d]", filtered, messageID)
	}
}

func TestFilterAndDeleteQueueIncludeDelayedMessages(t *testing.T) {
	q, _ := newTestQueue(t)
	createTestQueue(t, q)

	messageID, err := q.Enqueue(1, "orders", "cancel-order", map[string]string{"order_id": "42"}, 3600)
	if err != nil {
		t.Fatalf("Enqueue delayed: %v", err)
	}

	filtered := q.Filter(1, "orders", models.FilterCriteria{
		KV: map[string]string{"order_id": "42"},
	})
	if len(filtered) != 1 || filtered[0] != messageID {
		t.Fatalf("Filter returned %v, want [%d]", filtered, messageID)
	}

	if err := q.DeleteQueue(1, "orders"); err != nil {
		t.Fatalf("DeleteQueue: %v", err)
	}

	if got := countRows(t, q, "delayed_messages"); got != 0 {
		t.Fatalf("delayed_messages = %d after queue deletion, want 0", got)
	}
	if got := countRows(t, q, "delayed_kvs"); got != 0 {
		t.Fatalf("delayed_kvs = %d after queue deletion, want 0", got)
	}
}

func TestLegacyDelayedMessagesAreMigrated(t *testing.T) {
	q, path := newTestQueue(t)
	createTestQueue(t, q)

	messageID, err := q.Enqueue(1, "orders", "cancel-order", map[string]string{"order_id": "42"}, 3600)
	if err != nil {
		t.Fatalf("Enqueue delayed: %v", err)
	}

	if err := q.DBG.Exec(`
		INSERT INTO messages
			(id, tenant_id, queue_id, deliver_at, delivered_at, tries, max_tries, message)
		SELECT id, tenant_id, queue_id, deliver_at, 0, tries, max_tries, message
		FROM delayed_messages WHERE id = ?
	`, messageID).Error; err != nil {
		t.Fatalf("simulate legacy message: %v", err)
	}
	if err := q.DBG.Exec(`
		INSERT INTO kvs (tenant_id, queue_id, message_id, k, v)
		SELECT tenant_id, queue_id, message_id, k, v
		FROM delayed_kvs WHERE message_id = ?
	`, messageID).Error; err != nil {
		t.Fatalf("simulate legacy message attributes: %v", err)
	}
	if err := q.DBG.Exec("DELETE FROM delayed_kvs WHERE message_id = ?", messageID).Error; err != nil {
		t.Fatalf("remove legacy delayed attributes: %v", err)
	}
	if err := q.DBG.Exec("DELETE FROM delayed_messages WHERE id = ?", messageID).Error; err != nil {
		t.Fatalf("remove legacy delayed message: %v", err)
	}

	if err := q.Shutdown(); err != nil {
		t.Fatalf("Shutdown before migration: %v", err)
	}

	migrated := NewSQLiteQueue(config.SQLiteConfig{Path: path})
	t.Cleanup(func() {
		if err := migrated.Shutdown(); err != nil {
			t.Errorf("Shutdown after migration: %v", err)
		}
	})

	if got := countRows(t, migrated, "messages"); got != 0 {
		t.Fatalf("ready queue has %d migrated messages, want 0", got)
	}
	if got := countRows(t, migrated, "delayed_messages"); got != 1 {
		t.Fatalf("delayed store has %d migrated messages, want 1", got)
	}
	if got := countRows(t, migrated, "delayed_kvs"); got != 1 {
		t.Fatalf("delayed store has %d migrated attributes, want 1", got)
	}
}
