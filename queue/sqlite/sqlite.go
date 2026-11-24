package sqlite

import (
	"errors"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/poundifdef/smoothmq/config"
	"github.com/poundifdef/smoothmq/models"

	"github.com/rs/zerolog/log"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/bwmarrin/snowflake"
	_ "github.com/mattn/go-sqlite3"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type SQLiteQueue struct {
	Filename string
	DBG      *gorm.DB
	Mu       *sync.Mutex
	snow     *snowflake.Node
	ticker   *time.Ticker
}

var queueDiskSize = promauto.NewGauge(
	prometheus.GaugeOpts{
		Name: "queue_disk_size",
		Help: "Size of queue data on disk",
	},
)

type Queue struct {
	ID                int64   `gorm:"primaryKey;autoIncrement:false"`
	TenantID          int64   `gorm:"not null;index:idx_queue_name,priority:1,unique"`
	Name              string  `gorm:"not null;index:idx_queue_name,priority:2"`
	RateLimit         float64 `gorm:"not null"`
	MaxRetries        int     `gorm:"not null"`
	VisibilityTimeout int     `gorm:"not null"`

	Messages []Message `gorm:"foreignKey:QueueID;references:ID"`
}

type Message struct {
	ID       int64 `gorm:"primaryKey;autoIncrement:false"`
	TenantID int64 `gorm:"not null;index:idx_message,priority:1;not null"`
	QueueID  int64 `gorm:"not null;index:idx_message,priority:2;not null"`

	DeliverAt   int64 `gorm:"not null;index:idx_message,priority:3;not null"`
	DeliveredAt int64 `gorm:"not null;index:idx_message,priority:4;not null"`
	Tries       int   `gorm:"not null;index:idx_message,priority:5;not null"`
	MaxTries    int   `gorm:"not null;index:idx_message,priority:6;not null"`

	Message string `gorm:"not null"`

	KV []KV `gorm:"foreignKey:TenantID,QueueID,MessageID;references:TenantID,QueueID,ID"`
}

func (message *Message) ToModel() *models.Message {
	rc := &models.Message{
		ID:       message.ID,
		TenantID: message.TenantID,
		QueueID:  message.QueueID,

		DeliverAt:   int(message.DeliverAt),
		DeliveredAt: int(message.DeliveredAt),
		Tries:       message.Tries,
		MaxTries:    message.MaxTries,

		Message:   []byte(message.Message),
		KeyValues: make(map[string]string),
	}

	for _, kv := range message.KV {
		rc.KeyValues[kv.K] = kv.V
	}

	return rc
}

type KV struct {
	TenantID  int64  `gorm:"not null;index:idx_kv,priority:1"`
	QueueID   int64  `gorm:"not null;index:idx_kv,priority:2"`
	MessageID int64  `gorm:"not null;index:idx_kv,priority:3"`
	K         string `gorm:"not null"`
	V         string `gorm:"not null"`
}

type RateLimit struct {
	TenantID int64 `gorm:"not null;index:idx_ratelimit,priority:1,unique"`
	QueueID  int64 `gorm:"not null;index:idx_ratelimit,priority:2"`
	Ts       int64 `gorm:"not null;index:idx_ratelimit,priority:3"`
	N        int   `gorm:"not null;default:0"`
}

func NewSQLiteQueue(cfg config.SQLiteConfig) *SQLiteQueue {
	snow, err := snowflake.NewNode(1)
	if err != nil {
		log.Fatal().Err(err).Send()
	}

	db, err := gorm.Open(sqlite.Open(cfg.Path+"?_journal_mode=WAL&_foreign_keys=off&_auto_vacuum=full&_busy_timeout=5000"), &gorm.Config{TranslateError: true})
	if err != nil {
		log.Fatal().Err(err).Send()
	}

	err = db.AutoMigrate(&Queue{})
	if err != nil {
		log.Fatal().Err(err).Send()
	}

	err = db.AutoMigrate(&Message{})
	if err != nil {
		log.Fatal().Err(err).Send()
	}

	err = db.AutoMigrate(&KV{})
	if err != nil {
		log.Fatal().Err(err).Send()
	}

	err = db.AutoMigrate(&RateLimit{})
	if err != nil {
		log.Fatal().Err(err).Send()
	}

	rc := &SQLiteQueue{
		Filename: cfg.Path,
		DBG:      db,
		Mu:       &sync.Mutex{},
		snow:     snow,
		ticker:   time.NewTicker(1 * time.Second),
	}

	go func() {
		for {
			select {
			case <-rc.ticker.C:
				stat, err := os.Stat(rc.Filename)
				if err == nil {
					queueDiskSize.Set(float64(stat.Size()))
				}
			}
		}
	}()

	return rc
}

func (q *SQLiteQueue) CreateQueue(tenantId int64, properties models.QueueProperties) error {
	q.Mu.Lock()
	defer q.Mu.Unlock()

	if len(properties.Name) > 80 {
		return models.ErrInvalidQueueName
	}

	regex, err := regexp.Compile(`^[a-zA-Z0-9-_]+$`)
	if err != nil {
		return err
	}

	var nameTrimmed = strings.TrimSpace(properties.Name)
	if !regex.MatchString(nameTrimmed) {
		return models.ErrInvalidQueueName
	}

	qId := q.snow.Generate()

	res := q.DBG.Create(&Queue{
		ID:                qId.Int64(),
		TenantID:          tenantId,
		Name:              nameTrimmed,
		RateLimit:         properties.RateLimit,
		MaxRetries:        properties.MaxRetries,
		VisibilityTimeout: properties.VisibilityTimeout,
	})

	if errors.Is(res.Error, gorm.ErrDuplicatedKey) {
		return models.ErrQueueExists
	}

	return res.Error
}

func (q *SQLiteQueue) UpdateQueue(tenantId int64, queueName string, properties models.QueueProperties) error {
	q.Mu.Lock()
	defer q.Mu.Unlock()

	// TODO: validate, trim queue names. ensure length and valid characters.

	queue, err := q.getQueue(tenantId, queueName)
	if err != nil {
		return err
	}

	queue.RateLimit = properties.RateLimit
	queue.MaxRetries = properties.MaxRetries
	queue.VisibilityTimeout = properties.VisibilityTimeout

	// TODO: should we reset the rate limit table if the queue's rate limit changes?

	res := q.DBG.Save(queue)

	return res.Error
}

func (q *SQLiteQueue) GetQueue(tenantId int64, queueName string) (models.QueueProperties, error) {
	queue := models.QueueProperties{}

	properties, err := q.getQueue(tenantId, queueName)
	if err != nil {
		return queue, err
	}

	queue.Name = properties.Name
	queue.RateLimit = properties.RateLimit
	queue.MaxRetries = properties.MaxRetries
	queue.VisibilityTimeout = properties.VisibilityTimeout

	return queue, nil
}

func (q *SQLiteQueue) DeleteQueue(tenantId int64, queueName string) error {
	// Delete all messages with the queue, and then the queue itself

	queue, err := q.getQueue(tenantId, queueName)
	if err != nil {
		return err
	}

	q.Mu.Lock()
	defer q.Mu.Unlock()

	rc := q.DBG.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("tenant_id = ? AND queue_id = ?", tenantId, queue.ID).Delete(&KV{}).Error; err != nil {
			return err
		}

		if err := tx.Where("tenant_id = ? AND queue_id = ?", tenantId, queue.ID).Delete(&Message{}).Error; err != nil {
			return err
		}

		if err := tx.Where("tenant_id = ? AND id = ?", tenantId, queue.ID).Delete(&Queue{}).Error; err != nil {
			return err
		}

		return nil
	})

	return rc
}

func (q *SQLiteQueue) ListQueues(tenantId int64) ([]string, error) {
	var queues []Queue
	res := q.DBG.Where("tenant_id = ?", tenantId).Select("name").Find(&queues)
	if res.Error != nil {
		return nil, res.Error
	}

	rc := make([]string, len(queues))
	for i, queue := range queues {
		rc[i] = queue.Name
	}

	return rc, nil
}

func (q *SQLiteQueue) getQueue(tenantId int64, queueName string) (*Queue, error) {
	rc := &Queue{}
	res := q.DBG.Where("tenant_id = ? AND name = ?", tenantId, queueName).First(rc)
	if res.RowsAffected != 1 {
		return nil, errors.New("Queue not found")
	}
	return rc, res.Error
}

func (q *SQLiteQueue) Enqueue(tenantId int64, queueName string, message string, kv map[string]string, delay int) (int64, error) {
	messageSnow := q.snow.Generate()
	messageId := messageSnow.Int64()

	queue, err := q.getQueue(tenantId, queueName)
	if err != nil {
		return 0, err
	}

	now := time.Now().UTC().Unix()
	deliverAt := now + int64(delay)

	newMessage := &Message{
		ID:          messageId,
		TenantID:    tenantId,
		QueueID:     queue.ID,
		DeliverAt:   deliverAt,
		DeliveredAt: 0,
		MaxTries:    queue.MaxRetries,
		Message:     message,

		KV: make([]KV, 0),
	}

	for k, v := range kv {
		newKv := KV{
			TenantID:  tenantId,
			MessageID: messageId,
			QueueID:   queue.ID,
			K:         k,
			V:         v,
		}
		newMessage.KV = append(newMessage.KV, newKv)
	}

	q.Mu.Lock()
	defer q.Mu.Unlock()

	if err := q.DBG.Create(newMessage).Error; err != nil {
		return 0, err
	}

	log.Debug().Int64("message_id", messageId).Msg("Enqueued message")

	return messageId, nil
}

func (q *SQLiteQueue) UpdateMessage(tenantId int64, queue string, messageId int64, m *models.Message) error {
	q.Mu.Lock()
	defer q.Mu.Unlock()

	result := q.DBG.Model(&Message{}).
		Where("tenant_id = ? AND queue_id = (SELECT id FROM queues WHERE tenant_id = ? AND name = ?) AND id = ?", tenantId, tenantId, queue, messageId).
		Update("deliver_at", m.DeliverAt)

	if result.Error != nil {
		return result.Error
	}

	if result.RowsAffected == 0 {
		return errors.New("message not found")
	}
	return nil
}

// Atomically reserve up to numToDequeue messages from the queue's rate limit
// and return how many messages may be dequeued right now. The check and the
// deduction are a single UPDATE statement, so concurrent consumers can never
// observe the same counter value and collectively exceed the limit.
func (q *SQLiteQueue) reserveQuota(queue *Queue, now int64, numToDequeue int) (int, error) {
	if queue.RateLimit <= 0 {
		return numToDequeue, nil
	}

	res := q.DBG.Exec(
		"INSERT INTO rate_limits (tenant_id, queue_id, ts, n) VALUES (?, ?, ?, 0) ON CONFLICT DO NOTHING",
		queue.TenantID, queue.ID, now,
	)
	if res.Error != nil {
		return 0, res.Error
	}

	bucketToCheck := now
	granted := 0

	// If the rate limit is less than 1 per second, meaning 1 message every n seconds
	if queue.RateLimit < 1 {
		limitReciprocal := 1.0 / queue.RateLimit
		earliestBucket := now - int64(limitReciprocal) + 1
		bucketToCheck = earliestBucket

		// Allow a single message if none were dequeued in the current window
		var n []int64
		res = q.DBG.Raw(
			`UPDATE rate_limits SET n = n + 1
			 WHERE tenant_id = ? AND queue_id = ? AND ts = ?
			   AND (SELECT coalesce(sum(n), 0) FROM rate_limits
			        WHERE tenant_id = ? AND queue_id = ? AND ts >= ?) < 1
			 RETURNING n`,
			queue.TenantID, queue.ID, now,
			queue.TenantID, queue.ID, earliestBucket,
		).Scan(&n)
		if res.Error != nil {
			return 0, res.Error
		}
		if len(n) > 0 {
			granted = 1
		}
	} else {
		limit := int64(int(queue.RateLimit))

		// Increment the bucket first, then grant only the portion of the
		// increment that falls below the limit. Whatever the counter was
		// before this statement, no other consumer can claim the same quota.
		var n []int64
		res = q.DBG.Raw(
			`UPDATE rate_limits SET n = n + ?
			 WHERE tenant_id = ? AND queue_id = ? AND ts = ? AND n < ?
			 RETURNING n`,
			numToDequeue, queue.TenantID, queue.ID, now, limit,
		).Scan(&n)
		if res.Error != nil {
			return 0, res.Error
		}
		if len(n) > 0 {
			granted = numToDequeue - int(max(n[0]-limit, 0))
			granted = max(granted, 0)
		}
	}

	res = q.DBG.Exec("DELETE FROM rate_limits WHERE tenant_id = ? AND queue_id = ? AND ts < ? - 1", queue.TenantID, queue.ID, bucketToCheck)
	if res.Error != nil {
		log.Warn().Int64("tenant_id", queue.TenantID).Int64("queue_id", queue.ID).Int64("ts", bucketToCheck).Msg("Unable to clear previous rate limits")
	}

	return granted, nil
}

// Return previously reserved quota that ended up not being used, so that
// messages arriving later in the same window are not blocked.
func (q *SQLiteQueue) refundQuota(queue *Queue, now int64, n int) {
	if queue.RateLimit <= 0 || n <= 0 {
		return
	}

	res := q.DBG.Exec(
		"UPDATE rate_limits SET n = max(n - ?, 0) WHERE tenant_id = ? AND queue_id = ? AND ts = ?",
		n, queue.TenantID, queue.ID, now,
	)
	if res.Error != nil {
		log.Warn().Int64("tenant_id", queue.TenantID).Int64("queue_id", queue.ID).Err(res.Error).Msg("Unable to refund rate limit")
	}
}

func (q *SQLiteQueue) Dequeue(tenantId int64, queueName string, numToDequeue int, requeueIn int) ([]*models.Message, error) {
	queue, err := q.getQueue(tenantId, queueName)
	if err != nil {
		return nil, err
	}

	// Queue is "paused"
	if queue.RateLimit == 0 {
		return nil, nil
	}

	visibilityTimeout := queue.VisibilityTimeout
	if requeueIn > -1 {
		visibilityTimeout = requeueIn
	}

	now := time.Now().UTC().Unix()

	maxToDequeue, err := q.reserveQuota(queue, now, numToDequeue)
	if err != nil {
		return nil, err
	}

	if maxToDequeue <= 0 {
		return nil, nil
	}

	// Atomically claim up to maxToDequeue messages. Selecting which rows to
	// update and updating them is a single statement, so concurrent
	// consumers can never claim the same message.
	var messageIDs []int64
	res := q.DBG.Raw(
		`UPDATE messages
		 SET tries = tries + 1, delivered_at = ?, deliver_at = ?
		 WHERE tenant_id = ? AND queue_id = ? AND id IN (
			SELECT id FROM messages
			WHERE deliver_at <= ? AND delivered_at <= ? AND (tries < max_tries OR max_tries = -1)
			  AND tenant_id = ? AND queue_id = ?
			LIMIT ?
		 )
		 RETURNING id`,
		now, now+int64(visibilityTimeout), tenantId, queue.ID,
		now, now, tenantId, queue.ID, maxToDequeue,
	).Scan(&messageIDs)

	if res.Error != nil {
		q.refundQuota(queue, now, maxToDequeue)
		return nil, res.Error
	}

	if len(messageIDs) == 0 {
		q.refundQuota(queue, now, maxToDequeue)
		return nil, nil
	}

	q.refundQuota(queue, now, maxToDequeue-len(messageIDs))

	var messages []Message

	res = q.DBG.Preload("KV").Where(
		"tenant_id = ? AND queue_id = ? AND id IN ?",
		tenantId, queue.ID, messageIDs).
		Find(&messages)

	if res.Error != nil {
		return nil, res.Error
	}

	rc := make([]*models.Message, len(messages))

	for i, message := range messages {
		rc[i] = message.ToModel()
	}

	for _, messageId := range messageIDs {
		log.Debug().Int64("message_id", messageId).Msg("Dequeued message")
	}

	return rc, nil
}

func (q *SQLiteQueue) Peek(tenantId int64, queueName string, messageId int64) *models.Message {
	queue, err := q.getQueue(tenantId, queueName)
	if err != nil {
		return nil
	}

	message := &Message{}

	res := q.DBG.Preload("KV").Where(
		"tenant_id = ? AND queue_id = ? AND id = ?",
		tenantId, queue.ID, messageId).
		First(message)

	if res.Error != nil {
		return nil
	}

	return message.ToModel()
}

func (q *SQLiteQueue) Stats(tenantId int64, queueName string) models.QueueStats {
	queue, err := q.getQueue(tenantId, queueName)
	if err != nil {
		return models.QueueStats{}
	}

	now := time.Now().UTC().Unix()

	res := q.DBG.Raw(`
		SELECT
		CASE
			WHEN tries = max_tries AND deliver_at < ? THEN 3
			WHEN delivered_at <= ? AND deliver_at > ? THEN 2
			ELSE 1
		END AS s,
		count(*) FROM messages WHERE queue_id=? AND tenant_id=? GROUP BY s
	`, now, now, now,
		queue.ID, tenantId,
	)

	if res.Error != nil {
		return models.QueueStats{}
	}

	rows, err := res.Rows()
	if err != nil {
		return models.QueueStats{}
	}

	stats := models.QueueStats{
		Counts:        make(map[models.MessageStatus]int),
		TotalMessages: 0,
	}

	for rows.Next() {
		var statusType models.MessageStatus
		var count int

		rows.Scan(&statusType, &count)

		stats.TotalMessages += count
		stats.Counts[statusType] = count
	}

	rows.Close()

	return stats
}

func (q *SQLiteQueue) Filter(tenantId int64, queueName string, filterCriteria models.FilterCriteria) []int64 {
	var rc []int64

	queue, err := q.getQueue(tenantId, queueName)
	if err != nil {
		return nil
	}

	args := make([]any, 0)
	args = append(args, tenantId)
	args = append(args, queue.ID)

	sql := "SELECT id FROM messages WHERE tenant_id=? AND queue_id=? "

	if filterCriteria.MessageID > 0 {
		sql += " AND id = ? "
		args = append(args, filterCriteria.MessageID)
	}

	if len(filterCriteria.KV) > 0 {
		sql += " AND "
		sql += " id IN (SELECT message_id FROM kvs WHERE ("

		for i := range len(filterCriteria.KV) {
			sql += "(k=? AND v=? and tenant_id=? and queue_id=?)"

			if i < len(filterCriteria.KV)-1 {
				sql += " OR "
			}
		}

		sql += " ) GROUP BY message_id HAVING count(*) = ? LIMIT 10"
		sql += " ) "

	}

	for k, v := range filterCriteria.KV {
		args = append(args, k, v, tenantId, queue.ID)
	}

	args = append(args, len(filterCriteria.KV))

	sql += "LIMIT 10"

	res := q.DBG.Raw(sql, args...).Scan(&rc)
	if res.Error != nil {
		log.Error().Err(res.Error).Msg("Unable to filter")
	}

	return rc
}

func (q *SQLiteQueue) Delete(tenantId int64, queueName string, messageId int64) error {
	queue, err := q.getQueue(tenantId, queueName)
	if err != nil {
		return err
	}

	q.Mu.Lock()
	defer q.Mu.Unlock()

	err = q.DBG.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("tenant_id = ? AND queue_id = ? AND message_id = ?", tenantId, queue.ID, messageId).Delete(&KV{}).Error; err != nil {
			return err
		}

		if err := tx.Where("tenant_id = ? AND queue_id = ? AND id = ?", tenantId, queue.ID, messageId).Delete(&Message{}).Error; err != nil {
			return err
		}

		return nil
	})

	if err == nil {
		log.Debug().Int64("message_id", messageId).Msg("Deleted message")
	}

	return err
}

func (q *SQLiteQueue) Shutdown() error {
	db, err := q.DBG.DB()
	if err != nil {
		return err
	}

	return db.Close()
}
