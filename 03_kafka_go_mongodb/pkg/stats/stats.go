package stats

import (
	"sync/atomic"
	"time"
)

type KafkaConsumerStats struct {
	totalTransactions                      int64
	totalSuspiciousTransactions            int64
	totalUnmarshallingMsgErrors            int64
	totalInsertSuspiciousTransactionErrors int64
	elapsedTime                            time.Duration
}

type KafkaProducerStats struct {
	totalPublishedMessages int64
	totalFailedDeliveries  int64
	elapsedTime            time.Duration
}

func (stats *KafkaConsumerStats) IncrtotalTransactions() {
	atomic.AddInt64(&stats.totalTransactions, 1)
}

func (stats *KafkaConsumerStats) TotalTransactions() int64 {
	return stats.totalTransactions
}

func (stats *KafkaConsumerStats) IncrTotalSuspiciousTransactions() {
	atomic.AddInt64(&stats.totalSuspiciousTransactions, 1)
}

func (stats *KafkaConsumerStats) TotalSuspiciousTransactions() int64 {
	return stats.totalSuspiciousTransactions
}

func (stats *KafkaConsumerStats) IncrTotalUnmarshallingMsgErrors() {
	atomic.AddInt64(&stats.totalUnmarshallingMsgErrors, 1)
}

func (stats *KafkaConsumerStats) TotalUnmarshallingMsgErrors() int64 {
	return stats.totalUnmarshallingMsgErrors
}

func (stats *KafkaConsumerStats) IncrTotalInsertSuspiciousTransactionErrors() {
	atomic.AddInt64(&stats.totalInsertSuspiciousTransactionErrors, 1)
}

func (stats *KafkaConsumerStats) TotalInsertSuspiciousTransactionErrors() int64 {
	return stats.totalInsertSuspiciousTransactionErrors
}

func (stats *KafkaConsumerStats) UpdateElapsedTime(elapsedTime time.Duration) {
	stats.elapsedTime = elapsedTime
}

func (stats *KafkaConsumerStats) ElapsedTime() time.Duration {
	return stats.elapsedTime
}

func (stats *KafkaProducerStats) IncrTotalPublishedMessages() {
	atomic.AddInt64(&stats.totalPublishedMessages, 1)
}

func (stats *KafkaProducerStats) TotalPublishedMessages() int64 {
	return stats.totalPublishedMessages
}

func (stats *KafkaProducerStats) IncrTotalFailedMessageDeliveries() {
	atomic.AddInt64(&stats.totalFailedDeliveries, 1)
}

func (stats *KafkaProducerStats) TotalFailedMessageDeliveries() int64 {
	return stats.totalFailedDeliveries
}

func (stats *KafkaProducerStats) UpdateElapsedTime(elapsedTime time.Duration) {
	stats.elapsedTime = elapsedTime
}

// ElapsedTime returns the elapsed time for Kafka producer operations.
func (stats *KafkaProducerStats) ElapsedTime() time.Duration {
	return stats.elapsedTime
}
