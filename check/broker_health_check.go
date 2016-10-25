package check

import (
	"math/rand"
	"time"

	kafka "github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
)

// sends one message to the broker partition, wait for it to appear on the consumer.
func (check *HealthCheck) checkBrokerHealth(metadata *kafka.MetadataResponse) BrokerStatus {
	status := unhealthy
	payload := randomBytes(check.config.MessageLength, check.randSrc)
	message := &kafka.Message{Value: []byte(payload)}

	request := &kafka.ProduceRequest{}
	request.AddMessage(check.config.topicName, check.partitionID, message)
	if _, err := check.broker.broker.Produce(request); err != nil {
		log.Println("producer failure - broker unhealthy:", err)
	} else {
		log.Debug("Waiting for message from broker")
		status = check.waitForMessage(check.config.topicName, check.partitionID, message)
		log.Debug("Got message from broker")
	}

	brokerStatus := BrokerStatus{Status: status}
	if status == healthy {
		request := &kafka.ProduceRequest{}
		request.AddMessage(check.config.replicationTopicName, check.replicationPartitionID, message)
		check.broker.broker.Produce(request)
		check.brokerInSync(&brokerStatus, metadata)
		check.brokerReplicates(&brokerStatus, metadata)
	}

	return brokerStatus
}

// waits for a message with the payload of the given message to appear on the consumer side.
func (check *HealthCheck) waitForMessage(topic string, partition int32, message *kafka.Message) string {
	deadline := time.Now().Add(check.config.CheckTimeout)
	partitionConsumer, err := check.consumer.ConsumePartition(topic, partition, kafka.OffsetOldest)
	if err == nil {
		defer func() {
			if err := partitionConsumer.Close(); err != nil {
				log.Fatalln(err)
			}
		}()
	}
	for time.Now().Before(deadline) {
		if err != nil {
			if err != kafka.ErrRequestTimedOut {
				log.Println("consumer failure - broker unhealthy:", err)
				return unhealthy
			}
			if time.Now().Before(deadline) {
				time.Sleep(check.config.DataWaitInterval)
			}
			continue
		}
		receivedMessage := <-partitionConsumer.Messages()
		if string(receivedMessage.Value) == string(message.Value) {
			return healthy
		}
	}
	return unhealthy
}

// checks whether the broker is in all ISRs of all partitions it replicates.
func (check *HealthCheck) brokerInSync(brokerStatus *BrokerStatus, metadata *kafka.MetadataResponse) {
	brokerID := int32(check.config.brokerID)

	inSync := true
	for _, topic := range metadata.Topics {
		for _, partition := range topic.Partitions {
			if contains(partition.Replicas, brokerID) && !contains(partition.Isr, brokerID) {
				inSync = false
				status := ReplicationStatus{Topic: topic.Name, Partition: partition.ID}
				brokerStatus.OutOfSync = append(brokerStatus.OutOfSync, status)
			}
		}
	}

	if inSync {
		brokerStatus.Status = insync
	}
}

var replicationFailureCount uint

func (check *HealthCheck) brokerReplicates(brokerStatus *BrokerStatus, metadata *kafka.MetadataResponse) {
	brokerID := int32(check.config.brokerID)

	topic, ok := findTopic(check.config.replicationTopicName, metadata)
	if !ok {
		return
	}

	for _, p := range topic.Partitions {
		if p.ID != check.replicationPartitionID {
			continue
		}

		if !contains(p.Isr, brokerID) {
			replicationFailureCount++
			if replicationFailureCount > check.config.replicationFailureThreshold {
				brokerStatus.Status = unhealthy
			}
		} else {
			replicationFailureCount = 0
		}

		brokerStatus.ReplicationFailures = replicationFailureCount
	}
}

// creates a random message payload.
//
// based on the solution http://stackoverflow.com/a/31832326
// provided by http://stackoverflow.com/users/1705598/icza
func randomBytes(n int, src rand.Source) []byte {
	b := make([]byte, n)
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return b
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)
