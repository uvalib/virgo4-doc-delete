package main

import (
	"fmt"
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
	"log"
	"time"
)

// time to wait before flushing pending records
var flushTimeout = 5 * time.Second

// number of times to retry a message put before giving up and terminating
var sendRetries = uint(3)

func worker(id int, config ServiceConfig, aws awssqs.AWS_SQS, queue1 awssqs.QueueHandle, queue2 awssqs.QueueHandle, records <-chan Record) {

	count := uint(0)
	messages := make([]awssqs.Message, 0, awssqs.MAX_SQS_BLOCK_COUNT)
	var record Record
	for {

		timeout := false

		// process a message or wait...
		select {
		case record = <-records:

		case <-time.After(flushTimeout):
			timeout = true
		}

		// did we timeout, if not we have a message to process
		if timeout == false {

			messages = append(messages, constructMessage(config, record.Id()))

			// have we reached a block size limit
			if count != 0 && count%awssqs.MAX_SQS_BLOCK_COUNT == awssqs.MAX_SQS_BLOCK_COUNT-1 {

				// send the block
				err := sendOutboundMessages(aws, queue1, queue2, messages)
				fatalIfError(err)

				// reset the block
				messages = messages[:0]
			}
			count++

			if count%1000 == 0 {
				log.Printf("INFO: worker %d processed %d records", id, count)
			}
		} else {

			// we timed out waiting for new messages, let's flush what we have (if anything)
			if len(messages) != 0 {

				// send the block
				err := sendOutboundMessages(aws, queue1, queue2, messages)
				fatalIfError(err)

				// reset the block
				messages = messages[:0]

				log.Printf("INFO: worker %d processed %d records (flushing)", id, count)
			}

			// reset the count
			count = 0
		}
	}

	// should never get here
}

func constructMessage(config ServiceConfig, id string) awssqs.Message {

	payload := fmt.Sprintf(config.XmlDocFormat, id)
	attributes := make([]awssqs.Attribute, 0, 4)
	attributes = append(attributes, awssqs.Attribute{Name: awssqs.AttributeKeyRecordId, Value: id})
	attributes = append(attributes, awssqs.Attribute{Name: awssqs.AttributeKeyRecordType, Value: awssqs.AttributeValueRecordTypeXml})
	attributes = append(attributes, awssqs.Attribute{Name: awssqs.AttributeKeyRecordOperation, Value: awssqs.AttributeValueRecordOperationDelete})
	//attributes = append(attributes, awssqs.Attribute{Name: awssqs.AttributeKeyRecordSource, Value: datasource})
	return awssqs.Message{Attribs: attributes, Payload: []byte(payload)}
}

func sendOutboundMessages(aws awssqs.AWS_SQS, outQueue awssqs.QueueHandle, cacheQueue awssqs.QueueHandle, batch []awssqs.Message) error {

	opStatus1, err1 := aws.BatchMessagePut(outQueue, batch)
	if err1 != nil {
		// if an error we can handle, retry
		if err1 == awssqs.ErrOneOrMoreOperationsUnsuccessful {
			log.Printf("WARNING: one or more items failed to send to output queue, retrying...")

			// retry the failed items and bail out if we cannot retry
			err1 = aws.MessagePutRetry(outQueue, batch, opStatus1, sendRetries)
		}

		// bail out if an error and let someone else handle it
		if err1 != nil {
			return err1
		}
	}

	// if we are configured to send items to the cache
	if cacheQueue != "" {

		opStatus2, err2 := aws.BatchMessagePut(cacheQueue, batch)
		if err2 != nil {
			// if an error we can handle, retry
			if err2 == awssqs.ErrOneOrMoreOperationsUnsuccessful {
				log.Printf("WARNING: one or more items failed to send to cache queue, retrying...")

				// retry the failed items and bail out if we cannot retry
				err2 = aws.MessagePutRetry(cacheQueue, batch, opStatus2, sendRetries)
			}

			// bail out if an error and let someone else handle it
			if err2 != nil {
				return err2
			}
		}
	}

	return nil
}

//
// end of file
//
