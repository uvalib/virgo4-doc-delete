package main

import (
	"encoding/json"
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
	"log"
	"net/url"
	"time"
)

type InboundFile struct {
	SourceBucket string
	SourceKey    string
	ObjectSize   int64
}

func getInboundNotification(config ServiceConfig, aws awssqs.AWS_SQS, inQueueHandle awssqs.QueueHandle) ([]InboundFile, awssqs.ReceiptHandle, error) {

	for {

		// get the next message if one is available
		messages, err := aws.BatchMessageGet(inQueueHandle, 1, time.Duration(config.PollTimeOut)*time.Second)
		if err != nil {
			log.Printf("ERROR: during message get (%s), sleeping and retrying", err.Error())

			// sleep for a while
			time.Sleep(1 * time.Second)

			// and try again
			continue
		}

		// did we get anything to process
		if len(messages) == 1 {

			log.Printf("INFO: received a new notification")

			//log.Printf("%s", string( messages[0].Payload ) )

			// assume the message is an S3 event containing a list of one or more new objecxts
			newS3objects, err := decodeS3Event(messages[0])
			if err != nil {
				return nil, "", err
			}

			// we have some objects to download
			if len(newS3objects) != 0 {
				inboundFiles := make([]InboundFile, 0)
				for _, s3 := range newS3objects {

					// some file names may be HTML encoded... un-encode them here...
					key, err := url.QueryUnescape(s3.S3.Object.Key)
					if err != nil {
						return nil, "", err
					}

					inboundFiles = append(inboundFiles,
						InboundFile{
							SourceBucket: s3.S3.Bucket.Name,
							SourceKey:    key,
							ObjectSize:   s3.S3.Object.Size})
				}

				return inboundFiles, messages[0].ReceiptHandle, nil
			} else {
				log.Printf("WARNING: not an interesting notification, ignoring it")
			}

		} else {
			log.Printf("INFO: no new notifications...")
		}
	}
}

// turn a message received from the inbound queue into a list of zero or more new S3 objects
func decodeS3Event(message awssqs.Message) ([]S3EventRecord, error) {

	events := Events{}
	err := json.Unmarshal([]byte(message.Payload), &events)
	if err != nil {
		log.Printf("ERROR: json unmarshal: %s", err)
		return nil, err
	}
	return events.Records, nil
}

//
// end of file
//
