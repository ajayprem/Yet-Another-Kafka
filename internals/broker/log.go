package broker

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"yet-another-kafka/internals/types"
)

const (
	LOCATION_PREFIX = "tmp"
	// the logs are stored as csv files with the name as <topic-name>-<partition>.csv
	// each record in the csv file is as offset,key,value
	LOG_FILE_FORMAT = "%s-%d.csv"
)

type logStore struct {
	location string
}

func newLogStore(brokerId int) (*logStore, error) {
	// return error if base location does not exist
	if _, err := os.Stat(LOCATION_PREFIX); os.IsNotExist(err) {
		return nil, fmt.Errorf("store: base location does not exist")
	}

	location := filepath.Join(LOCATION_PREFIX, strconv.Itoa(brokerId))
	if _, err := os.Stat(location); os.IsNotExist(err) {
		if err := os.Mkdir(location, os.ModePerm); err != nil {
			return nil, fmt.Errorf("store.NewStore unable to create broker store: %s", err)
		}
	}

	return &logStore{
		location: location,
	}, nil
}

// TODO: make thread safe -> two threads running create at same time for same topic
func (l *logStore) createTopicFiles(topicName string, partitions int) error {
	topicDir := filepath.Join(l.location, topicName)
	if err := os.Mkdir(topicDir, os.ModePerm); err != nil {
		return fmt.Errorf("log.createTopicFiles unable to create topic dir: %s", err)
	}

	for partition := range partitions {
		fileName := fmt.Sprintf(LOG_FILE_FORMAT, topicName, partition)
		file, err := os.Create(filepath.Join(fileName, "log.txt"))
		if err != nil {
			return fmt.Errorf("store.createTopic: error creating log file: %s", err)
		}
		file.Close()
	}

	return nil
}

// TODO: make thread safe -> two producers writing to the same topic
func (l *logStore) appendRecord(topicName string, partition, offset int, msg types.Message) error {
	fileName := fmt.Sprintf(LOG_FILE_FORMAT, topicName, partition)
	file, err := os.OpenFile(filepath.Join(l.getTopicDir(topicName), fileName), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("log: failed opening file: %s", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	if err := writer.Write([]string{strconv.Itoa(offset), msg.Key, msg.Value}); err != nil {
		return fmt.Errorf("log: unable to write record: %v", err)
	}
	writer.Flush()
	return writer.Error()
}

func (l *logStore) getTopicDir(topicName string) string {
	return filepath.Join(l.location, topicName)
}

type topicFoundFunc func(topicName string, partitions, lastOffset int)

func (l *logStore) scanExistingTopics(onFound topicFoundFunc) error {

	entries, err := os.ReadDir(l.location)
	if err != nil {
		return fmt.Errorf("log.scanExistingTopics: error while reading locatoin: %s", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			topicDir := filepath.Join(l.location, entry.Name())
			files, err := os.ReadDir(topicDir)
			if err != nil {
				return err
			}

			lastOffset := -1
			for _, f := range files {
				offset, err := lastOffsetInFile(filepath.Join(topicDir, f.Name()))
				if err != nil {
					return err
				}
				if offset > lastOffset {
					lastOffset = offset
				}
			}
			onFound(entry.Name(), len(files), lastOffset)
		}
	}

	return nil
}

func lastOffsetInFile(path string) (int, error) {
	file, err := os.Open(path)
	if err != nil {
		return -1, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	last := -1
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return -1, err
		}
		offset, err := strconv.Atoi(record[0])
		if err != nil {
			return -1, err
		}
		last = offset
	}
	return last, nil
}
