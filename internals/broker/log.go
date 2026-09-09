package broker

import (
	"container/heap"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"yet-another-kafka/internals/types"
)

// TODO: lock all file operations in this file

const (
	LOCATION_PREFIX = "/tmp"
	BROKER_PREFIX   = "broker-%d" // the broker id
	// the logs are stored as csv files with the name as <topic-name>-<partition>.csv
	// each record in the csv file is as offset,key,value
	LOG_FILE_FORMAT = "%s-%d.csv"
)

type logStore struct {
	location string
}

func newLogStore(brokerId int) (*logStore, error) {
	// return error if base location does not exist
	_, err := os.Stat(LOCATION_PREFIX)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("logstore.newLogStore: base location does not exist")
		}
		return nil, fmt.Errorf("logstore.newLogStore: unable to inspect base location: %w", err)
	}

	location := filepath.Join(LOCATION_PREFIX, fmt.Sprintf(BROKER_PREFIX, brokerId))
	if _, err := os.Stat(location); os.IsNotExist(err) {
		if err := os.Mkdir(location, os.ModePerm); err != nil {
			return nil, fmt.Errorf("logstore.newLogStore: unable to create broker store: %s", err)
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
		return fmt.Errorf("newLogStore.createTopicFiles unable to create topic dir: %s", err)
	}

	for partition := range partitions {
		fileName := fmt.Sprintf(LOG_FILE_FORMAT, topicName, partition)
		file, err := os.Create(fileName)
		if err != nil {
			return fmt.Errorf("newLogStore.createTopicFiles: error creating log file: %s", err)
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

type messageFoundFunc func(msg types.Message) error

// heapEntry tracks one open partition file's current front-of-queue record
type heapEntry struct {
	offset int
	key    string
	value  string
	reader *csv.Reader
	file   *os.File
}

// heapEntries implements container/heap.Interface, ordered by offset ascending
type heapEntries []*heapEntry

func (h heapEntries) Len() int           { return len(h) }
func (h heapEntries) Less(i, j int) bool { return h[i].offset < h[j].offset }
func (h heapEntries) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *heapEntries) Push(x any)        { *h = append(*h, x.(*heapEntry)) }
func (h *heapEntries) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func (l *logStore) scanTopicFiles(topicName string, onMessage messageFoundFunc) error {
	topicDir := l.getTopicDir(topicName)
	files, err := os.ReadDir(topicDir)
	if err != nil {
		return fmt.Errorf("log.scanTopicFiles: unable to read topic dir: %s", err)
	}

	h := &heapEntries{}
	heap.Init(h)

	// open every partition file, seed the heap with each file's first record
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		file, err := os.Open(filepath.Join(topicDir, f.Name()))
		if err != nil {
			return fmt.Errorf("log.scanTopicFiles: unable to open %s: %s", f.Name(), err)
		}

		entry, ok, err := nextEntry(csv.NewReader(file), file)
		if err != nil {
			file.Close()
			return err
		}
		if ok {
			heap.Push(h, entry)
		} else {
			file.Close()
		}
	}

	// repeatedly pop the globally smallest offset, refill from the same file
	for h.Len() > 0 {
		entry := heap.Pop(h).(*heapEntry)
		err := onMessage(types.Message{Offset: entry.offset, Key: entry.key, Value: entry.value})
		if err != nil {
			return fmt.Errorf("log.scanTopicFiles: error: %s", err)
		}

		next, ok, err := nextEntry(entry.reader, entry.file)
		if err != nil {
			entry.file.Close()
			return err
		}
		if ok {
			heap.Push(h, next)
		} else {
			entry.file.Close()
		}
	}

	return nil
}

// nextEntry reads and parses the next CSV record. ok=false, err=nil means EOF.
func nextEntry(reader *csv.Reader, file *os.File) (*heapEntry, bool, error) {
	record, err := reader.Read()
	if err == io.EOF {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("log.scanTopicFiles: error reading record: %s", err)
	}

	offset, err := strconv.Atoi(record[0])
	if err != nil {
		return nil, false, fmt.Errorf("log.scanTopicFiles: invalid offset %q: %s", record[0], err)
	}

	return &heapEntry{offset: offset, key: record[1], value: record[2], reader: reader, file: file}, true, nil
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
