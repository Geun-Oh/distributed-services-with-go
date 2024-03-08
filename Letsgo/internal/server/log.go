package server

import (
	"fmt"
	"sync"
)

// 커밋 로그 프로토타입
// 로그에 레코드를 추가하려면 슬라이스에 추가한다. 인덱스로 레코드를 읽는다는 건 슬라이스의 해당 인덱스의 레코드를 찾는다는 것이다.
// 클라이언트가 찾는 오프셋이 없으면, 오프셋을 찾을 수 없다는 에러를 리턴한다.

type Log struct {
	mu      sync.Mutex
	records []Record
}

type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

func NewLog() *Log {
	return &Log{}
}

func (c *Log) Append(record Record) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record.Offset = uint64(len(c.records))
	c.records = append(c.records, record)
	return record.Offset, nil
}

func (c *Log) Read(offset uint64) (Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if offset >= uint64(len(c.records)) {
		return Record{}, ErrOffsetNotFound
	}
	return c.records[offset], nil
}

var ErrOffsetNotFound = fmt.Errorf("offset not found")
