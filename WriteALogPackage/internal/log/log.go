package log

import (
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/Geun-Oh/distributed-services-with-go/ServeRequestsWithgRPC/api/v1"
)

type Log struct {
	mu     sync.RWMutex
	Dir    string
	Config Config

	activeSegment *segment
	segments      []*segment
}

type originReader struct {
	*store
	off int64
}

/*
로그는 세그먼트 포인터의 슬라이스와 활성 세그먼트를 가리키는 포인터로 구현된다.
디렉터리는 세그먼트를 저장하는 위치이다.
*/

func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	l := &Log{
		Dir:    dir,
		Config: c,
	}

	return l, l.setup()
}

func (l *Log) setup() error {
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}
	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})
	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		// 베이스 오프셋은 index와 store 두 파일을 중복해서 담고 있기에
		// 갇은 값이 하나 더 있다. 그래서 한 번 건너뛴다.
		i++
	}
	if l.segments == nil {
		if err = l.newSegment(
			l.Config.Segment.InitialOffset,
		); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}

func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.activeSegment.IsMaxed() {
		off := l.activeSegment.nextOffset
		if err := l.newSegment(off); err != nil {
			return 0, err
		}
	}
	return l.activeSegment.Append(record)
}

func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.Lock()
	defer l.mu.RUnlock()
	var s *segment
	for _, segment := range l.segments {
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break
		}
	}
	if s == nil || s.nextOffset <= off {
		return nil, api.ErrOffsetOutOfRange{Offset: off}
	}
	return s.Read(off)
}

/*
Read 메서드는 해당 오프셋에 저장된 레코드를 읽는다. 먼저 레코드가 위치한 세그먼트를 찾는다.
세그먼트의 베이스 오프셋이 세그먼트에서 가장 작은 오프셋이라는 특성을 이용해 찾는다.
세그먼트를 찾고 나면 세그먼트의 인덱스에서 인덱스 항목을 찾아서 세그먼트의 저장 파일에서 데이터를 읽어 반환한다.
*/

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

/*
세 메서드는 연관이 있다.
Close(): 로그의 모든 세그먼트를 닫는다.
Remove(): 로그를 닫고 데이터를 모두 지운다.
Reset(): 로그를 제거하고 이를 대체할 새로운 로그를 생성한다.
*/
func (l *Log) LowestOffset() (uint64, error) {
	l.mu.Lock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset, nil
}

func (l *Log) HighestOffset() (uint64, error) {
	l.mu.Lock()
	defer l.mu.RUnlock()
	off := l.segments[len(l.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

/*
로그에 저장된 오프셋의 범위를 알려준다. 이후 다룰 복제 기능 지원이나 클러스터 조율 시 이러한 정보를 필요로 한다.
*/

func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var segments []*segment
	for _, s := range l.segments {
		if s.nextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}
	l.segments = segments
	return nil
}

/*
가장 큰 오프셋이 가장 작은 오프셋보다 작은 세그먼트를 찾아 모두 제거한다.
특정 시점보다 오래된 세그먼트를 모두 지우는 것이다.
*/

func (l *Log) Reader() io.Reader {
	l.mu.Lock()
	defer l.mu.RUnlock()
	readers := make([]io.Reader, len(l.segments))
	for i, segment := range l.segments {
		readers[i] = &originReader{segment.store, 0}
	}
	return io.MultiReader(readers...)
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}
