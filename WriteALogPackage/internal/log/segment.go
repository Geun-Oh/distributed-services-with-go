package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/Geun-Oh/distributed-services-with-go/ServeRequestsWithgRPC/api/v1"
	"google.golang.org/protobuf/proto"
)

type segment struct {
	store                  *store // 저장 파일
	index                  *index // 인덱스 파일
	baseOffset, nextOffset uint64
	config                 Config
}

/*
세그먼트는 내부의 스토어와 인덱스를 호출해야하므로 처음 두 필드에 각 포인터를 가진다.
베이스가 되는 오프셋과 다음에 추가할 오프셋 값도 가지는데, 인덱스 항목의 상대 오프셋을 계산하고 다음 항목을 추가할 때 사용한다.
config 필드를 두어 저장 파일과 인덱스 파일의 크기를 설정의 최댓값과 비교할 수 있으므로 세그먼트가 가득 찼는지 알 수 있도록 한다.
*/

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}
	var err error
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

/*
로그에 새로운 세그먼트가 필요한 경우 newSegment()를 호출한다. 저장 파일과 인덱스 파일을 os.OpenFile()에서 os.O_CREATE 모드로 열고, 파일이 없으면 생성하게 한다.
저장 파일을 만들 때는 os.O_APPEND 플래그를 주어 파일에 쓸 때 기존 데이터에 이어서 쓰도록 한다.
저장 파일과 인덱스 파일을 열고 난 뒤 이 파일들로 스토어와 인덱스를 만든다.
마지막으로, 세그먼트의 다음 오프셋을 설정해 다음에 레코드를 추가할 준비를 한다.
인덱스가 비었다면 다음 레코드는 세그먼트의 첫 레코드가 되고, 오프셋은 세그먼트의 베이스 오프셋이 된다.
인덱스에 하나 이상의 레코드가 있다면, 다음 레코드의 오프셋은 레코드의 마지막 오프셋이 된다.
이 값은 베이스 오프셋과 상대 오프셋에 1을 더하여 구한다.
*/

func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}
	if err = s.index.Write(
		// 인덱스의 오프셋은 베이스 오프셋에서의 상댓값이다.
		uint32(s.nextOffset-uint64(s.baseOffset)),
		pos,
	); err != nil {
		return 0, err
	}
	s.nextOffset++
	return cur, nil
}

/*
Append 메서드는 세그먼트에 레코드를 쓰고, 추가한 레코드의 오프셋을 반환한다. 이 오프셋은 api의 응답으로 반환된다.
세그먼트가 레코드를 추가할 때는 먼저 스토어에 데이터를 추가한 다음 인덱스 항목을 추가한다.
인덱스 오프셋은 베이스 오프셋의 상대적인 값이기에 세그먼트의 다음 오프셋에서 베이스 오프셋을 빼 항목의 상대적 오프셋을 알아낸다.
이후 다음 추가를 대비해서 다음 오프셋을 하나 증가시킨다.
*/

func (s *segment) Read(off uint64) (*api.Record, error) {
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}

/*
Read 메서드는 오프셋의 레코드를 리턴한다. 읽기와 비슷하게, 세그먼트의 레코드를 읽으려면 먼저 절댓값인 인덱스를 상대적 오프셋으로 변환하고 해당 인덱스 항목을 가져온다.
그 다음 세그먼트는 인덱스 항목에서 알아낸 스토어의 레코드 위치로 가서 해당하는 데이터를 찾는다.
마지막으로, 직렬화한 값을 반환한다.
*/

func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes || s.index.size+entWidth > s.config.Segment.MaxIndexBytes
}

/*
IsMaxed 세그먼트는 세그먼트의 스토어 또는 인덱스가 최대 크기에 도달했는지 체크한다.
추가하는 레코드의 저장 바이트는 가변이기 때문에 현재 크기가 저장 바이트 제한을 넘지 않으면 되고,
추가하는 레코드에 대한 인덱스 바이트는 고정적이기 때문에 현재의 크기에 인덱스 하나를 추가했을 때 인덱스 제한을 넘지 않아야 한다.
*/

func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}
