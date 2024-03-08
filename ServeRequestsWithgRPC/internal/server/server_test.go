package server

import (
	"net"
	"testing"

	api "github.com/Geun-Oh/distributed-services-with-go/ServeRequestsWithgRPC/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// gRPC 서버의 가동을 테스트한다.
// TestServer 함수는 각 테스트 케이스들을 정의하고 각 케이스에 대한 서브 테스트를 실행한다.
func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t testing.T,
		client api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeds.": testProduceConsume,
		"produce/consume a stream succeeds.":                  testProduceConsumeStream,
		"consume past log boundary fails.":                    testConsumePastBoundary,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (
	client api.LogClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()

	l, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	clientOptions := []grpc.DialOption{grpc.WithInsecure()}
	cc, err := grpc.Dial(l.Addr().String(), clientOptions...)
	require.NoError(t, err)
}
