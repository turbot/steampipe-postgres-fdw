package hub

import (
	"context"
	"io"

	"github.com/turbot/steampipe-plugin-sdk/v6/grpc/proto"
	"google.golang.org/grpc/metadata"
)

// localStreamBufferSize matches the SDK's anywhere.LocalPluginStream buffer.
const localStreamBufferSize = 1024

// The SDK skips the nil "done" row only for a stream of this type.
var _ proto.WrapperPlugin_ExecuteServer = (*localStream)(nil)

// localStream carries rows from an in-process plugin Execute to the scan iterator and keeps the
// error Execute ends with. anywhere.LocalPluginStream (used by CallExecuteAsync) loses it: the
// plugin sends a nil "done" row before returning its error, and Recv returns (nil, nil) once the
// stream is done, so a plugin error after the first row ended the scan as complete.
//
// It satisfies proto.WrapperPlugin_ExecuteServer, so the plugin streams rows to it as to a gRPC
// client and ends the stream by returning, not with a nil row.
type localStream struct {
	ctx  context.Context
	rows chan *proto.ExecuteResponse
	err  error // written once, before rows is closed
}

func newLocalStream(ctx context.Context) *localStream {
	return &localStream{
		ctx:  ctx,
		rows: make(chan *proto.ExecuteResponse, localStreamBufferSize),
	}
}

// Send is called only by Execute, so never after finish.
func (s *localStream) Send(r *proto.ExecuteResponse) error {
	select {
	case s.rows <- r:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

// finish records how Execute ended; Recv returns err (or io.EOF) once the rows sent before it are read.
func (s *localStream) finish(err error) {
	s.err = err
	close(s.rows)
}

func (s *localStream) Recv() (*proto.ExecuteResponse, error) {
	select {
	case r, ok := <-s.rows:
		if ok {
			return r, nil
		}
		if s.err != nil {
			return nil, s.err
		}
		return nil, io.EOF
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (s *localStream) Context() context.Context     { return s.ctx }
func (s *localStream) SetHeader(metadata.MD) error  { return nil }
func (s *localStream) SendHeader(metadata.MD) error { return nil }
func (s *localStream) SetTrailer(metadata.MD)       {}
func (s *localStream) SendMsg(any) error            { return nil }
func (s *localStream) RecvMsg(any) error            { return nil }
