package runner

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"
	"os"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/dynamic/grpcdynamic"
	"go.uber.org/multierr"
	"google.golang.org/grpc/metadata"
)

// Worker is used for doing a single stream of requests in parallel
type Worker struct {
	stub grpcdynamic.Stub
	mtd  *desc.MethodDescriptor

	config     *RunConfig
	stopCh     chan bool
	qpsTick    time.Duration
	reqCounter *int64
	nReq       int
	workerID   string

	// cached messages only for binary
	cachedMessages []*dynamic.Message

	// non-binary json optimization
	arrayJSONData []string
}

func (w *Worker) runWorker() error {
	var throttle <-chan time.Time
	if w.config.qps > 0 {
		throttle = time.Tick(w.qpsTick)
	}

	var err error
	for i := 0; i < w.nReq; i++ {
		// Check if application is stopped. Do not send into a closed channel.
		select {
		case <-w.stopCh:
			return nil
		default:
			if w.config.qps > 0 {
				<-throttle
			}

			rErr := w.makeRequest()

			err = multierr.Append(err, rErr)
		}
	}
	return err
}

func (w *Worker) makeRequest() error {

	reqNum := atomic.AddInt64(w.reqCounter, 1)

	ctd := newCallTemplateData(w.mtd, w.workerID, reqNum)

	var inputs []*dynamic.Message
	var err error

	// try the optimized path for JSON data for non client-streaming
	if !w.config.binary && !w.mtd.IsClientStreaming() && len(w.arrayJSONData) > 0 {
		indx := int((reqNum - 1) % int64(len(w.arrayJSONData))) // we want to start from inputs[0] so dec reqNum
		if inputs, err = w.getMessages(ctd, []byte(w.arrayJSONData[indx])); err != nil {
			return err
		}
	} else {
		if inputs, err = w.getMessages(ctd, w.config.data); err != nil {
			return err
		}
	}

    if w.config.dryRun {
        return nil
    }

	mdMap, err := ctd.executeMetadata(string(w.config.metadata))
	if err != nil {
		return err
	}

	var reqMD *metadata.MD
	if mdMap != nil && len(*mdMap) > 0 {
		md := metadata.New(*mdMap)
		reqMD = &md
	}

	ctx := context.Background()
	var cancel context.CancelFunc

	if w.config.timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, w.config.timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	// include the metadata
	if reqMD != nil {
		ctx = metadata.NewOutgoingContext(ctx, *reqMD)
	}

	// RPC errors are handled via stats handler

	if w.mtd.IsClientStreaming() && w.mtd.IsServerStreaming() {
		_ = w.makeBidiRequest(&ctx, inputs)
	}
	if w.mtd.IsClientStreaming() {
		_ = w.makeClientStreamingRequest(&ctx, inputs)
	}

	inputsLen := len(inputs)
	if inputsLen == 0 {
		return fmt.Errorf("no data provided for request")
	}
	inputIdx := int((reqNum - 1) % int64(inputsLen)) // we want to start from inputs[0] so dec reqNum

	if w.mtd.IsServerStreaming() {
		_ = w.makeServerStreamingRequest(&ctx, inputs[inputIdx])
	}
	// TODO: handle response?
	_, _ = w.stub.InvokeRpc(ctx, w.mtd, inputs[inputIdx])

	return err
}

func (w *Worker) getMessages(ctd *callTemplateData, inputData []byte) ([]*dynamic.Message, error) {
	var inputs []*dynamic.Message

	if w.cachedMessages != nil {
		return w.cachedMessages, nil
	}

	if !w.config.binary {
		data, err := ctd.executeData(string(inputData))
		if err != nil {
			return nil, err
		}
		if w.config.dryRun {
            fmt.Println(os.Stdout, string(data))
        }
		inputs, err = createPayloadsFromJSON(string(data), w.mtd)
		if err != nil {
			return nil, err
		}
		// Json messages are not cached due to templating
	} else {
		var err error
		inputs, err = createPayloadsFromBin(inputData, w.mtd)
		if err != nil {
			return nil, err
		}

		w.cachedMessages = inputs
	}

	return inputs, nil
}

func (w *Worker) makeClientStreamingRequest(ctx *context.Context, input []*dynamic.Message) error {
	str, err := w.stub.InvokeRpcClientStream(*ctx, w.mtd)
	counter := 0
	// TODO: need to handle and propagate errors
	for err == nil {
		inputLen := len(input)
		if input == nil || inputLen == 0 {
			// TODO: need to handle error
			_, _ = str.CloseAndReceive()
			break
		}

		if counter == inputLen {
			// TODO: need to handle error
			_, _ = str.CloseAndReceive()
			break
		}

		payload := input[counter]

		var wait <-chan time.Time
		if w.config.streamInterval > 0 {
			wait = time.Tick(w.config.streamInterval)
			<-wait
		}

		err = str.SendMsg(payload)
		if err == io.EOF {
			// We get EOF on send if the server says "go away"
			// We have to use CloseAndReceive to get the actual code
			// TODO: need to handle error
			_, _ = str.CloseAndReceive()
			break
		}
		counter++
	}
	return nil
}

func (w *Worker) makeServerStreamingRequest(ctx *context.Context, input *dynamic.Message) error {
	str, err := w.stub.InvokeRpcServerStream(*ctx, w.mtd, input)
	// TODO: need to handle and propagate errors
	for err == nil {
		_, err = str.RecvMsg()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}
	}

	return err
}

func (w *Worker) makeBidiRequest(ctx *context.Context, input []*dynamic.Message) error {
	str, err := w.stub.InvokeRpcBidiStream(*ctx, w.mtd)
	if err != nil {
		return err
	}

	counter := 0

	inputLen := len(input)

	recvDone := make(chan bool)

	if input == nil || inputLen == 0 {
		// TODO: need to handle error
		_ = str.CloseSend()
		return nil
	}

	go func() {
		for {
			_, err := str.RecvMsg()

			if err != nil {
				close(recvDone)
				break
			}
		}
	}()

	// TODO: need to handle and propagate errors
	for err == nil {
		if counter == inputLen {
			// TODO: need to handle error
			_ = str.CloseSend()
			break
		}

		payload := input[counter]

		var wait <-chan time.Time
		if w.config.streamInterval > 0 {
			wait = time.Tick(w.config.streamInterval)
			<-wait
		}

		err = str.SendMsg(payload)
		counter++
	}

	if err == nil {
		<-recvDone
	}

	return nil
}
