package runner

import (
	"testing"

	"github.com/bojand/ghz/protodesc"
	"github.com/stretchr/testify/assert"
)

func TestCallTemplateData_New(t *testing.T) {
	md, err := protodesc.GetMethodDescFromProto("helloworld.Greeter/SayHello", "../testdata/greeter.proto", []string{})
	assert.NoError(t, err)
	assert.NotNil(t, md)

	ctd := newCallTemplateData(md, "worker_id_123", 100)

	assert.NotNil(t, ctd)
	assert.Equal(t, "worker_id_123", ctd.WorkerID)
	assert.Equal(t, int64(100), ctd.RequestNumber)
	assert.Equal(t, "helloworld.Greeter.SayHello", ctd.FullyQualifiedName)
	assert.Equal(t, "SayHello", ctd.MethodName)
	assert.Equal(t, "Greeter", ctd.ServiceName)
	assert.Equal(t, "HelloRequest", ctd.InputName)
	assert.Equal(t, "HelloReply", ctd.OutputName)
	assert.Equal(t, false, ctd.IsClientStreaming)
	assert.Equal(t, false, ctd.IsServerStreaming)
	assert.NotEmpty(t, ctd.Timestamp)
	assert.NotZero(t, ctd.TimestampUnix)
}

func TestCallTemplateData_ExecuteData(t *testing.T) {
	md, err := protodesc.GetMethodDescFromProto("helloworld.Greeter/SayHello", "../testdata/greeter.proto", []string{})
	assert.NoError(t, err)
	assert.NotNil(t, md)

	ctd := newCallTemplateData(md, "worker_id_123", 200)

	assert.NotNil(t, ctd)

	var tests = []struct {
		name        string
		in          string
		expected    []byte
		expectError bool
	}{
		{"no template",
			`{"name":"bob"}`,
			[]byte(`{"name":"bob"}`),
			false,
		},
		{"with template",
			`{"name":"{{.WorkerID}} {{.RequestNumber}} bob {{.FullyQualifiedName}} {{.MethodName}} {{.ServiceName}} {{.InputName}} {{.OutputName}} {{.IsClientStreaming}} {{.IsServerStreaming}}"}`,
			[]byte(`{"name":"worker_id_123 200 bob helloworld.Greeter.SayHello SayHello Greeter HelloRequest HelloReply false false"}`),
			false,
		},
		{"with template command",
			`{"name":"{{Read "../testdata/somefolder/somefile"}} {{ListFile "../testdata/somefolder"}} {{ListFile "../testdata/somefolder" | RandomChoice}} {{ListFile "../testdata/somefolder" | RoundRobin}}"}`,
			[]byte(`{"name":"somecontent [../testdata/somefolder/somefile] ../testdata/somefolder/somefile ../testdata/somefolder/somefile"}`),
			false,
		},
		{"with template base64",
			`{"name":"{{Read "../testdata/somefolder/somefile"}} {{Read "../testdata/somefolder/somefile" | B64Encode}} {{Read "../testdata/somefolder/somefile" | B64Encode | B64Decode}}"}`,
			[]byte(`{"name":"somecontent c29tZWNvbnRlbnQ= somecontent"}`),
			false,
		},
		{"with template split range join",
			`{"name":"{{Range 0 10 2}} {{Split "1,2,3" ","}} {{Split (Read "../testdata/somefolder/somefile") "con"}} {{Join (Range 0 10 2) ","}}"}`,
			[]byte(`{"name":"[0 2 4 6 8] [1 2 3] [some tent] 0,2,4,6,8"}`),
			false,
		},
		{"with template randomslice shuffle",
			`{"name":"{{Range 0 1 1 | RandomSlice}} {{Range 0 1 1 | Shuffle}} {{RandomSliceK (Split "0,0,0,0,0,0,0,0,0" ",") 3}}"}`,
			[]byte(`{"name":"[0] [0] [0 0 0]"}`),
			false,
		},
		{"with template random int",
			`{"name":"{{RandomInt 0 1}}"}`,
			[]byte(`{"name":"0"}`),
			false,
		},
		{"with template variable",
			`{"name":"{{ $value := RandomInt (RandomInt 0 1 | ToInt) 1 }}{{ $value }}"}`,
			[]byte(`{"name":"0"}`),
			false,
		},
		{"with unknown action",
			`{"name":"asdf {{.Something}} {{.MethodName}} bob"}`,
			[]byte(`{"name":"asdf {{.Something}} {{.MethodName}} bob"}`),
			false,
		},
		{"with empty string",
			"",
			[]byte{},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := ctd.executeData(tt.in)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tt.expected, r)
		})
	}
}

func TestCallTemplateData_ExecuteMetadata(t *testing.T) {
	md, err := protodesc.GetMethodDescFromProto("helloworld.Greeter/SayHello", "../testdata/greeter.proto", []string{})
	assert.NoError(t, err)
	assert.NotNil(t, md)

	ctd := newCallTemplateData(md, "worker_id_123", 200)

	assert.NotNil(t, ctd)

	var tests = []struct {
		name        string
		in          string
		expected    interface{}
		expectError bool
	}{
		{"no template",
			`{"trace_id":"asdf"}`,
			&map[string]string{"trace_id": "asdf"},
			false,
		},
		{"with template",
			`{"trace_id":"{{.RequestNumber}} asdf {{.FullyQualifiedName}} {{.MethodName}} {{.ServiceName}} {{.InputName}} {{.OutputName}} {{.IsClientStreaming}} {{.IsServerStreaming}}"}`,
			&map[string]string{"trace_id": "200 asdf helloworld.Greeter.SayHello SayHello Greeter HelloRequest HelloReply false false"},
			false,
		},
		{"with unknown action",
			`{"trace_id":"asdf {{.Something}} {{.MethodName}} bob"}`,
			&map[string]string{"trace_id": "asdf {{.Something}} {{.MethodName}} bob"},
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := ctd.executeMetadata(tt.in)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tt.expected, r)
		})
	}
}
