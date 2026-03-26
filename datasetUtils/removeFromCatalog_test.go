package datasetUtils

import (
	"bytes"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

type mockCount struct {
	origDatablocks int
	attachments    int
	datasets       int
	datablocks     []int
}

func TestRemoveFromCatalog_AllCases(t *testing.T) {
	tests := []struct {
		name               string
		mockCount          mockCount
		expected           []string
		expectError        bool
		expectedErrSubstr  string
		failOrigCount      bool
		failOrigDeleteCall bool
		timeout            time.Duration
		jobStatusMessage   string
		emptyJobId         bool
	}{
		{
			name: "Delete none immediately",
			mockCount: mockCount{
				origDatablocks: 0,
				attachments:    0,
				datasets:       0,
				datablocks:     []int{0},
			},
			expected: []string{},
		},
		{
			name: "Delete 1 origdatablocks immediately",
			mockCount: mockCount{
				origDatablocks: 1,
				attachments:    0,
				datasets:       0,
				datablocks:     []int{0},
			},
			expected: []string{
				"/Datasets/dataset%2F1/origdatablocks",
			},
		},
		{
			name: "Delete 1 attachments immediately",
			mockCount: mockCount{
				origDatablocks: 0,
				attachments:    1,
				datasets:       0,
				datablocks:     []int{0},
			},
			expected: []string{
				"/Datasets/dataset%2F1/attachments",
			},
		},
		{
			name: "Delete 1 dataset immediately",
			mockCount: mockCount{
				origDatablocks: 0,
				attachments:    0,
				datasets:       1,
				datablocks:     []int{0},
			},
			expected: []string{
				"/Datasets/dataset%2F1",
			},
		},
		{
			name: "Delete all immediately",
			mockCount: mockCount{
				origDatablocks: 1,
				attachments:    1,
				datasets:       1,
				datablocks:     []int{0},
			},
			expected: []string{
				"/Datasets/dataset%2F1/origdatablocks",
				"/Datasets/dataset%2F1/attachments",
				"/Datasets/dataset%2F1",
			},
		},
		{
			name: "Delete all after waiting twice datablocks deletion",
			mockCount: mockCount{
				origDatablocks: 1,
				attachments:    1,
				datasets:       1,
				datablocks:     []int{1, 0},
			},
			expected: []string{
				"/Datasets/dataset%2F1/origdatablocks",
				"/Datasets/dataset%2F1/attachments",
				"/Datasets/dataset%2F1",
			},
		},
		{
			name: "Error when origdatablocks count fails",
			mockCount: mockCount{
				origDatablocks: 0,
				attachments:    0,
				datasets:       0,
				datablocks:     []int{},
			},
			expected:           []string{},
			expectError:        true,
			expectedErrSubstr:  "pre-check failed: could not count origdatablocks",
			failOrigCount:      true,
			failOrigDeleteCall: false,
		},
		{
			name: "Error when origdatablocks delete fails",
			mockCount: mockCount{
				origDatablocks: 1,
				attachments:    0,
				datasets:       0,
				datablocks:     []int{0},
			},
			expected:           []string{"/Datasets/dataset%2F1/origdatablocks"},
			expectError:        true,
			expectedErrSubstr:  "cleanup failed at origdatablocks",
			failOrigCount:      false,
			failOrigDeleteCall: true,
		},
		{
			name: "Timeout when job status checks fail",
			mockCount: mockCount{
				origDatablocks: 1,
				attachments:    0,
				datasets:       0,
				datablocks:     []int{0},
			},
			expected:           []string{},
			expectError:        true,
			expectedErrSubstr:  "timeout reached",
			failOrigCount:      false,
			failOrigDeleteCall: false,
			timeout:            time.Second / 100,
			jobStatusMessage:   "running",
		},
		{
			name: "Timeout when datablocks stay non-zero",
			mockCount: mockCount{
				origDatablocks: 1,
				attachments:    0,
				datasets:       0,
				datablocks:     []int{1, 1, 1},
			},
			expected:          []string{},
			expectError:       true,
			expectedErrSubstr: "timeout reached",
			timeout:           time.Second / 100,
		},
		{
			name: "Return error when job finishes with unsuccessful status",
			mockCount: mockCount{
				origDatablocks: 1,
				attachments:    0,
				datasets:       0,
				datablocks:     []int{0},
			},
			expected:          []string{},
			expectError:       true,
			expectedErrSubstr: "archive deletion job finished with unsuccessful status",
			jobStatusMessage:  string(JobFailed),
		},
		{
			name: "Delete all immediately when no job ID is returned",
			mockCount: mockCount{
				origDatablocks: 1,
				attachments:    1,
				datasets:       1,
				datablocks:     []int{0},
			},
			expected: []string{
				"/Datasets/dataset%2F1/origdatablocks",
				"/Datasets/dataset%2F1/attachments",
				"/Datasets/dataset%2F1",
			},
			jobStatusMessage: "running", // should be ignored since no job ID is returned and it's only to test that the function does not wait and proceeds to delete
			emptyJobId:       true,
		},
	}

	user := map[string]string{
		"mail":        "test@example.com",
		"username":    "testuser",
		"accessToken": "testtoken",
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			effectiveStatus := tt.jobStatusMessage
			if effectiveStatus == "" {
				effectiveStatus = string(JobSuccess)
			}
			oldTimeout := removeFromCatalogTimeout
			waitTime = tt.timeout
			if tt.timeout != 0 {
				removeFromCatalogTimeout = tt.timeout
			}
			defer func() {
				removeFromCatalogTimeout = oldTimeout
			}()
			calledDeletes := []string{}

			dbCounts := tt.mockCount.datablocks
			getDatablocksCount := func() int {
				if len(dbCounts) == 0 {
					return 0
				}
				count := dbCounts[0]
				dbCounts = dbCounts[1:]
				return count
			}
			dbCalls := 0

			client := &http.Client{
				Transport: &MockTransport{
					RoundTripFunc: func(req *http.Request) (*http.Response, error) {
						if req.Method == http.MethodGet && strings.HasPrefix(req.URL.Path, "/Jobs/") {
							return &http.Response{
								StatusCode: 200,
								Body:       io.NopCloser(bytes.NewBufferString(`{"jobStatusMessage":"` + effectiveStatus + `"}`)),
							}, nil
						}

						if req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/count") {
							if tt.failOrigCount && strings.Contains(req.URL.Path, "origdatablocks") {
								return &http.Response{
									StatusCode: 500,
									Body:       io.NopCloser(bytes.NewBufferString(`{"error":"boom"}`)),
								}, nil
							}

							var count int
							switch {
							case strings.Contains(req.URL.Path, "origdatablocks"):
								count = tt.mockCount.origDatablocks
								expected := "/Datasets/dataset%2F1/origdatablocks/count"
								if req.URL.RawPath != expected {
									t.Errorf("GET /count path mismatch for origdatablocks: got %s, expected %s", req.URL.RawPath, expected)
								}
							case strings.Contains(req.URL.Path, "attachments"):
								count = tt.mockCount.attachments
								expected := "/Datasets/dataset%2F1/attachments/count"
								if req.URL.RawPath != expected {
									t.Errorf("GET /count path mismatch for attachments: got %s, expected %s", req.URL.RawPath, expected)
								}
							case strings.Contains(req.URL.Path, "datablocks"):
								count = getDatablocksCount()
								dbCalls++
								expected := "/Datasets/dataset%2F1/datablocks/count"
								if req.URL.RawPath != expected {
									t.Errorf("GET /count path mismatch for datablocks: got %s, expected %s", req.URL.RawPath, expected)
								}
							case strings.Contains(req.URL.Path, "Datasets"):
								count = tt.mockCount.datasets
								expected := "/Datasets/count"
								if req.URL.Path != expected {
									t.Errorf("GET /count path mismatch for datasets: got %s, expected %s", req.URL.Path, expected)
								}
								expectedQuery := "filter=%7B%22where%22%3A%7B%22pid%22%3A%22dataset%2F1%22%7D%7D"
								if req.URL.RawQuery != expectedQuery {
									t.Errorf("GET /count path mismatch for datasets: got %s, expected %s", req.URL.RawQuery, expectedQuery)
								}
							default:
								count = 0
							}

							body := []byte(`{"count":` + strconv.Itoa(count) + `}`)
							return &http.Response{
								StatusCode: 200,
								Body:       io.NopCloser(bytes.NewBuffer(body)),
							}, nil
						}

						if req.Method == http.MethodDelete {
							calledDeletes = append(calledDeletes, req.URL.RawPath)
							if tt.failOrigDeleteCall && strings.Contains(req.URL.Path, "origdatablocks") {
								return &http.Response{
									StatusCode: 500,
									Body:       io.NopCloser(bytes.NewBufferString(`{"error":"delete failed"}`)),
								}, nil
							}
							return &http.Response{
								StatusCode: 200,
								Body:       io.NopCloser(bytes.NewBufferString(`{}`)),
							}, nil
						}

						return &http.Response{
							StatusCode: 400,
							Body:       io.NopCloser(bytes.NewBufferString(`{}`)),
						}, nil
					},
				},
			}

			effectiveJobID := "job123"
			if tt.emptyJobId {
				effectiveJobID = ""
			}
			err := RemoveFromCatalog(client, "http://mockserver", "dataset/1", effectiveJobID, user, true)
			if tt.expectError {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.expectedErrSubstr)
				}
				if !strings.Contains(err.Error(), tt.expectedErrSubstr) {
					t.Fatalf("expected error containing %q, got %v", tt.expectedErrSubstr, err)
				}
			} else if err != nil {
				t.Fatalf("RemoveFromCatalog returned unexpected error: %v", err)
			}

			if len(calledDeletes) != len(tt.expected) {
				t.Errorf("Expected %d DELETE calls, got %d: %v", len(tt.expected), len(calledDeletes), calledDeletes)
			}
			for i, endpoint := range tt.expected {
				if calledDeletes[i] != endpoint {
					t.Errorf("Expected DELETE to %s, got %s", endpoint, calledDeletes[i])
				}
			}

			if tt.jobStatusMessage == string(JobSuccess) && dbCalls != len(tt.mockCount.datablocks) {
				t.Errorf("Expected %d GET /datablocks calls, got %d", len(tt.mockCount.datablocks), dbCalls)
			}
		})
	}
}
